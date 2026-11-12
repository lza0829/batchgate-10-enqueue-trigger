"""Tests for the idempotent request queue and its serial consumer.

Covers the acceptance path: a new key enqueues and runs once, the same key
dedupes without starting another orchestration, and the queue accounting
matches both the request list and the orchestration records.
"""

from __future__ import annotations

import threading

import pytest
from fastapi.testclient import TestClient

from app import worker as worker_module
from app.worker import QueueWorker


def _drain(worker: QueueWorker, timeout: float = 10.0) -> None:
    assert worker.wait_until_drained(timeout=timeout), "queue did not drain in time"


def _new_worker() -> QueueWorker:
    return QueueWorker(worker_name="test-worker", poll_interval=0.01)


# ---------------------------------------------------------------- enqueue

def test_enqueue_new_key_creates_one_request(isolated_db):
    from app.services import request_service

    request, created = request_service.enqueue_request("key-001", "first run")
    assert created is True
    assert request["status"] == "pending"
    assert request["idempotency_key"] == "key-001"
    assert request["note"] == "first run"
    assert request["dedupe_count"] == 0
    assert request["orchestration_id"] is None


def test_same_idempotency_key_dedupes_and_does_not_enqueue_again(isolated_db):
    from app.services import request_service

    first, created_first = request_service.enqueue_request("dup-key", "note a")
    second, created_second = request_service.enqueue_request("dup-key", "note b")

    assert created_first is True
    assert created_second is False
    assert first["id"] == second["id"]
    assert second["note"] == "note a"  # original note is preserved
    assert second["dedupe_count"] == 1
    assert second["status"] == "pending"

    third, created_third = request_service.enqueue_request("dup-key", None)
    assert created_third is False
    assert third["dedupe_count"] == 2

    from app.db import connect

    conn = connect()
    try:
        rows = conn.execute(
            "SELECT COUNT(*) c FROM orchestration_requests WHERE idempotency_key=?",
            ("dup-key",),
        ).fetchone()["c"]
    finally:
        conn.close()
    assert rows == 1  # still exactly one queue entry


def test_empty_idempotency_key_rejected(isolated_db):
    from app.services import request_service

    with pytest.raises(request_service.IdempotencyKeyError):
        request_service.enqueue_request("   ", None)


def test_concurrent_enqueue_same_key_only_inserts_one(isolated_db):
    from app.services import request_service

    created_flags: list[bool] = []
    lock = threading.Lock()

    def submit() -> None:
        _, created = request_service.enqueue_request("race-key", None)
        with lock:
            created_flags.append(created)

    threads = [threading.Thread(target=submit) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert created_flags.count(True) == 1
    assert created_flags.count(False) == 7

    from app.db import connect

    conn = connect()
    try:
        row = conn.execute(
            "SELECT COUNT(*) c, SUM(dedupe_count) d FROM orchestration_requests "
            "WHERE idempotency_key='race-key'"
        ).fetchone()
    finally:
        conn.close()
    assert row["c"] == 1
    assert row["d"] == 7


# ------------------------------------------------------------- processing

def test_worker_consumes_pending_serially_and_marks_succeeded(isolated_db):
    from app.services import request_service

    w = _new_worker()
    request_service.enqueue_request("run-1", None)
    request_service.enqueue_request("run-2", None)

    assert w.run_once() is True
    assert w.run_once() is True
    assert w.run_once() is False  # queue drained

    requests = request_service.list_requests()
    assert len(requests) == 2
    by_key = {r["idempotency_key"]: r for r in requests}
    for key in ("run-1", "run-2"):
        r = by_key[key]
        assert r["status"] == "succeeded"
        assert r["worker_name"] == "test-worker"
        assert r["orchestration_id"] is not None
        assert r["started_at"] is not None
        assert r["finished_at"] is not None
        assert r["error"] is None

    # FIFO: oldest request got the smallest orchestration id.
    assert by_key["run-1"]["orchestration_id"] < by_key["run-2"]["orchestration_id"]


def test_deduped_request_never_starts_a_second_orchestration(isolated_db):
    """The headline acceptance case: enqueue once, dedupe once, run once."""
    from app.services import request_service

    w = _new_worker()
    first, created = request_service.enqueue_request("once-key", "acceptance")
    assert created is True
    duplicate, created = request_service.enqueue_request("once-key", "retry submit")
    assert created is False
    assert duplicate["id"] == first["id"]
    assert duplicate["dedupe_count"] == 1

    assert w.run_once() is True
    assert w.run_once() is False  # no second request to consume

    from app.db import connect

    conn = connect()
    try:
        request_count = conn.execute(
            "SELECT COUNT(*) c FROM orchestration_requests"
        ).fetchone()["c"]
        orch_count = conn.execute("SELECT COUNT(*) c FROM orchestrations").fetchone()["c"]
        request = conn.execute(
            "SELECT * FROM orchestration_requests WHERE idempotency_key=?",
            ("once-key",),
        ).fetchone()
    finally:
        conn.close()

    assert request_count == 1
    assert orch_count == 1
    assert request["status"] == "succeeded"
    assert request["dedupe_count"] == 1


def test_worker_failure_marks_request_failed_with_failed_orchestration(isolated_db, monkeypatch):
    from app.services import request_service

    def boom(_conn, _orch_id):
        raise RuntimeError("fixture exploded")

    # worker.py binds the name at import time, so patch the worker module.
    monkeypatch.setattr(worker_module, "execute_orchestration", boom)

    request_service.enqueue_request("fail-key", None)
    w = _new_worker()
    assert w.run_once() is True

    request = request_service.get_request(1)
    assert request["status"] == "failed"
    assert "fixture exploded" in request["error"]
    assert request["orchestration_id"] is not None

    from app.db import connect

    conn = connect()
    try:
        orch = conn.execute(
            "SELECT * FROM orchestrations WHERE id=?", (request["orchestration_id"],)
        ).fetchone()
    finally:
        conn.close()
    assert orch["status"] == "failed"


def test_stale_running_request_is_requeued_on_restart(isolated_db):
    """A crash mid-flight leaves 'running'; startup recovery requeues it."""
    from app.db import connect
    from app.seed import init_db

    conn = connect()
    try:
        conn.execute(
            """
            INSERT INTO orchestration_requests(
                idempotency_key, note, status, dedupe_count, orchestration_id,
                error, worker_name, created_at, enqueued_at, started_at, finished_at
            ) VALUES ('stale-key', NULL, 'running', 0, NULL, NULL, 'dead-worker',
                      '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z',
                      '2026-01-01T00:00:01Z', NULL)
            """
        )
        conn.commit()
    finally:
        conn.close()

    init_db()  # simulates process restart

    recovered = request_service_get("stale-key")
    assert recovered["status"] == "pending"
    assert recovered["worker_name"] is None
    assert recovered["started_at"] is None

    w = _new_worker()
    assert w.run_once() is True
    assert request_service_get("stale-key")["status"] == "succeeded"


def test_restart_reconciles_request_when_orchestration_already_finished(isolated_db):
    """Crash after the orchestration committed but before the status update:
    no re-run, request is reconciled to the existing finished orchestration."""
    from app.db import connect
    from app.repositories import orchestrations as orch_repo
    from app.seed import init_db

    conn = connect()
    try:
        orch_id = orch_repo.create_orchestration(conn)
        orch_repo.finish_orchestration(conn, orch_id, "ok", {"succeeded": 3})
        conn.execute(
            """
            INSERT INTO orchestration_requests(
                idempotency_key, note, status, dedupe_count, orchestration_id,
                error, worker_name, created_at, enqueued_at, started_at, finished_at
            ) VALUES ('done-key', NULL, 'running', 0, ?, NULL, 'dead-worker',
                      '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z',
                      '2026-01-01T00:00:01Z', NULL)
            """,
            (orch_id,),
        )
        conn.commit()
    finally:
        conn.close()

    init_db()  # restart: must reconcile, not re-enqueue

    recovered = request_service_get("done-key")
    assert recovered["status"] == "succeeded"
    assert recovered["orchestration_id"] == orch_id
    assert recovered["worker_name"] is None

    # The serial worker has nothing to pick up.
    w = _new_worker()
    assert w.run_once() is False

    conn = connect()
    try:
        assert conn.execute("SELECT COUNT(*) c FROM orchestrations").fetchone()["c"] == 1
    finally:
        conn.close()


def request_service_get(key: str):
    from app.db import connect

    conn = connect()
    try:
        return dict(
            conn.execute(
                "SELECT * FROM orchestration_requests WHERE idempotency_key=?", (key,)
            ).fetchone()
        )
    finally:
        conn.close()


# ------------------------------------------------------------- accounting

def test_queue_stats_match_request_list_and_orchestration_records(isolated_db):
    from app.services import request_service

    w = _new_worker()
    request_service.enqueue_request("k1", None)
    request_service.enqueue_request("k2", None)
    request_service.enqueue_request("k3", None)
    # k1 submitted three more times after enqueue -> three dedupe hits
    request_service.enqueue_request("k1", None)
    request_service.enqueue_request("k1", None)
    request_service.enqueue_request("k1", None)
    # k2 one duplicate
    request_service.enqueue_request("k2", None)

    pending_stats = request_service.request_stats()
    assert pending_stats["total"] == 3
    assert pending_stats["pending"] == 3
    assert pending_stats["deduped_submissions"] == 4
    assert pending_stats["succeeded"] == 0

    for _ in range(3):
        w.run_once()
    _drain(w)

    stats = request_service.request_stats()
    requests = request_service.list_requests()
    assert stats["total"] == len(requests) == 3
    assert stats["succeeded"] == 3
    assert stats["pending"] == 0 and stats["running"] == 0 and stats["failed"] == 0
    assert stats["deduped_submissions"] == 4

    from app.db import connect

    conn = connect()
    try:
        linked = conn.execute(
            "SELECT COUNT(*) c FROM orchestration_requests WHERE orchestration_id IS NOT NULL"
        ).fetchone()["c"]
        orch_rows = conn.execute("SELECT COUNT(*) c FROM orchestrations").fetchone()["c"]
        orphan = conn.execute(
            """
            SELECT COUNT(*) c FROM orchestrations o
            WHERE o.id NOT IN (
                SELECT orchestration_id FROM orchestration_requests
                WHERE orchestration_id IS NOT NULL
            )
            """
        ).fetchone()["c"]
    finally:
        conn.close()

    # Every consumed request maps to exactly one orchestration, no orphans.
    assert linked == 3
    assert orch_rows == 3
    assert orphan == 0


# ------------------------------------------------------------------- HTTP

def _client() -> TestClient:
    from app.main import app

    return TestClient(app)


def test_http_enqueue_dedupe_and_report_link(isolated_db):
    with _client() as client:
        r1 = client.post("/api/requests", json={"idempotency_key": "http-key", "note": "via api"})
        assert r1.status_code == 201
        assert r1.json()["created"] is True

        r2 = client.post("/api/requests", json={"idempotency_key": "http-key"})
        assert r2.status_code == 200
        body = r2.json()
        assert body["created"] is False and body["deduped"] is True
        assert body["request"]["dedupe_count"] == 1

        worker = worker_module.worker
        assert worker.wait_until_drained(timeout=10)

        listing = client.get("/api/requests").json()
        assert len(listing) == 1
        req_id = listing[0]["id"]
        assert listing[0]["status"] == "succeeded"

        stats = client.get("/api/requests/stats").json()
        assert stats["total"] == 1 and stats["succeeded"] == 1
        assert stats["deduped_submissions"] == 1

        link = client.get(f"/api/requests/{req_id}/report").json()
        assert link["request_id"] == req_id
        assert link["orchestration_id"] == listing[0]["orchestration_id"]
        assert link["report"]["succeeded"] >= 1

        detail = client.get(f"/api/requests/{req_id}").json()
        assert detail["idempotency_key"] == "http-key"


def test_http_enqueue_validation_and_removed_sync_endpoint(isolated_db):
    with _client() as client:
        bad = client.post("/api/requests", json={"idempotency_key": "   "})
        assert bad.status_code == 422

        gone = client.post("/api/orchestrations")
        assert gone.status_code == 410

        missing = client.get("/api/requests/9999")
        assert missing.status_code == 404


def test_http_stats_endpoint_reconciles_with_dashboard(isolated_db):
    with _client() as client:
        for key in ("a", "b"):
            client.post("/api/requests", json={"idempotency_key": key})
        client.post("/api/requests", json={"idempotency_key": "a"})  # dedupe

        assert worker_module.worker.wait_until_drained(timeout=10)

        stats = client.get("/api/requests/stats").json()
        dashboard = client.get("/api/dashboard").json()
        assert dashboard["request_queue"] == stats
        assert dashboard["orchestration_count"] == stats["succeeded"] + stats["failed"]
