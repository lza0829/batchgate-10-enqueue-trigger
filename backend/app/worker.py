"""Serial background consumer for the orchestration request queue.

Concurrency strategy (auditable by design): exactly ONE worker thread runs
in this process. It takes the oldest ``pending`` row via an atomic
``BEGIN IMMEDIATE`` claim, runs the whole orchestration synchronously, and
only then looks for the next row. No request is ever processed twice at the
same time; ``worker_name`` plus ``started_at``/``finished_at`` on every row
make the execution order reconstructable from the database alone.
"""

from __future__ import annotations

import logging
import os
import threading
from typing import Any

from app.db import connect
from app.repositories import orchestrations as orch_repo
from app.repositories import requests as req_repo
from app.services.orchestrate_service import execute_orchestration

logger = logging.getLogger("batchgate.worker")

POLL_INTERVAL = float(os.environ.get("WORKER_POLL_INTERVAL", "0.2"))
WORKER_NAME = os.environ.get("WORKER_NAME", f"worker-pid{os.getpid()}")


class QueueWorker:
    """Daemon thread that drains ``orchestration_requests`` in FIFO order."""

    def __init__(self, worker_name: str = WORKER_NAME, poll_interval: float = POLL_INTERVAL):
        self.worker_name = worker_name
        self.poll_interval = poll_interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    # -- lifecycle -------------------------------------------------------

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, name="orchestration-queue-worker", daemon=True
        )
        self._thread.start()
        logger.info("queue worker started: %s (poll=%.2fs)", self.worker_name, self.poll_interval)

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def wait_until_drained(self, timeout: float = 10.0) -> bool:
        """Block until no pending/running requests remain. Test helper."""
        waited = 0.0
        interval = 0.05
        while waited < timeout:
            conn = connect()
            try:
                busy = conn.execute(
                    "SELECT COUNT(*) c FROM orchestration_requests "
                    "WHERE status IN ('pending','running')"
                ).fetchone()["c"]
            finally:
                conn.close()
            if busy == 0:
                return True
            self._stop.wait(interval)
            waited += interval
        return False

    # -- processing ------------------------------------------------------

    def run_once(self) -> bool:
        """Claim and process at most one request. Returns True if work ran."""
        conn = connect()
        try:
            request = req_repo.claim_next_pending(conn, worker_name=self.worker_name)
            if request is None:
                return False
            self._process(conn, request)
            return True
        finally:
            conn.close()

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                did_work = self.run_once()
            except Exception:  # noqa: BLE001 - the loop must never die
                logger.exception("queue worker loop error")
                did_work = False
            if not did_work:
                self._stop.wait(self.poll_interval)

    def _process(self, conn, request: dict[str, Any]) -> None:
        req_id = request["id"]
        try:
            orch_id = orch_repo.create_orchestration(conn)
            req_repo.attach_orchestration(
                conn, request_id=req_id, orchestration_id=orch_id
            )
            execute_orchestration(conn, orch_id)
            conn.commit()
        except Exception as exc:  # noqa: BLE001 - request must land in 'failed'
            conn.rollback()
            logger.exception("request %s failed", req_id)
            self._record_failure(req_id, exc)
            return
        conn2 = connect()
        try:
            req_repo.mark_succeeded(conn2, request_id=req_id)
        finally:
            conn2.close()
        logger.info("request %s -> orchestration %s succeeded", req_id, orch_id)

    def _record_failure(self, req_id: int, exc: Exception) -> None:
        """Book a failed orchestration row so queue books stay 1:1.

        Every consumed request gets exactly one orchestration record:
        succeeded requests point at the ok/partial run, failed requests at a
        run closed with status 'failed' carrying the error report.
        """
        error = f"{type(exc).__name__}: {exc}"
        conn = connect()
        try:
            orch_id = orch_repo.create_orchestration(conn)
            orch_repo.finish_orchestration(
                conn,
                orch_id,
                "failed",
                {"error": error, "request_id": req_id},
            )
            req_repo.mark_failed(
                conn,
                request_id=req_id,
                error=error,
                orchestration_id=orch_id,
            )
        finally:
            conn.close()


# Single shared instance owned by the FastAPI lifespan.
worker = QueueWorker()
