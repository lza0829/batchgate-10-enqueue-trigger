"""Repository for orchestration request queue rows.

The queue lives in the same SQLite database as the orchestration records.
Every state transition is a single explicit ``BEGIN IMMEDIATE`` transaction
so concurrent submitters/consumers are serialised on the write lock and the
UNIQUE index on ``idempotency_key`` makes dedupe safe across processes.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import sqlite3

# Consumer is a single serial worker (see app.worker). These are the only
# legal transitions and the audit trail keeps them reconstructable.
PENDING = "pending"
RUNNING = "running"
SUCCEEDED = "succeeded"
FAILED = "failed"

TERMINAL_STATUSES = (SUCCEEDED, FAILED)


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _row(r: sqlite3.Row | None) -> dict[str, Any] | None:
    if r is None:
        return None
    return dict(r)


def enqueue(
    conn: sqlite3.Connection, *, idempotency_key: str, note: str | None
) -> tuple[dict[str, Any], bool]:
    """Insert a request unless the idempotency key already exists.

    Returns ``(request, created)``. On a repeated submission the existing row
    is returned untouched and its ``dedupe_count`` is incremented; no new
    orchestration is ever started.
    """
    now = _now()
    conn.execute("BEGIN IMMEDIATE")
    try:
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO orchestration_requests(
                idempotency_key, note, status, dedupe_count,
                orchestration_id, error, worker_name,
                created_at, enqueued_at, started_at, finished_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """,
            (idempotency_key, note, PENDING, 0, None, None, None, now, now, None, None),
        )
        created = cur.rowcount == 1
        if created:
            row = conn.execute(
                "SELECT * FROM orchestration_requests WHERE idempotency_key=?",
                (idempotency_key,),
            ).fetchone()
        else:
            conn.execute(
                "UPDATE orchestration_requests SET dedupe_count=dedupe_count+1 WHERE idempotency_key=?",
                (idempotency_key,),
            )
            row = conn.execute(
                "SELECT * FROM orchestration_requests WHERE idempotency_key=?",
                (idempotency_key,),
            ).fetchone()
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return _row(row), created  # type: ignore[return-value]


def claim_next_pending(
    conn: sqlite3.Connection, *, worker_name: str
) -> dict[str, Any] | None:
    """Atomically take the oldest pending request and mark it running."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT * FROM orchestration_requests WHERE status=? ORDER BY id LIMIT 1",
            (PENDING,),
        ).fetchone()
        if row is None:
            conn.commit()
            return None
        conn.execute(
            """
            UPDATE orchestration_requests
            SET status=?, worker_name=?, started_at=?
            WHERE id=? AND status=?
            """,
            (RUNNING, worker_name, _now(), row["id"], PENDING),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    out = conn.execute(
        "SELECT * FROM orchestration_requests WHERE id=?", (row["id"],)
    ).fetchone()
    return _row(out)


def attach_orchestration(
    conn: sqlite3.Connection, *, request_id: int, orchestration_id: int
) -> None:
    # No commit here: the worker owns the enclosing transaction.
    conn.execute(
        "UPDATE orchestration_requests SET orchestration_id=? WHERE id=?",
        (orchestration_id, request_id),
    )


def mark_succeeded(conn: sqlite3.Connection, *, request_id: int) -> None:
    conn.execute(
        "UPDATE orchestration_requests SET status=?, finished_at=? WHERE id=?",
        (SUCCEEDED, _now(), request_id),
    )
    conn.commit()


def mark_failed(
    conn: sqlite3.Connection,
    *,
    request_id: int,
    error: str,
    orchestration_id: int | None = None,
) -> None:
    if orchestration_id is None:
        conn.execute(
            "UPDATE orchestration_requests SET status=?, error=?, finished_at=? WHERE id=?",
            (FAILED, error[:2000], _now(), request_id),
        )
    else:
        conn.execute(
            """
            UPDATE orchestration_requests
            SET status=?, error=?, orchestration_id=?, finished_at=?
            WHERE id=?
            """,
            (FAILED, error[:2000], orchestration_id, _now(), request_id),
        )
    conn.commit()


def get_request(conn: sqlite3.Connection, request_id: int) -> dict[str, Any] | None:
    return _row(
        conn.execute(
            "SELECT * FROM orchestration_requests WHERE id=?", (request_id,)
        ).fetchone()
    )


def get_request_by_key(
    conn: sqlite3.Connection, idempotency_key: str
) -> dict[str, Any] | None:
    return _row(
        conn.execute(
            "SELECT * FROM orchestration_requests WHERE idempotency_key=?",
            (idempotency_key,),
        ).fetchone()
    )


def list_requests(conn: sqlite3.Connection, limit: int = 100) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM orchestration_requests ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()
    return [dict(r) for r in rows]


def stats(conn: sqlite3.Connection) -> dict[str, int]:
    rows = conn.execute(
        "SELECT status, COUNT(*) c FROM orchestration_requests GROUP BY status"
    ).fetchall()
    by_status = {r["status"]: r["c"] for r in rows}
    total = sum(by_status.values())
    deduped = conn.execute(
        "SELECT COALESCE(SUM(dedupe_count),0) d FROM orchestration_requests"
    ).fetchone()["d"]
    return {
        "total": total,
        "pending": by_status.get(PENDING, 0),
        "running": by_status.get(RUNNING, 0),
        "succeeded": by_status.get(SUCCEEDED, 0),
        "failed": by_status.get(FAILED, 0),
        "deduped_submissions": int(deduped),
    }


def request_report_link(conn: sqlite3.Connection, request_id: int) -> dict[str, Any] | None:
    """Join a request to its orchestration report for the UI deep link."""
    r = conn.execute(
        """
        SELECT q.id AS request_id, q.orchestration_id,
               o.status AS orchestration_status, o.report_json,
               o.started_at, o.finished_at
        FROM orchestration_requests q
        LEFT JOIN orchestrations o ON o.id=q.orchestration_id
        WHERE q.id=?
        """,
        (request_id,),
    ).fetchone()
    if r is None:
        return None
    out = dict(r)
    out["report"] = json.loads(out.pop("report_json")) if out["report_json"] else None
    return out
