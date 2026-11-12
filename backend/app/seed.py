import json
from datetime import datetime, timezone

from app.db import connect


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _recover_stale_requests(conn):
    """Requeue requests left 'running' by a crashed worker (at-least-once recovery).

    A 'running' row at startup can only belong to a dead process, since the
    consumer is a single serial worker. Two cases:

    * the linked orchestration already reached a terminal state -> reconcile
      the request to it (the work finished, only the status update was lost),
      so the run is never executed twice;
    * otherwise -> close the half-finished orchestration as 'aborted' and
      requeue the request as 'pending' for another attempt.
    """
    stale = conn.execute(
        "SELECT id, orchestration_id FROM orchestration_requests WHERE status='running'"
    ).fetchall()
    for row in stale:
        orch_id = row["orchestration_id"]
        if orch_id is not None:
            orch = conn.execute(
                "SELECT status FROM orchestrations WHERE id=?", (orch_id,)
            ).fetchone()
            if orch is not None and orch["status"] in ("ok", "partial", "failed"):
                recovered_status = "succeeded" if orch["status"] != "failed" else "failed"
                conn.execute(
                    """
                    UPDATE orchestration_requests
                    SET status=?, finished_at=?, worker_name=NULL
                    WHERE id=?
                    """,
                    (recovered_status, _now(), row["id"]),
                )
                continue
            conn.execute(
                """
                UPDATE orchestrations
                SET status='aborted', finished_at=?, report_json=?
                WHERE id=? AND status='running'
                """,
                (
                    _now(),
                    json.dumps(
                        {"aborted_reason": "worker restarted before orchestration finished"},
                        ensure_ascii=False,
                    ),
                    orch_id,
                ),
            )
        conn.execute(
            """
            UPDATE orchestration_requests
            SET status='pending', worker_name=NULL, started_at=NULL, orchestration_id=NULL
            WHERE id=?
            """,
            (row["id"],),
        )


def init_db():
    conn = connect()
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS settings(
            key TEXT PRIMARY KEY,
            value TEXT
        );
        CREATE TABLE IF NOT EXISTS jobs(
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            fixture_path TEXT NOT NULL,
            depends_on_json TEXT NOT NULL,
            max_attempts INTEGER NOT NULL DEFAULT 1,
            enabled INTEGER NOT NULL DEFAULT 1,
            note TEXT
        );
        CREATE TABLE IF NOT EXISTS orchestrations(
            id INTEGER PRIMARY KEY,
            status TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            report_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS job_attempts(
            id INTEGER PRIMARY KEY,
            orchestration_id INTEGER NOT NULL,
            job_id INTEGER NOT NULL,
            attempt_no INTEGER NOT NULL,
            status TEXT NOT NULL,
            detail TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS watermarks(
            job_id INTEGER PRIMARY KEY,
            watermark_ts TEXT,
            artifact_rows INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS orchestration_requests(
            id INTEGER PRIMARY KEY,
            idempotency_key TEXT NOT NULL UNIQUE,
            note TEXT,
            status TEXT NOT NULL,
            dedupe_count INTEGER NOT NULL DEFAULT 0,
            orchestration_id INTEGER,
            error TEXT,
            worker_name TEXT,
            created_at TEXT NOT NULL,
            enqueued_at TEXT NOT NULL,
            started_at TEXT,
            finished_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_orch_req_status
            ON orchestration_requests(status, id);
        """
    )
    _recover_stale_requests(conn)
    if conn.execute("SELECT COUNT(*) c FROM jobs").fetchone()["c"] == 0:
        jobs = [
            (
                "ingest_orders",
                "ingest_orders.json",
                "[]",
                1,
                1,
                "无依赖：写出订单水位",
            ),
            (
                "dim_customer",
                "dim_customer.json",
                "[]",
                1,
                1,
                "无依赖：写出客户维表水位",
            ),
            (
                "fact_order_daily",
                "fact_order_daily.json",
                '["ingest_orders","dim_customer"]',
                1,
                1,
                "依赖两路上游成功后才执行（底座仅查上游成功）",
            ),
        ]
        conn.executemany(
            """
            INSERT INTO jobs(name, fixture_path, depends_on_json, max_attempts, enabled, note)
            VALUES (?,?,?,?,?,?)
            """,
            jobs,
        )
        for row in conn.execute("SELECT id FROM jobs").fetchall():
            conn.execute(
                "INSERT OR IGNORE INTO watermarks(job_id, watermark_ts, artifact_rows, updated_at) VALUES (?,?,?,?)",
                (row["id"], None, 0, None),
            )
        conn.execute(
            "INSERT OR REPLACE INTO settings(key, value) VALUES (?, ?)",
            ("default_max_attempts", "1"),
        )
    conn.commit()
    conn.close()
