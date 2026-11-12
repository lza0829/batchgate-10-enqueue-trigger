from __future__ import annotations

from typing import Any

from app.config import FIXTURES_DIR
from app.db import connect
from app.engines.dependency import ready_by_upstream_success, topological_layers
from app.engines.job_runner import run_fixture_job
from app.repositories import jobs as jobs_repo
from app.repositories import orchestrations as orch_repo
from app.repositories import watermarks as wm_repo


def dashboard_stats() -> dict[str, Any]:
    from app.repositories import requests as req_repo

    conn = connect()
    try:
        stats = {
            "job_count": conn.execute("SELECT COUNT(*) c FROM jobs").fetchone()["c"],
            "enabled_jobs": conn.execute(
                "SELECT COUNT(*) c FROM jobs WHERE enabled=1"
            ).fetchone()["c"],
            "orchestration_count": conn.execute(
                "SELECT COUNT(*) c FROM orchestrations"
            ).fetchone()["c"],
            "attempt_count": conn.execute(
                "SELECT COUNT(*) c FROM job_attempts"
            ).fetchone()["c"],
            "watermarked_jobs": conn.execute(
                "SELECT COUNT(*) c FROM watermarks WHERE watermark_ts IS NOT NULL"
            ).fetchone()["c"],
        }
        stats["request_queue"] = req_repo.stats(conn)
        return stats
    finally:
        conn.close()


def execute_orchestration(conn, precreated_orch_id: int) -> dict[str, Any]:
    """Run the job graph into an already-created orchestration row.

    This is the only execution path and it is driven exclusively by the
    serial queue worker, which pre-creates the orchestration row so its id
    can be attached to the request before any job runs (visible/auditable
    mid-flight). The caller owns the transaction commit.
    """
    return _run_jobs_into_orchestration(conn, precreated_orch_id)


def _run_jobs_into_orchestration(conn, orch_id: int) -> dict[str, Any]:
    jobs = [j for j in jobs_repo.list_jobs(conn) if j["enabled"]]
    success_names: set[str] = set()
    skipped = 0
    failed = 0
    succeeded = 0
    layers = topological_layers(jobs)

    for layer in layers:
        for job in layer:
            ready, reason = ready_by_upstream_success(job["depends_on"], success_names)
            if not ready:
                orch_repo.insert_attempt(
                    conn,
                    orchestration_id=orch_id,
                    job_id=job["id"],
                    attempt_no=1,
                    status="skipped_dep",
                    detail={"reason": reason},
                )
                skipped += 1
                continue
            try:
                result = run_fixture_job(FIXTURES_DIR / job["fixture_path"])
                orch_repo.insert_attempt(
                    conn,
                    orchestration_id=orch_id,
                    job_id=job["id"],
                    attempt_no=1,
                    status="success",
                    detail=result,
                )
                wm_repo.set_watermark(
                    conn, job["id"], result["watermark_ts"], result["artifact_rows"]
                )
                success_names.add(job["name"])
                succeeded += 1
            except Exception as exc:  # noqa: BLE001
                orch_repo.insert_attempt(
                    conn,
                    orchestration_id=orch_id,
                    job_id=job["id"],
                    attempt_no=1,
                    status="failed",
                    detail={"error": str(exc)},
                )
                failed += 1

    attempts = orch_repo.list_attempts(conn, orch_id)
    report = {
        "orchestration_id": orch_id,
        "job_total": len(jobs),
        "succeeded": succeeded,
        "failed": failed,
        "skipped": skipped,
        "attempt_count": len(attempts),
        "watermarked_jobs": conn.execute(
            "SELECT COUNT(*) c FROM watermarks WHERE watermark_ts IS NOT NULL"
        ).fetchone()["c"],
        "layer_count": len(layers),
    }
    status = "ok" if failed == 0 else "partial"
    orch_repo.finish_orchestration(conn, orch_id, status, report)
    return orch_repo.get_orchestration(conn, orch_id) or report
