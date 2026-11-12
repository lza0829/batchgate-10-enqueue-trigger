"""HTTP entry points for the orchestration request queue.

Submissions never run an orchestration synchronously: they are enqueued
with an idempotency key and processed by the serial worker.
"""

from fastapi import APIRouter, HTTPException, Response

from app.schemas.entities import EnqueueRequestBody
from app.services.request_service import (
    IdempotencyKeyError,
    enqueue_request,
    get_request,
    get_request_report,
    list_requests,
    request_stats,
)

router = APIRouter(tags=["requests"])


@router.post("/requests")
def submit_request(body: EnqueueRequestBody, response: Response):
    """Enqueue an orchestration request.

    First submission for an idempotency key -> 201 (new queue entry).
    Repeated submission -> 200 with the existing request; no new
    orchestration is started and ``dedupe_count`` is incremented.
    """
    try:
        request, created = enqueue_request(body.idempotency_key, body.note)
    except IdempotencyKeyError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    response.status_code = 201 if created else 200
    return {"request": request, "created": created, "deduped": not created}


@router.get("/requests")
def get_requests(limit: int = 100):
    if limit < 1 or limit > 500:
        raise HTTPException(status_code=422, detail="limit must be between 1 and 500")
    return list_requests(limit=limit)


@router.get("/requests/stats")
def get_request_stats():
    """Queue accounting counters; reconcile against /requests and orchestrations."""
    return request_stats()


@router.get("/requests/{request_id}")
def get_one_request(request_id: int):
    request = get_request(request_id)
    if not request:
        raise HTTPException(status_code=404, detail="request not found")
    return request


@router.get("/requests/{request_id}/report")
def get_request_orchestration_report(request_id: int):
    """Deep link data: the orchestration record associated with a request."""
    link = get_request_report(request_id)
    if not link:
        raise HTTPException(status_code=404, detail="request not found")
    return link
