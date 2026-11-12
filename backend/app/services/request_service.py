"""Service layer for the orchestration request queue.

Submissions only ever land in the queue here; nothing starts an
orchestration synchronously. The background worker in :mod:`app.worker`
is the sole consumer.
"""

from __future__ import annotations

from typing import Any

from app.db import connect
from app.repositories import requests as req_repo

MAX_KEY_LENGTH = 128
MAX_NOTE_LENGTH = 500


class IdempotencyKeyError(ValueError):
    """Raised when an idempotency key is missing or malformed."""


def _validate_key(idempotency_key: str) -> str:
    key = (idempotency_key or "").strip()
    if not key:
        raise IdempotencyKeyError("idempotency_key is required")
    if len(key) > MAX_KEY_LENGTH:
        raise IdempotencyKeyError(
            f"idempotency_key must be at most {MAX_KEY_LENGTH} characters"
        )
    return key


def enqueue_request(idempotency_key: str, note: str | None) -> tuple[dict[str, Any], bool]:
    """Enqueue one orchestration request, deduplicating on the idempotency key.

    Returns the request row and ``created=True`` for a new queue entry or
    ``created=False`` when the key was already seen (dedupe hit).
    """
    key = _validate_key(idempotency_key)
    clean_note = note.strip() if isinstance(note, str) and note.strip() else None
    if clean_note and len(clean_note) > MAX_NOTE_LENGTH:
        clean_note = clean_note[:MAX_NOTE_LENGTH]
    conn = connect()
    try:
        return req_repo.enqueue(conn, idempotency_key=key, note=clean_note)
    finally:
        conn.close()


def list_requests(limit: int = 100) -> list[dict[str, Any]]:
    conn = connect()
    try:
        return req_repo.list_requests(conn, limit=limit)
    finally:
        conn.close()


def get_request(request_id: int) -> dict[str, Any] | None:
    conn = connect()
    try:
        return req_repo.get_request(conn, request_id)
    finally:
        conn.close()


def get_request_report(request_id: int) -> dict[str, Any] | None:
    conn = connect()
    try:
        return req_repo.request_report_link(conn, request_id)
    finally:
        conn.close()


def request_stats() -> dict[str, int]:
    """Queue accounting: enqueue/dedupe/success/failure counters.

    ``total`` equals the number of rows in the request list;
    ``total + deduped_submissions`` is the number of HTTP submissions
    accepted; ``succeeded + failed`` equals the number of orchestration
    rows reachable from terminal requests.
    """
    conn = connect()
    try:
        return req_repo.stats(conn)
    finally:
        conn.close()
