"""Reusable token-based auth helpers for webhook and cron routes."""

from __future__ import annotations

from fastapi import HTTPException, Request

from . import spec


def _token_from_authorization(header: str | None) -> str | None:
    if not header:
        return None
    parts = header.split()
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1]
    return None


async def require_webhook_token(request: Request) -> None:
    """Validate webhook authentication as described in the spec."""

    expected = spec.webhook_token()
    if not expected:
        return  # no auth configured

    provided = request.query_params.get("token")
    provided = provided or request.headers.get("x-webhook-token")
    provided = provided or _token_from_authorization(request.headers.get("Authorization"))

    if provided != expected:
        raise HTTPException(status_code=401, detail="Invalid webhook token")


async def require_cron_token(request: Request) -> None:
    expected = spec.cron_token()
    if not expected:
        return

    provided = request.query_params.get("token")
    provided = provided or request.headers.get("x-cron-token")
    provided = provided or _token_from_authorization(request.headers.get("Authorization"))

    if provided != expected:
        raise HTTPException(status_code=401, detail="Invalid cron token")
