# ops/webhooks.py
from __future__ import annotations

import os
import json
import traceback
from typing import Any, Dict, Optional
from datetime import datetime, timezone

from fastapi import FastAPI, Header, Request

# ---- pyairtable v1/v2 compatibility -----------------------------------------
try:
    from pyairtable import Table as _PyTable  # v1 style
except Exception:  # pragma: no cover
    _PyTable = None
try:
    from pyairtable import Api as _PyApi  # v2 style
except Exception:  # pragma: no cover
    _PyApi = None


def _make_table(api_key: Optional[str], base_id: Optional[str], table_name: str):
    """Return a Table-like object or None (never throws)."""
    if not (api_key and base_id):
        return None
    try:
        if _PyTable:
            # v1 signature: Table(api_key, base_id, table_name)
            return _PyTable(api_key, base_id, table_name)
        if _PyApi:
            # v2 signature: Api(api_key).table(base_id, table_name)
            return _PyApi(api_key).table(base_id, table_name)
    except Exception:
        traceback.print_exc()
    return None


# ---- Config & env ------------------------------------------------------------
AIRTABLE_API_KEY = (
    os.getenv("AIRTABLE_REPORTING_KEY")
    or os.getenv("PERFORMANCE_KEY")
    or os.getenv("AIRTABLE_API_KEY")
)
DEVOPS_BASE = (
    os.getenv("DEVOPS_BASE")
    or os.getenv("PERFORMANCE_BASE")
    or os.getenv("AIRTABLE_PERFORMANCE_BASE_ID")
)
LOGS_TABLE_NAME = os.getenv("DEVOPS_LOGS_TABLE", "Logs")

WEBHOOK_TOKEN = (
    os.getenv("WEBHOOK_TOKEN")
    or os.getenv("CRON_TOKEN")
    or os.getenv("TEXTGRID_AUTH_TOKEN")
)


def get_logs_table():
    """Lazy init; safe if env missing."""
    return _make_table(AIRTABLE_API_KEY, DEVOPS_BASE, LOGS_TABLE_NAME)


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def log_devops(event: str, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """
    Best-effort logging to Airtable; falls back to stdout if not configured.
    """
    fields = {
        "Event": event,
        "Payload": json.dumps(payload or {}, ensure_ascii=False)[:2000],
        "Timestamp": iso_now(),
    }

    tbl = get_logs_table()
    if not tbl:
        print(f"[DEVOPS_LOG] {event} | {fields['Payload']}")
        return {"ok": True, "mock": True, "note": "airtable not configured"}

    try:
        rec = tbl.create({k: v for k, v in fields.items() if v is not None})
        return {"ok": True, "id": rec.get("id")}
    except Exception as e:  # pragma: no cover
        traceback.print_exc()
        return {"ok": False, "error": str(e)}


# ---- FastAPI app -------------------------------------------------------------
app = FastAPI(title="Ops Webhooks", version="1.0.0")


@app.get("/health")
def health():
    """
    Always return ok=True so the service boots even without Airtable.
    Expose whether Airtable is configured for observability.
    """
    return {
        "ok": True,
        "airtable_configured": bool(AIRTABLE_API_KEY and DEVOPS_BASE),
        "table_ready": bool(get_logs_table()),
        "time": iso_now(),
    }


@app.post("/ops/webhook")
async def ops_webhook(
    request: Request,
    x_webhook_token: Optional[str] = Header(None),
):
    """
    Generic webhook endpoint that logs the payload.
    Optional token check via WEBHOOK_TOKEN/CRON_TOKEN/TEXTGRID_AUTH_TOKEN.
    """
    try:
        body = await request.json()
    except Exception:
        body = {"raw": await request.body()}

    if WEBHOOK_TOKEN and (x_webhook_token or "") != WEBHOOK_TOKEN:
        return {"ok": False, "error": "invalid token"}

    res = log_devops("webhook", body if isinstance(body, dict) else {"payload": str(body)})
    return {"ok": True, "logged": res.get("ok", False), "airtable": res}