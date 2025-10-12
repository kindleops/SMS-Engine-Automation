# ops/webhooks.py
from __future__ import annotations

import os, json, traceback
from typing import Any, Dict, Optional
from datetime import datetime, timezone

from fastapi import FastAPI, Header, Request, Query, HTTPException

# ─────────────────────────────────────────────────────────────
# Optional engine hooks (safe fallbacks if modules missing)
# ─────────────────────────────────────────────────────────────
try:
    from sms.campaign_runner import run_campaigns
except Exception:
    def run_campaigns(limit: int | str = 1, send_after_queue: Optional[bool] = None):
        return {"ok": True, "mock": True, "note": "campaign_runner not available"}

try:
    from sms.retry_runner import run_retry
except Exception:
    def run_retry(limit: int = 100, view: str | None = None):
        return {"ok": True, "mock": True, "note": "retry_runner not available"}

try:
    from sms.metrics_tracker import update_metrics
except Exception:
    def update_metrics(*args, **kwargs):
        return {"ok": True, "mock": True}

try:
    from sms.autoresponder import run as run_autoresponder
except Exception:
    def run_autoresponder(limit: int = 50, view: Optional[str] = None):
        return {"ok": True, "processed": 0, "mock": True}

# ─────────────────────────────────────────────────────────────
# pyairtable v1/v2 compatibility
# ─────────────────────────────────────────────────────────────
try:
    from pyairtable import Table as _PyTable  # v1
except Exception:
    _PyTable = None
try:
    from pyairtable import Api as _PyApi  # v2
except Exception:
    _PyApi = None


def _make_table(api_key: Optional[str], base_id: Optional[str], table_name: str):
    """Return a Table-like object or None (never throws)."""
    if not (api_key and base_id):
        return None
    try:
        if _PyTable:
            return _PyTable(api_key, base_id, table_name)        # v1
        if _PyApi:
            return _PyApi(api_key).table(base_id, table_name)    # v2
    except Exception:
        traceback.print_exc()
    return None


# ─────────────────────────────────────────────────────────────
# Config & env
# ─────────────────────────────────────────────────────────────
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

# Numbers table for quota reset
CAMPAIGN_CONTROL_BASE = os.getenv("CAMPAIGN_CONTROL_BASE") or os.getenv("AIRTABLE_CAMPAIGN_CONTROL_BASE_ID")
NUMBERS_TABLE = os.getenv("NUMBERS_TABLE", "Numbers")

# Worker/autoresponder knobs
SEND_BATCH_LIMIT = int(os.getenv("SEND_BATCH_LIMIT", "500"))
RETRY_LIMIT = int(os.getenv("RETRY_LIMIT", "100"))
AUTORESPONDER_LIMIT = int(os.getenv("AUTORESPONDER_LIMIT", "50"))
AUTORESPONDER_VIEW = os.getenv("AUTORESPONDER_VIEW", "Unprocessed Inbounds")


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────
def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def _norm(s: Any) -> Any:
    import re
    return re.sub(r"[^a-z0-9]+", "", s.strip().lower()) if isinstance(s, str) else s

def _auto_field_map(tbl) -> Dict[str, str]:
    try:
        rows = tbl.all(max_records=1)  # type: ignore[attr-defined]
        keys = list(rows[0].get("fields", {}).keys()) if rows else []
    except Exception:
        keys = []
    return { _norm(k): k for k in keys }

def _safe_update(tbl, rid: str, payload: Dict):
    if not (tbl and rid and payload):
        return None
    amap = _auto_field_map(tbl)
    data = {}
    for k, v in payload.items():
        mk = amap.get(_norm(k))
        if mk:
            data[mk] = v
    if not data:
        return None
    try:
        return tbl.update(rid, data)  # type: ignore[attr-defined]
    except Exception:
        traceback.print_exc()
        return None


# DevOps logging
def get_logs_table():
    return _make_table(AIRTABLE_API_KEY, DEVOPS_BASE, LOGS_TABLE_NAME)

def log_devops(event: str, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
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
    except Exception as e:
        traceback.print_exc()
        return {"ok": False, "error": str(e)}


# Auth helpers (support ?token=, x-webhook-token, x-cron-token, or Bearer)
def _extract_token(request: Request, qp_token: Optional[str], h_webhook: Optional[str], h_cron: Optional[str]) -> str:
    if qp_token:
        return qp_token
    if h_webhook:
        return h_webhook
    if h_cron:
        return h_cron
    auth = request.headers.get("authorization") or ""
    if auth.lower().startswith("bearer "):
        return auth.split(" ", 1)[1]
    return ""

def _require_token(request: Request, qp_token: Optional[str], h_webhook: Optional[str], h_cron: Optional[str]):
    if not WEBHOOK_TOKEN:
        return  # unsecured mode
    token = _extract_token(request, qp_token, h_webhook, h_cron)
    if token != WEBHOOK_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")


# ─────────────────────────────────────────────────────────────
# FastAPI app & routes
# ─────────────────────────────────────────────────────────────
app = FastAPI(title="SMS Engine Ops", version="3.0.0")

@app.get("/health")
def health():
    return {
        "ok": True,
        "airtable_configured": bool(AIRTABLE_API_KEY and DEVOPS_BASE),
        "table_ready": bool(get_logs_table()),
        "time": iso_now(),
    }

@app.get("/ping")
def ping():
    return {"ok": True, "pong": True, "time": iso_now()}

@app.post("/echo-token")
def echo_token(
    x_cron_token: str | None = Header(None),
    x_webhook_token: str | None = Header(None),
):
    return {"ok": True, "x_cron_token": x_cron_token, "x_webhook_token": x_webhook_token}

@app.get("/debug/env")
def debug_env():
    keys = [
        "AIRTABLE_API_KEY","LEADS_CONVOS_BASE","AIRTABLE_LEADS_CONVOS_BASE_ID",
        "CAMPAIGN_CONTROL_BASE","AIRTABLE_CAMPAIGN_CONTROL_BASE_ID",
        "PERFORMANCE_BASE","AIRTABLE_PERFORMANCE_BASE_ID",
        "UPSTASH_REDIS_REST_URL","UPSTASH_REDIS_REST_TOKEN",
        "WEBHOOK_TOKEN","CRON_TOKEN","TEXTGRID_AUTH_TOKEN"
    ]
    return {"ok": True, "present": {k: bool(os.getenv(k)) for k in keys}}

# Generic logger (optional)
@app.post("/ops/webhook")
async def ops_webhook(
    request: Request,
    x_webhook_token: Optional[str] = Header(None),
    x_cron_token: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
):
    try:
        body = await request.json()
    except Exception:
        body = {"raw": (await request.body()).decode("utf-8", errors="ignore")}
    _require_token(request, token, x_webhook_token, x_cron_token)
    res = log_devops("webhook", body if isinstance(body, dict) else {"payload": str(body)})
    return {"ok": True, "logged": res.get("ok", False), "airtable": res}

# ---- Cron/worker endpoints (single, canonical definitions) -------------------

@app.post("/run-campaigns")
def run_campaigns_ep(
    request: Request,
    limit: str = Query("1"),
    send_after_queue: Optional[bool] = Query(None),
    dry: bool = Query(False, description="If true, skip actual run and return quickly"),
    x_webhook_token: Optional[str] = Header(None),
    x_cron_token: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
):
    _require_token(request, token, x_webhook_token, x_cron_token)
    if dry:
        return {"ok": True, "dry": True, "time": iso_now(), "limit": limit, "send_after_queue": send_after_queue}
    result = run_campaigns(limit=limit, send_after_queue=send_after_queue)
    log_devops("run-campaigns", {"limit": limit, "send_after_queue": send_after_queue, "result": result})
    return result

@app.post("/retry")
def retry_ep(
    request: Request,
    limit: int = Query(RETRY_LIMIT),
    view: Optional[str] = Query("Failed Sends"),
    x_webhook_token: Optional[str] = Header(None),
    x_cron_token: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
):
    _require_token(request, token, x_webhook_token, x_cron_token)
    result = run_retry(limit=limit, view=view)
    log_devops("retry", {"limit": limit, "view": view, "result": result})
    return result

@app.post("/autoresponder/autoresponder")
def autoresponder_ep(
    request: Request,
    limit: int = Query(AUTORESPONDER_LIMIT),
    view: Optional[str] = Query(AUTORESPONDER_VIEW),
    x_webhook_token: Optional[str] = Header(None),
    x_cron_token: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
):
    _require_token(request, token, x_webhook_token, x_cron_token)
    result = run_autoresponder(limit=limit, view=view)
    log_devops("autoresponder", {"limit": limit, "view": view, "result": result})
    return result

@app.post("/reset-quotas")
def reset_quotas_ep(
    request: Request,
    x_webhook_token: Optional[str] = Header(None),
    x_cron_token: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
):
    _require_token(request, token, x_webhook_token, x_cron_token)

    tbl = _make_table(AIRTABLE_API_KEY, CAMPAIGN_CONTROL_BASE, NUMBERS_TABLE)
    if not tbl:
        return {"ok": False, "error": "numbers table unavailable"}

    try:
        rows = tbl.all()  # type: ignore[attr-defined]
    except Exception as e:
        traceback.print_exc()
        return {"ok": False, "error": str(e)}

    updated = 0
    for r in rows:
        rid = r.get("id")
        if not rid:
            continue
        payload = {"Sent Today": 0}
        if r.get("fields", {}).get("Daily Reset") is not None and r.get("fields", {}).get("Remaining") is not None:
            try:
                daily = int(r["fields"].get("Daily Reset") or 0)
                payload["Remaining"] = daily
            except Exception:
                pass
        if _safe_update(tbl, rid, payload):
            updated += 1

    log_devops("reset-quotas", {"updated": updated})
    return {"ok": True, "updated": updated}

@app.post("/aggregate-kpis")
def aggregate_kpis_ep(
    request: Request,
    x_webhook_token: Optional[str] = Header(None),
    x_cron_token: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
):
    _require_token(request, token, x_webhook_token, x_cron_token)
    res = update_metrics()
    log_devops("aggregate-kpis", {"result": res})
    return res