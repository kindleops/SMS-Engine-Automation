# sms/webhooks/delivery.py
"""
Optimized Delivery Webhook
--------------------------
• Accepts TextGrid/Twilio-like DLRs (JSON or form)
• Updates Drip Queue + Conversations via datastore CONNECTOR
• Increments Numbers counters
• Idempotent (Redis / Upstash / in-memory)
• Schema-safe (uses airtable_schema maps + unknown-field filtering)
• Routes: POST /delivery  and POST /status  (both return 200 quickly)
"""

from __future__ import annotations

import os, re, json, traceback, asyncio
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Iterable, List

from fastapi import APIRouter, Request, Header, HTTPException, Query
from sms.datastore import CONNECTOR, update_record, list_records
from sms.runtime import get_logger
from sms.inbound_webhook import normalize_e164

# Schema maps (avoid hard-coded Airtable column names)
from sms.airtable_schema import conversations_field_map, drip_field_map

# Optional Redis / Upstash
try:
    import redis as _redis  # type: ignore
except Exception:
    _redis = None
try:
    import requests  # type: ignore
except Exception:
    requests = None

# Optional number counters
try:
    from sms.number_pools import increment_delivered, increment_failed
except Exception:
    increment_delivered = None
    increment_failed = None

logger = get_logger(__name__)

# Two routers: one under /delivery, one at root (/status) for providers that post there.
router = APIRouter(prefix="/delivery", tags=["Delivery"])
router_root = APIRouter(tags=["Delivery"])

# ---------------------------------------------------------------------------
# ENV
# ---------------------------------------------------------------------------
WEBHOOK_TOKEN = (
    os.getenv("WEBHOOK_TOKEN")
    or os.getenv("CRON_TOKEN")
    or os.getenv("TEXTGRID_AUTH_TOKEN")
)

REDIS_URL = os.getenv("REDIS_URL") or os.getenv("UPSTASH_REDIS_URL")
REDIS_TLS = os.getenv("REDIS_TLS", "true").lower() in {"1", "true", "yes"}
UPSTASH_REST_URL = os.getenv("UPSTASH_REDIS_REST_URL")
UPSTASH_REST_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN") or os.getenv("upstash_redis_rest_token")

CONV_FIELDS = conversations_field_map()
DRIP_FIELDS = drip_field_map()

# Common field names (with safe fallbacks)
CONV_STATUS = CONV_FIELDS.get("STATUS", "Status")
CONV_TGID = CONV_FIELDS.get("TEXTGRID_ID", "TextGrid ID")
CONV_SENT_AT = CONV_FIELDS.get("SENT_AT", "Sent At")
CONV_DELIVERED_AT = CONV_FIELDS.get("DELIVERED_AT", "Delivered At")
CONV_LAST_ERR = CONV_FIELDS.get("LAST_ERROR", "Last Error")
CONV_UI = CONV_FIELDS.get("UI", "UI")

DRIP_STATUS = DRIP_FIELDS.get("STATUS", "Status")
DRIP_TGID = DRIP_FIELDS.get("TEXTGRID_ID", "TextGrid ID")
DRIP_SENT_AT = DRIP_FIELDS.get("SENT_AT", "Sent At")
DRIP_DELIVERED_AT = DRIP_FIELDS.get("DELIVERED_AT", "Delivered At")
DRIP_LAST_ERR = DRIP_FIELDS.get("LAST_ERROR", "Last Error")
DRIP_UI = DRIP_FIELDS.get("UI", "UI")

# When searching by SID, try multiple common column names
SID_SEARCH_CANDIDATES = [
    CONV_TGID, DRIP_TGID, "Message SID", "SID", "Provider ID", "messageSid", "Textgrid SID"
]

# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

def _digits_only(v: Any) -> Optional[str]:
    if not isinstance(v, str):
        return None
    ds = "".join(re.findall(r"\d+", v))
    return ds if len(ds) >= 10 else None

def _is_authorized(header_token: Optional[str], query_token: Optional[str]) -> bool:
    if not WEBHOOK_TOKEN:
        return True  # auth disabled
    return (header_token == WEBHOOK_TOKEN) or (query_token == WEBHOOK_TOKEN)

class IdemStore:
    """Redis / Upstash idempotency with local fallback."""
    def __init__(self):
        self.r = None
        self.rest = bool(UPSTASH_REST_URL and UPSTASH_REST_TOKEN and requests)
        if REDIS_URL and _redis:
            try:
                self.r = _redis.from_url(REDIS_URL, ssl=REDIS_TLS, decode_responses=True)
            except Exception:
                traceback.print_exc()
        self._mem = set()

    def seen(self, sid: Optional[str]) -> bool:
        if not sid:
            return False
        key = f"dlv:sid:{sid}"
        # Redis TCP
        if self.r:
            try:
                ok = self.r.set(key, "1", nx=True, ex=6 * 60 * 60)
                return not bool(ok)
            except Exception:
                traceback.print_exc()
        # Upstash REST
        if self.rest:
            try:
                resp = requests.post(
                    UPSTASH_REST_URL,
                    headers={"Authorization": f"Bearer {UPSTASH_REST_TOKEN}"},
                    json={"command": ["SET", key, "1", "EX", "21600", "NX"]},
                    timeout=5,
                )
                data = resp.json() if resp.ok else {}
                return data.get("result") != "OK"
            except Exception:
                traceback.print_exc()
        # Local fallback
        if key in self._mem:
            return True
        self._mem.add(key)
        return False

IDEM = IdemStore()

# ---------------------------------------------------------------------------
# NUMBER COUNTERS
# ---------------------------------------------------------------------------

def _bump_numbers(did: str, delivered: bool):
    """Increment counters on the Numbers table (best-effort)."""
    try:
        if delivered and increment_delivered:
            increment_delivered(did)
            return
        if not delivered and increment_failed:
            increment_failed(did)
            return
    except Exception:
        traceback.print_exc()

    # Fallback: manual update via datastore
    handle = CONNECTOR.numbers()
    rows = list_records(handle)
    for r in rows:
        f = r.get("fields", {})
        if _digits_only(f.get("Number")) == _digits_only(did):
            patch = {}
            if delivered:
                patch["Delivered Today"] = int(f.get("Delivered Today") or 0) + 1
                patch["Delivered Total"] = int(f.get("Delivered Total") or 0) + 1
            else:
                patch["Failed Today"] = int(f.get("Failed Today") or 0) + 1
                patch["Failed Total"] = int(f.get("Failed Total") or 0) + 1
            try:
                update_record(handle, r["id"], _filter_known(handle, patch))
            except Exception:
                traceback.print_exc()
            break

# ---------------------------------------------------------------------------
# AIRTABLE UTILS
# ---------------------------------------------------------------------------

def _existing_fields(handle) -> List[str]:
    try:
        rs = list_records(handle, max_records=1)
        if rs:
            return list((rs[0].get("fields") or {}).keys())
    except Exception:
        pass
    return []

def _filter_known(handle, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Keep only columns that exist in this table."""
    if not payload:
        return {}
    keys = set(_existing_fields(handle))
    if not keys:
        # If we can't probe, send raw; connector may ignore unknowns
        return dict(payload)
    return {k: v for k, v in payload.items() if k in keys}

def _find_by_sid(handle, sid: str) -> Optional[Dict[str, Any]]:
    """Find the record by trying multiple SID field names."""
    sid_esc = sid.replace("'", "\\'")
    for col in SID_SEARCH_CANDIDATES:
        try:
            recs = list_records(handle, formula=f"{{{col}}}='{sid_esc}'", max_records=1)
            if recs:
                return recs[0]
        except Exception:
            continue
    return None

async def _update_airtable_status(sid: str, status: str, error: Optional[str], from_did: str, to_phone: str, raw_status: Optional[str] = None, provider: Optional[str] = None):
    """Update Drip Queue + Conversations via datastore with schema-safe patches."""
    dq_handle = CONNECTOR.drip_queue()
    conv_handle = CONNECTOR.conversations()

    now = utcnow_iso()
    # Base patches (we'll filter to each table's known columns)
    if status == "delivered":
        conv_patch = {CONV_STATUS: "DELIVERED", CONV_DELIVERED_AT: now, CONV_UI: "✅"}
        dq_patch = {DRIP_STATUS: "DELIVERED", DRIP_DELIVERED_AT: now, DRIP_UI: "✅"}
    elif status in {"failed", "undelivered"}:
        msg = (error or status)[:500]
        conv_patch = {CONV_STATUS: "FAILED", CONV_LAST_ERR: msg, CONV_UI: "❌"}
        dq_patch = {DRIP_STATUS: "FAILED", DRIP_LAST_ERR: msg, DRIP_UI: "❌"}
    elif status == "queued":
        conv_patch = {CONV_STATUS: "QUEUED", CONV_UI: "⏳"}
        dq_patch = {DRIP_STATUS: "QUEUED", DRIP_UI: "⏳"}
    elif status == "optout":
        conv_patch = {CONV_STATUS: "OPT OUT", CONV_UI: "🚫"}
        dq_patch = {DRIP_STATUS: "OPT OUT", DRIP_UI: "🚫"}
    else:  # "sent" or unknown → mark sent
        conv_patch = {CONV_STATUS: "SENT", CONV_SENT_AT: now, CONV_UI: "✅"}
        dq_patch = {DRIP_STATUS: "SENT", DRIP_SENT_AT: now, DRIP_UI: "✅"}

    # Add optional telemetry fields (will be filtered by _filter_known)
    if raw_status:
        conv_patch["Delivery Raw Status"] = raw_status
        dq_patch["Delivery Raw Status"] = raw_status
    if provider:
        conv_patch["Provider"] = provider
        dq_patch["Provider"] = provider

    # Conversations
    try:
        conv_rec = _find_by_sid(conv_handle, sid)
        if conv_rec:
            update_record(conv_handle, conv_rec["id"], _filter_known(conv_handle, conv_patch))
    except Exception:
        traceback.print_exc()

    # Drip Queue
    try:
        dq_rec = _find_by_sid(dq_handle, sid)
        if dq_rec:
            update_record(dq_handle, dq_rec["id"], _filter_known(dq_handle, dq_patch))
    except Exception:
        traceback.print_exc()

# ---------------------------------------------------------------------------
# PAYLOAD PARSER
# ---------------------------------------------------------------------------

def _extract_payload(req_body: Any, headers: Dict[str, str]) -> Dict[str, Any]:
    """Normalize JSON or form body from any provider."""

    def lower(d):
        return {str(k).lower(): v for k, v in d.items()}

    def pick(d, *keys):
        for k in keys:
            key = k.lower()
            if key in d and d[key] not in (None, ""):
                return d[key]
        return None

    if isinstance(req_body, str):
        try:
            data = json.loads(req_body)
        except Exception:
            data = {}
    else:
        data = dict(req_body or {})

    d = lower(data)
    sid = pick(d, "message_sid", "sid", "id", "messageid")
    status_raw = (pick(d, "message_status", "status", "delivery_status", "eventtype") or "").lower()
    from_n = pick(d, "from", "sender", "source", "sourceaddress")
    to_n = pick(d, "to", "recipient", "destination", "destinationaddress")
    error = pick(d, "error_message", "error", "reason")

    delivered = {"delivered", "success", "delivrd"}
    failed = {"failed"}
    undelivered = {"undelivered", "rejected", "blocked", "expired", "error"}
    queued = {"queued", "accepted", "pending", "enroute", "submitted"}
    optout = {"optout", "opt-out"}

    if status_raw in delivered:
        norm = "delivered"
    elif status_raw in optout:
        norm = "optout"
    elif status_raw in queued:
        norm = "queued"
    elif status_raw in undelivered:
        norm = "undelivered"
    elif status_raw in failed:
        norm = "failed"
    else:
        norm = "sent"

    provider = headers.get("x-provider") or headers.get("user-agent") or "unknown"
    return {"sid": sid, "status": norm, "raw_status": status_raw, "from": from_n, "to": to_n, "error": error, "provider": provider}

# ---------------------------------------------------------------------------
# ROUTES
# ---------------------------------------------------------------------------

async def _handle_delivery(request: Request, header_token: Optional[str], content_type: Optional[str], query_token: Optional[str]):
    if not _is_authorized(header_token, query_token):
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Parse body safely (json or form)
    try:
        if content_type and "application/x-www-form-urlencoded" in content_type.lower():
            form = await request.form()
            body = {k: (v if isinstance(v, str) else str(v)) for k, v in dict(form).items()}
        else:
            try:
                body = await request.json()
            except Exception:
                form = await request.form()
                body = {k: (v if isinstance(v, str) else str(v)) for k, v in dict(form).items()}
    except Exception:
        traceback.print_exc()
        raise HTTPException(status_code=422, detail="Invalid payload")

    parsed = _extract_payload(body, dict(request.headers))
    sid, status, from_d, to_p, err = parsed["sid"], parsed["status"], parsed["from"], parsed["to"], parsed["error"]
    raw_status, provider = parsed["raw_status"], parsed["provider"]

    if not all([sid, status, from_d, to_p]):
        raise HTTPException(status_code=422, detail="Missing required fields")

    from_norm = normalize_e164(from_d, field="From")
    to_norm = normalize_e164(to_p, field="To")
    logger.info(f"📡 Delivery webhook | {status.upper()} | SID={sid} | from={from_norm} → {to_norm}")

    # Idempotency
    if IDEM.seen(sid):
        return {"status": "ok", "sid": sid, "note": "duplicate"}

    # Numbers counters
    try:
        if status == "delivered":
            _bump_numbers(from_norm, True)
        elif status in {"failed", "undelivered"}:
            _bump_numbers(from_norm, False)
    except Exception:
        traceback.print_exc()

    # Async Airtable updates (so we ack 200 immediately)
    asyncio.create_task(_update_airtable_status(sid, status, err, from_norm, to_norm, raw_status, provider))

    return {"status": "ok", "sid": sid, "normalized": status}

# Primary route under /delivery
@router.post("")
async def delivery_webhook(
    request: Request,
    x_webhook_token: Optional[str] = Header(None, convert_underscores=False),
    content_type: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
):
    return await _handle_delivery(request, x_webhook_token, content_type, token)

# Also accept /delivery/ (some providers insist on trailing slash)
@router.post("/")
async def delivery_webhook_slash(
    request: Request,
    x_webhook_token: Optional[str] = Header(None, convert_underscores=False),
    content_type: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
):
    return await _handle_delivery(request, x_webhook_token, content_type, token)

# Root-level alias for providers that post to /status (your logs showed this 404)
@router_root.post("/status")
async def delivery_webhook_root_status(
    request: Request,
    x_webhook_token: Optional[str] = Header(None, convert_underscores=False),
    content_type: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
):
    return await _handle_delivery(request, x_webhook_token, content_type, token)
