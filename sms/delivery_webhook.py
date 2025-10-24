# sms/webhooks/delivery.py
"""
Optimized Delivery Webhook
--------------------------
Handles provider delivery receipts (TextGrid / Twilio / others)
→ Updates Drip Queue + Conversations using datastore.py
→ Increments Numbers table counters
→ Deduplicates via Redis / Upstash idempotency
"""

from __future__ import annotations

import os, re, json, traceback, asyncio
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, Request, Header, HTTPException
from sms.datastore import CONNECTOR, update_record, list_records
from sms.runtime import get_logger
from sms.inbound_webhook import normalize_e164

# Optional Redis / Upstash
try:
    import redis as _redis
except Exception:
    _redis = None
try:
    import requests
except Exception:
    requests = None

# Optional number counters
try:
    from sms.number_pools import increment_delivered, increment_failed
except Exception:
    increment_delivered = None
    increment_failed = None

# ---------------------------------------------------------------------------
# ENVIRONMENT
# ---------------------------------------------------------------------------

logger = get_logger(__name__)
router = APIRouter(prefix="/delivery", tags=["Delivery"])

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE")
CAMPAIGN_CONTROL_BASE = os.getenv("CAMPAIGN_CONTROL_BASE")
WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN") or os.getenv("CRON_TOKEN") or os.getenv("TEXTGRID_AUTH_TOKEN")

REDIS_URL = os.getenv("REDIS_URL") or os.getenv("UPSTASH_REDIS_URL")
REDIS_TLS = os.getenv("REDIS_TLS", "true").lower() in {"1", "true", "yes"}
UPSTASH_REST_URL = os.getenv("UPSTASH_REDIS_REST_URL")
UPSTASH_REST_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN") or os.getenv("upstash_redis_rest_token")


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
    """Increment counters on the Numbers table."""
    try:
        if delivered and increment_delivered:
            return increment_delivered(did)
        if not delivered and increment_failed:
            return increment_failed(did)
    except Exception:
        traceback.print_exc()

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
            update_record(handle, r["id"], patch)
            break


# ---------------------------------------------------------------------------
# AIRTABLE UPDATES
# ---------------------------------------------------------------------------


async def _update_airtable_status(sid: str, status: str, error: Optional[str], from_did: str, to_phone: str):
    """Update Drip Queue + Conversations records via datastore."""
    dq_handle = CONNECTOR.drip_queue()
    conv_handle = CONNECTOR.conversations()

    now = utcnow_iso()
    status_map = {
        "delivered": {"Status": "DELIVERED", "Delivered At": now, "UI": "✅"},
        "failed": {"Status": "FAILED", "Last Error": (error or "failed")[:500], "UI": "❌"},
        "undelivered": {"Status": "FAILED", "Last Error": (error or "undelivered")[:500], "UI": "❌"},
        "queued": {"Status": "QUEUED", "UI": "⏳"},
        "optout": {"Status": "OPTOUT", "UI": "🚫"},
    }
    patch = status_map.get(status, {"Status": "SENT", "Sent At": now, "UI": "✅"})

    try:
        for handle in (dq_handle, conv_handle):
            records = list_records(handle, formula=f"{{TextGrid ID}}='{sid}'", max_records=1)
            if records:
                update_record(handle, records[0]["id"], patch)
    except Exception:
        traceback.print_exc()


# ---------------------------------------------------------------------------
# PAYLOAD PARSER
# ---------------------------------------------------------------------------


def _extract_payload(req_body: Any, headers: Dict[str, str]) -> Dict[str, Any]:
    """Normalize JSON or form body from any provider."""

    def lower(d):
        return {k.lower(): v for k, v in d.items()}

    def pick(d, *keys):
        for k in keys:
            if k.lower() in d:
                return d[k.lower()]
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
    status = (pick(d, "message_status", "status", "delivery_status", "eventtype") or "").lower()
    from_n = pick(d, "from", "sender", "source")
    to_n = pick(d, "to", "recipient", "destination")
    error = pick(d, "error_message", "error", "reason")

    delivered = {"delivered", "success", "delivrd"}
    failed = {"failed"}
    undelivered = {"undelivered", "rejected", "blocked", "expired", "error"}
    queued = {"queued", "accepted", "pending"}
    optout = {"optout", "opt-out"}

    if status in delivered:
        norm = "delivered"
    elif status in optout:
        norm = "optout"
    elif status in queued:
        norm = "queued"
    elif status in undelivered:
        norm = "undelivered"
    elif status in failed:
        norm = "failed"
    else:
        norm = "sent"

    provider = headers.get("x-provider") or headers.get("user-agent") or "unknown"
    return {"sid": sid, "status": norm, "from": from_n, "to": to_n, "error": error, "provider": provider}


# ---------------------------------------------------------------------------
# MAIN ROUTE
# ---------------------------------------------------------------------------


@router.post("")
async def delivery_webhook(
    request: Request,
    x_webhook_token: Optional[str] = Header(None, convert_underscores=False),
    content_type: Optional[str] = Header(None),
):
    """Handles message delivery receipts from any provider."""
    if WEBHOOK_TOKEN and x_webhook_token and x_webhook_token != WEBHOOK_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Parse body safely
    try:
        if content_type and "application/x-www-form-urlencoded" in content_type.lower():
            form = await request.form()
            body = dict(form)
        else:
            try:
                body = await request.json()
            except Exception:
                form = await request.form()
                body = dict(form)
    except Exception:
        traceback.print_exc()
        raise HTTPException(status_code=422, detail="Invalid payload")

    parsed = _extract_payload(body, dict(request.headers))
    sid, status, from_d, to_p, err = parsed["sid"], parsed["status"], parsed["from"], parsed["to"], parsed["error"]

    if not all([sid, status, from_d, to_p]):
        raise HTTPException(status_code=422, detail="Missing required fields")

    from_norm = normalize_e164(from_d, field="From")
    to_norm = normalize_e164(to_p, field="To")
    logger.info(f"📡 Delivery webhook | {status.upper()} | SID={sid} | from={from_norm} → {to_norm}")

    # Idempotency
    if IDEM.seen(sid):
        return {"status": "ok", "sid": sid, "note": "duplicate"}

    # Update Numbers
    try:
        if status == "delivered":
            _bump_numbers(from_norm, True)
        elif status in {"failed", "undelivered"}:
            _bump_numbers(from_norm, False)
    except Exception:
        traceback.print_exc()

    # Update Airtable asynchronously
    asyncio.create_task(_update_airtable_status(sid, status, err, from_norm, to_norm))

    return {"status": "ok", "sid": sid, "normalized": status}
