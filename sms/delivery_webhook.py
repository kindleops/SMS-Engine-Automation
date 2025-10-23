# sms/webhooks/delivery.py
"""
Delivery Webhook (TextGrid / Twilio / generic providers)
---------------------------------------------------------
Handles message delivery receipts:
  • Updates Drip Queue + Conversations with status
  • Increments Numbers table counters
  • Deduplicates via Redis / Upstash idempotency store
"""

from __future__ import annotations

import os, re, json, traceback
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, Request, Header, HTTPException

from sms.config import CONV_FIELDS
from sms.inbound_webhook import normalize_e164

# Optional: Redis / Upstash clients
try:
    import redis as _redis  # type: ignore
except Exception:
    _redis = None
try:
    import requests
except Exception:
    requests = None

# Airtable
try:
    from pyairtable import Table, Api
except Exception:
    Table = None
    Api = None

# Optional number pool helpers
try:
    from sms.number_pools import increment_delivered, increment_failed
except Exception:
    increment_delivered = None
    increment_failed = None

router = APIRouter(prefix="/delivery", tags=["Delivery"])

# ---------------------------------------------------------------------------
# ENVIRONMENT
# ---------------------------------------------------------------------------

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE")
CAMPAIGN_CONTROL_BASE = os.getenv("CAMPAIGN_CONTROL_BASE")

CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")
DRIP_QUEUE_TABLE = os.getenv("DRIP_QUEUE_TABLE", "Drip Queue")
NUMBERS_TABLE = os.getenv("NUMBERS_TABLE", "Numbers")

WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN") or os.getenv("CRON_TOKEN") or os.getenv("TEXTGRID_AUTH_TOKEN")

# Redis / Upstash
REDIS_URL = os.getenv("REDIS_URL") or os.getenv("UPSTASH_REDIS_URL")
REDIS_TLS = os.getenv("REDIS_TLS", "true").lower() in ("1", "true", "yes")
UPSTASH_REST_URL = os.getenv("UPSTASH_REDIS_REST_URL")
UPSTASH_REST_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN") or os.getenv("upstash_redis_rest_token")

# Conversations field map
CONV_FROM_FIELD = CONV_FIELDS["FROM"]
CONV_TO_FIELD = CONV_FIELDS["TO"]
CONV_MESSAGE_FIELD = CONV_FIELDS["BODY"]
CONV_STATUS_FIELD = CONV_FIELDS["STATUS"]
CONV_DIRECTION_FIELD = CONV_FIELDS["DIRECTION"]
CONV_TEXTGRID_ID_FIELD = CONV_FIELDS["TEXTGRID_ID"]
CONV_SENT_AT_FIELD = CONV_FIELDS["SENT_AT"]
CONV_RECEIVED_AT_FIELD = CONV_FIELDS["RECEIVED_AT"]
CONV_PROCESSED_BY_FIELD = CONV_FIELDS["PROCESSED_BY"]
CONV_INTENT_FIELD = CONV_FIELDS["INTENT"]

# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _digits_only(v: Any) -> Optional[str]:
    if not isinstance(v, str):
        return None
    ds = "".join(re.findall(r"\d+", v))
    return ds if len(ds) >= 10 else None


def _norm(s: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", s.strip().lower()) if isinstance(s, str) else str(s)


def _get_table(base: str, table: str):
    """Return Airtable handle, safe for both pyairtable v1/v2."""
    if not (AIRTABLE_API_KEY and base):
        return None
    try:
        if Table:
            return Table(AIRTABLE_API_KEY, base, table)
        if Api:
            return Api(AIRTABLE_API_KEY).table(base, table)
    except Exception:
        traceback.print_exc()
    return None


def _safe_update(tbl, rec_id: str, patch: Dict[str, Any]):
    if not (tbl and rec_id and patch):
        return None
    try:
        amap = { _norm(k): k for k in tbl.all(max_records=1)[0].get("fields", {}).keys() } if tbl else {}
        data = {amap.get(_norm(k), k): v for k, v in patch.items() if v is not None}
        return tbl.update(rec_id, data)
    except Exception:
        traceback.print_exc()
        return None


# ---------------------------------------------------------------------------
# IDEMPOTENCY STORE
# ---------------------------------------------------------------------------

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
        """Returns True if SID already seen."""
        if not sid:
            return False
        key = f"dlv:sid:{sid}"

        # TCP Redis
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

    nums = _get_table(CAMPAIGN_CONTROL_BASE, NUMBERS_TABLE)
    if not nums:
        return
    try:
        all_rows = nums.all()
        for r in all_rows:
            f = r.get("fields", {})
            if _digits_only(f.get("Number")) == _digits_only(did):
                patch = {}
                if delivered:
                    patch["Delivered Today"] = int(f.get("Delivered Today") or 0) + 1
                    patch["Delivered Total"] = int(f.get("Delivered Total") or 0) + 1
                else:
                    patch["Failed Today"] = int(f.get("Failed Today") or 0) + 1
                    patch["Failed Total"] = int(f.get("Failed Total") or 0) + 1
                _safe_update(nums, r["id"], patch)
                break
    except Exception:
        traceback.print_exc()


# ---------------------------------------------------------------------------
# DRIP QUEUE / CONVERSATIONS
# ---------------------------------------------------------------------------

def _update_airtable_status(sid: str, status: str, error: Optional[str], from_did: str, to_phone: str):
    """Update Drip Queue + Conversations based on TextGrid SID."""
    dq = _get_table(LEADS_CONVOS_BASE, DRIP_QUEUE_TABLE)
    conv = _get_table(LEADS_CONVOS_BASE, CONVERSATIONS_TABLE)
    if not dq or not conv:
        return

    # Normalize status mapping
    now = utcnow_iso()
    if status == "delivered":
        patch = {"Status": "DELIVERED", "Delivered At": now, "UI": "✅"}
    elif status in {"failed", "undelivered", "rejected", "blocked", "expired", "error"}:
        patch = {"Status": "FAILED", "Last Error": (error or status)[:500], "UI": "❌"}
    else:
        patch = {"Status": "SENT", "Sent At": now, "UI": "✅"}

    try:
        # Update Drip Queue
        dq_records = dq.all()
        for r in dq_records:
            f = r.get("fields", {})
            if str(f.get("TextGrid ID") or "") == sid or str(f.get("Message SID") or "") == sid:
                _safe_update(dq, r["id"], patch)
                break

        # Update Conversations
        conv_records = conv.all()
        for r in conv_records:
            f = r.get("fields", {})
            if str(f.get(CONV_TEXTGRID_ID_FIELD) or "") == sid:
                _safe_update(conv, r["id"], patch)
                break
    except Exception:
        traceback.print_exc()


# ---------------------------------------------------------------------------
# PAYLOAD PARSER
# ---------------------------------------------------------------------------

def _extract_payload(req_body: Any, headers: Dict[str, str]) -> Dict[str, Any]:
    """Normalize JSON or form body from any provider."""
    def lower(d): return {k.lower(): v for k, v in d.items()}
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
# ROUTE
# ---------------------------------------------------------------------------

@router.post("")
async def delivery_webhook(
    request: Request,
    x_webhook_token: Optional[str] = Header(None, convert_underscores=False),
    content_type: Optional[str] = Header(None),
):
    """Provider delivery receipts → Airtable sync."""
    # Auth
    if WEBHOOK_TOKEN and x_webhook_token and x_webhook_token != WEBHOOK_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Parse body
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
    print(f"📡 Delivery webhook | {status.upper()} | SID={sid} | from={from_norm} → {to_norm}")

    # Idempotency check
    if IDEM.seen(sid):
        return {"status": "ok", "sid": sid, "note": "duplicate"}

    # Update Numbers table
    try:
        if status == "delivered":
            _bump_numbers(from_norm, True)
        elif status in {"failed", "undelivered"}:
            _bump_numbers(from_norm, False)
    except Exception:
        traceback.print_exc()

    # Update Airtable
    try:
        _update_airtable_status(sid, status, err, from_norm, to_norm)
    except Exception:
        traceback.print_exc()

    return {"status": "ok", "sid": sid, "normalized": status}
