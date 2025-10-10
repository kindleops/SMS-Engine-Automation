# sms/webhooks/delivery.py
from __future__ import annotations

import os, re, json, traceback
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, Request, Header

# ----- Optional Redis backends -----
try:
    import redis as _redis  # TCP redis / Upstash Redis (TCP)
except Exception:
    _redis = None

try:
    import requests  # Upstash REST fallback
except Exception:
    requests = None

# ----- Airtable clients (Table primary, Api fallback) -----
try:
    from pyairtable import Table, Api
except Exception:
    Table = None
    Api = None

# Optional number pool helpers (if you already have these)
try:
    from sms.number_pools import increment_delivered, increment_failed
except Exception:
    increment_delivered = None
    increment_failed = None

router = APIRouter(prefix="/delivery", tags=["Delivery"])

# =========================
# ENV / CONFIG
# =========================
AIRTABLE_API_KEY          = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE         = os.getenv("LEADS_CONVOS_BASE")
CAMPAIGN_CONTROL_BASE     = os.getenv("CAMPAIGN_CONTROL_BASE")

CONVERSATIONS_TABLE_NAME  = os.getenv("CONVERSATIONS_TABLE", "Conversations")
DRIP_QUEUE_TABLE_NAME     = os.getenv("DRIP_QUEUE_TABLE", "Drip Queue")
NUMBERS_TABLE_NAME        = os.getenv("NUMBERS_TABLE", "Numbers")

# Conversations field names (from your .env)
CONV_FROM_FIELD           = os.getenv("CONV_FROM_FIELD", "phone")
CONV_TO_FIELD             = os.getenv("CONV_TO_FIELD", "to_number")
CONV_MESSAGE_FIELD        = os.getenv("CONV_MESSAGE_FIELD", "message")
CONV_STATUS_FIELD         = os.getenv("CONV_STATUS_FIELD", "status")
CONV_DIRECTION_FIELD      = os.getenv("CONV_DIRECTION_FIELD", "direction")
CONV_TEXTGRID_ID_FIELD    = os.getenv("CONV_TEXTGRID_ID_FIELD", "TextGrid ID")
CONV_SENT_AT_FIELD        = os.getenv("CONV_SENT_AT_FIELD", "sent_at")
CONV_RECEIVED_AT_FIELD    = os.getenv("CONV_RECEIVED_AT_FIELD", "received_at")
CONV_PROCESSED_BY_FIELD   = os.getenv("CONV_PROCESSED_BY_FIELD", "processed_by")
CONV_INTENT_FIELD         = os.getenv("CONV_INTENT_FIELD", "intent_detected")

# Optional shared secret (accept either)
WEBHOOK_TOKEN             = os.getenv("WEBHOOK_TOKEN") or os.getenv("CRON_TOKEN") or os.getenv("TEXTGRID_AUTH_TOKEN")

# Redis / Upstash
REDIS_URL                 = os.getenv("REDIS_URL") or os.getenv("UPSTASH_REDIS_URL")
REDIS_TLS                 = os.getenv("REDIS_TLS", "true").lower() in ("1","true","yes")

UPSTASH_REST_URL          = os.getenv("UPSTASH_REDIS_REST_URL")
UPSTASH_REST_TOKEN        = os.getenv("UPSTASH_REDIS_REST_TOKEN") or os.getenv("UPSTASH_REDIS_REST_TOKEN".lower())

# =========================
# Small helpers
# =========================
def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _digits_only(v: Any) -> Optional[str]:
    if not isinstance(v, str):
        return None
    ds = "".join(re.findall(r"\d+", v))
    return ds if len(ds) >= 10 else None

def _norm(s: Any) -> str:
    return re.sub(r"[^a-z0-9]+","", s.strip().lower()) if isinstance(s,str) else str(s)

def _get_table(base: str, table: str):
    """Returns a table handle using Table if available, else Api.table()."""
    if not (AIRTABLE_API_KEY and base):
        return None
    try:
        if Table:
            return Table(AIRTABLE_API_KEY, base, table)
        if Api:
            return Api(AIRTABLE_API_KEY).table(base, table)
        return None
    except Exception:
        traceback.print_exc()
        return None

def _auto_field_map(tbl) -> Dict[str,str]:
    try:
        one = tbl.all(max_records=1)
        keys = list(one[0].get("fields", {}).keys()) if one else []
    except Exception:
        keys = []
    return {_norm(k): k for k in keys}

def _remap_existing_only(tbl, payload: Dict) -> Dict:
    amap = _auto_field_map(tbl)
    if not amap:
        return dict(payload)
    out = {}
    for k, v in payload.items():
        mk = amap.get(_norm(k))
        if mk:
            out[mk] = v
    return out

def _safe_update(tbl, rec_id: str, patch: Dict) -> Optional[Dict]:
    try:
        data = _remap_existing_only(tbl, patch)
        return tbl.update(rec_id, data) if data else None
    except Exception:
        traceback.print_exc()
        return None

def _safe_create(tbl, payload: Dict) -> Optional[Dict]:
    try:
        data = _remap_existing_only(tbl, payload)
        return tbl.create(data) if data else None
    except Exception:
        traceback.print_exc()
        return None

# =========================
# Idempotency store
# =========================
class IdemStore:
    """Use redis-py if REDIS_URL set; else Upstash REST; else in-memory."""
    def __init__(self):
        self.r = None
        self.rest = bool(UPSTASH_REST_URL and UPSTASH_REST_TOKEN and requests)
        if REDIS_URL and _redis:
            try:
                self.r = _redis.from_url(REDIS_URL, ssl=REDIS_TLS, decode_responses=True)
            except Exception:
                traceback.print_exc()
                self.r = None
        self._mem = set()

    def _rest_set_nx(self, key: str, ttl_sec: int = 6*60*60) -> bool:
        """
        Upstash REST: SET key value EX ttl NX
        Returns True if created (i.e., not seen), False if already exists.
        """
        if not self.rest:
            return True
        try:
            resp = requests.post(
                UPSTASH_REST_URL,
                headers={"Authorization": f"Bearer {UPSTASH_REST_TOKEN}"},
                json={"command": ["SET", key, "1", "EX", str(ttl_sec), "NX"]},
                timeout=5,
            )
            # Upstash returns {"result":"OK"} when set; null/None otherwise
            data = resp.json() if resp.ok else {}
            return bool(data.get("result") == "OK")
        except Exception:
            # If REST hiccups, fail open (treat as new) to avoid wedging processing
            traceback.print_exc()
            return True

    def seen(self, sid: Optional[str]) -> bool:
        if not sid:
            return False
        key = f"dlv:sid:{sid}"

        # Redis TCP
        if self.r:
            try:
                ok = self.r.set(key, "1", nx=True, ex=6*60*60)
                # ok=True means we just created (not seen); None means already existed
                return not bool(ok)
            except Exception:
                traceback.print_exc()

        # Upstash REST
        if self.rest:
            created = self._rest_set_nx(key)
            return not created

        # Local fallback (per-process)
        if key in self._mem:
            return True
        self._mem.add(key)
        return False

IDEM = IdemStore()

# =========================
# Numbers helpers
# =========================
def _find_numbers_row_by_did(did: str) -> Optional[Dict]:
    if not did:
        return None
    nums = _get_table(CAMPAIGN_CONTROL_BASE, NUMBERS_TABLE_NAME)
    if not nums:
        return None
    try:
        for r in nums.all():
            f = r.get("fields", {})
            if _digits_only(f.get("Number")) == _digits_only(did):
                return {"tbl": nums, "row": r}
            if _digits_only(f.get("Friendly Name")) == _digits_only(did):
                return {"tbl": nums, "row": r}
        return None
    except Exception:
        traceback.print_exc()
        return None

def _bump_numbers_counters(did: str, delivered: bool):
    # Prefer your pooled helpers if available
    try:
        if delivered and increment_delivered:
            increment_delivered(did); return
        if (not delivered) and increment_failed:
            increment_failed(did); return
    except Exception:
        traceback.print_exc()

    # Fallback: write to Airtable
    hit = _find_numbers_row_by_did(did)
    if not hit:
        return
    tbl, row = hit["tbl"], hit["row"]
    f = row.get("fields", {})
    patch = {}
    if delivered:
        patch["Delivered Today"] = int(f.get("Delivered Today") or 0) + 1
        patch["Delivered Total"] = int(f.get("Delivered Total") or 0) + 1
    else:
        patch["Failed Today"] = int(f.get("Failed Today") or 0) + 1
        patch["Failed Total"] = int(f.get("Failed Total") or 0) + 1
    _safe_update(tbl, row["id"], patch)

# =========================
# Drip Queue / Conversations
# =========================
def _update_drip_queue_by_sid(sid: str, status: str, error: Optional[str], from_did: Optional[str], to_phone: Optional[str]):
    dq = _get_table(LEADS_CONVOS_BASE, DRIP_QUEUE_TABLE_NAME)
    if not dq or not sid:
        return
    try:
        rows = dq.all()
        target = None

        # 1) Exact SID match (common columns)
        sid_keys = ("TextGrid ID","textgrid_id","Message SID","message_sid","SID")
        for r in rows:
            f = r.get("fields", {})
            if any(str(f.get(k) or "") == str(sid) for k in sid_keys):
                target = r
                break

        # 2) Fallback: most recent SENDING/QUEUED for same phone/from combo
        if not target and (to_phone or from_did):
            cand = []
            for r in rows:
                f = r.get("fields", {})
                st = str(f.get("status") or f.get("Status") or "")
                if st not in ("SENDING","QUEUED","SENT"):
                    continue
                ph = f.get("phone") or f.get("Phone")
                fd = f.get("from_number") or f.get("From Number")
                if to_phone and _digits_only(ph) != _digits_only(to_phone):
                    continue
                if from_did and _digits_only(fd) != _digits_only(from_did):
                    continue
                cand.append(r)
            if cand:
                def _when(r):
                    f = r.get("fields", {})
                    return f.get("sent_at") or f.get("next_send_date") or f.get("created_at") or ""
                cand.sort(key=_when, reverse=True)
                target = cand[0]

        if not target:
            return

        patch = {}
        if status == "delivered":
            patch.update({"status": "DELIVERED", "delivered_at": utcnow_iso(), "UI": "✅"})
        elif status in {"failed","undeliverable","undelivered","rejected","blocked","expired","error"}:
            patch.update({"status": "FAILED", "last_error": (error or status)[:500], "UI": "❌"})
        else:
            # carrier accepted / enroute → treat as SENT
            patch.update({"status": "SENT", "sent_at": target.get("fields", {}).get("sent_at") or utcnow_iso(), "UI": "✅"})

        # Store the SID if the row doesn’t have one yet
        if sid and not any(k in target.get("fields", {}) for k in sid_keys):
            patch["TextGrid ID"] = sid

        _safe_update(dq, target["id"], patch)
    except Exception:
        traceback.print_exc()

def _update_conversation_by_sid(sid: str, status: str, error: Optional[str]):
    conv = _get_table(LEADS_CONVOS_BASE, CONVERSATIONS_TABLE_NAME)
    if not conv or not sid:
        return
    try:
        rows = conv.all()
        target = None

        # Match by your configured field first
        for r in rows:
            f = r.get("fields", {})
            if str(f.get(CONV_TEXTGRID_ID_FIELD) or "") == str(sid):
                target = r
                break

        # Fallback common keys
        if not target:
            for r in rows:
                f = r.get("fields", {})
                for k in ("TextGrid ID","Message SID","message_sid","SID"):
                    if str(f.get(k) or "") == str(sid):
                        target = r
                        break
                if target:
                    break

        if not target:
            return

        patch = {}
        if status == "delivered":
            patch.update({CONV_STATUS_FIELD: "DELIVERED", "delivered_at": utcnow_iso()})
        elif status in {"failed","undeliverable","undelivered","rejected","blocked","expired","error"}:
            patch.update({CONV_STATUS_FIELD: "FAILED", "last_error": (error or status)[:500]})
        else:
            patch.update({CONV_STATUS_FIELD: "SENT", "sent_at": target.get("fields", {}).get("sent_at") or utcnow_iso()})

        _safe_update(conv, target["id"], patch)
    except Exception:
        traceback.print_exc()

# =========================
# Provider-agnostic parser
# =========================
def _extract_payload(req_body: Any, headers: Dict[str, str]) -> Dict[str, Any]:
    """
    Accepts JSON or x-www-form-urlencoded and normalizes:
    - sid, status, from, to, error
    """
    def lowerize(d: Dict[str, Any]) -> Dict[str, Any]:
        return {str(k).lower(): v for k, v in d.items()}

    def pick(d: Dict[str, Any], *keys):
        for k in keys:
            lk = k.lower()
            if lk in d:
                return d[lk]
        return None

    data = {}
    if isinstance(req_body, dict):
        data = {**req_body}
    elif isinstance(req_body, str):
        try:
            data = json.loads(req_body)
        except Exception:
            data = {}
    ld = lowerize(data)

    msg_sid = pick(ld, "message_sid", "messagesid", "sid", "messageid", "id")
    status  = (pick(ld, "message_status", "messagestatus", "status", "delivery_status", "eventtype") or "").lower()
    from_n  = pick(ld, "from", "sender", "source")
    to_n    = pick(ld, "to", "destination", "recipient")
    err     = pick(ld, "error_message", "errormessage", "error", "reason")

    # normalize status
    if status in {"delivered", "success", "delivrd"}:
        norm = "delivered"
    elif status in {"failed","undelivered","undeliverable","rejected","blocked","expired","error"}:
        norm = "failed"
    else:
        norm = "sent"

    provider = headers.get("x-provider") or headers.get("user-agent") or "unknown"

    return {"sid": msg_sid, "status": norm, "from": from_n, "to": to_n, "error": err, "provider": provider}

# =========================
# Route
# =========================
@router.post("")
async def delivery_webhook(
    request: Request,
    x_webhook_token: Optional[str] = Header(None, convert_underscores=False),
    x_provider: Optional[str] = Header(None),
    content_type: Optional[str] = Header(None),
):
    """
    Delivery receipts handler:
    - Idempotent on message SID (Redis TCP or Upstash REST; local fallback)
    - Increments Numbers counters (Delivered/Failed Today & Total)
    - Updates Drip Queue + Conversations status/UI
    """
    # 1) Auth (optional but recommended)
    if WEBHOOK_TOKEN and x_webhook_token and x_webhook_token != WEBHOOK_TOKEN:
        return {"ok": False, "error": "unauthorized"}

    # 2) Parse body (json or form)
    try:
        body: Dict[str, Any] = {}
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
        return {"ok": False, "error": "invalid payload"}

    parsed = _extract_payload(body, dict(request.headers))
    sid, status, from_d, to_p, err = (
        parsed.get("sid"),
        parsed.get("status") or "sent",
        parsed.get("from"),
        parsed.get("to"),
        parsed.get("error"),
    )

    ts = utcnow_iso()
    print(f"📡 Delivery receipt | {ts} | from={from_d} → {status} | SID={sid} | provider={parsed.get('provider')}")

    # 3) Idempotency: ignore duplicate SIDs
    if sid and IDEM.seen(sid):
        return {"ok": True, "status": status, "sid": sid, "note": "duplicate ignored"}

    # 4) Update Numbers counters
    try:
        if status == "delivered":
            _bump_numbers_counters(from_d or "", delivered=True)
        elif status == "failed":
            _bump_numbers_counters(from_d or "", delivered=False)
    except Exception:
        traceback.print_exc()

    # 5) Update Drip Queue + Conversations
    try:
        _update_drip_queue_by_sid(sid or "", status, err, from_d, to_p)
        _update_conversation_by_sid(sid or "", status, err)
    except Exception:
        traceback.print_exc()

    return {"ok": True, "status": status, "sid": sid}
