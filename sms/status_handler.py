# sms/status_handler.py
from __future__ import annotations

import os, re, traceback, hashlib
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, Request, Header

# ---------------- optional deps ----------------
try:
    from pyairtable import Table as _AirTable
except Exception:
    _AirTable = None  # ensures no NameError if pyairtable is missing

try:
    import redis as _redis  # TCP client (best)
except Exception:
    _redis = None

try:
    import requests  # Upstash REST fallback
except Exception:
    requests = None

router = APIRouter(tags=["Status"])

# ---------------- env / config ----------------
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE")
TEMPLATES_TABLE_NAME = os.getenv("TEMPLATES_TABLE", "Templates")
CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")

# Template KPI fields (override via .env if your field names differ)
TEMPLATE_DELIVERED_FIELD = os.getenv("TEMPLATE_DELIVERED_FIELD", "Delivered")
TEMPLATE_FAILED_FIELD = os.getenv("TEMPLATE_FAILED_FIELD", "Failed Deliveries")

# Optional shared secret (header: X-Webhook-Token)
WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN") or os.getenv("CRON_TOKEN") or os.getenv("TEXTGRID_AUTH_TOKEN")

# Redis / Upstash for idempotency
REDIS_URL = os.getenv("REDIS_URL") or os.getenv("UPSTASH_REDIS_URL")
REDIS_TLS = os.getenv("REDIS_TLS", "true").lower() in ("1", "true", "yes")
UPSTASH_REDIS_REST_URL = os.getenv("UPSTASH_REDIS_REST_URL")  # e.g. https://xxxxx.upstash.io
UPSTASH_REDIS_REST_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN")

KEY_PREFIX = os.getenv("RATE_LIMIT_KEY_PREFIX", "sms")  # reuse prefix to namespace keys


# ---------------- small helpers ----------------
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _norm(s: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", s.strip().lower()) if isinstance(s, str) else str(s)


def _digits_only(v: Any) -> Optional[str]:
    if not isinstance(v, str):
        return None
    ds = "".join(re.findall(r"\d+", v))
    return ds if len(ds) >= 10 else None


def _get_table(base: str, table: str):
    """
    Return a live Airtable table instance or None (MOCK mode) if pyairtable
    isn't installed or env is missing. Never raises NameError on Table.
    """
    if not (AIRTABLE_API_KEY and base and _AirTable):
        return None
    try:
        return _AirTable(AIRTABLE_API_KEY, base, table)
    except Exception:
        traceback.print_exc()
        return None


def _auto_field_map(tbl) -> Dict[str, str]:
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


def _safe_update(tbl, rec_id: str, patch: Dict):
    try:
        data = _remap_existing_only(tbl, patch)
        return tbl.update(rec_id, data) if data else None
    except Exception:
        traceback.print_exc()
        return None


def _safe_get(tbl, rec_id: str):
    try:
        return tbl.get(rec_id)
    except Exception:
        traceback.print_exc()
        return None


def _increment_numeric(tbl, rec_id: str, field_name: str, by: int = 1) -> bool:
    """
    Airtable has no atomic increment. Read → add → write (best effort).
    We only attempt if the field exists on the table.
    """
    try:
        amap = _auto_field_map(tbl)
        real = amap.get(_norm(field_name))
        if not real:
            return False
        row = _safe_get(tbl, rec_id)
        cur = row.get("fields", {}).get(real, 0) if row else 0
        try:
            cur = int(cur)
        except Exception:
            cur = 0
        return bool(_safe_update(tbl, rec_id, {real: cur + int(by)}))
    except Exception:
        traceback.print_exc()
        return False


# ---------------- idempotency store (sid) ----------------
class _Idem:
    def __init__(self):
        self.r = None
        if REDIS_URL and _redis:
            try:
                self.r = _redis.from_url(REDIS_URL, ssl=REDIS_TLS, decode_responses=True, socket_timeout=3)
            except Exception:
                traceback.print_exc()
                self.r = None
        self._mem = set()

    @staticmethod
    def _key(sid: str) -> str:
        h = hashlib.md5((sid or "").encode()).hexdigest()
        return f"{KEY_PREFIX}:status:sid:{h}"

    def seen(self, sid: Optional[str]) -> bool:
        if not sid:
            return False
        key = self._key(sid)
        # Prefer Redis TCP
        if self.r:
            try:
                with self.r.pipeline() as p:
                    p.setnx(key, "1")
                    p.expire(key, 6 * 60 * 60)
                    rv = p.execute()
                setnx_ok = bool(rv and rv[0])
                return not setnx_ok  # if already present → duplicate
            except Exception:
                traceback.print_exc()
        # Upstash REST fallback
        if UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN and requests:
            try:
                # Check
                g = requests.post(
                    f"{UPSTASH_REDIS_REST_URL}/get/{key}", headers={"Authorization": f"Bearer {UPSTASH_REDIS_REST_TOKEN}"}, timeout=2
                )
                if g.ok and g.json().get("result") is not None:
                    return True
                # Set + expire
                requests.post(
                    f"{UPSTASH_REDIS_REST_URL}/pipeline",
                    json=[["SETNX", key, "1"], ["EXPIRE", key, "21600"]],
                    headers={"Authorization": f"Bearer {UPSTASH_REDIS_REST_TOKEN}"},
                    timeout=2,
                )
            except Exception:
                traceback.print_exc()
                # fall through to memory
        # memory fallback (process-local)
        if key in self._mem:
            return True
        self._mem.add(key)
        return False


IDEM = _Idem()


# ---------------- parser ----------------
def _extract_payload(data: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize keys across providers."""

    def pick(d: Dict, *keys):
        for k in keys:
            if k in d:
                return d[k]
        return None

    sid = pick(data, "sid", "message_sid", "MessageSid", "id")
    status = str(pick(data, "status", "message_status", "MessageStatus") or "").lower()
    template_id = pick(data, "template_id", "template", "TemplateId")

    # Normalize status
    if status in {"delivered", "success", "delivrd"}:
        norm = "delivered"
    elif status in {"failed", "undelivered", "undeliverable", "rejected", "blocked", "expired", "error"}:
        norm = "failed"
    else:
        norm = status or "sent"

    return {"sid": sid, "status": norm, "template_id": template_id}


# ---------------- template resolver (fallback from Conversations) ----------------
def _resolve_template_from_convos(sid: Optional[str]) -> Optional[str]:
    """If template_id missing, try to find it on Conversations by SID."""
    if not sid or not (AIRTABLE_API_KEY and LEADS_CONVOS_BASE and _AirTable):
        return None
    try:
        convos = _get_table(LEADS_CONVOS_BASE, CONVERSATIONS_TABLE)
        if not convos:
            return None
        rows = convos.all()
        for r in rows:
            f = r.get("fields", {})
            # common sid fields
            for k in ("TextGrid ID", "Message SID", "message_sid", "SID"):
                if str(f.get(k) or "") == str(sid):
                    # template_id might be stored as a text or linked id
                    t = f.get("template_id") or f.get("Template") or f.get("template")
                    if isinstance(t, list) and t:
                        return t[0]
                    if isinstance(t, str) and t.strip():
                        return t
                    return None
        return None
    except Exception:
        traceback.print_exc()
        return None


# ---------------- KPI logger ----------------
def log_template_kpi(template_id: str, delivered: bool) -> None:
    if not template_id or not (AIRTABLE_API_KEY and LEADS_CONVOS_BASE and _AirTable):
        print(f"[MOCK] Template KPI {'delivered' if delivered else 'failed'} for {template_id}")
        return
    tbl = _get_table(LEADS_CONVOS_BASE, TEMPLATES_TABLE_NAME)
    if not tbl:
        print("⚠️ Templates table unavailable")
        return
    field = TEMPLATE_DELIVERED_FIELD if delivered else TEMPLATE_FAILED_FIELD
    ok = _increment_numeric(tbl, template_id, field, by=1)
    if ok:
        print(f"📊 Template {template_id} KPI incremented → {field}")
    else:
        print(f"⚠️ KPI field '{field}' not found on Templates; no update performed")


# ---------------- route ----------------
@router.post("/status")
async def delivery_status(
    req: Request,
    x_webhook_token: Optional[str] = Header(None, convert_underscores=False),
):
    """
    Provider-agnostic delivery KPI webhook.
    Expected payload (flexible keys):
      {
        "sid": "SM123...",
        "status": "delivered|failed|undeliverable|...",
        "template_id": "recXXXX..."   # optional; we attempt to resolve from Conversations if absent
      }
    """
    # Auth (optional)
    if WEBHOOK_TOKEN and x_webhook_token and x_webhook_token != WEBHOOK_TOKEN:
        return {"ok": False, "error": "unauthorized"}

    # Parse body (JSON or form)
    try:
        try:
            data = await req.json()
            if not isinstance(data, dict):
                data = {}
        except Exception:
            form = await req.form()
            data = dict(form)
    except Exception:
        traceback.print_exc()
        data = {}

    parsed = _extract_payload(data)
    sid = parsed.get("sid")
    status = parsed.get("status")
    template_id = parsed.get("template_id")

    print(f"📡 Status webhook | sid={sid or 'N/A'} | status={status or 'N/A'}")

    # Idempotency: ignore duplicate SIDs
    if IDEM.seen(sid):
        return {"ok": True, "sid": sid, "status": status, "note": "duplicate ignored"}

    # If template_id missing, try resolving from Conversations
    if not template_id:
        try:
            template_id = _resolve_template_from_convos(sid)
        except Exception:
            traceback.print_exc()

    # Update template KPIs
    try:
        if status == "delivered":
            log_template_kpi(template_id, delivered=True)
        elif status == "failed":
            log_template_kpi(template_id, delivered=False)
        else:
            # No KPI change for intermediate states
            pass
    except Exception:
        traceback.print_exc()

    return {"ok": True, "sid": sid, "status": status, "template_id": template_id}
