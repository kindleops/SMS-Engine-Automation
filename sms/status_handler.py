"""
📦 TextGrid Delivery Status Handler (v2.1 – Spec-Aligned)
---------------------------------------------------------
Unified webhook that logs delivery/failure KPIs safely into Airtable.
Fully idempotent, provider-agnostic, and telemetry-rich.
"""

from __future__ import annotations
import os, re, traceback, hashlib
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from fastapi import APIRouter, Request, Header

# ────────────────────────────────────────────────
# Optional deps
# ────────────────────────────────────────────────
try:
    from pyairtable import Table as _AirTable
except ImportError:
    _AirTable = None

try:
    import redis as _redis
except ImportError:
    _redis = None

try:
    import requests
except ImportError:
    requests = None

# ────────────────────────────────────────────────
# Internal imports (shared spec)
# ────────────────────────────────────────────────
try:
    from sms.spec import normalize_delivery_status
except ImportError:
    def normalize_delivery_status(v: str) -> str:
        return str(v or "").upper()

# ────────────────────────────────────────────────
# Router & ENV
# ────────────────────────────────────────────────
router = APIRouter(tags=["Status"])

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE")
TEMPLATES_TABLE_NAME = os.getenv("TEMPLATES_TABLE", "Templates")
CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")

TEMPLATE_DELIVERED_FIELD = os.getenv("TEMPLATE_DELIVERED_FIELD", "Delivered")
TEMPLATE_FAILED_FIELD = os.getenv("TEMPLATE_FAILED_FIELD", "Failed Deliveries")

WEBHOOK_TOKEN = (
    os.getenv("WEBHOOK_TOKEN")
    or os.getenv("TEXTGRID_AUTH_TOKEN")
    or os.getenv("CRON_TOKEN")
)

REDIS_URL = os.getenv("REDIS_URL") or os.getenv("UPSTASH_REDIS_URL")
REDIS_TLS = os.getenv("REDIS_TLS", "true").lower() in ("1", "true", "yes")
UPSTASH_REDIS_REST_URL = os.getenv("UPSTASH_REDIS_REST_URL")
UPSTASH_REDIS_REST_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN")

KEY_PREFIX = os.getenv("RATE_LIMIT_KEY_PREFIX", "sms")

# ────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def _norm(s: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s).lower().strip()) if s else ""

def _get_table(base: str, name: str):
    if not (AIRTABLE_API_KEY and base and _AirTable):
        return None
    try:
        return _AirTable(AIRTABLE_API_KEY, base, name)
    except Exception:
        traceback.print_exc()
        return None

def _auto_field_map(tbl) -> Dict[str, str]:
    try:
        rec = tbl.all(max_records=1)
        keys = list(rec[0].get("fields", {}).keys()) if rec else []
    except Exception:
        keys = []
    return {_norm(k): k for k in keys}

def _remap_existing_only(tbl, patch: Dict) -> Dict:
    amap = _auto_field_map(tbl)
    return {amap.get(_norm(k), k): v for k, v in patch.items() if amap.get(_norm(k))} if amap else patch

def _safe_update(tbl, rec_id: str, patch: Dict):
    try:
        data = _remap_existing_only(tbl, patch)
        if data:
            return tbl.update(rec_id, data)
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
    """Airtable-safe numeric increment."""
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
        return bool(_safe_update(tbl, rec_id, {real: cur + by}))
    except Exception:
        traceback.print_exc()
        return False

# ────────────────────────────────────────────────
# Idempotency layer
# ────────────────────────────────────────────────
class _Idempotency:
    def __init__(self):
        self.mem = set()
        self.r = None
        if REDIS_URL and _redis:
            try:
                self.r = _redis.from_url(
                    REDIS_URL, ssl=REDIS_TLS, decode_responses=True, socket_timeout=3
                )
                print("✅ Redis connected for status handler")
            except Exception:
                traceback.print_exc()
                self.r = None

    @staticmethod
    def _key(sid: str) -> str:
        h = hashlib.md5((sid or "").encode()).hexdigest()
        return f"{KEY_PREFIX}:status:{h}"

    def seen(self, sid: Optional[str]) -> bool:
        """Return True if duplicate (already processed)."""
        if not sid:
            return False
        key = self._key(sid)

        # Redis direct
        if self.r:
            try:
                with self.r.pipeline() as p:
                    p.setnx(key, "1")
                    p.expire(key, 21600)  # 6h
                    rv = p.execute()
                return not bool(rv and rv[0])
            except Exception:
                print("⚠️ Redis error, falling back to memory cache")
                traceback.print_exc()

        # Upstash REST
        if UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN and requests:
            try:
                g = requests.post(
                    f"{UPSTASH_REDIS_REST_URL}/get/{key}",
                    headers={"Authorization": f"Bearer {UPSTASH_REDIS_REST_TOKEN}"},
                    timeout=2,
                )
                if g.ok and g.json().get("result") is not None:
                    return True
                requests.post(
                    f"{UPSTASH_REDIS_REST_URL}/pipeline",
                    json=[["SETNX", key, "1"], ["EXPIRE", key, "21600"]],
                    headers={"Authorization": f"Bearer {UPSTASH_REDIS_REST_TOKEN}"},
                    timeout=2,
                )
                return False
            except Exception:
                print("⚠️ Upstash fallback error")
                traceback.print_exc()

        # In-memory fallback
        if key in self.mem:
            return True
        self.mem.add(key)
        return False

IDEM = _Idempotency()

# ────────────────────────────────────────────────
# Payload extraction
# ────────────────────────────────────────────────
def _extract_payload(data: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize across Twilio, TextGrid, Telnyx, etc."""
    def pick(d, *keys): return next((d[k] for k in keys if k in d), None)
    sid = pick(data, "sid", "message_sid", "MessageSid", "id")
    status = str(pick(data, "status", "message_status", "MessageStatus") or "").lower()
    template_id = pick(data, "template_id", "Template", "TemplateId", "template")
    provider = pick(data, "provider", "source") or "textgrid"
    return {"sid": sid, "status": status, "template_id": template_id, "provider": provider}

# ────────────────────────────────────────────────
# Template fallback lookup
# ────────────────────────────────────────────────
def _resolve_template_from_convos(sid: Optional[str]) -> Optional[str]:
    if not sid or not (AIRTABLE_API_KEY and LEADS_CONVOS_BASE and _AirTable):
        return None
    try:
        convos = _get_table(LEADS_CONVOS_BASE, CONVERSATIONS_TABLE)
        if not convos:
            return None
        rows = convos.all()
        for r in rows:
            f = r.get("fields", {})
            for k in ("TextGrid ID", "Message SID", "SID"):
                if str(f.get(k) or "").strip() == str(sid):
                    t = f.get("Template") or f.get("template") or f.get("template_id")
                    if isinstance(t, list) and t:
                        return t[0]
                    elif isinstance(t, str) and t.strip():
                        return t
        return None
    except Exception:
        traceback.print_exc()
        return None

# ────────────────────────────────────────────────
# KPI logger
# ────────────────────────────────────────────────
def log_template_kpi(template_id: str, delivered: bool):
    """Increment Template KPIs safely."""
    if not template_id:
        print("⚠️ Missing template_id; KPI skip")
        return
    if not (AIRTABLE_API_KEY and LEADS_CONVOS_BASE and _AirTable):
        print(f"[MOCK] Template KPI {'delivered' if delivered else 'failed'} for {template_id}")
        return
    tbl = _get_table(LEADS_CONVOS_BASE, TEMPLATES_TABLE_NAME)
    if not tbl:
        print("⚠️ Templates table unavailable")
        return
    field = TEMPLATE_DELIVERED_FIELD if delivered else TEMPLATE_FAILED_FIELD
    if _increment_numeric(tbl, template_id, field):
        print(f"📊 Template {template_id} KPI incremented → {field}")
    else:
        print(f"⚠️ Field '{field}' not found on Templates table")

# ────────────────────────────────────────────────
# Endpoint
# ────────────────────────────────────────────────
@router.post("/status")
async def delivery_status(req: Request, x_webhook_token: Optional[str] = Header(None, convert_underscores=False)):
    """Universal webhook endpoint for all SMS provider delivery receipts."""
    if WEBHOOK_TOKEN and x_webhook_token != WEBHOOK_TOKEN:
        return {"ok": False, "error": "unauthorized"}

    # Parse payload
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
    provider = parsed.get("provider") or "unknown"
    raw_status = parsed.get("status", "")
    template_id = parsed.get("template_id")

    normalized = normalize_delivery_status(raw_status)

    print(f"📡 {provider.upper()} Delivery → sid={sid or 'N/A'} | status={normalized}")

    # Idempotency
    if IDEM.seen(sid):
        return {
            "ok": True,
            "sid": sid,
            "status": normalized,
            "note": "duplicate ignored",
            "processed_at": _now_iso(),
        }

    # Resolve missing template
    if not template_id:
        template_id = _resolve_template_from_convos(sid)

    # KPI tracking
    if normalized == "DELIVERED":
        log_template_kpi(template_id, True)
    elif normalized in {"FAILED", "UNDELIVERED"}:
        log_template_kpi(template_id, False)

    return {
        "ok": True,
        "sid": sid,
        "status": normalized,
        "provider": provider,
        "template_id": template_id,
        "processed_at": _now_iso(),
    }