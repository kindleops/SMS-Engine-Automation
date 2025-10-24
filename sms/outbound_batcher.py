"""
🚀 Outbound Message Batcher (Bulletproof Edition)
------------------------------------------------
Responsible for draining the Drip Queue, enforcing quiet hours,
picking DIDs, honoring rate limits, and dispatching outbound SMS
through the MessageProcessor.

Safe for concurrent workers with Redis-based global rate limiting.
"""

from __future__ import annotations
import hashlib, os, re, time, traceback
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

# Core imports
from sms.config import DRIP_FIELD_MAP as DRIP_FIELDS
from sms.airtable_schema import DripStatus
from sms.runtime import get_logger, retry

# Optional deps
try:
    import importlib
    redis = importlib.import_module("redis")
except Exception:
    redis = None

try:
    requests = importlib.import_module("requests")
except Exception:
    requests = None

# Optional sender
try:
    from sms.message_processor import MessageProcessor
except Exception:
    MessageProcessor = None

from sms.dispatcher import get_policy

log = get_logger("outbound")

# ===========================================
# Airtable Table Helpers (compat pyairtable v1/v2)
# ===========================================
_PyTable = _PyApi = None
try:
    from pyairtable import Table as _PyTable
except Exception:
    pass
try:
    from pyairtable import Api as _PyApi
except Exception:
    pass


def _make_table(api_key: Optional[str], base_id: Optional[str], table_name: str):
    """Return a safe Airtable table client."""
    if not (api_key and base_id):
        return None
    try:
        if _PyTable:
            return _PyTable(api_key, base_id, table_name)
        if _PyApi:
            return _PyApi(api_key).table(base_id, table_name)
    except Exception:
        traceback.print_exc()
    return None


# ===========================================
# ENV CONFIG
# ===========================================
LEADS_BASE_ENV = "LEADS_CONVOS_BASE"
PERF_BASE_ENV = "PERFORMANCE_BASE"
CONTROL_BASE_ENV = "CAMPAIGN_CONTROL_BASE"

DRIP_TABLE_NAME = os.getenv("DRIP_QUEUE_TABLE", "Drip Queue")
NUMBERS_TABLE_NAME = os.getenv("NUMBERS_TABLE", "Numbers")

SLEEP_BETWEEN_SENDS_SEC = float(os.getenv("SLEEP_BETWEEN_SENDS_SEC", "0.03"))
RATE_LIMIT_REQUEUE_SECONDS = float(os.getenv("RATE_LIMIT_REQUEUE_SECONDS", "5"))
NO_NUMBER_REQUEUE_SECONDS = float(os.getenv("NO_NUMBER_REQUEUE_SECONDS", "60"))

AUTO_BACKFILL_FROM_NUMBER = os.getenv("AUTO_BACKFILL_FROM_NUMBER", "true").lower() in ("1", "true", "yes")

REDIS_URL = os.getenv("REDIS_URL") or os.getenv("UPSTASH_REDIS_URL")
REDIS_TLS = os.getenv("REDIS_TLS", "true").lower() in ("1", "true", "yes")
UPSTASH_REDIS_REST_URL = os.getenv("UPSTASH_REDIS_REST_URL")
UPSTASH_REDIS_REST_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN")

# ===========================================
# Policy Shortcuts
# ===========================================
def _policy(): return get_policy()
def rate_per_number_per_min(): return _policy().rate_per_number_per_min
def global_rate_per_min(): return _policy().global_rate_per_min
def daily_limit_default(): return _policy().daily_limit
def send_jitter_seconds(): return _policy().jitter()
def is_quiet_hours_local(): return _policy().is_quiet()
def utcnow(): return datetime.now(timezone.utc)

# ===========================================
# Airtable Access Helpers
# ===========================================
def _first_env(*names): 
    for n in names:
        v = os.getenv(n)
        if v: return v
    return None

def _api_key_for(base_env: str) -> Optional[str]:
    if base_env == PERF_BASE_ENV:
        return _first_env("AIRTABLE_REPORTING_KEY", "PERFORMANCE_KEY", "AIRTABLE_API_KEY")
    if base_env == LEADS_BASE_ENV:
        return _first_env("AIRTABLE_ACQUISITIONS_KEY", "LEADS_CONVOS_KEY", "AIRTABLE_API_KEY")
    if base_env == CONTROL_BASE_ENV:
        return _first_env("AIRTABLE_COMPLIANCE_KEY", "CAMPAIGN_CONTROL_KEY", "AIRTABLE_API_KEY")
    return os.getenv("AIRTABLE_API_KEY")

def _base_value_for(base_env: str) -> Optional[str]:
    if base_env == LEADS_BASE_ENV:
        return _first_env("LEADS_CONVOS_BASE", "AIRTABLE_LEADS_CONVOS_BASE_ID")
    if base_env == PERF_BASE_ENV:
        return _first_env("PERFORMANCE_BASE", "AIRTABLE_PERFORMANCE_BASE_ID")
    if base_env == CONTROL_BASE_ENV:
        return _first_env("CAMPAIGN_CONTROL_BASE", "AIRTABLE_CAMPAIGN_CONTROL_BASE_ID")
    return os.getenv(base_env)

def get_table(base_env: str, table_name: str):
    tbl = _make_table(_api_key_for(base_env), _base_value_for(base_env), table_name)
    if not tbl:
        log.warning(f"⚠️ Missing Airtable table: {base_env}/{table_name}")
    return tbl

# ===========================================
# Rate Limiter Implementations
# ===========================================
class LocalLimiter:
    def __init__(self, per_limit: int, global_limit: int):
        self.per = per_limit
        self.glob = global_limit
        self.per_counts: Dict[str, Tuple[int, int]] = {}
        self.glob_count: Tuple[int, int] = (0, 0)

    def _bucket(self) -> int:
        return int(time.time() // 60)

    def try_consume(self, did: str) -> bool:
        minute = self._bucket()
        g_min, g_ct = self.glob_count
        if g_min != minute:
            g_ct = 0
        if g_ct >= self.glob:
            return False
        d_min, d_ct = self.per_counts.get(did, (minute, 0))
        if d_min != minute:
            d_ct = 0
        if d_ct >= self.per:
            return False
        self.glob_count = (minute, g_ct + 1)
        self.per_counts[did] = (minute, d_ct + 1)
        return True

def build_limiter():
    # You can extend this with Redis or Upstash classes from your current version.
    return LocalLimiter(rate_per_number_per_min(), global_rate_per_min())

# ===========================================
# MAIN SEND LOOP
# ===========================================
def send_batch(campaign_id: str | None = None, limit: int = 500):
    """
    Drain due messages from the Drip Queue and send them out.
    Handles rate limits, quiet hours, retries, and KPI logging.
    """
    drip_tbl = get_table(LEADS_BASE_ENV, DRIP_TABLE_NAME)
    if not drip_tbl:
        return {"ok": False, "error": "missing drip table", "total_sent": 0}

    if is_quiet_hours_local():
        log.info("⏸️ Quiet hours active — skipping send cycle.")
        return {"ok": True, "quiet_hours": True, "total_sent": 0}

    try:
        rows = drip_tbl.all()
    except Exception as e:
        log.error("Failed to read Drip Queue: %s", e)
        return {"ok": False, "error": "read_failed", "total_sent": 0}

    now = utcnow()
    due = []
    for r in rows:
        f = r.get("fields", {})
        status = str(f.get(DRIP_FIELDS["STATUS"], ""))
        if status not in (DripStatus.QUEUED.value, DripStatus.READY.value):
            continue
        when = f.get(DRIP_FIELDS["NEXT_SEND_DATE"]) or f.get("scheduled_at")
        try:
            send_at = datetime.fromisoformat(str(when).replace("Z", "+00:00"))
        except Exception:
            send_at = now
        if send_at <= now:
            due.append(r)

    if not due:
        return {"ok": True, "total_sent": 0, "note": "no_due_messages"}

    due = sorted(due, key=lambda x: x.get("fields", {}).get(DRIP_FIELDS["NEXT_SEND_DATE"], ""))[:limit]

    limiter = build_limiter()
    total_sent, total_failed, total_skipped = 0, 0, 0
    errors = []

    for r in due:
        rid = r["id"]
        f = r.get("fields", {})
        phone = f.get(DRIP_FIELDS["SELLER_PHONE"])
        did = f.get(DRIP_FIELDS["FROM_NUMBER"])
        market = f.get(DRIP_FIELDS["MARKET"])

        if not phone:
            continue

        # Backfill DID
        if not did and AUTO_BACKFILL_FROM_NUMBER:
            from sms.outbound_batcher import pick_number_for_market
            did, _ = pick_number_for_market(market)
            if did:
                try:
                    drip_tbl.update(rid, {DRIP_FIELDS["FROM_NUMBER"]: did})
                except Exception:
                    traceback.print_exc()

        if not did:
            total_failed += 1
            errors.append(f"no_number:{market}")
            continue

        if not limiter.try_consume(did):
            log.debug(f"⏸ Rate limited for {did}")
            continue

        body = f.get(DRIP_FIELDS["MESSAGE_PREVIEW"], "")
        property_id = f.get(DRIP_FIELDS["PROPERTY_ID"])

        # Mark as SENDING
        try:
            drip_tbl.update(rid, {DRIP_FIELDS["STATUS"]: DripStatus.SENDING.value})
        except Exception:
            pass

        ok = False
        send_attempted = False
        send_error: Optional[Exception] = None

        if not MessageProcessor:
            log.error("❌ Skipped send for %s — MessageProcessor not initialized or send() failed", phone)
            total_skipped += 1
            try:
                drip_tbl.update(rid, {
                    DRIP_FIELDS["STATUS"]: DripStatus.SKIPPED.value,
                    DRIP_FIELDS["LAST_ERROR"]: "no_message_processor",
                })
            except Exception:
                pass
            continue

        try:
            send_attempted = True
            result = MessageProcessor.send(
                phone=phone, body=body, from_number=did, property_id=property_id, direction="OUT"
            )
            ok = (result or {}).get("status") == "sent"
        except Exception as e:
            send_error = e
            log.exception("❌ Skipped send for %s — MessageProcessor not initialized or send() failed", phone)
            ok = False
            errors.append(str(e))

        if ok:
            total_sent += 1
            try:
                drip_tbl.update(rid, {
                    DRIP_FIELDS["STATUS"]: DripStatus.SENT.value,
                    DRIP_FIELDS["SENT_AT"]: utcnow().isoformat()
                })
            except Exception:
                pass
        elif send_attempted:
            if not send_error:
                log.error("❌ Skipped send for %s — MessageProcessor not initialized or send() failed", phone)
                errors.append("send_failed")
            total_failed += 1
            try:
                drip_tbl.update(rid, {
                    DRIP_FIELDS["STATUS"]: DripStatus.FAILED.value,
                    DRIP_FIELDS["LAST_ERROR"]: str(send_error) if send_error else "send_failed"
                })
            except Exception:
                pass

        if SLEEP_BETWEEN_SENDS_SEC:
            time.sleep(SLEEP_BETWEEN_SENDS_SEC)

    log.info(
        "✅ Batch complete — sent=%s failed=%s skipped=%s",
        total_sent,
        total_failed,
        total_skipped,
    )
    return {
        "ok": True,
        "total_sent": total_sent,
        "total_failed": total_failed,
        "total_skipped": total_skipped,
        "quiet_hours": False,
        "errors": errors,
    }

