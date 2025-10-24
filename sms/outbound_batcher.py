"""
🚀 Outbound Message Batcher (Bulletproof Edition)
------------------------------------------------
Drains the Drip Queue, enforces quiet hours, applies rate limits,
picks DIDs, and dispatches outbound SMS via MessageProcessor.

Design goals:
  • Respect "Next Send Date" and quiet hours
  • Never fail a row just because it's too early or missing a soft field
  • Requeue with backoff + clear Last Error, instead of thrashing
  • Safe for multi-worker (stateless limiter here; swap for Redis if desired)
"""

from __future__ import annotations

import os
import re
import time
import traceback
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from sms.config import DRIP_FIELD_MAP as DRIP_FIELDS
from sms.airtable_schema import DripStatus
from sms.runtime import get_logger

from sms.dispatcher import get_policy

log = get_logger("outbound")

# =========================================================
# Airtable clients (pyairtable v1/v2 compatible wrappers)
# =========================================================
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


# =========================================================
# ENV / Wiring
# =========================================================
LEADS_BASE_ENV = "LEADS_CONVOS_BASE"
PERF_BASE_ENV = "PERFORMANCE_BASE"
CONTROL_BASE_ENV = "CAMPAIGN_CONTROL_BASE"

DRIP_TABLE_NAME = os.getenv("DRIP_QUEUE_TABLE", "Drip Queue")
NUMBERS_TABLE_NAME = os.getenv("NUMBERS_TABLE", "Numbers")

# Backoff knobs
SLEEP_BETWEEN_SENDS_SEC = float(os.getenv("SLEEP_BETWEEN_SENDS_SEC", "0.03"))
REQUEUE_SOFT_ERROR_SECONDS = float(os.getenv("REQUEUE_SOFT_ERROR_SECONDS", "3600"))  # 1 hr
RATE_LIMIT_REQUEUE_SECONDS = float(os.getenv("RATE_LIMIT_REQUEUE_SECONDS", "30"))    # 30s
NO_NUMBER_REQUEUE_SECONDS = float(os.getenv("NO_NUMBER_REQUEUE_SECONDS", "300"))     # 5m

AUTO_BACKFILL_FROM_NUMBER = os.getenv("AUTO_BACKFILL_FROM_NUMBER", "true").lower() in ("1", "true", "yes")

# Optional: Upstash/Redis creds (not used in LocalLimiter; you can swap in later)
REDIS_URL = os.getenv("REDIS_URL") or os.getenv("UPSTASH_REDIS_URL")
REDIS_TLS = os.getenv("REDIS_TLS", "true").lower() in ("1", "true", "yes")

# =========================================================
# Policy shortcuts
# =========================================================
def _policy():
    return get_policy()


def rate_per_number_per_min():
    return _policy().rate_per_number_per_min


def global_rate_per_min():
    return _policy().global_rate_per_min


def daily_limit_default():
    return _policy().daily_limit


def is_quiet_hours_local():
    return _policy().is_quiet()


def utcnow():
    return datetime.now(timezone.utc)


# =========================================================
# Airtable helpers
# =========================================================
def _first_env(*names):
    for n in names:
        v = os.getenv(n)
        if v:
            return v
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


# =========================================================
# DID picking (no self-import loops)
# =========================================================
def _pick_number_for_market(market: Optional[str]) -> Optional[str]:
    """
    Pick a DID from Numbers table for the given market.
    Strategy: first available matching market; fallback to any unlocked number.
    """
    numbers_tbl = get_table(LEADS_BASE_ENV, NUMBERS_TABLE_NAME)
    if not numbers_tbl:
        return None

    try:
        rows = numbers_tbl.all()
    except Exception:
        traceback.print_exc()
        return None

    market_norm = (market or "").strip().lower()
    best = None
    for r in rows:
        f = r.get("fields", {})
        did = str(f.get("Phone Number") or f.get("Number") or "").strip()
        locked = bool(f.get("Locked") or False)
        m = str(f.get("Market") or "").strip().lower()
        if not did or locked:
            continue
        if market_norm and m == market_norm:
            return did
        if not best:
            best = did

    return best


# =========================================================
# Rate limiter (local, per-minute buckets)
# =========================================================
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
    # Swap this for a Redis-backed limiter later if needed.
    return LocalLimiter(rate_per_number_per_min(), global_rate_per_min())


# =========================================================
# Validators & utilities
# =========================================================
_PHONE_RE = re.compile(r"^\+1\d{10}$")


def _valid_us_e164(s: Optional[str]) -> bool:
    return bool(s and _PHONE_RE.match(s))


def _iso(s: datetime) -> str:
    return s.replace(microsecond=0).isoformat()


def _parse_dt(val: Any, fallback: datetime) -> datetime:
    if not val:
        return fallback
    try:
        return datetime.fromisoformat(str(val).replace("Z", "+00:00"))
    except Exception:
        return fallback


def _safe_update(tbl, record_id: str, payload: Dict[str, Any]) -> None:
    """
    Best-effort update; logs but never raises.
    Only touches columns we own from DRIP_FIELD_MAP.
    """
    try:
        whitelist = {
            DRIP_FIELDS["STATUS"],
            DRIP_FIELDS["NEXT_SEND_DATE"],
            DRIP_FIELDS["SENT_AT"],
            DRIP_FIELDS["LAST_ERROR"],
            DRIP_FIELDS["FROM_NUMBER"],
        }
        clean = {k: v for k, v in payload.items() if k in whitelist}
        if clean:
            tbl.update(record_id, clean)
    except Exception:
        traceback.print_exc()


# =========================================================
# Optional sender
# =========================================================
try:
    from sms.message_processor import MessageProcessor
except Exception:
    MessageProcessor = None  # type: ignore


# =========================================================
# MAIN: send_batch
# =========================================================
def send_batch(campaign_id: str | None = None, limit: int = 500):
    """
    Drain due messages from Drip Queue and send.
    • Honors quiet hours (no mutations performed during quiet)
    • Honors Next Send Date
    • Filters by campaign_id when provided
    • Requeues with backoff for soft errors (no phone, no body, no DID, rate limited)
    """
    drip_tbl = get_table(LEADS_BASE_ENV, DRIP_TABLE_NAME)
    if not drip_tbl:
        return {"ok": False, "error": "missing drip table", "total_sent": 0}

    # If quiet hours, do nothing (prevents status churn overnight)
    if is_quiet_hours_local():
        log.info("⏸️ Quiet hours active — skipping send cycle.")
        return {"ok": True, "quiet_hours": True, "total_sent": 0}

    try:
        rows = drip_tbl.all()
    except Exception as e:
        log.error("Failed to read Drip Queue: %s", e)
        return {"ok": False, "error": "read_failed", "total_sent": 0}

    now = utcnow()
    due: List[Dict[str, Any]] = []
    for r in rows:
        f = r.get("fields", {})
        if campaign_id:
            linked = f.get(DRIP_FIELDS["CAMPAIGN_LINK"])
            if linked and isinstance(linked, list):
                # linked list of record IDs; accept when our id is present
                if campaign_id not in {str(x) for x in linked}:
                    continue

        status = str(f.get(DRIP_FIELDS["STATUS"], "")).strip()
        if status not in (DripStatus.QUEUED.value, DripStatus.READY.value, DripStatus.SENDING.value):
            continue

        when = f.get(DRIP_FIELDS["NEXT_SEND_DATE"]) or f.get("scheduled_at")
        send_at = _parse_dt(when, fallback=now)

        if send_at <= now:
            due.append(r)

    if not due:
        return {"ok": True, "total_sent": 0, "note": "no_due_messages"}

    # Oldest first, cap by limit
    due = sorted(due, key=lambda x: _parse_dt(x.get("fields", {}).get(DRIP_FIELDS["NEXT_SEND_DATE"]), utcnow()))[:limit]

    limiter = build_limiter()
    total_sent, total_failed = 0, 0
    errors: List[str] = []

    for r in due:
        rid = r["id"]
        f = r.get("fields", {})

        phone = (f.get(DRIP_FIELDS["SELLER_PHONE"]) or "").strip()
        did = (f.get(DRIP_FIELDS["FROM_NUMBER"]) or "").strip()
        market = f.get(DRIP_FIELDS["MARKET"])
        body = (f.get(DRIP_FIELDS["MESSAGE_PREVIEW"]) or "").strip()
        property_id = f.get(DRIP_FIELDS["PROPERTY_ID"])

        # Validate core fields
        if not _valid_us_e164(phone):
            total_failed += 1
            _safe_update(
                drip_tbl,
                rid,
                {
                    DRIP_FIELDS["STATUS"]: DripStatus.READY.value,  # keep READY so it can be corrected
                    DRIP_FIELDS["LAST_ERROR"]: "invalid_or_missing_phone",
                    DRIP_FIELDS["NEXT_SEND_DATE"]: _iso(now + timedelta(seconds=REQUEUE_SOFT_ERROR_SECONDS)),
                },
            )
            continue

        if not body:
            total_failed += 1
            _safe_update(
                drip_tbl,
                rid,
                {
                    DRIP_FIELDS["STATUS"]: DripStatus.READY.value,
                    DRIP_FIELDS["LAST_ERROR"]: "empty_message_body",
                    DRIP_FIELDS["NEXT_SEND_DATE"]: _iso(now + timedelta(seconds=REQUEUE_SOFT_ERROR_SECONDS)),
                },
            )
            continue

        # Backfill DID if allowed
        if not did and AUTO_BACKFILL_FROM_NUMBER:
            did = _pick_number_for_market(market)
            if did:
                _safe_update(drip_tbl, rid, {DRIP_FIELDS["FROM_NUMBER"]: did})

        if not did:
            total_failed += 1
            _safe_update(
                drip_tbl,
                rid,
                {
                    DRIP_FIELDS["STATUS"]: DripStatus.READY.value,
                    DRIP_FIELDS["LAST_ERROR"]: "no_number_available",
                    DRIP_FIELDS["NEXT_SEND_DATE"]: _iso(now + timedelta(seconds=NO_NUMBER_REQUEUE_SECONDS)),
                },
            )
            continue

        # Rate limit gate
        if not limiter.try_consume(did):
            _safe_update(
                drip_tbl,
                rid,
                {
                    DRIP_FIELDS["STATUS"]: DripStatus.READY.value,
                    DRIP_FIELDS["LAST_ERROR"]: "rate_limited",
                    DRIP_FIELDS["NEXT_SEND_DATE"]: _iso(now + timedelta(seconds=RATE_LIMIT_REQUEUE_SECONDS)),
                },
            )
            # not a failure — just requeue
            continue

        # Mark as SENDING (best effort)
        _safe_update(drip_tbl, rid, {DRIP_FIELDS["STATUS"]: DripStatus.SENDING.value})

        # Dispatch
        delivered = False
        try:
            if MessageProcessor:
                res = MessageProcessor.send(
                    phone=phone,
                    body=body,
                    from_number=did,
                    property_id=property_id,
                    direction="OUT",
                )
                delivered = bool(res and res.get("status") == "sent")
            else:
                errors.append("MessageProcessor_unavailable")
        except Exception as e:
            errors.append(str(e))
            delivered = False

        if delivered:
            total_sent += 1
            _safe_update(
                drip_tbl,
                rid,
                {
                    DRIP_FIELDS["STATUS"]: DripStatus.SENT.value,
                    DRIP_FIELDS["SENT_AT"]: _iso(utcnow()),
                    DRIP_FIELDS["LAST_ERROR"]: "",
                },
            )
        else:
            total_failed += 1
            # soft fail → requeue quickly; do NOT flip to FAILED unless truly terminal
            _safe_update(
                drip_tbl,
                rid,
                {
                    DRIP_FIELDS["STATUS"]: DripStatus.READY.value,
                    DRIP_FIELDS["LAST_ERROR"]: "send_failed",
                    DRIP_FIELDS["NEXT_SEND_DATE"]: _iso(utcnow() + timedelta(seconds=REQUEUE_SOFT_ERROR_SECONDS)),
                },
            )

        if SLEEP_BETWEEN_SENDS_SEC:
            time.sleep(SLEEP_BETWEEN_SENDS_SEC)

    log.info("✅ Batch complete — sent=%s failed(soft)=%s", total_sent, total_failed)
    return {
        "ok": True,
        "total_sent": total_sent,
        "total_failed": total_failed,  # soft failures that were requeued
        "quiet_hours": False,
        "errors": errors,
    }

