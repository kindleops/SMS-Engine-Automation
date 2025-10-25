"""
🚀 Outbound Message Batcher v3.2 (Telemetry + No-Circulars Edition)
────────────────────────────────────────────────────────────────────
- No self-imports / circular imports
- Quiet hours via DispatchPolicy
- Per-number + global rate limiting
- Robust Airtable read/update with field whitelist
- Optional integrations (KPI, run logs, number pools, message sender)
"""

from __future__ import annotations
import os
import re
import time
import traceback
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

# ──────────────────────────────────────────────────────────────────────────────
# Logging / policy
# ──────────────────────────────────────────────────────────────────────────────
from sms.runtime import get_logger
log = get_logger("outbound")

from sms.dispatcher import get_policy  # provides quiet hours + rate caps

# ──────────────────────────────────────────────────────────────────────────────
# Schema + config
# ──────────────────────────────────────────────────────────────────────────────
from sms.config import DRIP_FIELD_MAP as DRIP_FIELDS
from sms.status_utils import _sanitize_status

DRIP_STATUS_F = DRIP_FIELDS.get("STATUS", "Status")
DRIP_UI_F = DRIP_FIELDS.get("UI", "UI")
DRIP_FROM_NUMBER_F = DRIP_FIELDS.get("FROM_NUMBER", "TextGrid Phone Number")
DRIP_SELLER_PHONE_F = DRIP_FIELDS.get("SELLER_PHONE", "Seller Phone Number")

# Optional integrations (all safe fallbacks)
try:
    from sms.kpi_logger import log_kpi
except Exception:  # pragma: no cover
    def log_kpi(*_a, **_k):  # type: ignore
        pass

try:
    from sms.logger import log_run
except Exception:  # pragma: no cover
    def log_run(*_a, **_k):  # type: ignore
        pass

try:
    from sms.number_pools import increment_sent
except Exception:  # pragma: no cover
    def increment_sent(*_a, **_k):  # type: ignore
        pass

# Primary sender candidates (MessageProcessor preferred; fallback to textgrid_sender)
MessageProcessor = None
try:
    from sms.message_processor import MessageProcessor as _MP  # type: ignore
    MessageProcessor = _MP
except Exception:
    try:
        # Fallback: legacy sender with a simple signature
        from sms.textgrid_sender import send_message as _legacy_send  # type: ignore
        class _LegacyAdapter:
            @staticmethod
            def send(*, phone: str, body: str, from_number: str, property_id: Optional[str] = None, direction: str = "OUT") -> Dict[str, Any]:
                # Legacy API often returns a SID or a dict. Normalize to {status: "sent"|...}
                try:
                    res = _legacy_send(from_number=from_number, to=phone, message=body)  # type: ignore
                    ok = bool(res)
                    return {"status": "sent" if ok else "failed", "raw": res}
                except Exception as e:
                    return {"status": "failed", "error": str(e)}
        MessageProcessor = _LegacyAdapter
    except Exception:
        MessageProcessor = None  # no sender available

# ──────────────────────────────────────────────────────────────────────────────
# Runtime constants
# ──────────────────────────────────────────────────────────────────────────────
LEADS_BASE_ENV = "LEADS_CONVOS_BASE"
DRIP_TABLE_NAME = os.getenv("DRIP_QUEUE_TABLE", "Drip Queue")
NUMBERS_TABLE_NAME = os.getenv("NUMBERS_TABLE", "Numbers")

SLEEP_BETWEEN_SENDS_SEC = float(os.getenv("SLEEP_BETWEEN_SENDS_SEC", "0.03"))
REQUEUE_SOFT_ERROR_SECONDS = float(os.getenv("REQUEUE_SOFT_ERROR_SECONDS", "3600"))
RATE_LIMIT_REQUEUE_SECONDS = float(os.getenv("RATE_LIMIT_REQUEUE_SECONDS", "30"))
NO_NUMBER_REQUEUE_SECONDS = float(os.getenv("NO_NUMBER_REQUEUE_SECONDS", "300"))
AUTO_BACKFILL_FROM_NUMBER = os.getenv("AUTO_BACKFILL_FROM_NUMBER", "true").lower() in {"1", "true", "yes"}

# ──────────────────────────────────────────────────────────────────────────────
# Utilities
# ──────────────────────────────────────────────────────────────────────────────
def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def _iso(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat()

def _parse_dt(val: Any, fallback: datetime) -> datetime:
    try:
        return datetime.fromisoformat(str(val).replace("Z", "+00:00"))
    except Exception:
        return fallback

_PHONE_RE = re.compile(r"^\+1\d{10}$")
def _valid_us_e164(s: Optional[str]) -> bool:
    return bool(s and _PHONE_RE.match(s))

# ──────────────────────────────────────────────────────────────────────────────
# Airtable thin wrappers (no circulars)
# ──────────────────────────────────────────────────────────────────────────────
def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    if v is None or not str(v).strip():
        return default
    return v.strip()

def get_table(base_env_name: str, table_name: str):
    """
    Create a pyairtable.Table from env vars without touching other local modules.
    """
    try:
        from pyairtable import Table  # type: ignore
    except Exception as e:
        log.error(f"pyairtable not available: {e}")
        return None

    api_key = _env("AIRTABLE_API_KEY")
    if not api_key:
        log.error("AIRTABLE_API_KEY missing")
        return None

    base_id = _env(base_env_name)
    if not base_id:
        log.error(f"{base_env_name} missing")
        return None

    try:
        return Table(api_key, base_id, table_name)
    except Exception as e:
        log.error(f"Failed to init Table({base_env_name}, {table_name}): {e}")
        return None

def _safe_update(tbl, rid: str, payload: Dict[str, Any]) -> None:
    """
    Only allow updates to known DRIP fields. Avoids 422 from unknown fields.
    """
    try:
        allow_keys = {
            k
            for k in ["STATUS", "NEXT_SEND_DATE", "SENT_AT", "LAST_ERROR", "FROM_NUMBER", "UI"]
            if k in DRIP_FIELDS
        }

        status_field = DRIP_STATUS_F
        ui_field = DRIP_UI_F
        clean: Dict[str, Any] = {}
        for key, value in payload.items():
            mapped = DRIP_FIELDS.get(key, key)
            if key in allow_keys or mapped in {status_field, ui_field}:
                if mapped == status_field and isinstance(value, (str, type(None))):
                    value = _sanitize_status(value)
                clean[mapped] = value

        if clean:
            try:
                tbl.update(rid, clean)
            except Exception as exc:
                if "INVALID_MULTIPLE_CHOICE_OPTIONS" in str(exc):
                    retry = dict(clean)
                    retry.pop(status_field, None)
                    if retry:
                        tbl.update(rid, retry)
                else:
                    raise
    except Exception as e:
        log.warning(f"⚠️ Update failed: {e}", exc_info=True)

# ──────────────────────────────────────────────────────────────────────────────
# Rate limiter (per-DID + global) using DispatchPolicy caps
# ──────────────────────────────────────────────────────────────────────────────
class _RateLimiter:
    def __init__(self, per_did_per_min: int, global_per_min: int):
        self.per = max(1, per_did_per_min)
        self.glob = max(1, global_per_min)
        self._per_counts: Dict[str, Tuple[int, float]] = {}   # did -> (count, window_start_epoch)
        self._global: Tuple[int, float] = (0, time.time())

    def _tick(self, key: str) -> bool:
        now = time.time()
        # per DID window
        cnt, start = self._per_counts.get(key, (0, now))
        if now - start >= 60.0:
            cnt, start = 0, now
        if cnt + 1 > self.per:
            return False
        # global window
        gcnt, gstart = self._global
        if now - gstart >= 60.0:
            gcnt, gstart = 0, now
        if gcnt + 1 > self.glob:
            return False

        # commit
        self._per_counts[key] = (cnt + 1, start)
        self._global = (gcnt + 1, gstart)
        return True

    def try_consume(self, did: str) -> bool:
        return self._tick(did)

def build_limiter() -> _RateLimiter:
    p = get_policy()
    return _RateLimiter(p.rate_per_number_per_min, p.global_rate_per_min)

def is_quiet_hours_local() -> bool:
    return get_policy().is_quiet()

# ──────────────────────────────────────────────────────────────────────────────
# Number selection (simple, robust)
# ──────────────────────────────────────────────────────────────────────────────
def _pick_number_for_market(market: Optional[str]) -> Optional[str]:
    """
    Pick an active sender DID from Numbers table, preferring the specified market.
    Falls back to any active number if none found for the market.
    """
    tbl = get_table(LEADS_BASE_ENV, NUMBERS_TABLE_NAME)
    if not tbl:
        return None

    def _first_or_none(records: List[Dict[str, Any]]) -> Optional[str]:
        for r in records or []:
            fields = r.get("fields", {})
            # Allow common field names
            did = fields.get("Number") or fields.get("phone") or fields.get("Name")
            active = fields.get("Active")
            status = (fields.get("Status") or "").strip().lower()
            if did and (active is True or str(active).lower() in {"1", "true", "yes"} or status == "active"):
                s = str(did).strip()
                if s.startswith("+1") and len(s) == 12:
                    return s
        return None

    try:
        # Prefer market match
        if market:
            recs = tbl.all(filterByFormula=f"LOWER({{Market}}) = '{str(market).strip().lower()}'")
            did = _first_or_none(recs)
            if did:
                return did

        # Fallback: any active
        recs = tbl.all(filterByFormula="OR({Active} = 1, LOWER({Status}) = 'active')")
        return _first_or_none(recs)
    except Exception as e:  # pragma: no cover
        log.warning(f"Number pick failed: {e}")
        return None

# ──────────────────────────────────────────────────────────────────────────────
# Core batch sender
# ──────────────────────────────────────────────────────────────────────────────
def send_batch(campaign_id: Optional[str] = None, limit: int = 500) -> Dict[str, Any]:
    """
    Process due rows in Drip Queue and attempt to send messages.
    Respects quiet hours and rate limits. Never crashes the process.
    """
    drip_tbl = get_table(LEADS_BASE_ENV, DRIP_TABLE_NAME)
    if not drip_tbl:
        return {"ok": False, "error": "missing_drip_table", "total_sent": 0}

    # Quiet hours guard
    if is_quiet_hours_local():
        log.info("⏸️ Quiet hours active — skipping send cycle.")
        log_kpi("BATCH_SKIPPED_QUIET", 1)
        log_run("OUTBOUND_BATCH", processed=0, breakdown={"quiet_hours": True})
        return {"ok": True, "quiet_hours": True, "total_sent": 0}

    # Read queue
    try:
        rows = drip_tbl.all()
    except Exception as e:
        log.error(f"Failed to read Drip Queue: {e}", exc_info=True)
        return {"ok": False, "error": "read_failed", "total_sent": 0}

    now = utcnow()

    # Determine canonical field names safely
    F = DRIP_FIELDS  # shorthand

    status_key = DRIP_STATUS_F
    next_send_date_key = F.get("NEXT_SEND_DATE", "Next Send Date")
    seller_phone_key = DRIP_SELLER_PHONE_F
    from_number_key = DRIP_FROM_NUMBER_F
    market_key = F.get("MARKET", "Market")
    message_preview_key = F.get("MESSAGE_PREVIEW", "Message")
    property_id_key = F.get("PROPERTY_ID", "Property ID")
    campaign_link_key = F.get("CAMPAIGN_LINK", "Campaign")

    # Filter for due rows
    due: List[Dict[str, Any]] = []
    for r in rows:
        f = r.get("fields", {})
        status = _sanitize_status(str(f.get(status_key, "")).strip())
        if status not in {"Queued", "Sending"}:
            continue
        due_at = _parse_dt(f.get(next_send_date_key), now)
        if due_at <= now:
            if campaign_id:
                links = f.get(campaign_link_key) or []
                link_ids = {str(x) for x in links} if isinstance(links, list) else {str(links)}
                if campaign_id not in link_ids:
                    continue
            due.append(r)

    if not due:
        return {"ok": True, "total_sent": 0, "note": "no_due_messages"}

    # Order oldest first, respect limit
    due = sorted(due, key=lambda x: _parse_dt(x.get("fields", {}).get(next_send_date_key), now))[: max(1, int(limit))]

    limiter = build_limiter()
    total_sent = 0
    total_failed = 0
    errors: List[str] = []

    for r in due:
        rid = r.get("id")
        f = r.get("fields", {}) or {}

        phone = (f.get(seller_phone_key) or "").strip()
        did = (f.get(from_number_key) or "").strip()
        market = f.get(market_key)
        body = (f.get(message_preview_key) or "").strip()
        property_id = f.get(property_id_key)

        # Validate phone
        if not _valid_us_e164(phone):
            _safe_update(
                drip_tbl,
                rid,
                {
                    "STATUS": "Queued",
                    "UI": "⏳",
                    "LAST_ERROR": "invalid_phone",
                    "NEXT_SEND_DATE": _iso(now + timedelta(seconds=REQUEUE_SOFT_ERROR_SECONDS)),
                },
            )
            total_failed += 1
            continue

        # Validate body
        if not body:
            _safe_update(
                drip_tbl,
                rid,
                {
                    "STATUS": "Queued",
                    "UI": "⏳",
                    "LAST_ERROR": "empty_message",
                    "NEXT_SEND_DATE": _iso(now + timedelta(seconds=REQUEUE_SOFT_ERROR_SECONDS)),
                },
            )
            total_failed += 1
            continue

        # Ensure DID
        if not did and AUTO_BACKFILL_FROM_NUMBER:
            did = _pick_number_for_market(market)
            if did:
                _safe_update(drip_tbl, rid, {"FROM_NUMBER": did})

        if not did:
            _safe_update(
                drip_tbl,
                rid,
                {
                    "STATUS": "Queued",
                    "UI": "⏳",
                    "LAST_ERROR": "no_did",
                    "NEXT_SEND_DATE": _iso(now + timedelta(seconds=NO_NUMBER_REQUEUE_SECONDS)),
                },
            )
            total_failed += 1
            continue

        # Rate limit
        if not limiter.try_consume(did):
            _safe_update(
                drip_tbl,
                rid,
                {
                    "STATUS": "Queued",
                    "UI": "⏳",
                    "LAST_ERROR": "rate_limited",
                    "NEXT_SEND_DATE": _iso(now + timedelta(seconds=RATE_LIMIT_REQUEUE_SECONDS)),
                },
            )
            continue

        # Transition to SENDING
        _safe_update(drip_tbl, rid, {"STATUS": "Sending", "UI": "⏳"})

        delivered = False
        try:
            if MessageProcessor is None:
                raise RuntimeError("no_sender_available")
            res = MessageProcessor.send(  # type: ignore[attr-defined]
                phone=phone,
                body=body,
                from_number=did,
                property_id=property_id,
                direction="OUT",
            )
            delivered = bool(res and str(res.get("status", "")).lower() in {"sent", "delivered"})
        except Exception as e:  # pragma: no cover
            errors.append(str(e))
            delivered = False

        if delivered:
            total_sent += 1
            _safe_update(
                drip_tbl,
                rid,
                {
                    "STATUS": "Sent",
                    "UI": "✅",
                    "SENT_AT": _iso(utcnow()),
                    "LAST_ERROR": "",
                },
            )
            try:
                increment_sent(did)
            except Exception:
                pass
            try:
                log_kpi("OUTBOUND_SENT", 1, campaign=campaign_id or "ALL")
            except Exception as kpi_exc:
                log.warning(f"KPI logging skipped: {kpi_exc}")
        else:
            total_failed += 1
            _safe_update(
                drip_tbl,
                rid,
                {
                    "STATUS": "Queued",
                    "UI": "⏳",
                    "LAST_ERROR": "send_failed",
                    "NEXT_SEND_DATE": _iso(now + timedelta(seconds=REQUEUE_SOFT_ERROR_SECONDS)),
                },
            )
            try:
                log_kpi("OUTBOUND_FAILED_SOFT", 1)
            except Exception as kpi_exc:
                log.warning(f"KPI logging skipped: {kpi_exc}")

        if SLEEP_BETWEEN_SENDS_SEC > 0:
            time.sleep(SLEEP_BETWEEN_SENDS_SEC)

    # Telemetry
    attempts = total_sent + total_failed
    delivery_rate = (total_sent / attempts * 100.0) if attempts else 0.0
    try:
        log_kpi("OUTBOUND_DELIVERY_RATE", delivery_rate)
    except Exception as kpi_exc:
        log.warning(f"KPI logging skipped: {kpi_exc}")
    log_run("OUTBOUND_BATCH", processed=total_sent, breakdown={
        "sent": total_sent, "failed": total_failed, "errors": len(errors)
    })
    log.info(f"✅ Batch complete — sent={total_sent}, failed={total_failed}, rate={delivery_rate:.1f}%")

    return {"ok": True, "total_sent": total_sent, "total_failed": total_failed, "errors": errors}

# ──────────────────────────────────────────────────────────────────────────────
# Campaign-level queuing interface
# ──────────────────────────────────────────────────────────────────────────────
def queue_campaign(campaign_id: str, limit: int = 500) -> int:
    """
    Backward-compatible stub used by campaign_runner.
    Simply runs send_batch() filtered by the given campaign_id,
    and returns how many messages were processed.
    """
    try:
        result = send_batch(campaign_id=campaign_id, limit=limit)
        return int(result.get("total_sent", 0))
    except Exception as e:
        log.error(f"queue_campaign() failed for {campaign_id}: {e}", exc_info=True)
        return 0