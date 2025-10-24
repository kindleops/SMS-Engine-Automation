"""
🚀 Outbound Message Batcher v3.1 (Telemetry Edition)
────────────────────────────────────────────
Adds:
 - KPI + Run telemetry
 - NumberPool counter sync
 - Structured logging
"""

from __future__ import annotations
import os, re, time, traceback
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from sms.config import DRIP_FIELD_MAP as DRIP_FIELDS
from sms.airtable_schema import DripStatus
from sms.runtime import get_logger

log = get_logger("outbound")

from sms.dispatcher import get_policy

# Optional integrations
try:
    from sms.kpi_logger import log_kpi
except Exception:

    def log_kpi(*_a, **_k):
        pass


try:
    from sms.logger import log_run
except Exception:

    def log_run(*_a, **_k):
        pass


try:
    from sms.number_pools import increment_sent
except Exception:

    def increment_sent(*_a, **_k):
        pass


try:
    from sms.message_processor import MessageProcessor
except Exception:
    MessageProcessor = None

# ========== CONFIG ==========
LEADS_BASE_ENV = "LEADS_CONVOS_BASE"
DRIP_TABLE_NAME = os.getenv("DRIP_QUEUE_TABLE", "Drip Queue")
NUMBERS_TABLE_NAME = os.getenv("NUMBERS_TABLE", "Numbers")

SLEEP_BETWEEN_SENDS_SEC = float(os.getenv("SLEEP_BETWEEN_SENDS_SEC", "0.03"))
REQUEUE_SOFT_ERROR_SECONDS = float(os.getenv("REQUEUE_SOFT_ERROR_SECONDS", "3600"))
RATE_LIMIT_REQUEUE_SECONDS = float(os.getenv("RATE_LIMIT_REQUEUE_SECONDS", "30"))
NO_NUMBER_REQUEUE_SECONDS = float(os.getenv("NO_NUMBER_REQUEUE_SECONDS", "300"))
AUTO_BACKFILL_FROM_NUMBER = os.getenv("AUTO_BACKFILL_FROM_NUMBER", "true").lower() in {"1", "true", "yes"}


# ========== Utilities ==========
def utcnow():
    return datetime.now(timezone.utc)


def _iso(dt: datetime):
    return dt.replace(microsecond=0).isoformat()


def _parse_dt(val: Any, fallback: datetime):
    try:
        return datetime.fromisoformat(str(val).replace("Z", "+00:00"))
    except Exception:
        return fallback


_PHONE_RE = re.compile(r"^\+1\d{10}$")


def _valid_us_e164(s: Optional[str]) -> bool:
    return bool(s and _PHONE_RE.match(s))


# ========== Airtable Wrappers ==========
from sms.outbound_batcher import get_table, build_limiter, _pick_number_for_market, is_quiet_hours_local


def _safe_update(tbl, rid: str, payload: Dict[str, Any]) -> None:
    try:
        whitelist = {DRIP_FIELDS[k] for k in ["STATUS", "NEXT_SEND_DATE", "SENT_AT", "LAST_ERROR", "FROM_NUMBER"]}
        clean = {k: v for k, v in payload.items() if k in whitelist}
        if clean:
            tbl.update(rid, clean)
    except Exception as e:
        log.warning(f"⚠️ Update failed: {e}", exc_info=True)


# ========== Core ==========
def send_batch(campaign_id: str | None = None, limit: int = 500):
    drip_tbl = get_table(LEADS_BASE_ENV, DRIP_TABLE_NAME)
    if not drip_tbl:
        return {"ok": False, "error": "missing drip table", "total_sent": 0}

    if is_quiet_hours_local():
        log.info("⏸️ Quiet hours active — skipping send cycle.")
        log_kpi("BATCH_SKIPPED_QUIET", 1)
        log_run("OUTBOUND_BATCH", processed=0, breakdown={"quiet_hours": True})
        return {"ok": True, "quiet_hours": True, "total_sent": 0}

    try:
        rows = drip_tbl.all()
    except Exception as e:
        log.error(f"Failed to read Drip Queue: {e}", exc_info=True)
        return {"ok": False, "error": "read_failed", "total_sent": 0}

    now = utcnow()
    due = [
        r
        for r in rows
        if (status := str(r.get("fields", {}).get(DRIP_FIELDS["STATUS"], "")).strip())
        in (DripStatus.QUEUED.value, DripStatus.READY.value, DripStatus.SENDING.value)
        and _parse_dt(r.get("fields", {}).get(DRIP_FIELDS["NEXT_SEND_DATE"]), now) <= now
    ]

    if campaign_id:
        due = [r for r in due if campaign_id in {str(x) for x in (r["fields"].get(DRIP_FIELDS["CAMPAIGN_LINK"]) or [])}]

    if not due:
        return {"ok": True, "total_sent": 0, "note": "no_due_messages"}

    due = sorted(due, key=lambda x: _parse_dt(x["fields"].get(DRIP_FIELDS["NEXT_SEND_DATE"]), now))[:limit]
    limiter = build_limiter()
    total_sent, total_failed, errors = 0, 0, []

    for r in due:
        rid, f = r["id"], r.get("fields", {})
        phone = (f.get(DRIP_FIELDS["SELLER_PHONE"]) or "").strip()
        did = (f.get(DRIP_FIELDS["FROM_NUMBER"]) or "").strip()
        market = f.get(DRIP_FIELDS["MARKET"])
        body = (f.get(DRIP_FIELDS["MESSAGE_PREVIEW"]) or "").strip()
        property_id = f.get(DRIP_FIELDS["PROPERTY_ID"])

        if not _valid_us_e164(phone):
            _safe_update(
                drip_tbl,
                rid,
                {
                    DRIP_FIELDS["STATUS"]: DripStatus.READY.value,
                    DRIP_FIELDS["LAST_ERROR"]: "invalid_phone",
                    DRIP_FIELDS["NEXT_SEND_DATE"]: _iso(now + timedelta(seconds=REQUEUE_SOFT_ERROR_SECONDS)),
                },
            )
            total_failed += 1
            continue

        if not body:
            _safe_update(
                drip_tbl,
                rid,
                {
                    DRIP_FIELDS["STATUS"]: DripStatus.READY.value,
                    DRIP_FIELDS["LAST_ERROR"]: "empty_message",
                    DRIP_FIELDS["NEXT_SEND_DATE"]: _iso(now + timedelta(seconds=REQUEUE_SOFT_ERROR_SECONDS)),
                },
            )
            total_failed += 1
            continue

        if not did and AUTO_BACKFILL_FROM_NUMBER:
            did = _pick_number_for_market(market)
            if did:
                _safe_update(drip_tbl, rid, {DRIP_FIELDS["FROM_NUMBER"]: did})
        if not did:
            _safe_update(
                drip_tbl,
                rid,
                {
                    DRIP_FIELDS["STATUS"]: DripStatus.READY.value,
                    DRIP_FIELDS["LAST_ERROR"]: "no_did",
                    DRIP_FIELDS["NEXT_SEND_DATE"]: _iso(now + timedelta(seconds=NO_NUMBER_REQUEUE_SECONDS)),
                },
            )
            total_failed += 1
            continue

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
            continue

        _safe_update(drip_tbl, rid, {DRIP_FIELDS["STATUS"]: DripStatus.SENDING.value})
        delivered = False
        try:
            if MessageProcessor:
                res = MessageProcessor.send(phone=phone, body=body, from_number=did, property_id=property_id, direction="OUT")
                delivered = bool(res and res.get("status") == "sent")
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
            increment_sent(did)
            log_kpi("OUTBOUND_SENT", 1, campaign=campaign_id or "ALL")
        else:
            total_failed += 1
            _safe_update(
                drip_tbl,
                rid,
                {
                    DRIP_FIELDS["STATUS"]: DripStatus.READY.value,
                    DRIP_FIELDS["LAST_ERROR"]: "send_failed",
                    DRIP_FIELDS["NEXT_SEND_DATE"]: _iso(now + timedelta(seconds=REQUEUE_SOFT_ERROR_SECONDS)),
                },
            )
            log_kpi("OUTBOUND_FAILED_SOFT", 1)

        if SLEEP_BETWEEN_SENDS_SEC:
            time.sleep(SLEEP_BETWEEN_SENDS_SEC)

    # Summary + telemetry
    delivery_rate = (total_sent / (total_sent + total_failed) * 100) if (total_sent + total_failed) else 0
    log_kpi("OUTBOUND_DELIVERY_RATE", delivery_rate)
    log_run("OUTBOUND_BATCH", processed=total_sent, breakdown={"sent": total_sent, "failed": total_failed, "errors": len(errors)})
    log.info(f"✅ Batch complete — sent={total_sent}, failed={total_failed}, rate={delivery_rate:.1f}%")

    return {"ok": True, "total_sent": total_sent, "total_failed": total_failed, "errors": errors}
