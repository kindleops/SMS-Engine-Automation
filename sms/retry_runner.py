"""
🔁 retry_runner.py (v3.1 — Telemetry Edition)
──────────────────────────────────────────────
Retries failed outbound messages from Conversations.
Adds:
 - Structured KPI/Run telemetry
 - Duration tracking
 - Improved logging & error metrics
"""

from __future__ import annotations
import os, time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from sms.airtable_schema import (
    CONVERSATIONS_TABLE,
    ConversationDeliveryStatus,
    ConversationDirection,
    conversations_field_map,
)
from sms.datastore import CONNECTOR, list_records, update_record
from sms.dispatcher import get_policy
from sms.runtime import get_logger, iso_now
from sms.textgrid_sender import send_message as _send_direct

try:
    from sms.message_processor import MessageProcessor as _Processor
except Exception:
    _Processor = None  # fallback

try:
    from sms.logger import log_run
except Exception:

    def log_run(*_a, **_k):
        pass


try:
    from sms.kpi_logger import log_kpi
except Exception:

    def log_kpi(*_a, **_k):
        pass


logger = get_logger("retry_runner")

# --------------------------
# Config & field setup
# --------------------------
CONV_FIELDS = conversations_field_map()
CONV_FIELD_NAMES = CONVERSATIONS_TABLE.field_names()

CONV_FROM_FIELD = CONV_FIELDS["FROM"]
CONV_TO_FIELD = CONV_FIELDS["TO"]
CONV_BODY_FIELD = CONV_FIELDS["BODY"]
CONV_STATUS_FIELD = CONV_FIELDS["STATUS"]
CONV_DIRECTION_FIELD = CONV_FIELDS["DIRECTION"]
CONV_TEXTGRID_ID_FIELD = CONV_FIELDS["TEXTGRID_ID"]
CONV_TEMPLATE_LINK_FIELD = CONV_FIELD_NAMES.get("TEMPLATE_LINK", "Template")
CONV_CAMPAIGN_LINK_FIELD = CONV_FIELD_NAMES.get("CAMPAIGN_LINK", "Campaign")
CONV_LEAD_LINK_FIELD = CONV_FIELD_NAMES.get("LEAD_LINK", "Lead")

RETRY_COUNT_FIELD = CONV_FIELD_NAMES.get("RETRY_COUNT", "Retry Count")
RETRY_AFTER_FIELD = CONV_FIELD_NAMES.get("RETRY_AFTER", "Retry After")
LAST_RETRY_AT_FIELD = CONV_FIELD_NAMES.get("LAST_RETRY_AT", "Last Retry Time")
LAST_ERROR_FIELD = CONV_FIELD_NAMES.get("LAST_ERROR", "Last Error")
PERM_FAIL_FIELD = CONV_FIELD_NAMES.get("PERMANENT_FAIL", "Permanent Fail Reason")

FAILED_STATUSES = {
    ConversationDeliveryStatus.FAILED.value,
    ConversationDeliveryStatus.UNDELIVERED.value,
    "DELIVERY_FAILED",
    "UNDELIVERABLE",
    "NEEDS_RETRY",
}

POLICY = get_policy()
MAX_RETRIES = int(os.getenv("MAX_RETRIES", str(getattr(POLICY, "retry_limit", 3))))
BASE_BACKOFF_MINUTES = int(os.getenv("BASE_BACKOFF_MINUTES", "30"))


# --------------------------
# Helpers
# --------------------------
def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(v: Any) -> Optional[datetime]:
    if not v:
        return None
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except Exception:
        return None


def _backoff_delay(retry_count: int) -> timedelta:
    exponent = max(0, retry_count - 1)
    return timedelta(minutes=BASE_BACKOFF_MINUTES * (2**exponent))


def _link_id(value: Any) -> Optional[str]:
    if isinstance(value, list) and value:
        return value[0]
    if isinstance(value, str) and value.strip():
        return value
    return None


def _qualifies(fields: Dict[str, Any]) -> bool:
    direction = str(fields.get(CONV_DIRECTION_FIELD) or "").upper()
    if ConversationDirection.OUTBOUND.value not in direction:
        return False
    status = str(fields.get(CONV_STATUS_FIELD) or "").upper()
    if status not in FAILED_STATUSES:
        return False
    retries = int(fields.get(RETRY_COUNT_FIELD) or 0)
    if retries >= MAX_RETRIES:
        return False
    retry_after = _parse_dt(fields.get(RETRY_AFTER_FIELD))
    return retry_after is None or retry_after <= _now()


# --------------------------
# Core class
# --------------------------
class RetryRunner:
    def __init__(self) -> None:
        self.convos = CONNECTOR.conversations()
        self.summary: Dict[str, Any] = {
            "retried": 0,
            "permanent_failures": 0,
            "rescheduled": 0,
            "errors": [],
            "duration_sec": 0.0,
        }

    def run(self, limit: int = 100, view: Optional[str] = None) -> Dict[str, Any]:
        start = time.time()
        candidates = self._fetch_candidates(limit, view)
        if not candidates:
            logger.info("No retry candidates found.")
            return {"ok": True, "retried": 0}

        logger.info(f"Found {len(candidates)} retry candidates.")
        for record in candidates:
            try:
                self._process(record)
            except Exception as exc:
                logger.exception("Retry failed for %s", record.get("id"))
                self.summary["errors"].append({"conversation": record.get("id"), "error": str(exc)})

        self.summary["duration_sec"] = round(time.time() - start, 2)
        self.summary["ok"] = True

        # --- Telemetry
        log_run("RETRY_RUNNER", processed=self.summary["retried"], breakdown=self.summary)
        log_kpi("RETRIED_MESSAGES", self.summary["retried"])
        log_kpi("RETRY_PERM_FAILS", self.summary["permanent_failures"])
        log_kpi("RETRY_RESCHEDULED", self.summary["rescheduled"])

        logger.info(
            f"✅ Retry cycle done | retried={self.summary['retried']} | "
            f"rescheduled={self.summary['rescheduled']} | "
            f"failures={self.summary['permanent_failures']} | "
            f"duration={self.summary['duration_sec']}s"
        )

        return self.summary

    def _fetch_candidates(self, limit: int, view: Optional[str]) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {"max_records": limit * 2}
        if view:
            params["view"] = view
        records = list_records(self.convos, **params)
        return [r for r in records if _qualifies(r.get("fields", {}) or {})][:limit]

    def _process(self, record: Dict[str, Any]) -> None:
        f = record.get("fields", {}) or {}
        retries = int(f.get(RETRY_COUNT_FIELD) or 0)
        phone = f.get(CONV_FROM_FIELD)
        if not phone:
            self._mark_permanent(record, retries, "Missing phone")
            return

        body = f.get(CONV_BODY_FIELD)
        if not body:
            self._mark_permanent(record, retries, "Missing body")
            return

        from_number = f.get(CONV_TO_FIELD)
        template_id = _link_id(f.get(CONV_TEMPLATE_LINK_FIELD))
        campaign_id = _link_id(f.get(CONV_CAMPAIGN_LINK_FIELD))
        lead_id = _link_id(f.get(CONV_LEAD_LINK_FIELD))

        result = self._send(phone, body, from_number, template_id, campaign_id, lead_id)
        if result.get("status") == "sent":
            self._mark_success(record, result.get("sid"), retries)
            self.summary["retried"] += 1
        else:
            err = result.get("error") or "Send failed"
            retries += 1
            if retries >= MAX_RETRIES:
                self._mark_permanent(record, retries, err)
                self.summary["permanent_failures"] += 1
            else:
                self._schedule_retry(record, retries, err)
                self.summary["rescheduled"] += 1

    def _send(self, phone, body, from_number, template_id, campaign_id, lead_id) -> Dict[str, Any]:
        if not from_number:
            return {"status": "failed", "error": "missing_from_number"}
        if _Processor:
            return _Processor.send(
                phone=phone,
                body=body,
                from_number=from_number,
                template_id=template_id,
                campaign_id=campaign_id,
                lead_id=lead_id,
                direction=ConversationDirection.OUTBOUND.value,
                metadata={"retry": True},
            )
        return _send_direct(from_number=from_number, to=phone, message=body)

    def _mark_success(self, record, sid, retries) -> None:
        update_record(
            self.convos,
            record["id"],
            {
                CONV_STATUS_FIELD: ConversationDeliveryStatus.SENT.value,
                RETRY_COUNT_FIELD: retries,
                RETRY_AFTER_FIELD: "",
                LAST_ERROR_FIELD: "",
                LAST_RETRY_AT_FIELD: iso_now(),
                CONV_TEXTGRID_ID_FIELD: sid,
                PERM_FAIL_FIELD: "",
            },
        )

    def _schedule_retry(self, record, retries, err) -> None:
        delay = _backoff_delay(retries)
        update_record(
            self.convos,
            record["id"],
            {
                CONV_STATUS_FIELD: "NEEDS_RETRY",
                RETRY_COUNT_FIELD: retries,
                RETRY_AFTER_FIELD: (_now() + delay).isoformat(),
                LAST_ERROR_FIELD: err,
                LAST_RETRY_AT_FIELD: iso_now(),
            },
        )

    def _mark_permanent(self, record, retries, err) -> None:
        update_record(
            self.convos,
            record["id"],
            {
                CONV_STATUS_FIELD: ConversationDeliveryStatus.FAILED.value,
                RETRY_COUNT_FIELD: retries,
                RETRY_AFTER_FIELD: "",
                LAST_ERROR_FIELD: err,
                LAST_RETRY_AT_FIELD: iso_now(),
                PERM_FAIL_FIELD: err,
            },
        )


def run_retry(limit: int = 100, view: Optional[str] = None) -> Dict[str, Any]:
    return RetryRunner().run(limit, view)


if __name__ == "__main__":
    result = run_retry(limit=int(os.getenv("RETRY_LIMIT", "100")))
    import json

    logger.info(json.dumps(result, indent=2))
