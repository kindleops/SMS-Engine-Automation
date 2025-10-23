"""Retry failed outbound messages using the datastore-driven pipeline."""

from __future__ import annotations

import os
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

try:  # Optional richer retry path that also updates linked tables
    from sms.message_processor import MessageProcessor as _Processor  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    _Processor = None  # type: ignore

logger = get_logger(__name__)

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


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def _backoff_delay(retry_count: int) -> timedelta:
    exponent = max(0, retry_count - 1)
    return timedelta(minutes=BASE_BACKOFF_MINUTES * (2 ** exponent))


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


class RetryRunner:
    def __init__(self) -> None:
        self.convos = CONNECTOR.conversations()
        self.summary: Dict[str, Any] = {
            "retried": 0,
            "permanent_failures": 0,
            "rescheduled": 0,
            "errors": [],
        }

    def run(self, limit: int, view: Optional[str]) -> Dict[str, Any]:
        candidates = self._fetch_candidates(limit, view)
        if not candidates:
            return {"retried": 0, "permanent_failures": 0, "rescheduled": 0, "errors": [], "ok": True}

        for record in candidates:
            try:
                self._process(record)
            except Exception as exc:  # pragma: no cover - defensive guard
                logger.exception("Retry failed for %s", record.get("id"))
                self.summary["errors"].append({"conversation": record.get("id"), "error": str(exc)})

        self.summary["ok"] = True
        return self.summary

    def _fetch_candidates(self, limit: int, view: Optional[str]) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {"max_records": limit * 2}
        if view:
            params["view"] = view
        records = list_records(self.convos, **params)
        return [rec for rec in records if _qualifies(rec.get("fields", {}) or {})][:limit]

    def _process(self, record: Dict[str, Any]) -> None:
        fields = record.get("fields", {}) or {}
        retries = int(fields.get(RETRY_COUNT_FIELD) or 0)
        phone = fields.get(CONV_FROM_FIELD)
        if not phone:
            self._mark_permanent(record, retries, "Missing phone number")
            return

        body = fields.get(CONV_BODY_FIELD)
        if not body:
            self._mark_permanent(record, retries, "Missing message body")
            return

        from_number = fields.get(CONV_TO_FIELD)
        template_id = _link_id(fields.get(CONV_TEMPLATE_LINK_FIELD))
        campaign_id = _link_id(fields.get(CONV_CAMPAIGN_LINK_FIELD))
        lead_id = _link_id(fields.get(CONV_LEAD_LINK_FIELD))

        result = self._send(phone, body, from_number, template_id, campaign_id, lead_id)
        if result.get("status") == "sent":
            self._mark_success(record, result.get("sid"), retries)
            self.summary["retried"] += 1
        else:
            error = result.get("error") or "Send failed"
            retries += 1
            if retries >= MAX_RETRIES:
                self._mark_permanent(record, retries, error)
                self.summary["permanent_failures"] += 1
            else:
                self._schedule_retry(record, retries, error)
                self.summary["rescheduled"] += 1

    def _send(
        self,
        phone: str,
        body: str,
        from_number: Optional[str],
        template_id: Optional[str],
        campaign_id: Optional[str],
        lead_id: Optional[str],
    ) -> Dict[str, Any]:
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
        from sms.textgrid_sender import send_message as _send_direct
        return _send_direct(
            phone,
            body,
            from_number=from_number,
            template_id=template_id,
            campaign_id=campaign_id,
            lead_id=lead_id,
        )

    def _mark_success(self, record: Dict[str, Any], sid: Optional[str], retries: int) -> None:
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

    def _schedule_retry(self, record: Dict[str, Any], retries: int, error: str) -> None:
        delay = _backoff_delay(retries)
        update_record(
            self.convos,
            record["id"],
            {
                CONV_STATUS_FIELD: "NEEDS_RETRY",
                RETRY_COUNT_FIELD: retries,
                RETRY_AFTER_FIELD: (_now() + delay).isoformat(),
                LAST_ERROR_FIELD: error,
                LAST_RETRY_AT_FIELD: iso_now(),
            },
        )

    def _mark_permanent(self, record: Dict[str, Any], retries: int, error: str) -> None:
        update_record(
            self.convos,
            record["id"],
            {
                CONV_STATUS_FIELD: ConversationDeliveryStatus.FAILED.value,
                RETRY_COUNT_FIELD: retries,
                RETRY_AFTER_FIELD: "",
                LAST_ERROR_FIELD: error,
                LAST_RETRY_AT_FIELD: iso_now(),
                PERM_FAIL_FIELD: error,
            },
        )


def run_retry(limit: int = 100, view: Optional[str] = None) -> Dict[str, Any]:
    runner = RetryRunner()
    return runner.run(limit, view)


if __name__ == "__main__":  # pragma: no cover - manual execution helper
    result = run_retry(limit=int(os.getenv("RETRY_LIMIT", "100")))
    print(result)
