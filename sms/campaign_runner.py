"""Campaign runner that dequeues Drip Queue records and sends messages."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from sms.airtable_schema import (
    DRIP_QUEUE_TABLE,
    ConversationDeliveryStatus,
    ConversationDirection,
    ConversationProcessor,
    conversations_field_map,
    drip_field_map,
)
from sms.config import settings
from sms.datastore import CONNECTOR, list_records, update_record
from sms.dispatcher import get_policy
from sms.message_processor import MessageProcessor
from sms.runtime import get_logger, iso_now

logger = get_logger(__name__)

DRIP_FIELDS = drip_field_map()
DRIP_FIELD_NAMES = DRIP_QUEUE_TABLE.field_names()

DRIP_STATUS_FIELD = DRIP_FIELDS["STATUS"]
DRIP_MESSAGE_FIELD = DRIP_FIELDS.get("MESSAGE_PREVIEW", "message_preview")
DRIP_SELLER_PHONE_FIELD = DRIP_FIELDS.get("SELLER_PHONE", "phone")
DRIP_FROM_NUMBER_FIELD = DRIP_FIELDS.get("FROM_NUMBER", "From Number")
DRIP_TEMPLATE_LINK_FIELD = DRIP_FIELDS.get("TEMPLATE_LINK", "Template")
DRIP_CAMPAIGN_LINK_FIELD = DRIP_FIELDS.get("CAMPAIGN_LINK", "Campaign")
DRIP_LEAD_LINK_FIELD = DRIP_FIELD_NAMES.get("LEAD_LINK", "Lead")
DRIP_NEXT_SEND_DATE_FIELD = DRIP_FIELDS.get("NEXT_SEND_DATE", "next_send_date")
DRIP_LAST_SENT_FIELD = DRIP_FIELD_NAMES.get("LAST_SENT", "Last Sent")
DRIP_LAST_ERROR_FIELD = DRIP_FIELD_NAMES.get("LAST_ERROR", "Last Error")
DRIP_PROCESSOR_FIELD = DRIP_FIELDS.get("PROCESSOR", "processor")

CONV_FIELDS = conversations_field_map()
CONV_TO_FIELD = CONV_FIELDS["TO"]

DEFAULT_PROCESSOR = os.getenv("CAMPAIGN_RUNNER_LABEL", ConversationProcessor.CAMPAIGN_RUNNER.value)


def _parse_dt(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def _link_id(value: Any) -> Optional[str]:
    if isinstance(value, list) and value:
        return value[0]
    if isinstance(value, str) and value.strip():
        return value
    return None


def _quiet_window(now_utc: datetime, policy) -> tuple[bool, Optional[datetime]]:
    cfg = settings()
    enabled = cfg.QUIET_HOURS_ENFORCED or bool(getattr(policy, "quiet_enforced", False))
    if not enabled:
        return False, None

    start_hour = cfg.QUIET_START_HOUR if cfg.QUIET_START_HOUR is not None else getattr(policy, "quiet_start_hour", 21)
    end_hour = cfg.QUIET_END_HOUR if cfg.QUIET_END_HOUR is not None else getattr(policy, "quiet_end_hour", 9)

    from zoneinfo import ZoneInfo

    tz_name = cfg.QUIET_TZ or getattr(policy, "quiet_tz_name", "America/Chicago")
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = timezone.utc

    local_now = now_utc.astimezone(tz)
    start = local_now.replace(hour=start_hour, minute=0, second=0, microsecond=0)
    end = local_now.replace(hour=end_hour, minute=0, second=0, microsecond=0)

    if start <= end:
        in_quiet = start <= local_now < end
        next_allowed = end if in_quiet else local_now
    else:
        in_quiet = not (end <= local_now < start)
        next_allowed = end if local_now < end else end + timedelta(days=1) if in_quiet else local_now

    return in_quiet, next_allowed.astimezone(timezone.utc) if next_allowed else None


class CampaignRunner:
    def __init__(self, *, send_after_queue: bool = False) -> None:
        self.drip = CONNECTOR.drip_queue()
        self.summary: Dict[str, Any] = {"sent": 0, "failed": 0, "deferred": 0, "errors": [], "ok": True}
        self.policy = get_policy()
        self.send_after_queue = send_after_queue

    def run(self, limit: int) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        is_quiet, next_allowed = _quiet_window(now, self.policy)
        records = self._fetch_queue(limit)
        if not records:
            return self.summary.copy()

        if not self.send_after_queue:
            self.summary["note"] = "send_after_queue disabled; leaving queue untouched"
            self.summary["queued_pending"] = len(records)
            return self.summary

        for record in records:
            fields = record.get("fields", {}) or {}
            send_at = _parse_dt(fields.get(DRIP_NEXT_SEND_DATE_FIELD)) or now
            if is_quiet and send_at <= now:
                self._defer(record, next_allowed or now)
                continue
            if send_at > now:
                self._defer(record, send_at)
                continue

            try:
                self._process(record)
            except Exception as exc:  # pragma: no cover - defensive guard
                logger.exception("Campaign runner failed for %s", record.get("id"))
                self.summary["failed"] += 1
                self.summary["errors"].append({"drip_id": record.get("id"), "error": str(exc)})

        self.summary["ok"] = not self.summary["errors"]
        return self.summary

    def _fetch_queue(self, limit: int) -> List[Dict[str, Any]]:
        records = list_records(self.drip, max_records=limit * 3)
        eligible = []
        for record in records:
            fields = record.get("fields", {}) or {}
            status = str(fields.get(DRIP_STATUS_FIELD) or "").upper()
            if status in {"QUEUED", "READY", ConversationDeliveryStatus.QUEUED.value}:
                eligible.append(record)
            if len(eligible) >= limit:
                break
        return eligible

    def _process(self, record: Dict[str, Any]) -> None:
        fields = record.get("fields", {}) or {}
        message = fields.get(DRIP_MESSAGE_FIELD)
        phone = fields.get(DRIP_SELLER_PHONE_FIELD)
        from_number = fields.get(DRIP_FROM_NUMBER_FIELD) or fields.get(CONV_TO_FIELD)
        if not phone or not message:
            self.summary["errors"].append({"drip_id": record.get("id"), "error": "Missing phone or message"})
            self.summary["failed"] += 1
            self._mark_failed(record, "Missing phone or message")
            return

        template_id = _link_id(fields.get(DRIP_TEMPLATE_LINK_FIELD))
        campaign_id = _link_id(fields.get(DRIP_CAMPAIGN_LINK_FIELD))
        lead_id = _link_id(fields.get(DRIP_LEAD_LINK_FIELD))

        result = MessageProcessor.send(
            phone=str(phone),
            body=str(message),
            from_number=str(from_number) if from_number else None,
            template_id=template_id,
            campaign_id=campaign_id,
            lead_id=lead_id,
            direction=ConversationDirection.OUTBOUND.value,
            metadata={"drip_queue_id": record.get("id")},
        )

        if result.get("status") == "sent":
            self.summary["sent"] += 1
            self._mark_sent(record, result.get("sid"))
        else:
            self.summary["failed"] += 1
            self._mark_failed(record, result.get("error") or "Send failed")

    def _defer(self, record: Dict[str, Any], when: datetime) -> None:
        update_record(
            self.drip,
            record["id"],
            {
                DRIP_STATUS_FIELD: ConversationDeliveryStatus.QUEUED.value,
                DRIP_NEXT_SEND_DATE_FIELD: when.isoformat(),
                DRIP_PROCESSOR_FIELD: DEFAULT_PROCESSOR,
            },
        )
        self.summary["deferred"] += 1

    def _mark_sent(self, record: Dict[str, Any], sid: Optional[str]) -> None:
        update_record(
            self.drip,
            record["id"],
            {
                DRIP_STATUS_FIELD: ConversationDeliveryStatus.SENT.value,
                DRIP_LAST_SENT_FIELD: iso_now(),
                DRIP_LAST_ERROR_FIELD: "",
                DRIP_PROCESSOR_FIELD: DEFAULT_PROCESSOR,
            },
        )

    def _mark_failed(self, record: Dict[str, Any], error: str) -> None:
        update_record(
            self.drip,
            record["id"],
            {
                DRIP_STATUS_FIELD: ConversationDeliveryStatus.FAILED.value,
                DRIP_LAST_ERROR_FIELD: error,
                DRIP_PROCESSOR_FIELD: DEFAULT_PROCESSOR,
                DRIP_NEXT_SEND_DATE_FIELD: iso_now(),
            },
        )


def run_campaigns(limit: int = 50, send_after_queue: bool = False) -> Dict[str, Any]:
    runner = CampaignRunner(send_after_queue=bool(send_after_queue))
    return runner.run(limit)


def get_campaigns_table():
    return CONNECTOR.campaigns().table


if __name__ == "__main__":  # pragma: no cover - manual invocation helper
    import pprint

    pprint.pprint(run_campaigns(limit=int(os.getenv("RUN_LIMIT", "25"))))
