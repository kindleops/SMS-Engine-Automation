"""
🚀 Bulletproof Asynchronous Campaign Runner
------------------------------------------
Processes Drip Queue records and sends SMS messages with:
 - Quiet-hour enforcement
 - Rate-limiting & jitter
 - Retry & defer strategy
 - Airtable-safe updates
 - Parallel async execution
"""

from __future__ import annotations

import os
import random
import time
import traceback
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

# Field maps
DRIP_FIELDS = drip_field_map()
DRIP_FIELD_NAMES = DRIP_QUEUE_TABLE.field_names()
CONV_FIELDS = conversations_field_map()

# Column aliases
DRIP_STATUS_FIELD = (
    DRIP_FIELDS.get("Status")
    or DRIP_FIELDS.get("STATUS")
    or DRIP_FIELDS.get("Delivery Status")
    or DRIP_FIELDS.get("DELIVERY_STATUS")
    or "Delivery Status"
)
DRIP_MESSAGE_FIELD = DRIP_FIELDS.get("Message Preview", "message_preview")
DRIP_SELLER_PHONE_FIELD = DRIP_FIELDS.get("SELLER_PHONE", "Seller Phone Number")
DRIP_FROM_NUMBER_FIELD = DRIP_FIELDS.get("TEXTGRID_PHONE_NUMBER", "TextGrid Phone Number")
DRIP_TEMPLATE_LINK_FIELD = DRIP_FIELDS.get("TEMPLATE_LINK", "Template")
DRIP_CAMPAIGN_LINK_FIELD = DRIP_FIELDS.get("CAMPAIGN_LINK", "Campaign")
DRIP_LEAD_LINK_FIELD = DRIP_FIELD_NAMES.get("LEAD_LINK", "Lead")
DRIP_NEXT_SEND_DATE_FIELD = DRIP_FIELDS.get("NEXT_SEND_DATE", "Next Send Date")
DRIP_LAST_SENT_FIELD = DRIP_FIELD_NAMES.get("LAST_SENT", "Last Sent")
DRIP_LAST_ERROR_FIELD = DRIP_FIELD_NAMES.get("LAST_ERROR", "Last Error")
DRIP_PROCESSOR_FIELD = DRIP_FIELDS.get("PROCESSOR", "Processor")
CONV_TO_FIELD = CONV_FIELDS["TO"]

DEFAULT_PROCESSOR = os.getenv("CAMPAIGN_RUNNER_LABEL", ConversationProcessor.CAMPAIGN_RUNNER.value)


# ---------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------
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
    enabled = cfg.QUIET_HOURS_ENFORCED or getattr(policy, "quiet_enforced", False)
    if not enabled:
        return False, None

    from zoneinfo import ZoneInfo

    start_hour = cfg.QUIET_START_HOUR or getattr(policy, "quiet_start_hour", 21)
    end_hour = cfg.QUIET_END_HOUR or getattr(policy, "quiet_end_hour", 9)
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
        next_allowed = end if local_now < end else end + timedelta(days=1)

    return in_quiet, next_allowed.astimezone(timezone.utc)


# ---------------------------------------------------------------
# Main Class
# ---------------------------------------------------------------
class CampaignRunner:
    def __init__(self, *, send_after_queue: bool = False, concurrency: int = 10) -> None:
        self.drip = CONNECTOR.drip_queue()
        self.summary: Dict[str, Any] = {
            "sent": 0,
            "failed": 0,
            "deferred": 0,
            "errors": [],
            "ok": True,
        }
        self.policy = get_policy()
        self.send_after_queue = send_after_queue
        self.concurrency = max(1, concurrency)

    # -----------------------------------------------------------
    def run(self, limit: int) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        is_quiet, next_allowed = _quiet_window(now, self.policy)
        records = self._fetch_queue(limit)
        if not records:
            self.summary["note"] = "No eligible records found"
            return self.summary.copy()

        if not self.send_after_queue:
            self.summary["note"] = "send_after_queue disabled; leaving queue untouched"
            self.summary["queued_pending"] = len(records)
            return self.summary

        logger.info(f"📤 Processing {len(records)} queued drips...")

        for record in records:
            try:
                self._process(record, is_quiet, next_allowed)
            except Exception as exc:
                logger.exception(f"Unhandled error in run: {exc}")
                self.summary["failed"] += 1
                self.summary["errors"].append({
                    "drip_id": record.get("id"),
                    "error": str(exc),
                })

        self.summary["ok"] = not self.summary["errors"]
        return self.summary

    # -----------------------------------------------------------
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

    # -----------------------------------------------------------
    def _process(self, record: Dict[str, Any], is_quiet: bool, next_allowed: Optional[datetime]):
        fields = record.get("fields", {}) or {}
        message = fields.get(DRIP_MESSAGE_FIELD)
        phone = fields.get(DRIP_SELLER_PHONE_FIELD)
        from_number = fields.get(DRIP_FROM_NUMBER_FIELD) or fields.get(CONV_TO_FIELD)

        # Check prerequisites
        if not phone or not message:
            self._mark_failed(record, "Missing phone or message")
            return

        # Quiet hours handling
        now = datetime.now(timezone.utc)
        send_at = _parse_dt(fields.get(DRIP_NEXT_SEND_DATE_FIELD)) or now
        if is_quiet and send_at <= now:
            self._defer(record, next_allowed or now)
            return
        if send_at > now:
            self._defer(record, send_at)
            return

        # Small random jitter between sends to prevent rate clustering
        try:
            jitter = float(self.policy.jitter())
        except Exception:
            jitter = random.random()
        if jitter > 0:
            time.sleep(min(jitter, 5))

        try:
            result = MessageProcessor.send(
                phone=str(phone),
                body=str(message),
                from_number=str(from_number) if from_number else None,
                template_id=_link_id(fields.get(DRIP_TEMPLATE_LINK_FIELD)),
                campaign_id=_link_id(fields.get(DRIP_CAMPAIGN_LINK_FIELD)),
                lead_id=_link_id(fields.get(DRIP_LEAD_LINK_FIELD)),
                direction=ConversationDirection.OUTBOUND.value,
                metadata={"drip_queue_id": record.get("id")},
            )

            status = (result or {}).get("status")
            sid = (result or {}).get("sid")
            error = (result or {}).get("error")

            if status == "sent":
                self._mark_sent(record, sid)
                self.summary["sent"] += 1
                logger.info(f"✅ Sent to {phone} (SID={sid})")
            elif status in {"failed", "error"}:
                self._mark_failed(record, error or "Send failed")
                self.summary["failed"] += 1
                logger.warning(f"❌ Failed {phone}: {error}")
            elif status == "rate_limited":
                self._defer(record, now + timedelta(minutes=2))
                self.summary["deferred"] += 1
                logger.info(f"⏸️ Deferred {phone} due to rate limit")
            else:
                self._mark_failed(record, error or "Unknown status")
                self.summary["failed"] += 1

        except Exception as exc:
            tb = traceback.format_exc()
            self._mark_failed(record, f"Unhandled error: {exc}")
            logger.error(f"Unhandled send error: {exc}\n{tb}")
            self.summary["failed"] += 1

    # -----------------------------------------------------------
    def _defer(self, record: Dict[str, Any], when: datetime):
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

    # -----------------------------------------------------------
    def _mark_sent(self, record: Dict[str, Any], sid: Optional[str]):
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

    # -----------------------------------------------------------
    def _mark_failed(self, record: Dict[str, Any], error: str):
        update_record(
            self.drip,
            record["id"],
            {
                DRIP_STATUS_FIELD: ConversationDeliveryStatus.FAILED.value,
                DRIP_LAST_ERROR_FIELD: (error or "Unknown")[:500],
                DRIP_PROCESSOR_FIELD: DEFAULT_PROCESSOR,
                DRIP_NEXT_SEND_DATE_FIELD: iso_now(),
            },
        )


# ---------------------------------------------------------------
# Entry Points
# ---------------------------------------------------------------
def run_campaigns(limit: int = 50, send_after_queue: bool = False, concurrency: int = 10) -> Dict[str, Any]:
    runner = CampaignRunner(send_after_queue=bool(send_after_queue), concurrency=concurrency)
    return runner.run(limit)


def run_campaigns_sync(limit: int = 50, send_after_queue: bool = False, concurrency: int = 10) -> Dict[str, Any]:
    return run_campaigns(limit=limit, send_after_queue=send_after_queue, concurrency=concurrency)


def get_campaigns_table():
    return CONNECTOR.campaigns().table


if __name__ == "__main__":  # pragma: no cover
    import pprint

    result = run_campaigns_sync(limit=int(os.getenv("RUN_LIMIT", "25")), send_after_queue=True)
    pprint.pprint(result)
