"""
🚀 Bulletproof Asynchronous Campaign Runner (Final Revision)
-------------------------------------------------------------
Runs continuously inside background workers or manual triggers.

Features:
 • Quiet-hour enforcement
 • Async concurrency with graceful backoff
 • Airtable-safe updates (auto-retry on 422/429)
 • Rate-limiting + jitter
 • Deferred + failed handling with rich context
"""

from __future__ import annotations
import asyncio, os, random, traceback
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

# Core imports
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

logger = get_logger("campaign_runner")

# Field maps
DRIP_FIELDS = drip_field_map()
CONV_FIELDS = conversations_field_map()

# Shortcuts
F_STATUS = DRIP_FIELDS["Status"]
F_MESSAGE = DRIP_FIELDS.get("Message Preview", "Message")
F_PHONE = DRIP_FIELDS.get("SELLER_PHONE", "Seller Phone Number")
F_FROM = DRIP_FIELDS.get("TEXTGRID_PHONE_NUMBER", "TextGrid Phone Number")
F_TEMPLATE = DRIP_FIELDS.get("TEMPLATE_LINK", "Template")
F_CAMPAIGN = DRIP_FIELDS.get("CAMPAIGN_LINK", "Campaign")
F_LEAD = DRIP_FIELDS.get("LEAD_LINK", "Lead")
F_NEXT_SEND = DRIP_FIELDS.get("NEXT_SEND_DATE", "Next Send Date")
F_LAST_SENT = DRIP_FIELDS.get("LAST_SENT", "Last Sent")
F_LAST_ERROR = DRIP_FIELDS.get("LAST_ERROR", "Last Error")
F_PROCESSOR = DRIP_FIELDS.get("PROCESSOR", "Processor")

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


async def _safe_update(table, rid: str, payload: Dict[str, Any], retries: int = 3):
    """Resilient Airtable update with backoff."""
    delay = 0.6
    for i in range(retries):
        try:
            update_record(table, rid, payload)
            return
        except Exception as e:
            msg = str(e)
            if "422" in msg or "429" in msg:
                await asyncio.sleep(delay)
                delay *= 2
                continue
            logger.warning(f"Airtable update failed ({i + 1}/{retries}): {msg}")
            await asyncio.sleep(delay)
    logger.error(f"❌ Gave up updating record {rid}")


# ---------------------------------------------------------------
# Campaign Runner Class
# ---------------------------------------------------------------
class CampaignRunner:
    def __init__(self, *, send_after_queue: bool = False, concurrency: int = 10):
        self.drip = CONNECTOR.drip_queue()
        self.summary: Dict[str, Any] = {"sent": 0, "failed": 0, "deferred": 0, "errors": [], "ok": True}
        self.policy = get_policy()
        self.send_after_queue = send_after_queue
        self.concurrency = max(1, concurrency)

    # -----------------------------------------------------------
    async def run(self, limit: int = 50) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        is_quiet, next_allowed = _quiet_window(now, self.policy)
        records = self._fetch_queue(limit)
        if not records:
            self.summary["note"] = "No eligible records found"
            return self.summary

        # 🚀 NEW: De-duplicate by Seller Phone
        records = self._deduplicate(records)
        if not records:
            self.summary["note"] = "All eligible records were duplicates"
            return self.summary

        if not self.send_after_queue:
            self.summary["note"] = "send_after_queue disabled"
            self.summary["queued_pending"] = len(records)
            return self.summary

        logger.info(f"📤 Processing {len(records)} unique queued drips...")

        sem = asyncio.Semaphore(self.concurrency)
        tasks = [self._process_wrapper(r, sem, is_quiet, next_allowed) for r in records]
        await asyncio.gather(*tasks, return_exceptions=True)

        self.summary["ok"] = not self.summary["errors"]
        logger.info(f"✅ CampaignRunner cycle complete — sent={self.summary['sent']} failed={self.summary['failed']}")
        return self.summary

    # -----------------------------------------------------------
    def _fetch_queue(self, limit: int) -> List[Dict[str, Any]]:
        records = list_records(self.drip, max_records=limit * 3)
        eligible = []
        for r in records:
            f = r.get("fields", {}) or {}
            status = str(f.get(F_STATUS) or "").upper()
            if status in {"QUEUED", "READY", ConversationDeliveryStatus.QUEUED.value}:
                eligible.append(r)
            if len(eligible) >= limit:
                break
        return eligible

    # -----------------------------------------------------------
    def _deduplicate(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Removes duplicates by normalized phone number."""
        seen: set[str] = set()
        unique: List[Dict[str, Any]] = []
        for r in records:
            f = r.get("fields", {}) or {}
            phone = str(f.get(F_PHONE, "")).strip()
            normalized = "".join(ch for ch in phone if ch.isdigit())
            if not normalized:
                continue
            if normalized in seen:
                # Optionally mark duplicates as deferred or ignored
                try:
                    update_record(
                        self.drip,
                        r["id"],
                        {
                            F_STATUS: "SKIPPED",
                            F_LAST_ERROR: "Duplicate phone skipped",
                            F_PROCESSOR: DEFAULT_PROCESSOR,
                        },
                    )
                except Exception:
                    pass
                continue
            seen.add(normalized)
            unique.append(r)
        if len(records) != len(unique):
            logger.info(f"🧹 De-duplication removed {len(records) - len(unique)} duplicates")
        return unique

    # -----------------------------------------------------------
    async def _process_wrapper(self, record, sem: asyncio.Semaphore, is_quiet: bool, next_allowed: Optional[datetime]):
        async with sem:
            try:
                await self._process(record, is_quiet, next_allowed)
            except Exception as exc:
                tb = traceback.format_exc()
                logger.error(f"Unhandled in _process_wrapper: {exc}\n{tb}")
                self.summary["failed"] += 1
                self.summary["errors"].append({"drip_id": record.get("id"), "error": str(exc)})

    # -----------------------------------------------------------
    async def _process(self, record, is_quiet: bool, next_allowed: Optional[datetime]):
        f = record.get("fields", {}) or {}
        msg = f.get(F_MESSAGE)
        phone = f.get(F_PHONE)
        from_number = f.get(F_FROM)

        if not phone or not msg:
            await self._mark_failed(record, "Missing phone or message")
            return

        now = datetime.now(timezone.utc)
        send_at = _parse_dt(f.get(F_NEXT_SEND)) or now
        if is_quiet and send_at <= now:
            await self._defer(record, next_allowed)
            return
        if send_at > now:
            await self._defer(record, send_at)
            return

        await asyncio.sleep(self.policy.jitter())

        try:
            result = await asyncio.to_thread(
                MessageProcessor.send,
                phone=str(phone),
                body=str(msg),
                from_number=str(from_number) if from_number else None,
                template_id=_link_id(f.get(F_TEMPLATE)),
                campaign_id=_link_id(f.get(F_CAMPAIGN)),
                lead_id=_link_id(f.get(F_LEAD)),
                direction=ConversationDirection.OUTBOUND.value,
                metadata={"drip_queue_id": record.get("id")},
            )

            status = (result or {}).get("status")
            sid = (result or {}).get("sid")
            err = (result or {}).get("error")

            if status == "sent":
                await self._mark_sent(record, sid)
                self.summary["sent"] += 1
                logger.info(f"✅ Sent to {phone}")
            elif status == "rate_limited":
                await self._defer(record, now + timedelta(minutes=2))
                self.summary["deferred"] += 1
                logger.info(f"⏸ Rate-limited {phone}")
            else:
                await self._mark_failed(record, err or "Send failed")
                self.summary["failed"] += 1
                logger.warning(f"❌ Failed to send {phone}: {err}")

        except Exception as exc:
            tb = traceback.format_exc()
            await self._mark_failed(record, f"Unhandled: {exc}")
            logger.error(f"❌ Exception in send: {exc}\n{tb}")
            self.summary["failed"] += 1

    # -----------------------------------------------------------
    async def _defer(self, record, when: Optional[datetime]):
        await _safe_update(
            self.drip,
            record["id"],
            {
                F_STATUS: ConversationDeliveryStatus.QUEUED.value,
                F_NEXT_SEND: (when or datetime.now(timezone.utc)).isoformat(),
                F_PROCESSOR: DEFAULT_PROCESSOR,
            },
        )
        self.summary["deferred"] += 1

    # -----------------------------------------------------------
    async def _mark_sent(self, record, sid: Optional[str]):
        await _safe_update(
            self.drip,
            record["id"],
            {
                F_STATUS: ConversationDeliveryStatus.SENT.value,
                F_LAST_SENT: iso_now(),
                F_LAST_ERROR: "",
                F_PROCESSOR: DEFAULT_PROCESSOR,
            },
        )

    # -----------------------------------------------------------
    async def _mark_failed(self, record, error: str):
        await _safe_update(
            self.drip,
            record["id"],
            {
                F_STATUS: ConversationDeliveryStatus.FAILED.value,
                F_LAST_ERROR: (error or "Unknown")[:500],
                F_PROCESSOR: DEFAULT_PROCESSOR,
                F_NEXT_SEND: iso_now(),
            },
        )


# ---------------------------------------------------------------
# Entry Points
# ---------------------------------------------------------------
async def run_campaigns(limit: int = 50, send_after_queue: bool = False, concurrency: int = 10):
    runner = CampaignRunner(send_after_queue=send_after_queue, concurrency=concurrency)
    return await runner.run(limit)


def run_campaigns_sync(limit: int = 50, send_after_queue: bool = False, concurrency: int = 10):
    return asyncio.run(run_campaigns(limit=limit, send_after_queue=send_after_queue, concurrency=concurrency))
