"""Authoritative schema and runtime policy derived from README2.md.

This module centralises the canonical field names, select options, and
behavioural rules for the SMS engine.  Every other module imports from this
file to guarantee that the implementation matches the product specification.

The intent of concentrating the specification here is twofold:

1.  Provide a single source of truth for field names so that Airtable tables
    and in-memory fallbacks stay aligned with the README contract.
2.  Expose helper utilities (stage progression, intent promotion, quiet-hour
    checks, etc.) that orchestrators can rely on without duplicating logic.

Whenever the README contract changes we only need to update this module; the
rest of the code automatically follows the new policy.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, Optional


# ---------------------------------------------------------------------------
# Field helpers
# ---------------------------------------------------------------------------


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        return default
    return str(value)


@dataclass(frozen=True)
class ConversationFields:
    stage: str = _env("CONV_STAGE_FIELD", "Stage")
    processed_by: str = _env("CONV_PROCESSED_BY_FIELD", "Processed By")
    intent_detected: str = _env("CONV_INTENT_FIELD", "Intent Detected")
    direction: str = _env("CONV_DIRECTION_FIELD", "Direction")
    delivery_status: str = _env("CONV_STATUS_FIELD", "Delivery Status")
    ai_intent: str = _env("CONV_AI_INTENT_FIELD", "AI Intent")
    textgrid_phone: str = _env("CONV_TEXTGRID_PHONE_FIELD", "TextGrid Phone Number")
    textgrid_id: str = _env("CONV_TEXTGRID_ID_FIELD", "TextGrid ID")
    template_record_id: str = _env("CONV_TEMPLATE_RECORD_FIELD", "Template Record ID")
    seller_phone: str = _env("CONV_SELLER_PHONE_FIELD", "Seller Phone Number")
    prospect_record_id: str = _env("CONV_PROSPECT_LINK_FIELD", "Prospect Record ID")
    lead_record_id: str = _env("CONV_LEAD_LINK_FIELD", "Lead Record ID")
    campaign_record_id: str = _env("CONV_CAMPAIGN_RECORD_FIELD", "Campaign Record ID")
    sent_count: str = _env("CONV_SENT_COUNT_FIELD", "Sent Count")
    reply_count: str = _env("CONV_REPLY_COUNT_FIELD", "Reply Count")
    message_summary: str = _env("CONV_MESSAGE_SUMMARY_FIELD", "Message Summary (AI)")
    message_long: str = _env("CONV_MESSAGE_LONG_FIELD", "Message Long text")
    template_link: str = _env("CONV_TEMPLATE_LINK_FIELD", "Template")
    prospect_link: str = _env("CONV_PROSPECT_REL_FIELD", "Prospect")
    lead_link: str = _env("CONV_LEAD_REL_FIELD", "Lead")
    campaign_link: str = _env("CONV_CAMPAIGN_REL_FIELD", "Campaign")
    record_id_formula: str = _env("CONV_RECORD_ID_FIELD", "Record ID")
    received_time: str = _env("CONV_RECEIVED_AT_FIELD", "Received Time")
    processed_time: str = _env("CONV_PROCESSED_AT_FIELD", "Processed Time")
    last_sent_time: str = _env("CONV_LAST_SENT_FIELD", "Last Sent Time")
    last_retry_time: str = _env("CONV_LAST_RETRY_FIELD", "Last Retry Time")
    ai_response_trigger: str = _env("CONV_AI_TRIGGER_FIELD", "AI Response Trigger")
    message_body: str = _env("CONV_MESSAGE_FIELD", "Message Long text")


@dataclass(frozen=True)
class LeadFields:
    delivered_count: str = "Delivered Count"
    failed_count: str = "Failed Count"
    last_activity: str = "Last Activity"
    last_delivery_status: str = "Last Delivery Status"
    last_direction: str = "Last Direction"
    last_inbound: str = "Last Inbound"
    last_message: str = "Last Message"
    last_outbound: str = "Last Outbound"
    lead_status: str = "Lead Status"
    phone: str = "Phone"
    prospect_link: str = "Prospect"
    reply_count: str = "Reply Count"
    response_time: str = "Response Time (Minutes)"
    sent_count: str = "Sent Count"
    record_id_formula: str = "Record ID"


@dataclass(frozen=True)
class NumbersFields:
    number: str = "Number"
    delivered_today: str = "Delivered Today"
    failed_today: str = "Failed Today"
    optout_today: str = "Opt-Outs Today"
    sent_today: str = "Sent Today"
    delivered_total: str = "Delivered Total"
    failed_total: str = "Failed Total"
    optout_total: str = "Opt-Outs Total"
    sent_total: str = "Sent Total"


@dataclass(frozen=True)
class CampaignFields:
    status: str = "Status"
    start_time: str = "Start Time"
    end_time: str = "End Time"
    last_run_at: str = "Last Run At"


CONVERSATION_FIELDS = ConversationFields()
LEAD_FIELDS = LeadFields()
NUMBER_FIELDS = NumbersFields()
CAMPAIGN_FIELDS = CampaignFields()


# ---------------------------------------------------------------------------
# Enumerations & policy constants
# ---------------------------------------------------------------------------

STAGES: tuple[str, ...] = (
    "STAGE 1 - OWNERSHIP CONFIRMATION",
    "STAGE 2 - INTEREST FEELER",
    "STAGE 3 - PRICE QUALIFICATION",
    "STAGE 4 - PROPERTY CONDITION",
    "STAGE 5 - MOTIVATION / TIMELINE",
    "STAGE 6 - OFFER FOLLOW UP",
    "STAGE 7 - CONTRACT READY",
    "STAGE 8 - CONTRACT SENT",
    "STAGE 9 - CONTRACT FOLLOW UP",
    "OPT OUT",
    "DNC",
)

INTENTS: tuple[str, ...] = (
    "Positive",
    "Neutral",
    "Delay",
    "Reject",
    "DNC",
)

AI_INTENTS: tuple[str, ...] = (
    "intro",
    "who_is_this",
    "how_got_number",
    "interest_detected",
    "ask_price",
    "offer_discussion",
    "motivation_detected",
    "condition_question",
    "not_interested",
    "wrong_number",
    "delay",
    "neutral",
    "other",
    "timeline_question",
)

STOP_TERMS = {
    "stop",
    "unsubscribe",
    "remove",
    "opt out",
    "opt-out",
    "optout",
    "quit",
}

PROMOTION_INTENTS = {
    "Positive",
    "interest_detected",
    "offer_discussion",
    "ask_price",
}

PROMOTION_MIN_STAGE_INDEX = STAGES.index("STAGE 3 - PRICE QUALIFICATION")

DELIVERY_STATUS_VALUES = {
    "QUEUED",
    "SENT",
    "DELIVERED",
    "FAILED",
    "UNDELIVERED",
    "OPT OUT",
}

NORMALIZED_DELIVERY_STATUSES = {
    "queued": "QUEUED",
    "sent": "SENT",
    "delivered": "DELIVERED",
    "failed": "FAILED",
    "undelivered": "UNDELIVERED",
    "optout": "OPT OUT",
    "opt-out": "OPT OUT",
    "opt out": "OPT OUT",
    "canceled": "FAILED",
}


def normalize_delivery_status(value: str) -> str:
    if not value:
        return "UNKNOWN"
    key = re.sub(r"[^a-z]", "", value.strip().lower())
    return NORMALIZED_DELIVERY_STATUSES.get(key, value.upper())


MODEL_PRIORITY = (
    "AI: Phi-3 Mini",
    "AI: Mistral 7B",
    "AI: Gemma 2",
    "AI: GPT-4o",
)


# ---------------------------------------------------------------------------
# Time helpers & quiet hours
# ---------------------------------------------------------------------------


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat(timespec="seconds").replace("+00:00", "Z")


def quiet_hours_enforced() -> bool:
    value = os.getenv("QUIET_HOURS_ENFORCED", "true").strip().lower()
    return value in {"1", "true", "yes", "on"}


def quiet_hour_window() -> tuple[int, int]:
    start = int(os.getenv("QUIET_START_HOUR_LOCAL", "21"))
    end = int(os.getenv("QUIET_END_HOUR_LOCAL", "9"))
    return start, end


def is_quiet_hours(now: Optional[datetime] = None) -> bool:
    if not quiet_hours_enforced():
        return False
    now = now or datetime.now()
    start, end = quiet_hour_window()
    hour = now.hour
    return (hour >= start) or (hour < end)


# ---------------------------------------------------------------------------
# Stage logic helpers
# ---------------------------------------------------------------------------


STAGE_INDEX: Dict[str, int] = {stage: idx for idx, stage in enumerate(STAGES)}


def max_stage(current_stage: Optional[str], candidate_stage: Optional[str]) -> str:
    """Return the highest-priority stage between current and candidate."""

    if candidate_stage is None and current_stage is None:
        return STAGES[0]
    if candidate_stage is None:
        return current_stage or STAGES[0]
    if current_stage is None:
        return candidate_stage

    current_index = STAGE_INDEX.get(current_stage, -1)
    candidate_index = STAGE_INDEX.get(candidate_stage, -1)
    return candidate_stage if candidate_index > current_index else current_stage


def stage_for_intent(intent: Optional[str]) -> Optional[str]:
    if not intent:
        return None
    normalized = intent.strip().lower()
    if normalized in {"interest_detected", "offer_discussion", "ask_price"}:
        return "STAGE 3 - PRICE QUALIFICATION"
    if normalized in {"motivation_detected", "timeline_question"}:
        return "STAGE 5 - MOTIVATION / TIMELINE"
    if normalized in {"condition_question"}:
        return "STAGE 4 - PROPERTY CONDITION"
    if normalized in {"intro", "who_is_this", "how_got_number"}:
        return "STAGE 1 - OWNERSHIP CONFIRMATION"
    if normalized in {"delay", "neutral"}:
        return "STAGE 2 - INTEREST FEELER"
    if normalized in {"other"}:
        return None
    if normalized in {"not_interested", "wrong_number"}:
        return "STAGE 2 - INTEREST FEELER"
    return None


def should_promote(intent_detected: Optional[str], ai_intent: Optional[str], stage: Optional[str]) -> bool:
    stage_index = STAGE_INDEX.get(stage or "", -1)
    if stage_index >= PROMOTION_MIN_STAGE_INDEX:
        return True
    if intent_detected and intent_detected.strip() in PROMOTION_INTENTS:
        return True
    if ai_intent and ai_intent.strip().lower() in PROMOTION_INTENTS:
        return True
    return False


# ---------------------------------------------------------------------------
# Rate limit configuration
# ---------------------------------------------------------------------------


def rate_limits() -> Dict[str, int]:
    return {
        "rate_per_number": int(os.getenv("RATE_PER_NUMBER_PER_MIN", "20")),
        "global_per_min": int(os.getenv("GLOBAL_RATE_PER_MIN", "5000")),
        "daily_limit": int(os.getenv("DAILY_LIMIT", "750")),
        "jitter_seconds": int(os.getenv("JITTER_SECONDS", "2")),
    }


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------


def webhook_token() -> Optional[str]:
    return os.getenv("WEBHOOK_TOKEN")


def cron_token() -> Optional[str]:
    return os.getenv("CRON_TOKEN")


def valid_stop_payload(body: str) -> bool:
    normalized = re.sub(r"\s+", " ", body or "").strip().lower()
    return normalized in STOP_TERMS


# ---------------------------------------------------------------------------
# Phone normalisation helpers
# ---------------------------------------------------------------------------


def normalize_phone(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None
    digits = re.sub(r"\D", "", str(raw))
    if not digits:
        return None
    if digits.startswith("1") and len(digits) == 11:
        digits = digits[1:]
    if len(digits) == 10:
        return "+1" + digits
    if len(digits) == 11 and digits.startswith("1"):
        return "+" + digits
    if digits.startswith("+"):
        return digits
    if raw.startswith("+") and len(digits) >= 10:
        return raw
    return "+" + digits if digits else None


def last_10_digits(phone: Optional[str]) -> Optional[str]:
    digits = re.sub(r"\D", "", phone or "")
    if len(digits) >= 10:
        return digits[-10:]
    return None


PHONE_FIELD_CANDIDATES = (
    "Seller Phone Number",
    "Phone",
    "Mobile",
    "Cell",
    "Phone Number",
    "Primary Phone",
    "Owner Phone",
)


def record_matches_phone(record_fields: Dict[str, object], phone: str) -> bool:
    wanted = last_10_digits(phone)
    if not wanted:
        return False
    for field in PHONE_FIELD_CANDIDATES:
        value = record_fields.get(field)
        if value and last_10_digits(str(value)) == wanted:
            return True
    return False


# ---------------------------------------------------------------------------
# Misc helpers
# ---------------------------------------------------------------------------


def merge_iterable(iterable: Iterable[str]) -> str:
    return ", ".join(sorted(set(iterable)))


