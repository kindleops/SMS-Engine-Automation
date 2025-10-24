"""
🧠 Authoritative Schema & Runtime Policy (v3.1 – Telemetry Edition)
────────────────────────────────────────────────────────────────────
Central source of truth for field names, allowed values, and behavioural
rules across the SMS Engine.

Every module (scheduler, dispatcher, AI router, etc.) imports from here to
stay consistent with the product contract. Updating this file automatically
propagates schema and logic changes to the rest of the system.
"""

from __future__ import annotations
import os, re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, Optional
from enum import Enum

# ---------------------------------------------------------------------------
# Env helper
# ---------------------------------------------------------------------------


def _env(name: str, default: str) -> str:
    v = os.getenv(name)
    return default if not v or not str(v).strip() else str(v)


# ---------------------------------------------------------------------------
# Field definitions
# ---------------------------------------------------------------------------


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
# Enumerations
# ---------------------------------------------------------------------------


class Stage(str, Enum):
    STAGE_1 = "STAGE 1 - OWNERSHIP CONFIRMATION"
    STAGE_2 = "STAGE 2 - INTEREST FEELER"
    STAGE_3 = "STAGE 3 - PRICE QUALIFICATION"
    STAGE_4 = "STAGE 4 - PROPERTY CONDITION"
    STAGE_5 = "STAGE 5 - MOTIVATION / TIMELINE"
    STAGE_6 = "STAGE 6 - OFFER FOLLOW UP"
    STAGE_7 = "STAGE 7 - CONTRACT READY"
    STAGE_8 = "STAGE 8 - CONTRACT SENT"
    STAGE_9 = "STAGE 9 - CONTRACT FOLLOW UP"
    OPT_OUT = "OPT OUT"
    DNC = "DNC"


class Intent(str, Enum):
    POSITIVE = "Positive"
    NEUTRAL = "Neutral"
    DELAY = "Delay"
    REJECT = "Reject"
    DNC = "DNC"


class AIIntent(str, Enum):
    INTRO = "intro"
    WHO_IS_THIS = "who_is_this"
    HOW_GOT_NUMBER = "how_got_number"
    INTEREST = "interest_detected"
    ASK_PRICE = "ask_price"
    OFFER_DISCUSSION = "offer_discussion"
    MOTIVATION = "motivation_detected"
    CONDITION = "condition_question"
    NOT_INTERESTED = "not_interested"
    WRONG_NUMBER = "wrong_number"
    DELAY = "delay"
    NEUTRAL = "neutral"
    OTHER = "other"
    TIMELINE = "timeline_question"


class DeliveryStatus(str, Enum):
    QUEUED = "QUEUED"
    SENT = "SENT"
    DELIVERED = "DELIVERED"
    FAILED = "FAILED"
    UNDELIVERED = "UNDELIVERED"
    OPT_OUT = "OPT OUT"


# Canonical stage constants (for readability)
STAGE_OWNERSHIP = Stage.STAGE_1.value
STAGE_INTEREST = Stage.STAGE_2.value

# ---------------------------------------------------------------------------
# Static tuples and mappings (backward compatible)
# ---------------------------------------------------------------------------

STAGES = tuple(s.value for s in Stage)
INTENTS = tuple(i.value for i in Intent)
AI_INTENTS = tuple(a.value for a in AIIntent)

STOP_TERMS = {
    "stop",
    "unsubscribe",
    "remove",
    "opt out",
    "opt-out",
    "optout",
    "quit",
}

PROMOTION_INTENTS = {"Positive", "interest_detected", "offer_discussion", "ask_price"}
PROMOTION_MIN_STAGE_INDEX = STAGES.index(Stage.STAGE_3.value)

DELIVERY_STATUS_VALUES = set(s.value for s in DeliveryStatus)
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

MODEL_PRIORITY = ("AI: Phi-3 Mini", "AI: Mistral 7B", "AI: Gemma 2", "AI: GPT-4o")

# ---------------------------------------------------------------------------
# Delivery & stage logic
# ---------------------------------------------------------------------------


def normalize_delivery_status(value: str) -> str:
    if not value:
        return "UNKNOWN"
    key = re.sub(r"[^a-z]", "", value.strip().lower())
    return NORMALIZED_DELIVERY_STATUSES.get(key, value.upper())


STAGE_INDEX: Dict[str, int] = {s: i for i, s in enumerate(STAGES)}


def max_stage(current: Optional[str], candidate: Optional[str]) -> str:
    """Return higher-priority stage between current and candidate."""
    if candidate is None and current is None:
        return STAGES[0]
    if candidate is None:
        return current or STAGES[0]
    if current is None:
        return candidate
    ci, ni = STAGE_INDEX.get(current, -1), STAGE_INDEX.get(candidate, -1)
    return candidate if ni > ci else current


def next_stage(stage: str) -> Optional[str]:
    """Return next sequential stage or None if at final."""
    idx = STAGE_INDEX.get(stage, -1)
    if idx >= 0 and idx + 1 < len(STAGES):
        return STAGES[idx + 1]
    return None


def stage_for_intent(intent: Optional[str]) -> Optional[str]:
    if not intent:
        return None
    normalized = intent.strip().lower()
    if normalized in {"interest_detected", "offer_discussion", "ask_price"}:
        return Stage.STAGE_3.value
    if normalized in {"motivation_detected", "timeline_question"}:
        return Stage.STAGE_5.value
    if normalized in {"condition_question"}:
        return Stage.STAGE_4.value
    if normalized in {"intro", "who_is_this", "how_got_number"}:
        return Stage.STAGE_1.value
    if normalized in {"delay", "neutral"}:
        return Stage.STAGE_2.value
    if normalized in {"not_interested", "wrong_number"}:
        return Stage.STAGE_2.value
    return None


def should_promote(intent_detected: Optional[str], ai_intent: Optional[str], stage: Optional[str]) -> bool:
    idx = STAGE_INDEX.get(stage or "", -1)
    if idx >= PROMOTION_MIN_STAGE_INDEX:
        return True
    if intent_detected and intent_detected.strip() in PROMOTION_INTENTS:
        return True
    if ai_intent and ai_intent.strip().lower() in PROMOTION_INTENTS:
        return True
    return False


# ---------------------------------------------------------------------------
# Quiet hours / rate limits
# ---------------------------------------------------------------------------


def quiet_hours_enforced() -> bool:
    v = os.getenv("QUIET_HOURS_ENFORCED", "true").strip().lower()
    return v in {"1", "true", "yes", "on"}


def quiet_hour_window() -> tuple[int, int]:
    start = int(os.getenv("QUIET_START_HOUR_LOCAL", "21"))
    end = int(os.getenv("QUIET_END_HOUR_LOCAL", "9"))
    return start, end


def is_quiet_hours(now: Optional[datetime] = None) -> bool:
    if not quiet_hours_enforced():
        return False
    now = now or datetime.now()
    s, e = quiet_hour_window()
    h = now.hour
    return (h >= s) or (h < e)


def rate_limits() -> Dict[str, int]:
    return {
        "rate_per_number": int(os.getenv("RATE_PER_NUMBER_PER_MIN", "20")),
        "global_per_min": int(os.getenv("GLOBAL_RATE_PER_MIN", "5000")),
        "daily_limit": int(os.getenv("DAILY_LIMIT", "750")),
        "jitter_seconds": int(os.getenv("JITTER_SECONDS", "2")),
    }


# ---------------------------------------------------------------------------
# STOP / phone / misc helpers
# ---------------------------------------------------------------------------


def detect_opt_out(message: str) -> bool:
    msg = (message or "").lower().strip()
    return any(term in msg for term in STOP_TERMS)


def valid_stop_payload(body: str) -> bool:
    return detect_opt_out(body)


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
    return digits[-10:] if len(digits) >= 10 else None


PHONE_FIELD_CANDIDATES = (
    "Seller Phone Number",
    "Phone",
    "Mobile",
    "Cell",
    "Phone Number",
    "Primary Phone",
    "Owner Phone",
)


def record_matches_phone(fields: Dict[str, object], phone: str) -> bool:
    wanted = last_10_digits(phone)
    if not wanted:
        return False
    for f in PHONE_FIELD_CANDIDATES:
        val = fields.get(f)
        if val and last_10_digits(str(val)) == wanted:
            return True
    return False


def merge_iterable(it: Iterable[str]) -> str:
    return ", ".join(sorted(set(it)))


# ---------------------------------------------------------------------------
# Runtime diagnostics
# ---------------------------------------------------------------------------


def summary() -> Dict[str, object]:
    s, e = quiet_hour_window()
    return {
        "quiet_hours_enforced": quiet_hours_enforced(),
        "quiet_start_hour": s,
        "quiet_end_hour": e,
        "rate_limits": rate_limits(),
        "promotion_min_stage": STAGES[PROMOTION_MIN_STAGE_INDEX],
        "model_priority": MODEL_PRIORITY,
        "stop_terms": sorted(STOP_TERMS),
    }


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat(timespec="seconds").replace("+00:00", "Z")
