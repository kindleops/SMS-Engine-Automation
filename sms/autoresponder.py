# sms/autoresponder.py
"""
Intent-aware autoresponder (Stages 1→4 only) backed by the schema-driven datastore.

Flow summary
------------
Stage 1 (Ownership Confirmation):
  - "yes" -> Stage 2 (Interest Feeler) and prompt interest.
  - "no", "wrong number", "stop" -> DNC/Opt-Out (no follow-up).
  - "not interested / not selling" -> schedule 30-day follow-up (Drip), mark phone verified.

Stage 2 (Interest Feeler):
  - "yes" -> Stage 3 (Price Qualification), ask their asking price.
  - "no / not selling" -> schedule 30-day follow-up (Drip), mark phone verified.

Stage 3 (Price Qualification):
  - Provides price -> move to Stage 4 (Condition Ask), acknowledge price and ask condition.
  - "what's your offer?" -> move to Stage 4 (Condition Ask), explain we’ll run numbers, ask condition.

Stage 4 (Property Condition):
  - Any condition response -> stay Stage 4 and STOP autoresponder (handoff to team/AI closer).
"""

from __future__ import annotations

import hashlib
import os
import random
import re
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

# ---- Schema & config (stable entry points) -----------------------------------
from sms.airtable_schema import (
    ConversationDirection,
    ConversationProcessor,
    ConversationStage,
    DripStatus,
    conversations_field_map,
    drip_field_map,
    leads_field_map,
    prospects_field_map,
    template_field_map,
)
from sms.config import settings
from sms.dispatcher import get_policy
from sms.runtime import get_logger, iso_now, last_10_digits

# Optional immediate transport (best-effort)
try:
    from sms.message_processor import MessageProcessor
except Exception:  # pragma: no cover
    MessageProcessor = None  # type: ignore

# Optional follow-up hook
try:
    from sms.followup_flow import schedule_from_response
except Exception:  # pragma: no cover
    def schedule_from_response(**_: Any) -> None:
        pass

# Local templates (optional)
try:
    from sms import templates as local_templates
except Exception:  # pragma: no cover
    local_templates = None  # type: ignore

# Optional lead promotion utility
try:
    from sms.lead_promotion import promote_to_lead
except Exception:  # pragma: no cover
    promote_to_lead = None  # type: ignore

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Airtable/datastore facades (CONNECTOR-compatible, with safe fallbacks)
# ---------------------------------------------------------------------------
from sms.datastore import CONNECTOR, list_records, update_record

# Hardening: bring in guaranteed logging fallbacks
try:
    from sms.datastore import create_conversation  # idempotent upsert by TextGrid ID (if provided)
except Exception:
    create_conversation = None  # type: ignore

try:
    from sms.datastore import safe_create_conversation, safe_log_message  # unconditional logging fallbacks
except Exception:
    # If these are truly unavailable, define no-op stubs to avoid NameError
    def safe_create_conversation(fields: dict) -> Optional[dict]:  # type: ignore
        try:
            # Very last-resort write (only if handle/table is present)
            h = CONNECTOR.conversations()
            tbl = getattr(h, "table", None)
            if not tbl:
                return None
            fixed = {k.title() if " " not in k else k: v for k, v in (fields or {}).items()}
            return tbl.create({"fields": fixed})
        except Exception:
            logger.warning("safe_create_conversation not available; skipping.", exc_info=True)
            return None

    def safe_log_message(direction: str, to: str, from_: str, body: str, status: str = "SENT", sid=None, error=None):  # type: ignore
        try:
            h = CONNECTOR.conversations()
            tbl = getattr(h, "table", None)
            if not tbl:
                return None
            return tbl.create({
                "fields": {
                    "Direction": direction,
                    "TextGrid Phone Number": to,
                    "Seller Phone Number": from_,
                    "Message": body or "",
                    "Status": status,
                    "TextGrid ID": sid or "",
                    "Error": error or "",
                    "Timestamp": datetime.now(timezone.utc).isoformat(),
                }
            })
        except Exception:
            logger.warning("safe_log_message not available; skipping.", exc_info=True)
            return None


def conversations():
    return TableFacade(CONNECTOR.conversations(), kind="conversations")

def leads_tbl():
    return TableFacade(CONNECTOR.leads(), kind="leads")

def prospects_tbl():
    return TableFacade(CONNECTOR.prospects(), kind="prospects")

def templates_tbl():
    return TableFacade(CONNECTOR.templates(), kind="templates")

def drip_tbl():
    try:
        return TableFacade(CONNECTOR.drip_queue(), kind="drip")
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Field maps (schema-driven with safe fallbacks)
# ---------------------------------------------------------------------------

CONV_FIELDS = conversations_field_map()
DRIP_FIELDS = drip_field_map()
LEAD_FIELDS = leads_field_map()
PROSPECT_FIELDS = prospects_field_map()
TEMPLATE_FIELDS = template_field_map()

# --- Conversations fields ---
CONV_FROM_FIELD = CONV_FIELDS.get("FROM", "Seller Phone Number")
CONV_TO_FIELD = CONV_FIELDS.get("TO", "TextGrid Phone Number")  # normalized fallback
CONV_BODY_FIELD = CONV_FIELDS.get("BODY", "Message")            # normalized fallback
CONV_STATUS_FIELD = CONV_FIELDS.get("STATUS", "Status")
CONV_DIRECTION_FIELD = CONV_FIELDS.get("DIRECTION", "Direction")
CONV_RECEIVED_AT_FIELD = CONV_FIELDS.get("RECEIVED_AT", "Received At")
CONV_INTENT_FIELD = CONV_FIELDS.get("INTENT", "Intent")
CONV_PROCESSED_BY_FIELD = CONV_FIELDS.get("PROCESSED_BY", "Processed By")
CONV_SENT_AT_FIELD = CONV_FIELDS.get("SENT_AT", "Sent At")
CONV_STAGE_FIELD = CONV_FIELDS.get("STAGE", "Stage")
CONV_PROCESSED_AT_FIELD = CONV_FIELDS.get("PROCESSED_AT", "Processed At")
CONV_TEMPLATE_RECORD_FIELD = CONV_FIELDS.get("TEMPLATE_RECORD_ID", "Template Record ID")
CONV_TEMPLATE_LINK_FIELD = CONV_FIELDS.get("TEMPLATE_LINK", "Template")
CONV_PROSPECT_LINK_FIELD = CONV_FIELDS.get("PROSPECT_LINK", "Prospect")
CONV_LEAD_LINK_FIELD = CONV_FIELDS.get("LEAD_LINK", "Lead")
CONV_PROSPECT_RECORD_FIELD = CONV_FIELDS.get("PROSPECT_RECORD_ID", "Prospect Record ID")
CONV_PROPERTY_ID_FIELD = CONV_FIELDS.get("PROPERTY_ID", "Property ID")
CONV_CAMPAIGN_LINK_FIELD = CONV_FIELDS.get("CAMPAIGN_LINK", "Campaign")
CONV_DRIP_LINK_FIELD = CONV_FIELDS.get("DRIP_QUEUE_LINK", "Drip Queue Link")
CONV_AI_INTENT_FIELD = CONV_FIELDS.get("AI_INTENT", "AI Intent")
CONV_TEXTGRID_ID_FIELD = CONV_FIELDS.get("TEXTGRID_ID", "TextGrid ID")

# --- Candidates for robust extraction ---
CONV_FROM_CANDIDATES = [CONV_FROM_FIELD, "Seller Phone Number", "From", "phone"]
CONV_TO_CANDIDATES = [CONV_TO_FIELD, "TextGrid Phone Number", "TextGrid Number", "From Number", "to_number", "To"]
CONV_BODY_CANDIDATES = [CONV_BODY_FIELD, "Message", "Body", "message"]
CONV_DIRECTION_CANDIDATES = [CONV_DIRECTION_FIELD, "Direction", "direction"]
CONV_PROCESSED_BY_CANDIDATES = [CONV_PROCESSED_BY_FIELD, "Processed By", "processed_by"]

# --- Templates ---
TEMPLATE_INTENT_FIELD = TEMPLATE_FIELDS.get("INTERNAL_ID", "Internal ID")
TEMPLATE_MESSAGE_FIELD = TEMPLATE_FIELDS.get("MESSAGE", "Message")

# --- Drip Queue fields ---
DRIP_STATUS_FIELD = DRIP_FIELDS.get("STATUS", "Status")
DRIP_PROCESSOR_FIELD = DRIP_FIELDS.get("PROCESSOR", "Processor")
DRIP_MARKET_FIELD = DRIP_FIELDS.get("MARKET", "Market")
DRIP_TEMPLATE_LINK_FIELD = DRIP_FIELDS.get("TEMPLATE_LINK", "Template")
DRIP_PROSPECT_LINK_FIELD = DRIP_FIELDS.get("PROSPECT_LINK", "Prospect")
DRIP_CAMPAIGN_LINK_FIELD = DRIP_FIELDS.get("CAMPAIGN_LINK", "Campaign")
DRIP_SELLER_PHONE_FIELD = DRIP_FIELDS.get("SELLER_PHONE", "Seller Phone Number")
DRIP_TEXTGRID_PHONE_FIELD = DRIP_FIELDS.get("TEXTGRID_PHONE", "TextGrid Phone Number")
DRIP_FROM_NUMBER_FIELD = DRIP_FIELDS.get("FROM_NUMBER", "From Number")
DRIP_MESSAGE_PREVIEW_FIELD = DRIP_FIELDS.get("MESSAGE_PREVIEW", "Message")
DRIP_NEXT_SEND_DATE_FIELD = DRIP_FIELDS.get("NEXT_SEND_DATE", "Next Send Date")
DRIP_PROPERTY_ID_FIELD = DRIP_FIELDS.get("PROPERTY_ID", "Property ID")
DRIP_UI_FIELD = DRIP_FIELDS.get("UI", "UI")

# --- Leads ---
LEAD_STATUS_FIELD = LEAD_FIELDS.get("STATUS", "Status")
LEAD_PHONE_FIELD = LEAD_FIELDS.get("PHONE", "Phone")
LEAD_SOURCE_FIELD = LEAD_FIELDS.get("SOURCE", "Source")
LEAD_LAST_MESSAGE = LEAD_FIELDS.get("LAST_MESSAGE", "Last Message")
LEAD_LAST_DIRECTION = LEAD_FIELDS.get("LAST_DIRECTION", "Last Direction")
LEAD_LAST_ACTIVITY = LEAD_FIELDS.get("LAST_ACTIVITY", "Last Activity")

# Conversation delivery statuses (schema-safe)
SAFE_CONVERSATION_STATUS = {"QUEUED", "SENT", "DELIVERED", "FAILED", "UNDELIVERED", "OPT OUT"}

STATUS_ICON = {
    "QUEUED": "⏳",
    "Sending…": "🔄",
    "Sent": "✅",
    "Retry": "🔁",
    "Throttled": "🕒",
    "Failed": "❌",
    "DNC": "⛔",
}

# ---------------------------------------------------------------------------
# Templates (deterministic buckets)
# ---------------------------------------------------------------------------

EVENT_TEMPLATE_POOLS: Dict[str, Tuple[str, ...]] = {
    # Stage 1 outcomes
    "ownership_yes": ("stage2_interest_prompt",),
    "ownership_no": tuple(),  # no reply; DNC/stop
    "interest_no_30d": ("followup_30d_queue",),  # to be queued, not sent now
    # Stage 2 outcomes
    "interest_yes": ("stage3_ask_price",),
    # Stage 3 outcomes
    "ask_offer": ("stage4_condition_prompt",),
    "price_provided": ("stage4_condition_ack_prompt",),
    # Stage 4 outcomes
    "condition_info": ("handoff_ack",),
}

# ---------------------------------------------------------------------------
# Intent lexicon
# ---------------------------------------------------------------------------

STOP_WORDS = {"stop", "unsubscribe", "remove", "quit", "cancel", "end"}
WRONG_NUM_WORDS = {"wrong number", "not mine", "new number"}
NOT_OWNER_PHRASES = {"not the owner", "i sold", "no longer own", "dont own", "do not own", "sold this", "wrong person"}
INTEREST_NO_PHRASES = {
    "not interested",
    "not selling",
    "dont want to sell",
    "don't want to sell",
    "no interest",
    "keep for now",
    "holding for now",
    "keeping it",
    "not looking to sell",
}
ASK_OFFER_PHRASES = {"your offer", "what's your offer", "whats your offer", "what is your offer", "what can you offer"}
COND_WORDS = {"condition", "repairs", "needs work", "renovated", "updated", "tenant", "vacant", "occupied", "as-is", "roof", "hvac"}
YES_WORDS = {"yes", "yeah", "yep", "sure", "affirmative", "correct", "that's me", "that is me", "i am"}
NO_WORDS = {"no", "nope", "nah"}

PRICE_REGEX = re.compile(r"(\$?\s?\d{2,3}(?:,\d{3})*(?:\.\d{1,2})?\b)|(\b\d+\s?k\b)|(\b\d{2,3}k\b)", re.IGNORECASE)

# ---------------------------------------------------------------------------
# Local schema helpers for resilient create() if datastore safe_create is absent
# ---------------------------------------------------------------------------

def _norm_key(s: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s or "").strip().lower())

def _auto_field_map_tbl(tbl: Any) -> Dict[str, str]:
    try:
        probe = tbl.all(max_records=1)
        keys = list(probe[0].get("fields", {}).keys()) if probe else []
    except Exception:
        keys = []
    return {_norm_key(k): k for k in keys}

def _remap_existing_only_tbl(tbl: Any, payload: Dict[str, Any]) -> Dict[str, Any]:
    amap = _auto_field_map_tbl(tbl)
    if not amap:
        return dict(payload or {})
    out: Dict[str, Any] = {}
    for k, v in (payload or {}).items():
        mk = amap.get(_norm_key(k))
        if mk:
            out[mk] = v
    return out

# ---------------------------------------------------------------------------
# Airtable TableFacade with hardened create()
# ---------------------------------------------------------------------------

class TableFacade:
    def __init__(self, handle, kind: str | None = None):
        self.handle = handle
        self.kind = kind  # 'conversations' | 'leads' | 'prospects' | 'templates' | 'drip' | None

    def all(self, view: str | None = None, max_records: Optional[int] = None, **kwargs):
        params: Dict[str, Any] = {}
        if view:
            params["view"] = view
        if max_records is not None:
            params["max_records"] = max_records
        params.update(kwargs)
        return list_records(self.handle, **params)

    def create(self, payload: Dict[str, Any]):
        # Conversations: enforce guaranteed logging
        if self.kind == "conversations":
            sid = (
                payload.get(CONV_TEXTGRID_ID_FIELD)
                or payload.get("TextGridId")
                or payload.get("sid")
                or payload.get("MessageSid")
                or None
            )
            try:
                if create_conversation:
                    return create_conversation(sid, payload)
                # Fallback (schema-agnostic)
                return safe_create_conversation(payload)
            except Exception:
                logger.warning("create_conversation failed; using safe_create_conversation", exc_info=True)
                return safe_create_conversation(payload)

        # Non-conversations: try datastore safe path first (if provided)
        try:
            # Access underlying pyairtable Table if available
            tbl = getattr(self.handle, "table", None)
            if tbl and hasattr(tbl, "create"):
                return tbl.create(_remap_existing_only_tbl(tbl, payload))
        except Exception as e:
            logger.warning(f"Fallback create via underlying table failed: {e}", exc_info=True)

        # No valid path
        logger.warning("No create path available for kind=%s; payload skipped.", self.kind)
        return None

    def update(self, record_id: str, payload: Dict[str, Any]):
        return update_record(self.handle, record_id, payload)

# ---------------------------------------------------------------------------
# Utils
# ---------------------------------------------------------------------------

def _ct_naive(dt_utc: datetime) -> str:
    """
    Convert UTC datetime to America/Chicago naive ISO (seconds precision).
    """
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("America/Chicago")
    except Exception:
        tz = timezone.utc
    return dt_utc.astimezone(tz).replace(tzinfo=None).isoformat(timespec="seconds")

def _get_first(fields: Dict[str, Any], candidates: Iterable[Optional[str]]) -> Optional[Any]:
    for key in candidates:
        if not key:
            continue
        value = fields.get(key)
        if value not in (None, ""):
            return value
    return None

def _normalise_link(value: Any) -> Optional[str]:
    if isinstance(value, list) and value:
        return value[0]
    if isinstance(value, str) and value.strip():
        return value
    return None

def _pick_status(preferred: str) -> str:
    up = preferred.upper()
    return up if up in SAFE_CONVERSATION_STATUS else "DELIVERED"

def _det_rand_choice(key: str, items: List[Any]) -> Any:
    if not items:
        return None
    h = hashlib.md5(key.encode("utf-8")).hexdigest()
    rnd = random.Random(int(h, 16))
    return rnd.choice(items)

def _parse_timestamp(value: Any) -> Optional[datetime]:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)
    except Exception:
        return None

def _recently_responded(fields: Dict[str, Any], processed_by: str) -> bool:
    last_by = str(fields.get(CONV_PROCESSED_BY_FIELD) or "").strip()
    allowed_labels = {processed_by, ConversationProcessor.AUTORESPONDER.value}
    if last_by and last_by not in allowed_labels:
        return False
    candidates = [
        fields.get(CONV_PROCESSED_AT_FIELD),
        fields.get("Processed Time"),
        fields.get(CONV_SENT_AT_FIELD),
        fields.get("Last Sent Time"),
    ]
    timestamp = None
    for val in candidates:
        timestamp = _parse_timestamp(val)
        if timestamp:
            break
    if not timestamp:
        return False
    return datetime.now(timezone.utc) - timestamp < timedelta(minutes=30)

def _resolve_timezone(policy) -> timezone:
    tz_name = settings().QUIET_TZ or getattr(policy, "quiet_tz_name", None)
    tz = None
    if tz_name:
        try:
            from zoneinfo import ZoneInfo
            tz = ZoneInfo(tz_name)
        except Exception:
            tz = None
    if tz is None and getattr(policy, "quiet_tz", None):
        tz = policy.quiet_tz
    if tz is None:
        tz = timezone.utc
    return tz

def _quiet_window(now_utc: datetime, policy) -> Tuple[bool, Optional[datetime]]:
    enabled = settings().QUIET_HOURS_ENFORCED or bool(getattr(policy, "quiet_enforced", False))
    if not enabled:
        return False, None
    start_hour = settings().QUIET_START_HOUR if settings().QUIET_START_HOUR is not None else getattr(policy, "quiet_start_hour", 21)
    end_hour = settings().QUIET_END_HOUR if settings().QUIET_END_HOUR is not None else getattr(policy, "quiet_end_hour", 9)
    tz = _resolve_timezone(policy)

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

# ---------------------------------------------------------------------------
# Stage helpers (canonical write targets)
# ---------------------------------------------------------------------------

STAGE1 = ConversationStage.STAGE_1_OWNERSHIP_CONFIRMATION.value
STAGE2 = ConversationStage.STAGE_2_INTEREST_FEELER.value
STAGE3 = ConversationStage.STAGE_3_PRICE_QUALIFICATION.value
STAGE4 = ConversationStage.STAGE_4_PROPERTY_CONDITION.value
STAGE_DNC = ConversationStage.DNC.value
STAGE_OPTOUT = ConversationStage.OPT_OUT.value

# ---------------------------------------------------------------------------
# Intent classification → event mapping
# ---------------------------------------------------------------------------

def _base_intent(body: str) -> str:
    text = (body or "").lower().strip()
    if not text:
        return "neutral"

    if any(p in text for p in ["who is this", "how did you get", "why are you", "what is this about"]):
        return "inquiry"

    if any(w in text for w in STOP_WORDS):
        return "optout"
    if any(w in text for w in WRONG_NUM_WORDS) or any(w in text for w in NOT_OWNER_PHRASES):
        return "ownership_no"
    if any(w in text for w in INTEREST_NO_PHRASES):
        return "interest_no"

    if any(w in text for w in YES_WORDS):
        return "affirm"
    if any(w in text for w in NO_WORDS):
        return "deny"

    if PRICE_REGEX.search(text):
        return "price_provided"
    if any(w in text for w in ASK_OFFER_PHRASES):
        return "ask_offer"
    if any(w in text for w in COND_WORDS):
        return "condition_info"

    return "neutral"

def _event_for_stage(stage_label: str, base_intent: str) -> str:
    # Stage-agnostic hard stops
    if base_intent == "optout":
        return "optout"
    if base_intent == "ownership_no":
        return "ownership_no"

    if stage_label == STAGE1:
        if base_intent in {"affirm", "inquiry"}:
            return "ownership_yes"
        if base_intent == "deny":
            return "ownership_no"
        if base_intent == "interest_no":
            return "interest_no_30d"
        return "noop"

    if stage_label == STAGE2:
        if base_intent == "affirm":
            return "interest_yes"
        if base_intent in {"deny", "interest_no"}:
            return "interest_no_30d"
        return "noop"

    if stage_label == STAGE3:
        if base_intent == "price_provided":
            return "price_provided"
        if base_intent in {"ask_offer", "affirm"}:
            return "ask_offer"
        if base_intent in {"deny", "interest_no"}:
            return "interest_no_30d"
        return "noop"

    # Stage 4
    if stage_label == STAGE4:
        if base_intent == "condition_info":
            return "condition_info"
        return "noop"

# ---------------------------------------------------------------------------
# AI intent mapping (for analytics)
# ---------------------------------------------------------------------------

AI_INTENT_MAP = {
    "ownership_yes": "interest_detected",
    "ownership_no": "wrong_number",
    "interest_yes": "ask_price",
    "interest_no_30d": "neutral",
    "price_provided": "offer_discussion",
    "ask_offer": "ask_price",
    "condition_info": "condition_question",
    "optout": "not_interested",
    "noop": "neutral",
}

# ---------------------------------------------------------------------------
# Autoresponder service
# ---------------------------------------------------------------------------

class Autoresponder:
    def __init__(self) -> None:
        self.settings = settings()
        self.policy = get_policy()
        self.convos = conversations()
        self.leads = leads_tbl()
        self.prospects = prospects_tbl()
        self.templates = templates_tbl()
        self.drip = drip_tbl()
        self.processed_by = (
            os.getenv("PROCESSED_BY_LABEL") or ConversationProcessor.AUTORESPONDER.value
        ).strip() or ConversationProcessor.AUTORESPONDER.value

        self.summary: Dict[str, Any] = {"processed": 0, "breakdown": {}, "errors": [], "skipped": {}}
        self.templates_by_key = self._index_templates()

        # Phone fields
        self.lead_phone_fields = [v for v in [LEAD_PHONE_FIELD, "Phone", "phone", "Mobile"] if v]
        self.prospect_phone_fields = [
            v
            for v in [
                PROSPECT_FIELDS.get("PHONE_PRIMARY"),
                PROSPECT_FIELDS.get("PHONE_PRIMARY_LINKED"),
                PROSPECT_FIELDS.get("PHONE_SECONDARY"),
                PROSPECT_FIELDS.get("PHONE_SECONDARY_LINKED"),
                "Phone",
                "phone",
            ]
            if v
        ]

        # Prospect field mappings for comprehensive updates
        self.prospect_field_map = {
            "SELLER_ASKING_PRICE": PROSPECT_FIELDS.get("SELLER_ASKING_PRICE", "Seller Asking Price"),
            "CONDITION_NOTES": PROSPECT_FIELDS.get("CONDITION_NOTES", "Condition Notes"),
            "TIMELINE_MOTIVATION": PROSPECT_FIELDS.get("TIMELINE_MOTIVATION", "Timeline / Motivation"),
            "LAST_INBOUND": PROSPECT_FIELDS.get("LAST_INBOUND", "Last Inbound"),
            "LAST_OUTBOUND": PROSPECT_FIELDS.get("LAST_OUTBOUND", "Last Outbound"),
            "LAST_ACTIVITY": PROSPECT_FIELDS.get("LAST_ACTIVITY", "Last Activity"),
            "OWNERSHIP_CONFIRMED_DATE": PROSPECT_FIELDS.get("OWNERSHIP_CONFIRMED_DATE", "Ownership Confirmation Timeline"),
            "LEAD_PROMOTION_DATE": PROSPECT_FIELDS.get("LEAD_PROMOTION_DATE", "Lead Promotion Date"),
            "PHONE_1_VERIFIED": PROSPECT_FIELDS.get("PHONE_PRIMARY_VERIFIED", "Phone 1 Ownership Verified"),
            "PHONE_2_VERIFIED": PROSPECT_FIELDS.get("PHONE_SECONDARY_VERIFIED", "Phone 2 Ownership Verified"),
            "INTENT_LAST_DETECTED": PROSPECT_FIELDS.get("INTENT_LAST_DETECTED", "Intent Last Detected"),
            "LAST_DIRECTION": PROSPECT_FIELDS.get("LAST_DIRECTION", "Last Direction"),
            "ACTIVE_PHONE_SLOT": PROSPECT_FIELDS.get("ACTIVE_PHONE_SLOT", "Active Phone Slot"),
            "LAST_TRIED_SLOT": PROSPECT_FIELDS.get("LAST_TRIED_SLOT", "Last Tried Slot"),
            "TEXTGRID_PHONE": PROSPECT_FIELDS.get("TEXTGRID_PHONE", "TextGrid Phone Number"),
            "LAST_MESSAGE": PROSPECT_FIELDS.get("LAST_MESSAGE", "Last Message"),
            "REPLY_COUNT": PROSPECT_FIELDS.get("REPLY_COUNT", "Reply Count"),
            "OPT_OUT": PROSPECT_FIELDS.get("OPT_OUT", "Opt Out?"),
            "SEND_COUNT": PROSPECT_FIELDS.get("SEND_COUNT", "Send Count"),
            "STAGE": PROSPECT_FIELDS.get("STAGE", "Stage"),
            "STATUS": PROSPECT_FIELDS.get("STATUS", "Status"),
        }

    # -------------------------- Template indexing & selection
    def _index_templates(self) -> Dict[str, List[Dict[str, Any]]]:
        pools: Dict[str, List[Dict[str, Any]]] = {}
        try:
            records = self.templates.all()
        except Exception:
            records = []
        for rec in records:
            fields = rec.get("fields", {}) or {}
            key = str(fields.get(TEMPLATE_INTENT_FIELD) or "").strip().lower()
            if not key:
                continue
            pools.setdefault(key, []).append(rec)
        return pools

    def _pick_message(
        self, pool_keys: Tuple[str, ...], personalization: Dict[str, str], rand_key: str
    ) -> Tuple[str, Optional[str], Optional[str]]:
        for pool in pool_keys:
            items = self.templates_by_key.get(pool, [])
            if items:
                chosen = _det_rand_choice(rand_key + "::" + pool, items)
                if not chosen:
                    continue
                fields = chosen.get("fields", {}) or {}
                raw = str(fields.get(TEMPLATE_MESSAGE_FIELD) or "").strip()
                if not raw and local_templates:
                    try:
                        raw = local_templates.get_template(pool, personalization)
                    except Exception:
                        raw = ""
                try:
                    msg = raw.format(**personalization) if raw else ""
                except Exception as e:
                    logger.debug(f"Template format fallback (missing keys?): {e}; raw kept.")
                    msg = raw
                return (msg or "Thanks for the reply.", chosen.get("id"), pool)
        return ("Thanks for the reply.", None, None)

    # -------------------------- Fetch inbound
    def _fetch_inbound(self, limit: int) -> List[Dict[str, Any]]:
        view = os.getenv("CONV_VIEW_INBOUND", "Unprocessed Inbounds")
        try:
            records = self.convos.all(view=view, max_records=limit)
            if records:
                return records
        except Exception:
            logger.warning("Failed to fetch Conversations view '%s'; falling back to scan", view, exc_info=True)

        # Fallback: INBOUND & unprocessed
        fallback = []
        try:
            scan = self.convos.all(max_records=limit * 2)
        except Exception:
            scan = []
        for record in scan:
            fields = record.get("fields", {}) or {}
            direction = str(_get_first(fields, CONV_DIRECTION_CANDIDATES) or "").upper()
            processed_by = _get_first(fields, CONV_PROCESSED_BY_CANDIDATES)
            if direction in ("IN", "INBOUND") and not processed_by:
                fallback.append(record)
            if len(fallback) >= limit:
                break
        return fallback

    # -------------------------- Prospect helpers
    def _mark_phone_verified(self, prospect: Optional[Dict[str, Any]], phone: str) -> None:
        if not prospect or not phone:
            return
        fields = prospect.get("fields", {}) or {}

        def _matching_phone_field(prospect_fields: Dict[str, Any], digits: Optional[str]) -> Optional[str]:
            if not digits:
                return None
            mapping = {
                "PHONE_PRIMARY": PROSPECT_FIELDS.get("PHONE_PRIMARY"),
                "PHONE_PRIMARY_LINKED": PROSPECT_FIELDS.get("PHONE_PRIMARY_LINKED"),
                "PHONE_SECONDARY": PROSPECT_FIELDS.get("PHONE_SECONDARY"),
                "PHONE_SECONDARY_LINKED": PROSPECT_FIELDS.get("PHONE_SECONDARY_LINKED"),
            }
            for key, column in mapping.items():
                if not column:
                    continue
                if last_10_digits(prospect_fields.get(column)) == digits:
                    return key
            return None

        def _verified_field_for(key: Optional[str]) -> Optional[str]:
            if key in ("PHONE_PRIMARY", "PHONE_PRIMARY_LINKED"):
                return PROSPECT_FIELDS.get("PHONE_PRIMARY_VERIFIED")
            if key in ("PHONE_SECONDARY", "PHONE_SECONDARY_LINKED"):
                return PROSPECT_FIELDS.get("PHONE_SECONDARY_VERIFIED")
            return None

        digits = last_10_digits(phone)
        matched_key = _matching_phone_field(fields, digits)
        verified_column = _verified_field_for(matched_key)
        if verified_column:
            try:
                self.prospects.update(prospect["id"], {verified_column: True})
            except Exception:
                pass

    def _find_record_by_phone(self, table, candidates: List[Optional[str]], phone: str) -> Optional[Dict[str, Any]]:
        digits = last_10_digits(phone)
        if not digits:
            return None
        clean_fields = [c for c in candidates if c]
        try:
            rows = table.all()
        except Exception:
            rows = []
        for record in rows:
            fields = record.get("fields", {}) or {}
            for field in clean_fields:
                if last_10_digits(fields.get(field)) == digits:
                    return record
        return None

    def _find_prospect(self, phone: str) -> Optional[Dict[str, Any]]:
        return self._find_record_by_phone(self.prospects, self.prospect_phone_fields, phone)

    # -------------------------- Prospect comprehensive updates
    def _extract_price_from_message(self, body: str) -> Optional[str]:
        """Extract price information from message text with enhanced pattern matching"""
        if not body:
            return None
        
        text = body.lower()
        
        # Enhanced price patterns including more variations
        import re
        
        # Pattern 1: Standard price formats ($250,000 or $250000)
        standard_pattern = r'\$\s*(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)'
        
        # Pattern 2: 'k' notation (250k, 250K)
        k_pattern = r'(\d{1,4})\s*k(?:\s|$|[^\w])'
        
        # Pattern 3: Written amounts (two hundred fifty thousand)
        written_pattern = r'((?:one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty|thirty|forty|fifty|sixty|seventy|eighty|ninety|hundred|thousand|million)\s*)+(?:dollars?)?'
        
        # Pattern 4: Around/about patterns (around 250k, about $250,000)
        around_pattern = r'(?:around|about|approximately|roughly)\s*[\$]?\s*(\d{1,3}(?:,\d{3})*|\d{1,4}k)'
        
        # Try standard pattern first
        standard_matches = re.findall(standard_pattern, text)
        if standard_matches:
            price = standard_matches[0].replace(',', '').strip()
            # Validate reasonable price range (25k to 10M)
            try:
                price_val = float(price)
                if 25000 <= price_val <= 10000000:
                    return price
            except ValueError:
                pass
        
        # Try 'k' notation
        k_matches = re.findall(k_pattern, text)
        if k_matches:
            try:
                k_value = float(k_matches[0])
                if 25 <= k_value <= 10000:  # 25k to 10Mk
                    return str(int(k_value * 1000))
            except ValueError:
                pass
        
        # Try around/about patterns
        around_matches = re.findall(around_pattern, text)
        if around_matches:
            price_text = around_matches[0].replace('$', '').replace(',', '').strip()
            if price_text.endswith('k'):
                try:
                    k_value = float(price_text[:-1])
                    if 25 <= k_value <= 10000:
                        return str(int(k_value * 1000))
                except ValueError:
                    pass
            else:
                try:
                    price_val = float(price_text)
                    if 25000 <= price_val <= 10000000:
                        return price_text
                except ValueError:
                    pass
        
        # Fallback to original PRICE_REGEX
        price_matches = PRICE_REGEX.findall(text)
        if price_matches:
            for match in price_matches:
                if match[0]:  # Full price format
                    return match[0].replace(',', '').strip()
                elif match[1] or match[2]:  # 'k' format
                    k_value = (match[1] or match[2]).replace('k', '').strip()
                    try:
                        return str(int(float(k_value)) * 1000)
                    except ValueError:
                        continue
        
        return None

    def _extract_condition_info(self, body: str) -> Optional[str]:
        """Extract condition information from message text with enhanced analysis"""
        if not body:
            return None
        
        text = body.lower()
        condition_indicators = []
        
        # Enhanced condition keywords
        enhanced_cond_words = list(COND_WORDS) + [
            "renovated", "updated", "new", "old", "vintage", "remodeled", "restored",
            "needs work", "fixer upper", "handyman special", "as-is", "move-in ready",
            "turnkey", "cosmetic", "structural", "foundation", "electrical", "plumbing",
            "hvac", "roof", "flooring", "kitchen", "bathroom", "paint", "carpet",
            "appliances", "windows", "siding", "landscaping", "pool", "deck", "garage"
        ]
        
        # Check for condition-related keywords with enhanced context
        for word in enhanced_cond_words:
            if word in text:
                # Extract surrounding context (up to 15 words around the keyword)
                words = body.split()
                for i, w in enumerate(words):
                    if word in w.lower():
                        start = max(0, i - 7)
                        end = min(len(words), i + 8)
                        context = ' '.join(words[start:end])
                        condition_indicators.append(context.strip())
                        break
        
        # Look for specific condition patterns
        import re
        
        # Pattern for "needs X" statements
        needs_pattern = r'needs?\s+(?:a\s+)?(?:new\s+)?(\w+(?:\s+\w+){0,2})'
        needs_matches = re.findall(needs_pattern, text)
        for match in needs_matches:
            condition_indicators.append(f"needs {match}")
        
        # Pattern for "X is/are Y" statements about condition
        condition_statement_pattern = r'(roof|foundation|kitchen|bathroom|flooring|hvac|plumbing|electrical|windows)\s+(?:is|are)\s+(\w+(?:\s+\w+){0,2})'
        condition_matches = re.findall(condition_statement_pattern, text)
        for item, condition in condition_matches:
            condition_indicators.append(f"{item} is {condition}")
        
        # Remove duplicates while preserving order
        unique_indicators = []
        for indicator in condition_indicators:
            if indicator not in unique_indicators:
                unique_indicators.append(indicator)
        
        return '; '.join(unique_indicators[:3]) if unique_indicators else None  # Limit to top 3 most relevant

    def _extract_timeline_motivation(self, body: str) -> Optional[str]:
        """Extract timeline and motivation information from message text with enhanced patterns"""
        if not body:
            return None
        
        text = body.lower()
        
        # Enhanced timeline and motivation keywords
        timeline_words = {
            "urgent", "asap", "soon", "immediately", "quickly", "fast", "rush",
            "month", "months", "week", "weeks", "year", "years", "day", "days",
            "deadline", "date", "timeline", "schedule", "time frame",
            "move", "moving", "relocate", "relocating", "relocation",
            "divorce", "divorcing", "separated", "separation",
            "financial", "finances", "money", "cash", "debt", "bills", "mortgage",
            "foreclosure", "foreclosing", "behind", "payments",
            "inheritance", "inherited", "estate", "probate",
            "job", "work", "employment", "transfer", "promotion",
            "health", "medical", "illness", "sick", "hospital",
            "family", "children", "kids", "school", "education",
            "retirement", "retiring", "downsize", "downsizing",
            "upgrade", "upgrading", "bigger", "smaller", "expand"
        }
        
        timeline_indicators = []
        
        # Check for timeline/motivation keywords with enhanced context
        for word in timeline_words:
            if word in text:
                words = body.split()
                for i, w in enumerate(words):
                    if word in w.lower():
                        start = max(0, i - 6)
                        end = min(len(words), i + 7)
                        context = ' '.join(words[start:end])
                        timeline_indicators.append(context.strip())
                        break
        
        # Look for specific timeline patterns
        import re
        
        # Pattern for "need to sell by/before X"
        deadline_pattern = r'(?:need|have|must)\s+to\s+sell\s+(?:by|before|within)\s+(\w+(?:\s+\w+){0,3})'
        deadline_matches = re.findall(deadline_pattern, text)
        for match in deadline_matches:
            timeline_indicators.append(f"deadline: {match}")
        
        # Pattern for "because of X" motivation
        motivation_pattern = r'because\s+(?:of\s+)?(\w+(?:\s+\w+){0,4})'
        motivation_matches = re.findall(motivation_pattern, text)
        for match in motivation_matches:
            timeline_indicators.append(f"motivation: {match}")
        
        # Pattern for "due to X" motivation
        due_to_pattern = r'due\s+to\s+(\w+(?:\s+\w+){0,4})'
        due_to_matches = re.findall(due_to_pattern, text)
        for match in due_to_matches:
            timeline_indicators.append(f"due to: {match}")
        
        # Pattern for time expressions (in X months, within X weeks)
        time_expression_pattern = r'(?:in|within|by)\s+(\d+\s+(?:day|week|month|year)s?)'
        time_matches = re.findall(time_expression_pattern, text)
        for match in time_matches:
            timeline_indicators.append(f"timeframe: {match}")
        
        # Remove duplicates while preserving order
        unique_indicators = []
        for indicator in timeline_indicators:
            if indicator not in unique_indicators:
                unique_indicators.append(indicator)
        
        return '; '.join(unique_indicators[:3]) if unique_indicators else None  # Limit to top 3 most relevant

    def _determine_active_phone_slot(self, prospect_record: Optional[Dict[str, Any]], used_phone: str) -> str:
        """Determine which phone slot (1 or 2) is active based on the phone used"""
        if not prospect_record:
            return "1"  # Default to slot 1
        
        fields = prospect_record.get("fields", {}) or {}
        phone1 = fields.get(PROSPECT_FIELDS.get("PHONE_PRIMARY")) or fields.get(PROSPECT_FIELDS.get("PHONE_PRIMARY_LINKED"))
        phone2 = fields.get(PROSPECT_FIELDS.get("PHONE_SECONDARY")) or fields.get(PROSPECT_FIELDS.get("PHONE_SECONDARY_LINKED"))
        
        digits_used = last_10_digits(used_phone)
        
        if phone2 and last_10_digits(phone2) == digits_used:
            return "2"
        return "1"

    def _extract_property_details(self, message: str) -> Optional[str]:
        """Extract property details like size, bedrooms, etc. from message"""
        text = message.lower()
        property_details = []
        
        # Look for bedroom/bathroom info
        import re
        bed_bath_pattern = r'(\d+)\s*(bed|bedroom|br|bath|bathroom|ba)'
        matches = re.findall(bed_bath_pattern, text)
        if matches:
            property_details.extend([f"{count} {room_type}" for count, room_type in matches])
        
        # Look for square footage
        sqft_pattern = r'(\d+,?\d*)\s*(sq\s*ft|square\s*feet|sqft)'
        sqft_match = re.search(sqft_pattern, text)
        if sqft_match:
            property_details.append(f"{sqft_match.group(1)} sq ft")
        
        # Look for property type
        property_types = ['house', 'condo', 'townhouse', 'duplex', 'apartment', 'mobile home', 'manufactured']
        for prop_type in property_types:
            if prop_type in text:
                property_details.append(prop_type)
                break
        
        return "; ".join(property_details) if property_details else None
    
    def _extract_contact_preferences(self, message: str) -> Optional[str]:
        """Extract communication preferences from message"""
        text = message.lower()
        preferences = []
        
        if any(phrase in text for phrase in ['call me', 'phone me', 'give me a call']):
            preferences.append("prefers calls")
        if any(phrase in text for phrase in ['text me', 'send me a text', 'message me']):
            preferences.append("prefers texts")
        if any(phrase in text for phrase in ['email me', 'send me an email']):
            preferences.append("prefers email")
        if any(phrase in text for phrase in ['morning', 'before noon']):
            preferences.append("morning contact")
        if any(phrase in text for phrase in ['evening', 'after work', 'after 5']):
            preferences.append("evening contact")
        
        return "; ".join(preferences) if preferences else None
    
    def _assess_urgency_level(self, message: str, event: str) -> int:
        """Assess urgency level from 1-5 based on message content and event"""
        text = message.lower()
        urgency = 1
        
        # Event-based urgency
        if event in ['ownership_yes', 'interest_yes']:
            urgency += 1
        if event in ['price_provided', 'ask_offer']:
            urgency += 2
        
        # Keyword-based urgency
        high_urgency_words = ['urgent', 'asap', 'quickly', 'soon', 'deadline', 'foreclosure', 'emergency']
        medium_urgency_words = ['need to sell', 'moving', 'relocating', 'divorce', 'financial']
        
        if any(word in text for word in high_urgency_words):
            urgency += 2
        elif any(word in text for word in medium_urgency_words):
            urgency += 1
        
        return min(urgency, 5)
    
    def _calculate_engagement_score(self, message: str, event: str) -> int:
        """Calculate engagement score from 1-10 based on message quality"""
        score = 5  # baseline
        
        # Message length indicates engagement
        if len(message) > 100:
            score += 2
        elif len(message) > 50:
            score += 1
        
        # Event quality
        high_engagement_events = ['price_provided', 'ask_offer', 'condition_info']
        medium_engagement_events = ['ownership_yes', 'interest_yes']
        
        if event in high_engagement_events:
            score += 3
        elif event in medium_engagement_events:
            score += 2
        
        # Question asking indicates engagement
        if '?' in message:
            score += 1
        
        return min(score, 10)
    
    def _calculate_response_time(self, last_outbound: str, current_time: str) -> Optional[str]:
        """Calculate response time between outbound and inbound messages"""
        try:
            from datetime import datetime
            last_out = datetime.fromisoformat(last_outbound.replace('Z', '+00:00'))
            current = datetime.fromisoformat(current_time.replace('Z', '+00:00'))
            diff = current - last_out
            
            if diff.days > 0:
                return f"{diff.days} days"
            elif diff.seconds > 3600:
                return f"{diff.seconds // 3600} hours"
            else:
                return f"{diff.seconds // 60} minutes"
        except:
            return None
    
    def _calculate_ownership_timeline(self, conversation_fields: Dict[str, Any]) -> str:
        """Calculate how long it took to confirm ownership"""
        # This would analyze the conversation history to determine ownership confirmation timeline
        # For now, return a placeholder
        return "Confirmed in current conversation"
    
    def _calculate_intent_confidence(self, message: str, event: str, ai_intent: str) -> float:
        """Calculate confidence score for intent detection"""
        # Simple confidence scoring based on clarity of intent signals
        confidence = 0.5  # baseline
        
        if event in ['ownership_yes', 'ownership_no', 'optout']:
            confidence = 0.9
        elif event in ['price_provided', 'interest_yes']:
            confidence = 0.8
        elif event in ['ask_offer', 'condition_info']:
            confidence = 0.7
        
        # Adjust based on message clarity
        if len(message.split()) >= 5:  # More detailed messages = higher confidence
            confidence += 0.1
        
        return min(confidence, 1.0)
    
    def _generate_conversation_summary(self, message: str, event: str, ai_intent: str) -> Optional[str]:
        """Generate a brief summary of the conversation interaction"""
        summaries = {
            'ownership_yes': 'Confirmed property ownership',
            'ownership_no': 'Denied property ownership',
            'interest_yes': 'Expressed interest in selling',
            'interest_no': 'Not interested in selling',
            'price_provided': f'Provided asking price information',
            'ask_offer': 'Asked about our offer',
            'condition_info': 'Discussed property condition',
            'optout': 'Requested to opt out',
        }
        
        base_summary = summaries.get(event, f'Responded with {ai_intent} intent')
        
        # Add key details if available
        if self._extract_price_from_message(message):
            base_summary += f' (mentioned price)'
        if self._extract_condition_info(message):
            base_summary += f' (discussed condition)'
        
        return base_summary
    
    def _extract_optout_reason(self, message: str) -> str:
        """Extract reason for opting out"""
        text = message.lower()
        
        if any(phrase in text for phrase in ['not interested', "don't want", 'no thanks']):
            return "Not interested"
        elif any(phrase in text for phrase in ['wrong number', 'not my property', 'not the owner']):
            return "Wrong contact"
        elif any(phrase in text for phrase in ['stop', 'unsubscribe', 'remove me']):
            return "Explicit opt-out request"
        else:
            return "General opt-out"
    
    def _calculate_lead_quality_score(
        self, 
        event: str, 
        ai_intent: str, 
        price: Optional[str], 
        condition: Optional[str], 
        timeline: Optional[str], 
        urgency: int
    ) -> int:
        """Calculate overall lead quality score from 1-100"""
        score = 20  # baseline
        
        # Event scoring
        event_scores = {
            'ownership_yes': 25,
            'interest_yes': 20,
            'price_provided': 15,
            'ask_offer': 15,
            'condition_info': 10,
        }
        score += event_scores.get(event, 0)
        
        # Data completeness scoring
        if price:
            score += 15
        if condition:
            score += 10
        if timeline:
            score += 10
        
        # Urgency scoring
        score += urgency * 4
        
        return min(score, 100)

    def _update_prospect_comprehensive(
        self,
        prospect_record: Optional[Dict[str, Any]],
        conversation_fields: Dict[str, Any],
        body: str,
        event: str,
        direction: str,
        from_number: str,
        to_number: str,
        stage: str,
        ai_intent: str
    ) -> None:
        """Comprehensive prospect update with all required fields"""
        if not prospect_record:
            return
        
        prospect_id = prospect_record["id"]
        now_iso = iso_now()
        prospect_fields = prospect_record.get("fields", {}) or {}
        
        # Build comprehensive update payload
        update_payload = {}
        
        # Extract conversation data with enhanced analysis
        extracted_price = self._extract_price_from_message(body)
        condition_info = self._extract_condition_info(body)
        timeline_motivation = self._extract_timeline_motivation(body)
        
        # Additional extractions from conversation content
        property_details = self._extract_property_details(body)
        contact_preferences = self._extract_contact_preferences(body)
        urgency_level = self._assess_urgency_level(body, event)
        
        # Seller Asking Price (if found in conversation)
        if extracted_price and event in {"price_provided", "ask_offer", "ownership_yes", "interest_yes"}:
            update_payload[self.prospect_field_map["SELLER_ASKING_PRICE"]] = extracted_price
        
        # Condition Notes (accumulate from conversations with enhanced analysis)
        if condition_info:
            existing_conditions = prospect_fields.get(self.prospect_field_map["CONDITION_NOTES"], "")
            if existing_conditions:
                # Avoid duplicating similar condition information
                if condition_info.lower() not in existing_conditions.lower():
                    update_payload[self.prospect_field_map["CONDITION_NOTES"]] = f"{existing_conditions}; {condition_info}"
            else:
                update_payload[self.prospect_field_map["CONDITION_NOTES"]] = condition_info
        
        # Timeline / Motivation (accumulate from conversations with priority)
        if timeline_motivation:
            existing_timeline = prospect_fields.get(self.prospect_field_map["TIMELINE_MOTIVATION"], "")
            if existing_timeline:
                # Prioritize more urgent or specific timeline information
                if urgency_level > self._assess_urgency_level(existing_timeline, "neutral"):
                    update_payload[self.prospect_field_map["TIMELINE_MOTIVATION"]] = f"{timeline_motivation}; {existing_timeline}"
                elif timeline_motivation.lower() not in existing_timeline.lower():
                    update_payload[self.prospect_field_map["TIMELINE_MOTIVATION"]] = f"{existing_timeline}; {timeline_motivation}"
            else:
                update_payload[self.prospect_field_map["TIMELINE_MOTIVATION"]] = timeline_motivation
        
        # Property details (size, type, etc.)
        if property_details:
            existing_property_details = prospect_fields.get("Property Details", "")
            if existing_property_details:
                if property_details.lower() not in existing_property_details.lower():
                    update_payload["Property Details"] = f"{existing_property_details}; {property_details}"
            else:
                update_payload["Property Details"] = property_details
        
        # Activity timestamps with enhanced tracking
        if direction.upper() in ("IN", "INBOUND"):
            update_payload[self.prospect_field_map["LAST_INBOUND"]] = now_iso
            # Increment reply count
            current_replies = prospect_fields.get(self.prospect_field_map["REPLY_COUNT"], 0) or 0
            update_payload[self.prospect_field_map["REPLY_COUNT"]] = current_replies + 1
            
            # Track conversation engagement quality
            engagement_score = self._calculate_engagement_score(body, event)
            update_payload["Engagement Score"] = engagement_score
            
            # Track response time if previous outbound exists
            last_outbound = prospect_fields.get(self.prospect_field_map["LAST_OUTBOUND"])
            if last_outbound:
                response_time = self._calculate_response_time(last_outbound, now_iso)
                if response_time:
                    update_payload["Average Response Time"] = response_time
        else:
            update_payload[self.prospect_field_map["LAST_OUTBOUND"]] = now_iso
            # Increment send count
            current_sends = prospect_fields.get(self.prospect_field_map["SEND_COUNT"], 0) or 0
            update_payload[self.prospect_field_map["SEND_COUNT"]] = current_sends + 1
        
        update_payload[self.prospect_field_map["LAST_ACTIVITY"]] = now_iso
        
        # Ownership confirmation tracking with enhanced verification
        if event == "ownership_yes":
            update_payload[self.prospect_field_map["OWNERSHIP_CONFIRMED_DATE"]] = now_iso
            # Mark the active phone as verified
            active_slot = self._determine_active_phone_slot(prospect_record, from_number)
            if active_slot == "1":
                update_payload[self.prospect_field_map["PHONE_1_VERIFIED"]] = True
                update_payload["Phone 1 Verification Date"] = now_iso
            else:
                update_payload[self.prospect_field_map["PHONE_2_VERIFIED"]] = True
                update_payload["Phone 2 Verification Date"] = now_iso
            update_payload[self.prospect_field_map["ACTIVE_PHONE_SLOT"]] = active_slot
            
            # Mark ownership verification timeline
            update_payload["Ownership Confirmation Timeline"] = self._calculate_ownership_timeline(conversation_fields)
        
        # Intent tracking with confidence scoring
        update_payload[self.prospect_field_map["INTENT_LAST_DETECTED"]] = ai_intent
        intent_confidence = self._calculate_intent_confidence(body, event, ai_intent)
        update_payload["Intent Confidence Score"] = intent_confidence
        
        # Direction tracking
        update_payload[self.prospect_field_map["LAST_DIRECTION"]] = direction
        
        # Phone slot tracking with enhanced verification
        active_slot = self._determine_active_phone_slot(prospect_record, from_number)
        update_payload[self.prospect_field_map["LAST_TRIED_SLOT"]] = active_slot
        
        # TextGrid phone number tracking
        textgrid_phone = _get_first(conversation_fields, CONV_TO_CANDIDATES)
        if textgrid_phone:
            update_payload[self.prospect_field_map["TEXTGRID_PHONE"]] = textgrid_phone
        
        # Last message with conversation context
        update_payload[self.prospect_field_map["LAST_MESSAGE"]] = body[:500] if body else ""
        
        # Enhanced conversation tracking
        conversation_summary = self._generate_conversation_summary(body, event, ai_intent)
        if conversation_summary:
            existing_summary = prospect_fields.get("Conversation Summary", "")
            if existing_summary:
                update_payload["Conversation Summary"] = f"{existing_summary}\n{now_iso}: {conversation_summary}"
            else:
                update_payload["Conversation Summary"] = f"{now_iso}: {conversation_summary}"
        
        # Opt out tracking with reason
        if event == "optout":
            update_payload[self.prospect_field_map["OPT_OUT"]] = True
            update_payload["Opt Out Date"] = now_iso
            update_payload["Opt Out Reason"] = self._extract_optout_reason(body)
        
        # Communication preferences extraction
        if contact_preferences:
            update_payload["Communication Preferences"] = contact_preferences
        
        # Market and property information from conversation
        market_info = conversation_fields.get("Market") or prospect_fields.get(PROSPECT_FIELDS.get("MARKET"))
        if market_info:
            update_payload["Market"] = market_info
        
        # Enhanced stage mapping (convert conversation stages to prospect stages)
        prospect_stage_map = {
            STAGE1: "Stage #1 – Ownership Check",
            STAGE2: "Stage #2 – Offer Interest", 
            STAGE3: "Stage #3 – Price/Condition",
            STAGE4: "Stage #3 – Price/Condition",  # Stage 4 still maps to price/condition
            STAGE_OPTOUT: "Opt-Out",
            STAGE_DNC: "Opt-Out"
        }
        if stage in prospect_stage_map:
            update_payload[self.prospect_field_map["STAGE"]] = prospect_stage_map[stage]
        
        # Enhanced status mapping based on events, stages, and conversation quality
        status_map = {
            "ownership_yes": "Owner Verified",
            "interest_yes": "Interested", 
            "price_provided": "Price Disclosed",
            "ask_offer": "Awaiting Offer",
            "condition_info": "Condition Discussed",
            "optout": "Opt-Out",
            "ownership_no": "Not Owner"
        }
        
        if event in status_map:
            update_payload[self.prospect_field_map["STATUS"]] = status_map[event]
        elif direction.upper() in ("IN", "INBOUND"):
            # More nuanced status based on conversation quality
            if urgency_level >= 3:
                update_payload[self.prospect_field_map["STATUS"]] = "Hot Lead"
            elif event in {"affirm", "price_provided", "ask_offer"}:
                update_payload[self.prospect_field_map["STATUS"]] = "Qualified Lead"
            else:
                update_payload[self.prospect_field_map["STATUS"]] = "Replied"
        elif direction.upper() in ("OUT", "OUTBOUND"):
            current_status = prospect_fields.get(self.prospect_field_map["STATUS"])
            if current_status in ("Unmessaged", "Queued"):
                update_payload[self.prospect_field_map["STATUS"]] = "Messaged"
        
        # Lead quality scoring
        lead_quality_score = self._calculate_lead_quality_score(
            event, ai_intent, extracted_price, condition_info, timeline_motivation, urgency_level
        )
        update_payload["Lead Quality Score"] = lead_quality_score
        
        # Update conversation count and progression tracking
        total_conversations = prospect_fields.get("Total Conversations", 0) or 0
        update_payload["Total Conversations"] = total_conversations + 1
        
        # Track stage progression history
        current_stage_field = prospect_fields.get(self.prospect_field_map["STAGE"])
        new_stage_field = update_payload.get(self.prospect_field_map["STAGE"])
        if current_stage_field != new_stage_field and new_stage_field:
            stage_history = prospect_fields.get("Stage History", "") or ""
            stage_entry = f"{now_iso}: {current_stage_field} → {new_stage_field}"
            if stage_history:
                update_payload["Stage History"] = f"{stage_history}\n{stage_entry}"
            else:
                update_payload["Stage History"] = stage_entry
        
        # Apply the update
        try:
            self.prospects.update(prospect_id, update_payload)
            logger.info(f"Updated prospect {prospect_id} with comprehensive data: {len(update_payload)} fields")
        except Exception as exc:
            logger.warning(f"Failed to update prospect {prospect_id}: {exc}")
            self.summary["errors"].append({"prospect": prospect_id, "error": f"prospect update failed: {exc}"})

    def _create_follow_up_campaign_if_needed(self, prospect_record: Optional[Dict[str, Any]], phone: str) -> None:
        """Create a new campaign for prospects where phone verification failed"""
        if not prospect_record:
            return
        
        fields = prospect_record.get("fields", {}) or {}
        
        # Check if any phone is verified
        phone1_verified = fields.get(self.prospect_field_map["PHONE_1_VERIFIED"], False)
        phone2_verified = fields.get(self.prospect_field_map["PHONE_2_VERIFIED"], False)
        
        # If neither phone is verified, we may need to create a follow-up campaign
        if not phone1_verified and not phone2_verified:
            try:
                # This would integrate with your campaign creation system
                # For now, we'll just log it as a placeholder
                logger.info(f"Prospect {prospect_record['id']} needs follow-up campaign - no verified phones")
                
                # TODO: Implement campaign creation logic here
                # This could create a new campaign specifically targeting unverified prospects
                # with alternative phone numbers or different messaging strategies
                
            except Exception as exc:
                logger.warning(f"Failed to create follow-up campaign for prospect {prospect_record['id']}: {exc}")

    # -------------------------- Drip enqueue (canonical)
    def _enqueue_reply(
        self,
        record: Dict[str, Any],
        fields: Dict[str, Any],
        reply_text: str,
        queue_time: datetime,
        template_id: Optional[str],
        prospect_id: Optional[str],
    ) -> Optional[Dict[str, Any]]:
        if not self.drip:
            return None

        campaign_link = _normalise_link(fields.get(CONV_CAMPAIGN_LINK_FIELD))
        payload = {
            DRIP_STATUS_FIELD: DripStatus.QUEUED.value,
            DRIP_PROCESSOR_FIELD: self.processed_by,
            DRIP_MARKET_FIELD: fields.get("Market"),
            DRIP_SELLER_PHONE_FIELD: _get_first(fields, CONV_FROM_CANDIDATES),
            DRIP_TEXTGRID_PHONE_FIELD: _get_first(fields, CONV_TO_CANDIDATES),
            DRIP_FROM_NUMBER_FIELD: _get_first(fields, CONV_TO_CANDIDATES),
            DRIP_MESSAGE_PREVIEW_FIELD: reply_text,
            DRIP_NEXT_SEND_DATE_FIELD: _ct_naive(queue_time),
            DRIP_PROPERTY_ID_FIELD: fields.get(CONV_PROPERTY_ID_FIELD),
            DRIP_UI_FIELD: STATUS_ICON.get("QUEUED", "⏳"),
        }
        if template_id:
            payload[DRIP_TEMPLATE_LINK_FIELD] = [template_id]
        if prospect_id:
            payload[DRIP_PROSPECT_LINK_FIELD] = [prospect_id]
        if campaign_link:
            payload[DRIP_CAMPAIGN_LINK_FIELD] = [campaign_link]

        try:
            created = self.drip.create(payload)
        except Exception as exc:
            self.summary["errors"].append({"conversation": record.get("id"), "error": f"Queue failed: {exc}"})
            return None

        if created and created.get("id"):
            try:
                self.convos.update(
                    record["id"],
                    {
                        CONV_DRIP_LINK_FIELD: [created["id"]],
                        CONV_TEMPLATE_LINK_FIELD: [template_id] if template_id else None,
                    },
                )
            except Exception:
                pass
        return created

    # Back-compat shim (some call sites referenced _enqueue_drip)
    def _enqueue_drip(
        self,
        record: Dict[str, Any],
        fields: Dict[str, Any],
        reply_text: str,
        when: datetime,
        template_id: Optional[str],
        prospect_id: Optional[str],
    ) -> Optional[Dict[str, Any]]:
        return self._enqueue_reply(
            record=record,
            fields=fields,
            reply_text=reply_text,
            queue_time=when,
            template_id=template_id,
            prospect_id=prospect_id,
        )

    # -------------------------- Immediate send (fallback if no drip engine)
    def _send_immediate(
        self,
        from_number: str,
        body: str,
        to_number: Optional[str],
        lead_id: Optional[str],
        property_id: Optional[str],
        *,
        is_quiet: bool,
    ) -> None:
        # Log AR-initiated outbound trail regardless of transport result
        try:
            safe_log_message(
                "OUTBOUND",
                to_number or "",
                from_number,
                body,
                status="QUEUED" if is_quiet else "SENT",
            )
        except Exception:
            pass

        if not MessageProcessor or not to_number:
            return
        try:
            result = MessageProcessor.send(
                phone=from_number,
                body=body,
                lead_id=lead_id,
                property_id=property_id,
                direction="OUT",
                from_number=to_number,
            )
            if (result or {}).get("status") != "sent":
                self.summary["errors"].append({"phone": from_number, "error": (result or {}).get("error", "Send failed")})
        except Exception as exc:
            self.summary["errors"].append({"phone": from_number, "error": f"Immediate send failed: {exc}"})

    # -------------------------- Core loop
    def process(self, limit: int) -> Dict[str, Any]:
        records = self._fetch_inbound(limit)
        if not records:
            return {"ok": False, "processed": 0, "breakdown": {}, "errors": []}

        now = datetime.now(timezone.utc)
        is_quiet, next_allowed = _quiet_window(now, self.policy)

        for record in records:
            try:
                self._process_record(record, is_quiet, next_allowed or now)
            except Exception as exc:
                logger.exception("Autoresponder failed for %s", record.get("id"))
                self.summary["errors"].append({"conversation": record.get("id"), "error": str(exc)})

        self.summary["ok"] = self.summary["processed"] > 0
        return self.summary

    def _process_record(self, record: Dict[str, Any], is_quiet: bool, next_allowed: datetime) -> None:
        fields = record.get("fields", {}) or {}
        from_value = _get_first(fields, CONV_FROM_CANDIDATES)
        body_value = _get_first(fields, CONV_BODY_CANDIDATES)
        if not from_value or not body_value:
            return
        if fields.get(CONV_PROCESSED_BY_FIELD):
            return
        direction = str(_get_first(fields, CONV_DIRECTION_CANDIDATES) or "").upper()
        if direction not in ("IN", "INBOUND"):
            return

        from_number = str(from_value)
        body = str(body_value)
        current_stage = str(fields.get(CONV_STAGE_FIELD) or "").strip() or STAGE1

        prospect_record = self._find_prospect(from_number)
        prospect_fields = (prospect_record or {}).get("fields", {}) or {}
        # Mark phone verified opportunistically (used especially for 30-day follow-up case)
        self._mark_phone_verified(prospect_record, from_number)

        base = _base_intent(body)
        event = _event_for_stage(current_stage, base)
        self.summary["processed"] += 1
        self.summary["breakdown"][event] = self.summary["breakdown"].get(event, 0) + 1

        # Quiet hours scheduling
        send_time = next_allowed if is_quiet else datetime.now(timezone.utc)

        # Stage resolution & reply planning
        next_stage = current_stage
        reply_text: Optional[str] = None
        template_id: Optional[str] = None
        template_pool_used: Optional[str] = None
        queue_reply = False

        # Personalization & deterministic pick
        def _personalize(fields_: Dict[str, Any]) -> Dict[str, str]:
            first = ""
            owner_name = fields_.get(PROSPECT_FIELDS.get("OWNER_NAME"))
            if isinstance(owner_name, str) and owner_name.strip():
                first = owner_name.split()[0]
            else:
                owner_first = fields_.get(PROSPECT_FIELDS.get("OWNER_FIRST_NAME"))
                if isinstance(owner_first, str):
                    first = owner_first.strip()
            if not first:
                first = "there"
            address = (
                fields_.get(PROSPECT_FIELDS.get("PROPERTY_ADDRESS"))
                or fields_.get("Property Address")
                or fields_.get("Address")
                or "your property"
            )
            city = (
                fields_.get(PROSPECT_FIELDS.get("PROPERTY_CITY"))
                or fields_.get("Property City")
                or fields_.get("City")
                or ""
            )
            return {"First": first, "Address": address, "Property City": city}

        personalization = _personalize(prospect_fields)
        rand_key = f"{last_10_digits(from_number) or from_number}:{event}"
        ai_intent = AI_INTENT_MAP.get(event, "other")

        # Hard-stop events
        if event == "optout":
            self._update_prospect_comprehensive(
                prospect_record=prospect_record,
                conversation_fields=fields,
                body=body,
                event="optout",
                direction="INBOUND",
                from_number=from_number,
                to_number=_get_first(fields, CONV_TO_CANDIDATES),
                stage=STAGE_OPTOUT,
                ai_intent=ai_intent
            )
            self._update_conversation(
                record["id"], status=_pick_status("OPT OUT"), stage=STAGE_OPTOUT, ai_intent=ai_intent,
                lead_id=None, prospect_id=_normalise_link(fields.get(CONV_PROSPECT_RECORD_FIELD)) or (prospect_record or {}).get("id")
            )
            return

        if event == "ownership_no":
            self._update_prospect_comprehensive(
                prospect_record=prospect_record,
                conversation_fields=fields,
                body=body,
                event="ownership_no",
                direction="INBOUND",
                from_number=from_number,
                to_number=_get_first(fields, CONV_TO_CANDIDATES),
                stage=STAGE_DNC,
                ai_intent=ai_intent
            )
            self._update_conversation(
                record["id"], status=_pick_status("DELIVERED"), stage=STAGE_DNC, ai_intent=ai_intent,
                lead_id=None, prospect_id=_normalise_link(fields.get(CONV_PROSPECT_RECORD_FIELD)) or (prospect_record or {}).get("id")
            )
            return

        # 30-day follow-up path
        if event == "interest_no_30d":
            pool = EVENT_TEMPLATE_POOLS.get("interest_no_30d", tuple())
            preview, tpl_id, pool_used = self._pick_message(pool, personalization, rand_key)
            drip_when = datetime.now(timezone.utc) + timedelta(days=30)
            if self.drip:
                self._enqueue_drip(record, fields, preview, drip_when, tpl_id, (prospect_record or {}).get("id"))
            self._update_conversation(
                record["id"], status=_pick_status("DELIVERED"), stage=STAGE2, ai_intent=ai_intent,
                lead_id=None, prospect_id=(prospect_record or {}).get("id") or _normalise_link(fields.get(CONV_PROSPECT_RECORD_FIELD)),
                template_id=None,
            )
            try:
                schedule_from_response(
                    phone=from_number,
                    intent="followup_30d",
                    lead_id=None,
                    market=fields.get("Market") or prospect_fields.get(PROSPECT_FIELDS.get("MARKET")),
                    property_id=fields.get(CONV_PROPERTY_ID_FIELD),
                    current_stage=STAGE2,
                )
            except Exception:
                pass
            self._update_prospect_comprehensive(
                prospect_record=prospect_record,
                conversation_fields=fields,
                body=body,
                event="interest_no_30d",
                direction="INBOUND",
                from_number=from_number,
                to_number=_get_first(fields, CONV_TO_CANDIDATES),
                stage=STAGE2,
                ai_intent=ai_intent
            )
            return

        # Stage-progress events
        if event == "ownership_yes":
            next_stage = STAGE2
            pool = EVENT_TEMPLATE_POOLS.get("ownership_yes", tuple())
            reply_text, template_id, template_pool_used = self._pick_message(pool, personalization, rand_key)
            queue_reply = True
        elif event == "interest_yes":
            next_stage = STAGE3
            pool = EVENT_TEMPLATE_POOLS.get("interest_yes", tuple())
            reply_text, template_id, template_pool_used = self._pick_message(pool, personalization, rand_key)
            queue_reply = True
        elif event in {"ask_offer", "price_provided"}:
            next_stage = STAGE4
            pool_key = "price_provided" if event == "price_provided" else "ask_offer"
            pool = EVENT_TEMPLATE_POOLS.get(pool_key, tuple())
            reply_text, template_id, template_pool_used = self._pick_message(pool, personalization, rand_key)
            queue_reply = True
        elif event == "condition_info":
            next_stage = STAGE4
            pool = EVENT_TEMPLATE_POOLS.get("condition_info", tuple())
            if pool:
                reply_text, template_id, template_pool_used = self._pick_message(pool, personalization, rand_key)
                queue_reply = bool(reply_text)
        else:  # "noop"
            if _recently_responded(fields, self.processed_by) or current_stage == STAGE4:
                self._update_conversation(
                    record["id"], status=_pick_status("DELIVERED"), stage=current_stage, ai_intent=ai_intent,
                    lead_id=None, prospect_id=(prospect_record or {}).get("id") or _normalise_link(fields.get(CONV_PROSPECT_RECORD_FIELD)),
                )
                self.summary["skipped"]["noop_or_recent"] = self.summary["skipped"].get("noop_or_recent", 0) + 1
                return
            # Nudge forward
            if current_stage == STAGE1:
                next_stage = STAGE2
                pool = EVENT_TEMPLATE_POOLS.get("ownership_yes", tuple())
            elif current_stage == STAGE2:
                next_stage = STAGE3
                pool = EVENT_TEMPLATE_POOLS.get("interest_yes", tuple())
            elif current_stage == STAGE3:
                next_stage = STAGE4
                pool = EVENT_TEMPLATE_POOLS.get("ask_offer", tuple())
            else:
                pool = tuple()
            if pool:
                reply_text, template_id, template_pool_used = self._pick_message(pool, personalization, rand_key)
                queue_reply = bool(reply_text)

        # Create/attach lead ONLY for interested events (Stage 2+ confirmations)
        lead_id, property_id = self._ensure_lead_if_interested(event, from_number, fields, prospect_record)
        prospect_id = (prospect_record or {}).get("id") or _normalise_link(fields.get(CONV_PROSPECT_RECORD_FIELD))

        # Enqueue or send reply
        if queue_reply and reply_text:
            queued = False
            if self.drip:
                queued = bool(self._enqueue_drip(record, fields, reply_text, send_time, template_id, prospect_id))
            if not queued:
                to_number = _get_first(fields, CONV_TO_CANDIDATES)
                self._send_immediate(from_number, reply_text, to_number, lead_id, property_id, is_quiet=is_quiet)

        # Update conversation row
        self._update_conversation(
            record["id"],
            status=_pick_status("DELIVERED"),
            stage=next_stage,
            ai_intent=ai_intent,
            lead_id=lead_id,
            prospect_id=prospect_id,
            template_id=template_id,
        )

        # Update prospect with comprehensive data
        self._update_prospect_comprehensive(
            prospect_record=prospect_record,
            conversation_fields=fields,
            body=body,
            event=event,
            direction="INBOUND",
            from_number=from_number,
            to_number=_get_first(fields, CONV_TO_CANDIDATES),
            stage=next_stage,
            ai_intent=ai_intent
        )

        # Check if follow-up campaign is needed for unverified phones
        self._create_follow_up_campaign_if_needed(prospect_record, from_number)

        # Update lead trail (best-effort)
        if lead_id:
            try:
                self.leads.update(
                    lead_id,
                    {
                        LEAD_LAST_MESSAGE: body[:500],
                        LEAD_LAST_DIRECTION: ConversationDirection.INBOUND.value,
                        LEAD_LAST_ACTIVITY: iso_now(),
                    },
                )
            except Exception:
                pass

        # Update lead promotion date in prospect if lead was created
        if lead_id and event in {"ownership_yes", "interest_yes", "price_provided", "ask_offer", "condition_info"}:
            try:
                self.prospects.update(
                    prospect_id,
                    {self.prospect_field_map["LEAD_PROMOTION_DATE"]: iso_now()}
                )
            except Exception:
                pass

        # Notify follow-up engine (best-effort)
        try:
            schedule_from_response(
                phone=from_number,
                intent=event,
                lead_id=lead_id,
                market=fields.get("Market") or prospect_fields.get(PROSPECT_FIELDS.get("MARKET")),
                property_id=property_id or fields.get(CONV_PROPERTY_ID_FIELD),
                current_stage=next_stage,
            )
        except Exception:
            pass

    # -------------------------- Writes
    def _update_conversation(
        self,
        conv_id: str,
        *,
        status: str,
        stage: str,
        ai_intent: Optional[str],
        lead_id: Optional[str],
        prospect_id: Optional[str],
        template_id: Optional[str] = None,
    ) -> None:
        payload = {
            CONV_STATUS_FIELD: status,
            CONV_PROCESSED_BY_FIELD: self.processed_by,
            CONV_PROCESSED_AT_FIELD: iso_now(),
            CONV_STAGE_FIELD: stage,
        }
        if CONV_AI_INTENT_FIELD and ai_intent:
            payload[CONV_AI_INTENT_FIELD] = ai_intent
        if template_id:
            payload[CONV_TEMPLATE_RECORD_FIELD] = template_id
            payload[CONV_TEMPLATE_LINK_FIELD] = [template_id]
        if lead_id:
            payload[CONV_LEAD_LINK_FIELD] = [lead_id]
        if prospect_id:
            payload[CONV_PROSPECT_LINK_FIELD] = [prospect_id]
        try:
            self.convos.update(conv_id, payload)
        except Exception as exc:
            self.summary["errors"].append({"conversation": conv_id, "error": f"conversation update failed: {exc}"})

        # System trail entry (best-effort; does not fail pipeline)
        try:
            safe_log_message(
                "SYSTEM",
                "",
                "",
                f"Autoresponder processed {conv_id} → Stage {stage}",
                status=status,
            )
        except Exception:
            pass

    def _ensure_lead_if_interested(
        self,
        event: str,
        from_number: str,
        conv_fields: Dict[str, Any],
        prospect_record: Optional[Dict[str, Any]],
    ) -> Tuple[Optional[str], Optional[str]]:
        """Create/attach a Lead only when the intent shows interest (Stage 2+)."""
        interested = {"ownership_yes", "interest_yes", "price_provided", "ask_offer", "condition_info"}
        property_id = conv_fields.get(CONV_PROPERTY_ID_FIELD)
        if event not in interested:
            return None, property_id

        # Prefer shared promote utility
        if promote_to_lead:
            try:
                lid, pid = promote_to_lead(
                    from_number,
                    source=self.processed_by,
                    conversation_fields=conv_fields,
                )
                return lid, (pid or property_id)
            except Exception:
                pass

        # Best-effort: find by phone, else create
        if self.leads:
            existing = self._find_record_by_phone(self.leads, self.lead_phone_fields, from_number)
            if existing:
                return existing["id"], property_id

            created = self.leads.create({
                LEAD_PHONE_FIELD: from_number,
                (LEAD_STATUS_FIELD or "Lead Status"): "Contacted",
                (LEAD_SOURCE_FIELD or "Source"): self.processed_by,
            })
            if isinstance(created, dict):
                return created.get("id"), property_id

        return None, property_id

# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def run_autoresponder(limit: int = 50) -> Dict[str, Any]:
    service = Autoresponder()
    return service.process(limit)

if __name__ == "__main__":
    limit = int(os.getenv("AR_LIMIT", "50"))
    out = run_autoresponder(limit=limit)
    print("\n=== Autoresponder Summary ===")
    import pprint
    pprint.pprint(out)