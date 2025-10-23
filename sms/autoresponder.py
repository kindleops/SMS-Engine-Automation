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

from sms.airtable_schema import (
    CONVERSATIONS_TABLE,
    ConversationDirection,
    ConversationProcessor,
    ConversationStage,
    conversations_field_map,
    drip_field_map,
    leads_field_map,
    prospects_field_map,
    template_field_map,
)
from sms.config import settings
from sms.datastore import CONNECTOR, create_record, list_records, promote_to_lead, update_record
from sms.dispatcher import get_policy
from sms.runtime import get_logger, iso_now, last_10_digits

try:  # Optional immediate transport
    from sms.message_processor import MessageProcessor
except Exception:  # pragma: no cover
    MessageProcessor = None  # type: ignore

try:  # Optional follow-up hook
    from sms.followup_flow import schedule_from_response
except Exception:  # pragma: no cover
    def schedule_from_response(**_: Any) -> None:
        pass

try:  # Local fallback templates for tests
    from sms import templates as local_templates
except Exception:  # pragma: no cover
    local_templates = None  # type: ignore

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Airtable facades
# ---------------------------------------------------------------------------

class TableFacade:
    def __init__(self, handle):
        self.handle = handle

    def all(self, view: str | None = None, max_records: Optional[int] = None, **kwargs):
        params: Dict[str, Any] = {}
        if view:
            params["view"] = view
        if max_records is not None:
            params["max_records"] = max_records
        params.update(kwargs)
        return list_records(self.handle, **params)

    def create(self, payload: Dict[str, Any]):
        return create_record(self.handle, payload)

    def update(self, record_id: str, payload: Dict[str, Any]):
        return update_record(self.handle, record_id, payload)


def conversations():
    return TableFacade(CONNECTOR.conversations())


def leads_tbl():
    return TableFacade(CONNECTOR.leads())


def prospects_tbl():
    return TableFacade(CONNECTOR.prospects())


def templates_tbl():
    return TableFacade(CONNECTOR.templates())


def drip_tbl():
    try:
        return TableFacade(CONNECTOR.drip_queue())
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Field maps
# ---------------------------------------------------------------------------

CONV_FIELDS = conversations_field_map()
CONV_FIELD_NAMES = CONVERSATIONS_TABLE.field_names()
DRIP_FIELDS = drip_field_map()
LEAD_FIELDS = leads_field_map()
PROSPECT_FIELDS = prospects_field_map()
TEMPLATE_FIELDS = template_field_map()

# --- Conversations fields ---
CONV_FROM_FIELD = CONV_FIELDS.get("FROM", "Seller Phone Number")
CONV_TO_FIELD = CONV_FIELDS.get("TO", "TextGrid Number")
CONV_BODY_FIELD = CONV_FIELDS.get("BODY", "Body")
CONV_STATUS_FIELD = CONV_FIELDS.get("STATUS", "Status")
CONV_DIRECTION_FIELD = CONV_FIELDS.get("DIRECTION", "Direction")
CONV_RECEIVED_AT_FIELD = CONV_FIELDS.get("RECEIVED_AT", "Received At")
CONV_INTENT_FIELD = CONV_FIELDS.get("INTENT", "Intent")
CONV_PROCESSED_BY_FIELD = CONV_FIELDS.get("PROCESSED_BY", "Processed By")
CONV_SENT_AT_FIELD = CONV_FIELDS.get("SENT_AT", "Sent At")
CONV_STAGE_FIELD = CONV_FIELD_NAMES.get("STAGE", "Stage")
CONV_PROCESSED_AT_FIELD = CONV_FIELD_NAMES.get("PROCESSED_AT", "Processed At")
CONV_TEMPLATE_RECORD_FIELD = CONV_FIELD_NAMES.get("TEMPLATE_RECORD_ID", "Template Record ID")
CONV_TEMPLATE_LINK_FIELD = CONV_FIELD_NAMES.get("TEMPLATE_LINK", "Template Link")
CONV_PROSPECT_LINK_FIELD = CONV_FIELD_NAMES.get("PROSPECT_LINK", "Prospect")
CONV_LEAD_LINK_FIELD = CONV_FIELD_NAMES.get("LEAD_LINK", "Lead")
CONV_PROSPECT_RECORD_FIELD = CONV_FIELD_NAMES.get("PROSPECT_RECORD_ID", "Prospect Record ID")
CONV_PROPERTY_ID_FIELD = CONV_FIELD_NAMES.get("PROPERTY_ID", "Property ID")
CONV_CAMPAIGN_LINK_FIELD = CONV_FIELD_NAMES.get("CAMPAIGN_LINK", "Campaign")
CONV_DRIP_LINK_FIELD = CONV_FIELD_NAMES.get("DRIP_QUEUE_LINK", "Drip Queue Link")
CONV_AI_INTENT_FIELD = CONV_FIELD_NAMES.get("AI_INTENT", "AI Intent")

# --- Candidate fallbacks ---
CONV_FROM_CANDIDATES = [CONV_FROM_FIELD, "Seller Phone Number", "From"]
CONV_TO_CANDIDATES = [CONV_TO_FIELD, "TextGrid Number", "From Number", "To"]
CONV_BODY_CANDIDATES = [CONV_BODY_FIELD, "Body", "Message"]
CONV_DIRECTION_CANDIDATES = [CONV_DIRECTION_FIELD, "Direction", "direction"]
CONV_PROCESSED_BY_CANDIDATES = [CONV_PROCESSED_BY_FIELD, "Processed By", "processed_by"]

TEMPLATE_INTENT_FIELD = TEMPLATE_FIELDS.get("INTERNAL_ID", "Internal ID")
TEMPLATE_MESSAGE_FIELD = TEMPLATE_FIELDS.get("MESSAGE", "Message")

# --- Drip Queue fields (fully safe, aligned with your Airtable schema) ---
DRIP_STATUS_FIELD = DRIP_FIELDS.get("STATUS", "Status")
DRIP_PROCESSOR_FIELD = DRIP_FIELDS.get("PROCESSOR", "Processor")
DRIP_MARKET_FIELD = DRIP_FIELDS.get("MARKET", "Market")
DRIP_TEMPLATE_LINK_FIELD = DRIP_FIELDS.get("TEMPLATE_LINK", "Template")
DRIP_PROSPECT_LINK_FIELD = DRIP_FIELDS.get("PROSPECT_LINK", "Prospect")
DRIP_CAMPAIGN_LINK_FIELD = DRIP_FIELDS.get("CAMPAIGN_LINK", "Campaign")
DRIP_SELLER_PHONE_FIELD = DRIP_FIELDS.get("SELLER_PHONE", "Seller Phone Number")
DRIP_TEXTGRID_PHONE_FIELD = DRIP_FIELDS.get("TEXTGRID_PHONE", "TextGrid Number")
DRIP_FROM_NUMBER_FIELD = DRIP_FIELDS.get("FROM_NUMBER", "From Number")
DRIP_MESSAGE_PREVIEW_FIELD = DRIP_FIELDS.get("MESSAGE_PREVIEW", "Message")
DRIP_NEXT_SEND_DATE_FIELD = DRIP_FIELDS.get("NEXT_SEND_DATE", "Next Send Date")
# Airtable will store ISO timestamps, so reuse same field for UTC
DRIP_NEXT_SEND_AT_UTC_FIELD = "Next Send Date"
DRIP_PROPERTY_ID_FIELD = DRIP_FIELDS.get("PROPERTY_ID", "Property ID")
DRIP_UI_FIELD = DRIP_FIELDS.get("UI", "UI")

# --- Leads ---
LEAD_STATUS_FIELD = LEAD_FIELDS.get("STATUS", "Status")

CONV_FROM_CANDIDATES = [CONV_FROM_FIELD, "Seller Phone Number", "phone"]
CONV_TO_CANDIDATES = [CONV_TO_FIELD, "TextGrid Phone Number", "to_number"]
CONV_BODY_CANDIDATES = [CONV_BODY_FIELD, "Body", "Message"]
CONV_DIRECTION_CANDIDATES = [CONV_DIRECTION_FIELD, "Direction", "direction"]
CONV_PROCESSED_BY_CANDIDATES = [CONV_PROCESSED_BY_FIELD, "Processed By", "processed_by"]

# Conversation delivery statuses (schema)
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
# Templates (deterministic categories)
# ---------------------------------------------------------------------------

# We select by INTERNAL_ID (TEMPLATE_INTENT_FIELD) to keep freedom from Airtable "Category".
# You can map your templates to these keys:
EVENT_TEMPLATE_POOLS: Dict[str, Tuple[str, ...]] = {
    # Stage 1 outcomes
    "ownership_yes": ("stage2_interest_prompt",),         # "Great—are you open to an offer on {Address}?"
    "ownership_no": tuple(),                               # no reply; DNC/stop
    "interest_no_30d": ("followup_30d_queue",),           # text to be queued for 30 days (not sent now)

    # Stage 2 outcomes
    "interest_yes": ("stage3_ask_price",),                # "Do you have a ballpark asking price?"
    # interest_no_30d reused

    # Stage 3 outcomes
    "ask_offer": ("stage4_condition_prompt",),            # "We’ll run numbers. Quick check—what’s the current condition?"
    "price_provided": ("stage4_condition_ack_prompt",),   # "Thanks for that. What’s the current condition?"

    # Stage 4 outcomes
    "condition_info": ("handoff_ack",),                   # optional short acknowledgement (we don't advance beyond Stage 4)
}

# ---------------------------------------------------------------------------
# Intent lexicon
# ---------------------------------------------------------------------------

STOP_WORDS = {"stop", "unsubscribe", "remove", "quit", "cancel", "end"}
WRONG_NUM_WORDS = {"wrong number", "not mine", "new number"}
NOT_OWNER_PHRASES = {"not the owner", "i sold", "no longer own", "dont own", "do not own", "sold this", "wrong person"}
INTEREST_NO_PHRASES = {
    "not interested", "not selling", "dont want to sell", "don't want to sell", "no interest",
    "keep for now", "holding for now", "keeping it", "not looking to sell",
}
ASK_OFFER_PHRASES = {"your offer", "what's your offer", "whats your offer", "what is your offer", "what can you offer"}
COND_WORDS = {"condition", "repairs", "needs work", "renovated", "updated", "tenant", "vacant", "occupied", "as-is", "roof", "hvac"}
YES_WORDS = {"yes", "yeah", "yep", "sure", "affirmative", "correct", "that's me", "that is me", "i am"}
NO_WORDS = {"no", "nope", "nah"}

PRICE_REGEX = re.compile(
    r"(\$?\s?\d{2,3}(?:,\d{3})*(?:\.\d{1,2})?\b)|(\b\d+\s?k\b)|(\b\d{2,3}k\b)", re.IGNORECASE
)

# ---------------------------------------------------------------------------
# Utils
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Stage helpers (canonical write targets)
# ---------------------------------------------------------------------------

STAGE1 = ConversationStage.STAGE_1_OWNERSHIP_CONFIRMATION.value
STAGE2 = ConversationStage.STAGE_2_INTEREST_FEELER.value
STAGE3 = ConversationStage.STAGE_3_PRICE_QUALIFICATION.value
STAGE4 = ConversationStage.STAGE_4_PROPERTY_CONDITION.value
STAGE_DNC = ConversationStage.DNC.value
STAGE_OPTOUT = ConversationStage.OPT_OUT.value

def _current_stage_label(fields: Dict[str, Any]) -> str:
    label = str(fields.get(CONV_STAGE_FIELD) or "").strip()
    if label in (STAGE1, STAGE2, STAGE3, STAGE4, STAGE_DNC, STAGE_OPTOUT):
        return label
    return STAGE1  # default start


# ---------------------------------------------------------------------------
# Intent classification (base intent), then stage-aware event mapping
# ---------------------------------------------------------------------------

def _base_intent(body: str) -> str:
    text = (body or "").lower().strip()
    if not text:
        return "neutral"

    if any(w in text for w in STOP_WORDS):
        return "optout"
    if any(w in text for w in WRONG_NUM_WORDS) or any(w in text for w in NOT_OWNER_PHRASES):
        return "wrong_number"
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
    if base_intent == "wrong_number":
        return "ownership_no"

    if stage_label == STAGE1:
        if base_intent == "affirm":
            return "ownership_yes"
        if base_intent in {"deny"}:
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

    # Stage 4 (we don't advance beyond; we still capture condition_info)
    if stage_label == STAGE4:
        if base_intent == "condition_info":
            return "condition_info"
        return "noop"


# ---------------------------------------------------------------------------
# AI intent mapping (ConversationAIIntent values in schema)
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
# Autoresponder
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
        self.processed_by = (os.getenv("PROCESSED_BY_LABEL") or ConversationProcessor.AUTORESPONDER.value).strip() or ConversationProcessor.AUTORESPONDER.value

        self.summary: Dict[str, Any] = {"processed": 0, "breakdown": {}, "errors": [], "skipped": {}}
        self.templates_by_key = self._index_templates()

        # Phone fields
        self.lead_phone_fields = [v for v in [LEAD_FIELDS.get("PHONE"), "Phone", "phone", "Mobile"] if v]
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

    # -------------------------- Template indexing & selection (deterministic)
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

    def _pick_message(self, pool_keys: Tuple[str, ...], personalization: Dict[str, str], rand_key: str) -> Tuple[str, Optional[str], Optional[str]]:
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
                except Exception:
                    msg = raw
                return (msg or "Thanks for the reply.", chosen.get("id"), pool)
        # Pure fallback
        return ("Thanks for the reply.", None, None)

    # -------------------------- Fetching
    def _fetch_inbound(self, limit: int) -> List[Dict[str, Any]]:
        view = os.getenv("CONV_VIEW_INBOUND", "Unprocessed Inbounds")
        try:
            records = self.convos.all(view=view, max_records=limit)
            if records:
                return records
        except Exception:
            logger.warning("Failed to fetch Conversations view '%s'; falling back to scan", view, exc_info=True)

        # Fallback scan: INBOUND & unprocessed
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

    # -------------------------- Lead gating: only when interest confirmed (Stage 2+)
    def _ensure_lead_if_interested(self, event: str, phone: str, conv_fields: Dict[str, Any], prospect: Optional[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
        promote_events = {"interest_yes", "price_provided", "ask_offer", "condition_info"}
        if event not in promote_events:
            return None, None
        # Use the shared promote util if available (keeps schema-logic in one place)
        try:
            return promote_to_lead(phone, source=self.processed_by, conversation_fields=conv_fields)
        except Exception:
            pass

        # Best-effort manual create (fallback)
        existing = self._find_record_by_phone(self.leads, self.lead_phone_fields, phone)
        if existing:
            property_id = (prospect or {}).get("fields", {}).get(PROSPECT_FIELDS.get("PROPERTY_ID")) if prospect else None
            return existing["id"], property_id
        payload = {}
        phone_field = LEAD_FIELDS.get("PHONE") or "Phone"
        payload[phone_field] = phone
        status_field = LEAD_STATUS_FIELD or "Lead Status"
        payload[status_field] = "Contacted"
        source_field = LEAD_FIELDS.get("SOURCE") or "Source"
        payload[source_field] = self.processed_by
        created = self.leads.create(payload)
        property_id = (prospect or {}).get("fields", {}).get(PROSPECT_FIELDS.get("PROPERTY_ID")) if prospect else None
        return created.get("id"), property_id

    # -------------------------- Drip: enqueue a message at a specific UTC time
    def _enqueue_reply(
        self,
        record: Dict[str, Any],
        fields: Dict[str, Any],
        reply_text: str,
        template_id: Optional[str],
        queue_time: datetime,
        lead_id: Optional[str],
        prospect_id: Optional[str],
        conv_stage_label: str,
        template_category: Optional[str],
    ) -> bool:
        if not self.drip:
            return False

        campaign_link = _normalise_link(fields.get(CONV_CAMPAIGN_LINK_FIELD))

        # ✅ Match Airtable casing exactly
        payload = {
            DRIP_STATUS_FIELD: "Queued",
            DRIP_PROCESSOR_FIELD: self.processed_by,
            DRIP_MARKET_FIELD: fields.get("Market"),
            DRIP_SELLER_PHONE_FIELD: _get_first(fields, CONV_FROM_CANDIDATES),
            DRIP_TEXTGRID_PHONE_FIELD: _get_first(fields, CONV_TO_CANDIDATES),
            DRIP_FROM_NUMBER_FIELD: _get_first(fields, CONV_TO_CANDIDATES),
            DRIP_MESSAGE_PREVIEW_FIELD: reply_text,
            DRIP_NEXT_SEND_DATE_FIELD: queue_time.astimezone(timezone.utc).date().isoformat(),
            DRIP_NEXT_SEND_AT_UTC_FIELD: queue_time.astimezone(timezone.utc).isoformat(),
            DRIP_PROPERTY_ID_FIELD: fields.get(CONV_PROPERTY_ID_FIELD),
            DRIP_UI_FIELD: STATUS_ICON.get("QUEUED", "⏳"),
        }

        if template_category:
            payload.setdefault("Template Category", template_category)
        if template_id:
            payload[DRIP_TEMPLATE_LINK_FIELD] = [template_id]
        if prospect_id:
            payload[DRIP_PROSPECT_LINK_FIELD] = [prospect_id]
        if campaign_link:
            payload[DRIP_CAMPAIGN_LINK_FIELD] = [campaign_link]

        try:
            created = self.drip.create(payload)
        except Exception as exc:
            self.summary["errors"].append(
                {"conversation": record.get("id"), "error": f"Queue failed: {exc}"}
            )
            return False

        if created and created.get("id"):
            self.convos.update(
                record["id"],
                {
                    CONV_DRIP_LINK_FIELD: [created["id"]],
                    CONV_TEMPLATE_LINK_FIELD: [template_id] if template_id else None,
                },
            )
            return True

        return False

    # -------------------------- Immediate send (fallback if no drip engine)
    def _send_immediate(self, from_number: str, body: str, to_number: Optional[str], lead_id: Optional[str], property_id: Optional[str]) -> None:
        if not MessageProcessor:
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

    # -------------------------- Process loop
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
        current_stage = _current_stage_label(fields)

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
        queue_30d = False
        drip_when: Optional[datetime] = None

        # For deterministic template pick
        personalization = self._personalize(prospect_fields)
        rand_key = f"{last_10_digits(from_number) or from_number}:{event}"

        # Canonical stage + AI intent to write
        def write_stage(stage: str) -> str:
            return stage

        ai_intent = AI_INTENT_MAP.get(event, "other")

        if event == "optout":
            # Mark conversation as Opt-Out; do not create leads/drips
            self._update_conversation(
                record["id"],
                status=_pick_status("OPT OUT"),
                stage=STAGE_OPTOUT,
                ai_intent=ai_intent,
                lead_id=None,
                prospect_id=_normalise_link(fields.get(CONV_PROSPECT_RECORD_FIELD)) or (prospect_record or {}).get("id"),
            )
            return

        if event == "ownership_no":
            # Wrong number / not owner / explicit "no" at Stage 1 → DNC
            self._update_conversation(
                record["id"],
                status=_pick_status("DELIVERED"),
                stage=STAGE_DNC,
                ai_intent=ai_intent,
                lead_id=None,
                prospect_id=_normalise_link(fields.get(CONV_PROSPECT_RECORD_FIELD)) or (prospect_record or {}).get("id"),
            )
            return

        if event == "interest_no_30d":
            # Schedule re-engagement in 30 days; don't send immediate message
            pool = EVENT_TEMPLATE_POOLS.get("interest_no_30d", tuple())
            preview, tpl_id, pool_used = self._pick_message(pool, personalization, rand_key)
            queue_30d = True
            drip_when = datetime.now(timezone.utc) + timedelta(days=30)
            if self.drip:
                self._enqueue_drip(record, fields, preview, drip_when, tpl_id, (prospect_record or {}).get("id"))
            # Record conversation as processed with Stage 2 (interest filter) so analytics stay tidy
            self._update_conversation(
                record["id"],
                status=_pick_status("DELIVERED"),
                stage=STAGE2,
                ai_intent=ai_intent,
                lead_id=None,
                prospect_id=(prospect_record or {}).get("id") or _normalise_link(fields.get(CONV_PROSPECT_RECORD_FIELD)),
                template_id=None,
            )
            # Also ping follow-up engine (best-effort)
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
            return

        if event == "ownership_yes":
            next_stage = write_stage(STAGE2)
            pool = EVENT_TEMPLATE_POOLS.get("ownership_yes", tuple())
            reply_text, template_id, template_pool_used = self._pick_message(pool, personalization, rand_key)
            queue_reply = True

        elif event == "interest_yes":
            next_stage = write_stage(STAGE3)
            pool = EVENT_TEMPLATE_POOLS.get("interest_yes", tuple())
            reply_text, template_id, template_pool_used = self._pick_message(pool, personalization, rand_key)
            queue_reply = True

        elif event in {"ask_offer", "price_provided"}:
            next_stage = write_stage(STAGE4)
            pool_key = "price_provided" if event == "price_provided" else "ask_offer"
            pool = EVENT_TEMPLATE_POOLS.get(pool_key, tuple())
            reply_text, template_id, template_pool_used = self._pick_message(pool, personalization, rand_key)
            queue_reply = True

        elif event == "condition_info":
            # Stay at Stage 4; do not advance beyond—we hand off / stop here.
            next_stage = write_stage(STAGE4)
            pool = EVENT_TEMPLATE_POOLS.get("condition_info", tuple())
            # optional short acknowledgement; fine if empty
            if pool:
                reply_text, template_id, template_pool_used = self._pick_message(pool, personalization, rand_key)
                queue_reply = bool(reply_text)

        else:  # "noop"
            # If we already interacted recently or stage is 4, just mark processed and exit
            if _recently_responded(fields, self.processed_by) or current_stage == STAGE4:
                self._update_conversation(
                    record["id"],
                    status=_pick_status("DELIVERED"),
                    stage=current_stage,
                    ai_intent=ai_intent,
                    lead_id=None,
                    prospect_id=(prospect_record or {}).get("id") or _normalise_link(fields.get(CONV_PROSPECT_RECORD_FIELD)),
                )
                self.summary["skipped"]["noop_or_recent"] = self.summary["skipped"].get("noop_or_recent", 0) + 1
                return
            # Otherwise gently push forward based on current stage
            if current_stage == STAGE1:
                next_stage = write_stage(STAGE2)
                pool = EVENT_TEMPLATE_POOLS.get("ownership_yes", tuple())
                reply_text, template_id, template_pool_used = self._pick_message(pool, personalization, rand_key)
                queue_reply = True
            elif current_stage == STAGE2:
                next_stage = write_stage(STAGE3)
                pool = EVENT_TEMPLATE_POOLS.get("interest_yes", tuple())
                reply_text, template_id, template_pool_used = self._pick_message(pool, personalization, rand_key)
                queue_reply = True
            elif current_stage == STAGE3:
                next_stage = write_stage(STAGE4)
                pool = EVENT_TEMPLATE_POOLS.get("ask_offer", tuple())
                reply_text, template_id, template_pool_used = self._pick_message(pool, personalization, rand_key)
                queue_reply = True
            else:
                next_stage = current_stage  # Stage 4

        # Create/attach lead ONLY for interested events (Stage 2+ confirmations)
        lead_id, property_id = self._ensure_lead_if_interested(event, from_number, fields, prospect_record)
        prospect_id = (prospect_record or {}).get("id") or _normalise_link(fields.get(CONV_PROSPECT_RECORD_FIELD))

        # Enqueue/send reply if needed
        if queue_reply and reply_text:
            queued = False
            if self.drip:
                queued = bool(self._enqueue_drip(record, fields, reply_text, send_time, template_id, prospect_id))
            if not queued:
                to_number = _get_first(fields, CONV_TO_CANDIDATES)
                self._send_immediate(from_number, reply_text, to_number, lead_id, property_id)

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

        # Update lead activity (best-effort)
        if lead_id:
            try:
                self.leads.update(
                    lead_id,
                    {
                        LEAD_FIELDS["LAST_MESSAGE"]: body[:500],
                        LEAD_FIELDS["LAST_DIRECTION"]: ConversationDirection.INBOUND.value,
                        LEAD_FIELDS["LAST_ACTIVITY"]: iso_now(),
                    },
                )
            except Exception:
                pass

        # Notify follow-up engine for analytics / next-steps (best-effort)
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

    # -------------------------- Small helpers
    def _personalize(self, fields: Dict[str, Any]) -> Dict[str, str]:
        first = ""
        owner_name = fields.get(PROSPECT_FIELDS.get("OWNER_NAME"))
        if isinstance(owner_name, str) and owner_name.strip():
            first = owner_name.split()[0]
        else:
            owner_first = fields.get(PROSPECT_FIELDS.get("OWNER_FIRST_NAME"))
            if isinstance(owner_first, str):
                first = owner_first.strip()
        if not first:
            first = "there"
        address = (
            fields.get(PROSPECT_FIELDS.get("PROPERTY_ADDRESS"))
            or fields.get("Property Address")
            or fields.get("Address")
            or "your property"
        )
        return {"First": first, "Address": address}

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


def run_autoresponder(limit: int = 50) -> Dict[str, Any]:
    service = Autoresponder()
    return service.process(limit)


if __name__ == "__main__":
    limit = int(os.getenv("AR_LIMIT", "50"))
    out = run_autoresponder(limit=limit)
    print("\n=== Autoresponder Summary ===")
    import pprint
    pprint.pprint(out)
