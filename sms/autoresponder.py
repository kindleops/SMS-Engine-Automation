"""Intent-aware autoresponder backed by the schema-driven datastore.
Stages handled: 1) Ownership → 2) Interest → 3) Price → 4) Condition.

Flow:
- Stage 1 (Ownership):
  • YES  -> move to Stage 2 (ask interest) and send interest probe.
  • NO / WRONG NUMBER / NOT OWNER / STOP -> mark DNC (STOP -> OPT OUT), no reply.
  • NOT INTERESTED / NOT SELLING -> schedule 30d follow-up, no DNC.

- Stage 2 (Interest):
  • YES  -> move to Stage 3 (ask price) and send ask-price.
  • NO   -> schedule 30d follow-up, mark Phone 1 & Verified on Prospect.

- Stage 3 (Price):
  • If they give a number -> move to Stage 4 and ask condition.
  • If they ask “what’s your offer” -> move to Stage 4 and ask condition.

- Stage 4 (Condition):
  • Ask for condition (template-based). Further stages intentionally not used.

Notes:
- Writes only canonical ConversationStage labels to Airtable.
- Drip Queue used for quiet-hours deferrals and 30-day follow-ups.
- Prospects: when “interest = no”, ensure Phone 1 is set to sender and mark Phone 1 Verified = True.
"""

from __future__ import annotations

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

try:
    from sms.message_processor import MessageProcessor
except Exception:  # pragma: no cover
    MessageProcessor = None  # type: ignore

try:
    from sms.followup_flow import schedule_from_response
except Exception:  # pragma: no cover
    def schedule_from_response(**_: Any) -> None:
        pass

try:
    from sms import templates as local_templates
except Exception:  # pragma: no cover
    local_templates = None  # type: ignore


logger = get_logger(__name__)


# ------------------------------------------------------------------------------
# Airtable facades
# ------------------------------------------------------------------------------

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


# ------------------------------------------------------------------------------
# Schema maps
# ------------------------------------------------------------------------------

CONV_FIELDS = conversations_field_map()
CONV_FIELD_NAMES = CONVERSATIONS_TABLE.field_names()
DRIP_FIELDS = drip_field_map()
LEAD_FIELDS = leads_field_map()
PROSPECT_FIELDS = prospects_field_map()
TEMPLATE_FIELDS = template_field_map()

CONV_FROM_FIELD = CONV_FIELDS["FROM"]
CONV_TO_FIELD = CONV_FIELDS["TO"]
CONV_BODY_FIELD = CONV_FIELDS["BODY"]
CONV_STATUS_FIELD = CONV_FIELDS["STATUS"]
CONV_DIRECTION_FIELD = CONV_FIELDS["DIRECTION"]
CONV_INTENT_FIELD = CONV_FIELDS["INTENT"]
CONV_PROCESSED_BY_FIELD = CONV_FIELDS["PROCESSED_BY"]
CONV_SENT_AT_FIELD = CONV_FIELDS["SENT_AT"]
CONV_STAGE_FIELD = CONV_FIELD_NAMES["STAGE"]
CONV_PROCESSED_AT_FIELD = CONV_FIELD_NAMES["PROCESSED_AT"]
CONV_TEMPLATE_RECORD_FIELD = CONV_FIELD_NAMES["TEMPLATE_RECORD_ID"]
CONV_TEMPLATE_LINK_FIELD = CONV_FIELD_NAMES["TEMPLATE_LINK"]
CONV_PROSPECT_LINK_FIELD = CONV_FIELD_NAMES["PROSPECT_LINK"]
CONV_LEAD_LINK_FIELD = CONV_FIELD_NAMES["LEAD_LINK"]
CONV_PROSPECT_RECORD_FIELD = CONV_FIELD_NAMES["PROSPECT_RECORD_ID"]
CONV_PROPERTY_ID_FIELD = CONV_FIELD_NAMES["PROPERTY_ID"]
CONV_CAMPAIGN_LINK_FIELD = CONV_FIELD_NAMES["CAMPAIGN_LINK"]
CONV_DRIP_LINK_FIELD = CONV_FIELD_NAMES["DRIP_QUEUE_LINK"]

DRIP_STATUS_FIELD = DRIP_FIELDS["STATUS"]
DRIP_PROCESSOR_FIELD = DRIP_FIELDS.get("PROCESSOR", "Processor")
DRIP_MARKET_FIELD = DRIP_FIELDS.get("MARKET", "Market")
DRIP_TEMPLATE_LINK_FIELD = DRIP_FIELDS.get("TEMPLATE_LINK", "Template")
DRIP_PROSPECT_LINK_FIELD = DRIP_FIELDS.get("PROSPECT_LINK", "Prospect")
DRIP_CAMPAIGN_LINK_FIELD = DRIP_FIELDS.get("CAMPAIGN_LINK", "Campaign")
DRIP_SELLER_PHONE_FIELD = DRIP_FIELDS.get("SELLER_PHONE", "Seller Phone Number")
DRIP_TEXTGRID_PHONE_FIELD = DRIP_FIELDS.get("TEXTGRID_PHONE", "TextGrid Phone Number")
DRIP_FROM_NUMBER_FIELD = DRIP_FIELDS.get("FROM_NUMBER", "From Number")
DRIP_MESSAGE_PREVIEW_FIELD = DRIP_FIELDS.get("MESSAGE_PREVIEW", "Message Preview")
DRIP_NEXT_SEND_DATE_FIELD = DRIP_FIELDS.get("NEXT_SEND_DATE", "Next Send Date")
DRIP_NEXT_SEND_AT_UTC_FIELD = DRIP_FIELDS.get("NEXT_SEND_AT_UTC", "next_send_at_utc")
DRIP_PROPERTY_ID_FIELD = DRIP_FIELDS.get("PROPERTY_ID", "Property ID")
DRIP_UI_FIELD = DRIP_FIELDS.get("UI", "UI")
DRIP_STAGE_FIELD = DRIP_FIELDS.get("DRIP_STAGE", "Drip Stage")  # 30/60/90 scheduling

TEMPLATE_INTENT_FIELD = TEMPLATE_FIELDS.get("INTERNAL_ID", "Internal ID")
TEMPLATE_MESSAGE_FIELD = TEMPLATE_FIELDS.get("MESSAGE", "Message")

LEAD_STATUS_FIELD = LEAD_FIELDS["STATUS"]

CONV_FROM_CANDIDATES = [CONV_FROM_FIELD, "From", "phone"]
CONV_TO_CANDIDATES = [CONV_TO_FIELD, "To", "to_number"]
CONV_BODY_CANDIDATES = [CONV_BODY_FIELD, "Body", "message"]
CONV_DIRECTION_CANDIDATES = [CONV_DIRECTION_FIELD, "Direction", "direction"]
CONV_PROCESSED_BY_CANDIDATES = [CONV_PROCESSED_BY_FIELD, "Processed By", "processed_by"]

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

FOLLOWUP_DAYS = int(os.getenv("AR_FOLLOWUP_DAYS", "30"))


# ------------------------------------------------------------------------------
# Intent detection (strings → coarse intents)
# ------------------------------------------------------------------------------

STOP_WORDS = {"stop", "unsubscribe", "remove", "quit", "cancel", "end"}
NO_WORDS = {"no", "nope", "nah"}
NOT_INTENT_SELL = {"not selling", "not interested", "no interest", "dont want to sell", "don’t want to sell"}
WRONG_WORDS = {"wrong number", "not my number", "not mine"}
NOT_OWNER_PHRASES = {"not the owner", "i sold", "no longer own", "dont own", "do not own", "sold this", "wrong person"}
INTEREST_YES = {"yes", "yeah", "yep", "sure", "affirmative", "correct", "that is me", "that's me", "i am", "this is"}
ASK_OFFER_PHRASES = {
    "what's your offer",
    "whats your offer",
    "what is your offer",
    "you tell me",
    "you asked me",
    "make me an offer",
    "your offer",
    "what can you offer",
}
PRICE_WORDS = {"price", "asking", "range", "ballpark", "how much", "what can you pay"}
COND_WORDS = {"condition", "repairs", "needs work", "renovated", "tenant", "tenants", "vacant", "occupied", "as-is", "updates"}
DELAY_WORDS = {"later", "next week", "tomorrow", "busy", "follow up", "text later", "call me later"}


_money_like = re.compile(
    r"(?i)(?:\$+\s?\d{2,3}\,?\d{0,3}(?:\.\d{1,2})?|(?:\d{2,3}\,)?\d{3}(?:\.\d{1,2})?|\d{2,3}\s?[kK])"
)
_price_context = re.compile(r"(?i)\b(asking|want|take|for|price|at|about|around|offer)\b")


def _has_any(text: str, phrases: Iterable[str]) -> bool:
    return any(p in text for p in phrases)


def _looks_like_price_value(text: str) -> bool:
    if not _money_like.search(text):
        return False
    if "$" in text or "k" in text or "K" in text:
        return True
    return bool(_price_context.search(text))


def classify_coarse(body: str) -> str:
    """Return coarse intent independent of stage context."""
    text = (body or "").lower().strip()
    if not text:
        return "unknown"

    if _has_any(text, STOP_WORDS):
        return "optout"
    if _has_any(text, WRONG_WORDS):
        return "wrong_number"
    if _has_any(text, NOT_OWNER_PHRASES):
        return "not_owner"
    if _has_any(text, NOT_INTENT_SELL):
        return "not_interested"
    if _has_any(text, DELAY_WORDS):
        return "delay"
    if _looks_like_price_value(text):
        return "price_provided"
    if _has_any(text, ASK_OFFER_PHRASES):
        return "ask_offer"
    if _has_any(text, COND_WORDS):
        return "condition_info"

    # Generic affirm/deny
    if re.search(r"\b(" + "|".join(map(re.escape, INTEREST_YES)) + r")\b", text):
        return "yes_generic"
    if re.search(r"\b(" + "|".join(map(re.escape, NO_WORDS)) + r")\b", text):
        return "no_generic"

    if _has_any(text, PRICE_WORDS):
        return "price_question"

    return "unknown"


# ------------------------------------------------------------------------------
# Stage-aware event mapping
# ------------------------------------------------------------------------------

def _event_from_stage(coarse: str, current_stage_label: str | None) -> str:
    """Refine the coarse intent using the current ConversationStage label."""
    st = (current_stage_label or "").strip()
    s1 = ConversationStage.STAGE_1_OWNERSHIP_CONFIRMATION.value
    s2 = ConversationStage.STAGE_2_INTEREST_FEELER.value
    s3 = ConversationStage.STAGE_3_PRICE_QUALIFICATION.value

    # Terminal routes regardless of stage
    if coarse in {"optout", "wrong_number", "not_owner"}:
        return coarse
    if coarse == "not_interested":
        return "followup_30"

    # Stage-specific
    if st == s1 or not st:
        if coarse == "yes_generic":
            return "ownership_yes"
        if coarse in {"no_generic"}:
            return "ownership_no"
        # price/condition chatter at S1 → treat as interest seen, move on
        if coarse in {"price_provided", "ask_offer", "price_question", "condition_info"}:
            return "ownership_yes"
        return "unknown"

    if st == s2:
        if coarse == "yes_generic":
            return "interest_yes"
        if coarse in {"no_generic", "not_interested"}:
            return "interest_no"
        # drift into price/condition at S2 → advance
        if coarse in {"price_provided", "ask_offer", "price_question", "condition_info"}:
            return "interest_yes"
        return "unknown"

    if st == s3:
        if coarse in {"price_provided"}:
            return "price_provided"
        if coarse in {"ask_offer", "price_question"}:
            return "ask_offer"
        if coarse == "condition_info":
            return "condition_info"
        if coarse in {"no_generic", "not_interested"}:
            return "followup_30"
        return "unknown"

    # If we're already beyond S3, still allow S4 transition triggers
    if coarse in {"price_provided", "ask_offer", "condition_info"}:
        return coarse

    return "unknown"


def _conversation_stage_for_event(event: str, current_label: Optional[str]) -> str:
    """Canonical ConversationStage to WRITE to Airtable."""
    s1 = ConversationStage.STAGE_1_OWNERSHIP_CONFIRMATION.value
    s2 = ConversationStage.STAGE_2_INTEREST_FEELER.value
    s3 = ConversationStage.STAGE_3_PRICE_QUALIFICATION.value
    s4 = ConversationStage.STAGE_4_PROPERTY_CONDITION.value

    if event == "ownership_yes":
        return s2
    if event in {"ownership_no", "wrong_number", "not_owner"}:
        return ConversationStage.DNC.value
    if event == "interest_yes":
        return s3
    if event in {"price_provided", "ask_offer", "condition_info"}:
        return s4
    if event in {"interest_no", "followup_30"}:
        return s2
    if event == "optout":
        return ConversationStage.OPT_OUT.value
    return current_label or s2


# ------------------------------------------------------------------------------
# Template selection
# ------------------------------------------------------------------------------

# We route by TEMPLATE.INTERNAL_ID first; fall back to category; then safe text.
TEMPLATE_ROUTES: Dict[str, Tuple[str, ...]] = {
    "ownership_yes": ("confirm_interest", "interest_probe", "follow_up"),
    "interest_yes": ("ask_price", "price_question"),
    "price_provided": ("condition_question", "stage4_condition"),
    "ask_offer": ("condition_question", "stage4_condition"),
    "condition_info": ("ack_condition", "follow_up"),
    # If we decide to message after delay/followup_30 immediately (usually no)
    "followup_30": ("follow_up",),
}


# ------------------------------------------------------------------------------
# Utilities
# ------------------------------------------------------------------------------

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
    alias = {"RESPONDED": "DELIVERED", "AI_HANDOFF": "SENT"}
    up = alias.get(up, up)
    return up if up in SAFE_CONVERSATION_STATUS else "DELIVERED"


def _personalize(fields: Dict[str, Any]) -> Dict[str, str]:
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


def _recent_ar_reply(fields: Dict[str, Any], processed_by_label: str) -> bool:
    last_by = str(fields.get(CONV_PROCESSED_BY_FIELD) or "").strip()
    allowed = {processed_by_label, ConversationProcessor.AUTORESPONDER.value}
    if last_by and last_by not in allowed:
        return False

    candidates = [
        fields.get(CONV_PROCESSED_AT_FIELD),
        fields.get("processed_at"),
        fields.get("Processed Time"),
        fields.get(CONV_SENT_AT_FIELD),
        fields.get("last_sent_at"),
        fields.get("Last Sent At"),
    ]
    ts = None
    for v in candidates:
        ts = _parse_timestamp(v)
        if ts:
            break
    if not ts:
        return False
    return datetime.now(timezone.utc) - ts < timedelta(minutes=30)


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


# ------------------------------------------------------------------------------
# Core service
# ------------------------------------------------------------------------------

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
        self.summary: Dict[str, Any] = {"processed": 0, "breakdown": {}, "errors": [], "transitions": []}
        self.templates_by_key, self.templates_by_category = self._index_templates()
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

    # ---------------- Templates
    def _index_templates(self) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, List[Dict[str, Any]]]]:
        records = self.templates.all()
        by_key: Dict[str, List[Dict[str, Any]]] = {}
        by_cat: Dict[str, List[Dict[str, Any]]] = {}
        for rec in records:
            f = rec.get("fields", {}) or {}
            key = str(f.get(TEMPLATE_INTENT_FIELD) or f.get("intent") or "").strip().lower()
            cat = str(f.get(TEMPLATE_FIELDS.get("CATEGORY", "Category")) or "").strip().lower()
            if key:
                by_key.setdefault(key, []).append(rec)
            if cat:
                by_cat.setdefault(cat, []).append(rec)
        return by_key, by_cat

    def _choose_template(self, route_keys: Iterable[str], personalization: Dict[str, str]) -> Tuple[str, Optional[str], Optional[str]]:
        for k in route_keys:
            pool = self.templates_by_key.get(k)
            if pool:
                chosen = random.choice(pool)
                f = chosen.get("fields", {}) or {}
                raw = str(f.get(TEMPLATE_MESSAGE_FIELD) or "").strip()
                try:
                    msg = raw.format(**personalization)
                except Exception:
                    msg = raw
                if msg:
                    return msg, chosen.get("id"), k
        # category fallback (use same key text as category hint if present)
        for k in route_keys:
            pool = self.templates_by_category.get(k)
            if pool:
                chosen = random.choice(pool)
                f = chosen.get("fields", {}) or {}
                raw = str(f.get(TEMPLATE_MESSAGE_FIELD) or "").strip()
                try:
                    msg = raw.format(**personalization)
                except Exception:
                    msg = raw
                if msg:
                    return msg, chosen.get("id"), k

        # Local fallback (tests)
        if local_templates:
            try:
                msg = local_templates.get_template(",".join(route_keys), personalization)
                if msg:
                    return msg, None, None
            except Exception:
                pass

        # Safe string fallback based on first route
        first = next(iter(route_keys), "ask_price")
        if "condition" in first:
            fallback = "Got it, thanks {First}. What’s the current condition of {Address}? Any needed repairs or recent updates?"
        elif "interest" in first:
            fallback = "Thanks {First}! Are you open to an offer on {Address}?"
        else:
            fallback = "Thanks {First}! What price range do you have in mind for {Address}?"
        try:
            return fallback.format(**personalization), None, None
        except Exception:
            return fallback, None, None

    # ---------------- Fetch
    def _fetch_inbound(self, limit: int) -> List[Dict[str, Any]]:
        view = os.getenv("CONV_VIEW_INBOUND", "Unprocessed Inbounds")
        try:
            records = self.convos.all(view=view, max_records=limit)
            if records:
                return records
        except Exception:
            logger.warning("Failed to fetch Conversations view '%s'; falling back to scan", view, exc_info=True)

        # Fallback: scan and pick INBOUND without Processed By
        fallback: List[Dict[str, Any]] = []
        for rec in self.convos.all(max_records=limit * 2):
            fields = rec.get("fields", {}) or {}
            direction = str(_get_first(fields, CONV_DIRECTION_CANDIDATES) or "").upper()
            processed_by = _get_first(fields, CONV_PROCESSED_BY_CANDIDATES)
            if direction in ("IN", "INBOUND") and not processed_by:
                fallback.append(rec)
            if len(fallback) >= limit:
                break
        return fallback

    # ---------------- Prospect/Lead helpers
    def _find_record_by_phone(self, table, candidates: List[Optional[str]], phone: str) -> Optional[Dict[str, Any]]:
        digits = last_10_digits(phone)
        if not digits:
            return None
        clean_fields = [c for c in candidates if c]
        try:
            rows = table.all()
        except Exception:
            rows = []
        for rec in rows:
            fields = rec.get("fields", {}) or {}
            for field in clean_fields:
                if last_10_digits(fields.get(field)) == digits:
                    return rec
        return None

    def _find_prospect(self, phone: str) -> Optional[Dict[str, Any]]:
        return self._find_record_by_phone(self.prospects, self.prospect_phone_fields, phone)

    def _ensure_lead(self, phone: str, fields: Dict[str, Any], prospect: Optional[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
        if isinstance(self.leads, TableFacade):
            return promote_to_lead(phone, source=self.processed_by, conversation_fields=fields)
        existing = self._find_record_by_phone(self.leads, self.lead_phone_fields, phone)
        if existing:
            property_id = (prospect or {}).get("fields", {}).get(PROSPECT_FIELDS.get("PROPERTY_ID")) if prospect else None
            return existing["id"], property_id
        payload = {LEAD_FIELDS.get("PHONE") or "Phone": phone, (LEAD_FIELDS.get("SOURCE") or "Source"): self.processed_by}
        payload[LEAD_STATUS_FIELD or "Lead Status"] = "New"
        created = self.leads.create(payload)
        property_id = (prospect or {}).get("fields", {}).get(PROSPECT_FIELDS.get("PROPERTY_ID")) if prospect else None
        return created.get("id"), property_id

    def _mark_phone1_and_verify(self, prospect_id: Optional[str], from_number: str, prospect_fields: Dict[str, Any]) -> None:
        if not prospect_id or not from_number:
            return
        update: Dict[str, Any] = {}
        digits = last_10_digits(from_number)
        matched_key = _matching_phone_field(prospect_fields, digits)
        primary_col = PROSPECT_FIELDS.get("PHONE_PRIMARY")
        verified_col = _verified_field_for(matched_key) or PROSPECT_FIELDS.get("PHONE_PRIMARY_VERIFIED")
        # If no phone matched, set Phone 1; otherwise just verify the matched one.
        if primary_col and not prospect_fields.get(primary_col):
            update[primary_col] = from_number
        if verified_col:
            update[verified_col] = True
        if update:
            try:
                self.prospects.update(prospect_id, update)
            except Exception:
                pass

    def _update_lead_touch(self, lead_id: Optional[str], body: str) -> None:
        if not lead_id:
            return
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

    # ---------------- Drip queue
    def _enqueue_drip(self, fields: Dict[str, Any], reply_text: str, when_utc: datetime,
                      template_id: Optional[str], prospect_id: Optional[str], category_hint: Optional[str]) -> Optional[str]:
        if not self.drip:
            return None
        campaign_link = _normalise_link(fields.get(CONV_CAMPAIGN_LINK_FIELD))
        payload = {
            DRIP_STATUS_FIELD: "QUEUED",
            DRIP_PROCESSOR_FIELD: self.processed_by,
            DRIP_MARKET_FIELD: fields.get("Market"),
            DRIP_SELLER_PHONE_FIELD: _get_first(fields, CONV_FROM_CANDIDATES),
            DRIP_TEXTGRID_PHONE_FIELD: _get_first(fields, CONV_TO_CANDIDATES),
            DRIP_FROM_NUMBER_FIELD: _get_first(fields, CONV_TO_CANDIDATES),
            DRIP_MESSAGE_PREVIEW_FIELD: reply_text,
            DRIP_NEXT_SEND_DATE_FIELD: when_utc.date().isoformat(),
            DRIP_NEXT_SEND_AT_UTC_FIELD: when_utc.isoformat(),
            DRIP_PROPERTY_ID_FIELD: fields.get(CONV_PROPERTY_ID_FIELD),
            DRIP_UI_FIELD: STATUS_ICON.get("QUEUED"),
        }
        if DRIP_STAGE_FIELD:
            # Use "30" for explicit 30-day follow-ups; omit for same-day quiet-hour deferrals.
            if (when_utc - datetime.now(timezone.utc)) >= timedelta(days=29, hours=23):
                payload[DRIP_STAGE_FIELD] = "30"
        if category_hint:
            payload.setdefault("Template Category", category_hint)
        if template_id:
            payload[DRIP_TEMPLATE_LINK_FIELD] = [template_id]
        if prospect_id:
            payload[DRIP_PROSPECT_LINK_FIELD] = [prospect_id]
        if campaign_link:
            payload[DRIP_CAMPAIGN_LINK_FIELD] = [campaign_link]
        try:
            created = self.drip.create(payload)
            return (created or {}).get("id")
        except Exception as exc:
            self.summary["errors"].append({"error": f"Queue failed: {exc}"})
            return None

    def _send_immediate(self, from_number: str, body: str, to_number: Optional[str],
                        lead_id: Optional[str], property_id: Optional[str]) -> None:
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

    # ---------------- Main
    def process(self, limit: int) -> Dict[str, Any]:
        records = self._fetch_inbound(limit)
        if not records:
            return {"ok": False, "processed": 0, "breakdown": {}, "errors": []}

        now = datetime.now(timezone.utc)
        is_quiet, next_allowed = _quiet_window(now, self.policy)
        next_allowed = next_allowed or now

        for record in records:
            try:
                self._process_record(record, is_quiet, next_allowed)
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
        if str(_get_first(fields, CONV_DIRECTION_CANDIDATES) or "").upper() not in ("IN", "INBOUND"):
            return

        from_number = str(from_value)
        body = str(body_value)
        current_stage_label = str(fields.get(CONV_STAGE_FIELD) or "")

        # Prospect context
        prospect_record = self._find_prospect(from_number)
        prospect_fields = (prospect_record or {}).get("fields", {}) or {}
        prospect_id = (prospect_record or {}).get("id") or _normalise_link(fields.get(CONV_PROSPECT_RECORD_FIELD))

        # Detect event
        coarse = classify_coarse(body)
        event = _event_from_stage(coarse, current_stage_label)
        conv_stage_label = _conversation_stage_for_event(event, current_stage_label)
        stage_changed = (conv_stage_label or "") != (current_stage_label or "")

        logger.info("AR IN %s coarse=%s event=%s stage=%s→%s", from_number, coarse, event, current_stage_label or "∅", conv_stage_label)
        self.summary["processed"] += 1
        self.summary["breakdown"][event] = self.summary["breakdown"].get(event, 0) + 1

        # Hard exits: STOP / Wrong / Not owner
        if event == "optout":
            self.convos.update(
                record["id"],
                {
                    CONV_STATUS_FIELD: _pick_status("OPT OUT"),
                    CONV_PROCESSED_BY_FIELD: self.processed_by,
                    CONV_PROCESSED_AT_FIELD: iso_now(),
                    CONV_INTENT_FIELD: event,
                    CONV_STAGE_FIELD: conv_stage_label,
                },
            )
            return
        if event in {"ownership_no", "wrong_number", "not_owner"}:
            self.convos.update(
                record["id"],
                {
                    CONV_STATUS_FIELD: _pick_status("DELIVERED"),
                    CONV_PROCESSED_BY_FIELD: self.processed_by,
                    CONV_PROCESSED_AT_FIELD: iso_now(),
                    CONV_INTENT_FIELD: event,
                    CONV_STAGE_FIELD: conv_stage_label,  # DNC
                },
            )
            return

        # Ensure/attach lead (we always create so the thread is tracked)
        lead_id, property_id = self._ensure_lead(from_number, fields, prospect_record)

        # 30-day follow-ups (not interested)
        if event in {"interest_no", "followup_30"}:
            # Mark Prospect Phone 1 & Verified
            self._mark_phone1_and_verify(prospect_id, from_number, prospect_fields)
            when = datetime.now(timezone.utc) + timedelta(days=FOLLOWUP_DAYS)
            drip_id = self._enqueue_drip(
                fields=fields,
                reply_text="(30-day follow-up)",
                when_utc=when,
                template_id=None,
                prospect_id=prospect_id,
                category_hint="followup_30",
            )
            payload = {
                CONV_STATUS_FIELD: _pick_status("DELIVERED"),
                CONV_PROCESSED_BY_FIELD: self.processed_by,
                CONV_PROCESSED_AT_FIELD: iso_now(),
                CONV_INTENT_FIELD: event,
                CONV_STAGE_FIELD: conv_stage_label,
                CONV_LEAD_LINK_FIELD: [lead_id] if lead_id else None,
                CONV_PROSPECT_LINK_FIELD: [prospect_id] if prospect_id else None,
            }
            if drip_id:
                payload[CONV_DRIP_LINK_FIELD] = [drip_id]
            self.convos.update(record["id"], payload)
            self._update_lead_touch(lead_id, body)
            return

        # Recent AR reply suppression
        if _recent_ar_reply(fields, self.processed_by) and event not in {"price_provided", "ask_offer"}:
            self.convos.update(
                record["id"],
                {
                    CONV_STATUS_FIELD: _pick_status("DELIVERED"),
                    CONV_PROCESSED_BY_FIELD: self.processed_by,
                    CONV_PROCESSED_AT_FIELD: iso_now(),
                    CONV_INTENT_FIELD: event,
                    CONV_STAGE_FIELD: conv_stage_label,
                    CONV_LEAD_LINK_FIELD: [lead_id] if lead_id else None,
                    CONV_PROSPECT_LINK_FIELD: [prospect_id] if prospect_id else None,
                },
            )
            self._update_lead_touch(lead_id, body)
            return

        # Choose template & send/queue
        route_keys = TEMPLATE_ROUTES.get(event) or ()
        personalization = _personalize(prospect_fields)
        reply_text, template_id, route_used = self._choose_template(route_keys, personalization)

        # Quiet hours: queue at next_allowed; else immediate or drip if transport available
        queue_time = next_allowed if is_quiet else datetime.now(timezone.utc)
        queued = False
        drip_id: Optional[str] = None
        if self.drip:
            drip_id = self._enqueue_drip(
                fields=fields,
                reply_text=reply_text,
                when_utc=queue_time,
                template_id=template_id,
                prospect_id=prospect_id,
                category_hint=route_used,
            )
            queued = bool(drip_id)
        if not queued:
            to_number = _get_first(fields, CONV_TO_CANDIDATES)
            self._send_immediate(from_number, reply_text or "", to_number, lead_id, property_id)

        # Update conversation after action
        update_payload = {
            CONV_STATUS_FIELD: _pick_status("DELIVERED"),
            CONV_PROCESSED_BY_FIELD: self.processed_by,
            CONV_PROCESSED_AT_FIELD: iso_now(),
            CONV_INTENT_FIELD: event,
            CONV_STAGE_FIELD: conv_stage_label,
            CONV_TEMPLATE_RECORD_FIELD: template_id,
            CONV_TEMPLATE_LINK_FIELD: [template_id] if template_id else None,
            CONV_LEAD_LINK_FIELD: [lead_id] if lead_id else None,
            CONV_PROSPECT_LINK_FIELD: [prospect_id] if prospect_id else None,
        }
        if drip_id:
            update_payload[CONV_DRIP_LINK_FIELD] = [drip_id]
        self.convos.update(record["id"], update_payload)

        # Lead touches
        self._update_lead_touch(lead_id, body)

        # Follow-up scheduler for ops (non-terminal)
        try:
            schedule_from_response(
                phone=from_number,
                intent=event,
                lead_id=lead_id,
                market=fields.get("Market") or prospect_fields.get(PROSPECT_FIELDS.get("MARKET")),
                property_id=fields.get(CONV_PROPERTY_ID_FIELD),
                current_stage=conv_stage_label,
            )
        except Exception as exc:
            self.summary["errors"].append({"conversation": record.get("id"), "error": f"followup schedule failed: {exc}"})


def run_autoresponder(limit: int = 50) -> Dict[str, Any]:
    service = Autoresponder()
    return service.process(limit)


if __name__ == "__main__":
    limit = int(os.getenv("AR_LIMIT", "50"))
    result = run_autoresponder(limit=limit)
    print("\n=== Autoresponder Summary ===")
    import pprint
    pprint.pprint(result)
