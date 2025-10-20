"""Intent-aware autoresponder backed by the schema-driven datastore."""

from __future__ import annotations

import os
import random
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from sms.airtable_schema import (
    CONVERSATIONS_TABLE,
    DRIP_QUEUE_TABLE,
    TEMPLATES_TABLE,
    ConversationDirection,
    ConversationProcessor,
    conversations_field_map,
    drip_field_map,
    leads_field_map,
    prospects_field_map,
    template_field_map,
)
from sms.config import settings
from sms.datastore import (
    CONNECTOR,
    create_record,
    list_records,
    promote_to_lead,
    update_record,
)
from sms.dispatcher import get_policy
from sms.runtime import get_logger, iso_now, last_10_digits

try:  # Optional messaging transport for immediate replies
    from sms.message_processor import MessageProcessor
except Exception:  # pragma: no cover - optional dependency
    MessageProcessor = None  # type: ignore

try:  # Follow-up scheduling hook
    from sms.followup_flow import schedule_from_response
except Exception:  # pragma: no cover - optional dependency

    def schedule_from_response(**_: Any) -> None:
        pass


try:  # Local fallback templates for tests
    from sms import templates as local_templates
except Exception:  # pragma: no cover - optional dependency
    local_templates = None  # type: ignore


logger = get_logger(__name__)


class TableFacade:
    """Adapter exposing Airtable handles with the FakeTable interface used in tests."""

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
CONV_TEXTGRID_ID_FIELD = CONV_FIELDS["TEXTGRID_ID"]
CONV_RECEIVED_AT_FIELD = CONV_FIELDS["RECEIVED_AT"]
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
CONV_LEAD_RECORD_FIELD = CONV_FIELD_NAMES["LEAD_RECORD_ID"]
CONV_PROPERTY_ID_FIELD = CONV_FIELD_NAMES["PROPERTY_ID"]
CONV_CAMPAIGN_LINK_FIELD = CONV_FIELD_NAMES["CAMPAIGN_LINK"]
CONV_DRIP_LINK_FIELD = CONV_FIELD_NAMES["DRIP_QUEUE_LINK"]

DRIP_STATUS_FIELD = DRIP_FIELDS["STATUS"]
DRIP_PROCESSOR_FIELD = DRIP_FIELDS.get("PROCESSOR", "processor")
DRIP_MARKET_FIELD = DRIP_FIELDS.get("MARKET", "Market")
DRIP_TEMPLATE_LINK_FIELD = DRIP_FIELDS.get("TEMPLATE_LINK", "Template")
DRIP_PROSPECT_LINK_FIELD = DRIP_FIELDS.get("PROSPECT_LINK", "Prospect")
DRIP_CAMPAIGN_LINK_FIELD = DRIP_FIELDS.get("CAMPAIGN_LINK", "Campaign")
DRIP_SELLER_PHONE_FIELD = DRIP_FIELDS.get("SELLER_PHONE", "phone")
DRIP_TEXTGRID_PHONE_FIELD = DRIP_FIELDS.get("TEXTGRID_PHONE", "from_number")
DRIP_FROM_NUMBER_FIELD = DRIP_FIELDS.get("FROM_NUMBER", "From Number")
DRIP_MESSAGE_PREVIEW_FIELD = DRIP_FIELDS.get("MESSAGE_PREVIEW", "message_preview")
DRIP_NEXT_SEND_DATE_FIELD = DRIP_FIELDS.get("NEXT_SEND_DATE", "next_send_date")
DRIP_PROPERTY_ID_FIELD = DRIP_FIELDS.get("PROPERTY_ID", "Property ID")
DRIP_UI_FIELD = DRIP_FIELDS.get("UI", "UI")

TEMPLATE_INTENT_FIELD = TEMPLATE_FIELDS.get("INTERNAL_ID", "Internal ID")
TEMPLATE_MESSAGE_FIELD = TEMPLATE_FIELDS.get("MESSAGE", "Message")

LEAD_STATUS_FIELD = LEAD_FIELDS["STATUS"]

CONV_FROM_CANDIDATES = [CONV_FROM_FIELD, "From", "phone"]
CONV_TO_CANDIDATES = [CONV_TO_FIELD, "To", "to_number"]
CONV_BODY_CANDIDATES = [CONV_BODY_FIELD, "Body", "message"]
CONV_DIRECTION_CANDIDATES = [CONV_DIRECTION_FIELD, "Direction", "direction"]
CONV_PROCESSED_BY_CANDIDATES = [CONV_PROCESSED_BY_FIELD, "Processed By", "processed_by"]

SAFE_CONVERSATION_STATUS = {"RESPONDED", "AI_HANDOFF", "DNC"}

STATUS_ICON = {
    "QUEUED": "⏳",
    "READY": "⏳",
    "SENDING": "🔄",
    "SENT": "✅",
    "DELIVERED": "✅",
    "FAILED": "❌",
    "CANCELLED": "❌",
}

STAGE_MAP = {
    "intro": "Stage 1 - Owner Check",
    "who_is_this": "Stage 1 - Identity",
    "how_get_number": "Stage 1 - Compliance",
    "neutral": "Stage 1 - Owner Check",
    "followup_yes": "Stage 2 - Offer Interest",
    "followup_no": "Stage 2 - Offer Declined",
    "followup_wrong": "Stage 2 - Wrong Number",
    "not_owner": "Stage 2 - Not Owner",
    "interest": "Stage 2 - Offer Interest",
    "price_response": "Stage 3 - Price Discussion",
    "condition_response": "Stage 3 - Condition Discussion",
    "optout": "Opt-Out",
    "negative": "Stage 2 - Negative",
    "delay": "Stage 2 - Follow Up Later",
}

# Intent lexicon ---------------------------------------------------------------
STOP_WORDS = {"stop", "unsubscribe", "remove", "quit", "cancel", "end"}
YES_WORDS = {"yes", "yeah", "yep", "sure", "affirmative", "correct", "that is me", "that's me", "of course", "i am"}
NO_WORDS = {"no", "nope", "nah", "not interested", "dont bother", "stop texting"}
WRONG_WORDS = {"wrong number", "not mine", "dont own", "do not own", "no owner", "new number"}
INTEREST_WORDS = {
    "offer",
    "what can you offer",
    "how much",
    "cash",
    "interested",
    "curious",
    "talk",
    "price",
    "numbers",
    "what’s your number",
    "whats your number",
    "what is your number",
}
PRICE_WORDS = {"price", "asking", "$", " k", "k ", "number you have in mind", "how much", "range", "ballpark"}
COND_WORDS = {"condition", "repairs", "needs work", "renovated", "tenant", "tenants", "vacant", "occupied", "as-is"}
DELAY_WORDS = {"later", "next week", "tomorrow", "busy", "call me later", "text later", "reach out later", "follow up"}
NEG_WORDS = {"scam", "spam", "go away", "lose my number", "stop harassing", "reported", "lawsuit"}
WHO_PHRASES = {"who is this", "who's this", "whos this", "who are you", "who dis", "identify yourself"}
HOW_NUM_PHRASES = {
    "how did you get my number",
    "how did you get my #",
    "how you get my number",
    "why do you have my number",
    "where did you get my number",
    "how got my number",
}


# ------------------------------------------------------------------------------
# Intent classification
# ------------------------------------------------------------------------------
def _has_any(text: str, phrases: Iterable[str]) -> bool:
    return any(phrase in text for phrase in phrases)


def classify_intent(body: str) -> str:
    text = (body or "").lower().strip()

    if _has_any(text, STOP_WORDS):
        return "optout"
    if _has_any(text, WHO_PHRASES):
        return "who_is_this"
    if _has_any(text, HOW_NUM_PHRASES):
        return "how_get_number"
    if _has_any(text, WRONG_WORDS):
        return "followup_wrong"
    if _has_any(text, NEG_WORDS):
        return "negative"
    if _has_any(text, DELAY_WORDS):
        return "delay"

    if re.search(r"\b(" + "|".join(map(re.escape, YES_WORDS)) + r")\b", text):
        return "followup_yes"
    if re.search(r"\b(" + "|".join(map(re.escape, NO_WORDS)) + r")\b", text):
        return "followup_no"

    if _has_any(text, PRICE_WORDS):
        return "price_response"
    if _has_any(text, COND_WORDS):
        return "condition_response"
    if _has_any(text, INTEREST_WORDS):
        return "interest"
    if any(term in text for term in ["maybe", "not sure", "thinking", "depends", "idk", "i don’t know", "i don't know"]):
        return "neutral"
    return "intro"


# ------------------------------------------------------------------------------
# Quiet hours
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


# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------
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
    return preferred if preferred in SAFE_CONVERSATION_STATUS else next(iter(SAFE_CONVERSATION_STATUS))


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
    if key == "PHONE_PRIMARY" or key == "PHONE_PRIMARY_LINKED":
        return PROSPECT_FIELDS.get("PHONE_PRIMARY_VERIFIED")
    if key == "PHONE_SECONDARY" or key == "PHONE_SECONDARY_LINKED":
        return PROSPECT_FIELDS.get("PHONE_SECONDARY_VERIFIED")
    return None


# ------------------------------------------------------------------------------
# Autoresponder implementation
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
        self.summary: Dict[str, Any] = {"processed": 0, "breakdown": {}, "errors": []}
        self.templates_by_intent = self._index_templates()
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

    # ------------------------------------------------------------------ Template loading
    def _index_templates(self) -> Dict[str, List[Dict[str, Any]]]:
        records = self.templates.all()
        pools: Dict[str, List[Dict[str, Any]]] = {}
        for record in records:
            fields = record.get("fields", {}) or {}
            intent_key = str(fields.get(TEMPLATE_INTENT_FIELD) or fields.get("intent") or "").strip().lower()
            if not intent_key:
                continue
            pools.setdefault(intent_key, []).append(record)
        return pools

    def _choose_template(self, intent: str, personalization: Dict[str, str]) -> Tuple[str, Optional[str]]:
        pool = self.templates_by_intent.get(intent) or []
        if pool:
            chosen = random.choice(pool)
            fields = chosen.get("fields", {}) or {}
            raw = str(fields.get(TEMPLATE_MESSAGE_FIELD) or "")
            try:
                message = raw.format(**personalization)
            except Exception:
                message = raw
            return message or "Thanks for the reply.", chosen.get("id")

        if local_templates:
            try:
                message = local_templates.get_template(intent, personalization)
                return message, None
            except Exception:
                pass

        return "Thanks for the reply.", None

    # ------------------------------------------------------------------ Fetching
    def _fetch_inbound(self, limit: int) -> List[Dict[str, Any]]:
        view = os.getenv("CONV_VIEW_INBOUND", "Unprocessed Inbounds")
        try:
            records = self.convos.all(view=view, max_records=limit)
            if records:
                return records
        except Exception:
            logger.warning("Failed to fetch Conversations view '%s'; falling back to scan", view, exc_info=True)

        fallback = []
        for record in self.convos.all(max_records=limit * 2):
            fields = record.get("fields", {}) or {}
            direction = str(_get_first(fields, CONV_DIRECTION_CANDIDATES) or "").upper()
            processed_by = _get_first(fields, CONV_PROCESSED_BY_CANDIDATES)
            if direction in ("IN", "INBOUND") and not processed_by:
                fallback.append(record)
            if len(fallback) >= limit:
                break
        return fallback

    # ------------------------------------------------------------------ Prospect helpers
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

    def _ensure_lead(self, phone: str, fields: Dict[str, Any], prospect: Optional[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
        if isinstance(self.leads, TableFacade):
            return promote_to_lead(phone, source=self.processed_by, conversation_fields=fields)

        existing = self._find_record_by_phone(self.leads, self.lead_phone_fields, phone)
        if existing:
            property_id = (prospect or {}).get("fields", {}).get(PROSPECT_FIELDS.get("PROPERTY_ID")) if prospect else None
            return existing["id"], property_id

        payload = {}
        phone_field = LEAD_FIELDS.get("PHONE") or "Phone"
        payload[phone_field] = phone
        status_field = LEAD_STATUS_FIELD or "Lead Status"
        payload[status_field] = "New"
        source_field = LEAD_FIELDS.get("SOURCE") or "Source"
        payload[source_field] = self.processed_by
        created = self.leads.create(payload)
        property_id = (prospect or {}).get("fields", {}).get(PROSPECT_FIELDS.get("PROPERTY_ID")) if prospect else None
        return created.get("id"), property_id

    def _find_prospect(self, phone: str) -> Optional[Dict[str, Any]]:
        return self._find_record_by_phone(self.prospects, self.prospect_phone_fields, phone)

    # ------------------------------------------------------------------ Lead helpers
    def _update_lead_status(self, lead_id: Optional[str], status: str) -> None:
        if not lead_id:
            return
        try:
            self.leads.update(lead_id, {LEAD_STATUS_FIELD: status})
        except Exception:
            pass

    # ------------------------------------------------------------------ Conversational flow
    def _handle_optout(self, record: Dict[str, Any], fields: Dict[str, Any], intent: str, stage: str) -> None:
        from_number = str(_get_first(fields, CONV_FROM_CANDIDATES) or "")
        body = str(_get_first(fields, CONV_BODY_CANDIDATES) or "")

        lead_id, _ = self._ensure_lead(from_number, fields, None)
        if lead_id:
            self._update_lead_status(lead_id, "DNC")

        self.convos.update(
            record["id"],
            {
                CONV_STATUS_FIELD: _pick_status("DNC"),
                CONV_PROCESSED_BY_FIELD: self.processed_by,
                CONV_PROCESSED_AT_FIELD: iso_now(),
                CONV_INTENT_FIELD: intent,
                CONV_STAGE_FIELD: stage,
            },
        )

        if lead_id:
            try:
                self.leads.update(
                    lead_id,
                    {
                        LEAD_FIELDS["LAST_MESSAGE"]: body[:500],
                        LEAD_FIELDS["LAST_DIRECTION"]: ConversationDirection.INBOUND.value,
                        LEAD_STATUS_FIELD: "DNC",
                    },
                )
            except Exception:
                pass

    def _handle_ai_handoff(
        self,
        record: Dict[str, Any],
        fields: Dict[str, Any],
        intent: str,
        stage: str,
        lead_id: Optional[str],
        prospect_id: Optional[str],
    ) -> None:
        from_number = str(_get_first(fields, CONV_FROM_CANDIDATES) or "")
        body = str(_get_first(fields, CONV_BODY_CANDIDATES) or "")

        try:
            schedule_from_response(
                phone=from_number,
                intent=intent,
                lead_id=lead_id,
                market=fields.get("Market"),
                property_id=fields.get(CONV_PROPERTY_ID_FIELD),
                current_stage=stage,
            )
        except Exception as exc:
            self.summary["errors"].append({"conversation": record.get("id"), "error": f"followup schedule failed: {exc}"})

        update = {
            CONV_STATUS_FIELD: _pick_status("AI_HANDOFF"),
            CONV_PROCESSED_BY_FIELD: "AI Closer",
            CONV_PROCESSED_AT_FIELD: iso_now(),
            CONV_INTENT_FIELD: intent,
            CONV_STAGE_FIELD: stage,
            CONV_LEAD_LINK_FIELD: [lead_id] if lead_id else None,
            CONV_PROSPECT_LINK_FIELD: [prospect_id] if prospect_id else None,
        }
        self.convos.update(record["id"], update)

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

    def _enqueue_reply(
        self,
        record: Dict[str, Any],
        fields: Dict[str, Any],
        reply_text: str,
        template_id: Optional[str],
        queue_time: datetime,
        lead_id: Optional[str],
        prospect_id: Optional[str],
    ) -> bool:
        if not self.drip:
            return False

        campaign_link = _normalise_link(fields.get(CONV_CAMPAIGN_LINK_FIELD))
        payload = {
            DRIP_STATUS_FIELD: "QUEUED",
            DRIP_PROCESSOR_FIELD: self.processed_by,
            DRIP_MARKET_FIELD: fields.get("Market"),
            DRIP_SELLER_PHONE_FIELD: _get_first(fields, CONV_FROM_CANDIDATES),
            DRIP_TEXTGRID_PHONE_FIELD: _get_first(fields, CONV_TO_CANDIDATES),
            DRIP_FROM_NUMBER_FIELD: _get_first(fields, CONV_TO_CANDIDATES),
            DRIP_MESSAGE_PREVIEW_FIELD: reply_text,
            DRIP_NEXT_SEND_DATE_FIELD: queue_time.astimezone(timezone.utc).isoformat(),
            DRIP_PROPERTY_ID_FIELD: fields.get(CONV_PROPERTY_ID_FIELD),
            DRIP_UI_FIELD: STATUS_ICON.get("QUEUED"),
        }
        if template_id:
            payload[DRIP_TEMPLATE_LINK_FIELD] = [template_id]
        if prospect_id:
            payload[DRIP_PROSPECT_LINK_FIELD] = [prospect_id]
        if campaign_link:
            payload[DRIP_CAMPAIGN_LINK_FIELD] = [campaign_link]

        created = None
        try:
            created = self.drip.create(payload) if self.drip else None
        except Exception as exc:
            self.summary["errors"].append({"conversation": record.get("id"), "error": f"Queue failed: {exc}"})
            return False
        if created and created.get("id"):
            self.convos.update(
                record["id"],
                {CONV_DRIP_LINK_FIELD: [created["id"]], CONV_TEMPLATE_LINK_FIELD: [template_id] if template_id else None},
            )
            return True
        return False

    def _send_immediate(
        self,
        from_number: str,
        body: str,
        to_number: Optional[str],
        lead_id: Optional[str],
        property_id: Optional[str],
    ) -> None:
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

    # ------------------------------------------------------------------ Main processing loop
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

        prospect_record = self._find_prospect(from_number)
        prospect_fields = (prospect_record or {}).get("fields", {}) or {}

        self._mark_phone_verified(prospect_record, from_number)

        intent = classify_intent(body)
        stage = STAGE_MAP.get(intent, "Stage 1 - Owner Check")

        logger.info("Autoresponder IN %s intent=%s", from_number, intent)
        self.summary["processed"] += 1
        self.summary["breakdown"][intent] = self.summary["breakdown"].get(intent, 0) + 1

        if intent == "optout":
            self._handle_optout(record, fields, intent, stage)
            return

        lead_id, property_id = self._ensure_lead(from_number, fields, prospect_record)
        prospect_id = None
        if prospect_record:
            prospect_id = prospect_record.get("id")
        elif fields.get(CONV_PROSPECT_RECORD_FIELD):
            prospect_id = _normalise_link(fields.get(CONV_PROSPECT_RECORD_FIELD))

        if intent in ("price_response", "condition_response"):
            self._handle_ai_handoff(record, fields, intent, stage, lead_id, prospect_id)
        else:
            personalization = _personalize(prospect_fields)
            reply_text, template_id = self._choose_template(intent, personalization)

            queue_time = next_allowed if is_quiet else datetime.now(timezone.utc)
            queued = False
            if self.drip:
                queued = self._enqueue_reply(record, fields, reply_text, template_id, queue_time, lead_id, prospect_id)
            if not queued:
                to_number = _get_first(fields, CONV_TO_CANDIDATES)
                self._send_immediate(from_number, reply_text, to_number, lead_id, property_id)

            update_payload = {
                CONV_STATUS_FIELD: _pick_status("RESPONDED"),
                CONV_PROCESSED_BY_FIELD: self.processed_by,
                CONV_PROCESSED_AT_FIELD: iso_now(),
                CONV_INTENT_FIELD: intent,
                CONV_STAGE_FIELD: stage,
                CONV_TEMPLATE_RECORD_FIELD: template_id,
                CONV_TEMPLATE_LINK_FIELD: [template_id] if template_id else None,
                CONV_LEAD_LINK_FIELD: [lead_id] if lead_id else None,
                CONV_PROSPECT_LINK_FIELD: [prospect_id] if prospect_id else None,
            }
            self.convos.update(record["id"], update_payload)

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
            if intent == "followup_yes":
                self._update_lead_status(lead_id, "Interested")
            elif intent == "optout":
                self._update_lead_status(lead_id, "DNC")

        try:
            schedule_from_response(
                phone=from_number,
                intent=intent,
                lead_id=lead_id,
                market=fields.get("Market") or prospect_fields.get(PROSPECT_FIELDS.get("MARKET")),
                property_id=property_id or fields.get(CONV_PROPERTY_ID_FIELD),
                current_stage=stage,
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
