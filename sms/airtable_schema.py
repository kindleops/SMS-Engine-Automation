from __future__ import annotations

"""
Central Airtable schema definitions and helpers.

This module keeps the canonical field names for Airtable bases/tables together
so business logic can import lightweight helpers instead of hard-coding strings.
Environment variables can still override individual field names (to align with
custom Airtable copies), but the defaults here should always reflect the live
schema.
"""

import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Iterable, Tuple


# ---------------------------------------------------------------------------
# Core data containers
# ---------------------------------------------------------------------------


def _clean(value: str | None) -> str | None:
    if value is None:
        return None
    v = value.strip()
    return v if v else None


@dataclass(frozen=True)
class FieldDefinition:
    """
    Represents an Airtable column.

    Args:
        default: Canonical field name in Airtable.
        env_vars: Ordered list of env vars that can override the field name.
                   (first non-empty wins; keeps compatibility with legacy envs).
        options: Allowed values for single-select fields (if applicable).
        fallbacks: Optional legacy column names to try when reading old rows.
    """

    default: str
    env_vars: Tuple[str, ...] = field(default_factory=tuple)
    options: Tuple[str, ...] = field(default_factory=tuple)
    fallbacks: Tuple[str, ...] = field(default_factory=tuple)

    def resolve(self) -> str:
        """Return the active field name (env override or default)."""
        for env in self.env_vars:
            override = _clean(os.getenv(env))
            if override:
                return override
        return self.default

    def candidates(self) -> Tuple[str, ...]:
        """
        Return the primary field name plus unique fallbacks.

        Helpful when reading from Airtable snapshots that might predate the
        canonical rename (e.g., "phone" before "Seller Phone Number").
        """
        ordered: Tuple[str, ...] = (self.resolve(),) + self.fallbacks
        seen: set[str] = set()
        unique: list[str] = []
        for name in ordered:
            if not name:
                continue
            if name not in seen:
                unique.append(name)
                seen.add(name)
        return tuple(unique)


@dataclass(frozen=True)
class TableDefinition:
    """
    Airtable table metadata with helpers to resolve field names.

    Args:
        default: Human-readable table name in Airtable.
        env_vars: Env vars that can rename the table.
        fields: Mapping of logical keys → FieldDefinition.
    """

    default: str
    env_vars: Tuple[str, ...] = field(default_factory=tuple)
    fields: Dict[str, FieldDefinition] = field(default_factory=dict)

    def name(self) -> str:
        for env in self.env_vars:
            override = _clean(os.getenv(env))
            if override:
                return override
        return self.default

    def field_name(self, key: str) -> str:
        return self.fields[key].resolve()

    def field_names(self) -> Dict[str, str]:
        return {key: field.resolve() for key, field in self.fields.items()}

    def field_candidates(self) -> Dict[str, Tuple[str, ...]]:
        return {key: field.candidates() for key, field in self.fields.items()}


# ---------------------------------------------------------------------------
# Conversations table enumerations
# ---------------------------------------------------------------------------


class ConversationStage(str, Enum):
    STAGE_1_OWNERSHIP_CONFIRMATION = "STAGE 1 - OWNERSHIP CONFIRMATION"
    STAGE_2_INTEREST_FEELER = "STAGE 2 - INTEREST FEELER"
    STAGE_3_PRICE_QUALIFICATION = "STAGE 3 - PRICE QUALIFICATION"
    STAGE_4_PROPERTY_CONDITION = "STAGE 4 - PROPERTY CONDITION"
    STAGE_5_MOTIVATION_TIMELINE = "STAGE 5 - MOTIVATION / TIMELINE"
    STAGE_6_OFFER_FOLLOW_UP = "STAGE 6 - OFFER FOLLOW UP"
    STAGE_7_CONTRACT_READY = "STAGE 7 - CONTRACT READY"
    STAGE_8_CONTRACT_SENT = "STAGE 8 - CONTRACT SENT"
    STAGE_9_CONTRACT_FOLLOW_UP = "STAGE 9 - CONTRACT FOLLOW UP"
    OPT_OUT = "OPT OUT"
    DNC = "DNC"


class ConversationProcessor(str, Enum):
    CAMPAIGN_RUNNER = "Campaign Runner"
    AUTORESPONDER = "Autoresponder"
    AI_PHI_3_MINI = "AI: Phi-3 Mini"
    AI_PHI_3_MEDIUM = "AI: Phi-3 Medium"
    AI_GPT_4O = "AI: GPT-4o"
    AI_MISTRAL_7B = "AI: Mistral 7B"
    AI_GEMMA_2 = "AI: Gemma 2"
    MANUAL = "Manual / Human"


class ConversationIntent(str, Enum):
    POSITIVE = "Positive"
    NEUTRAL = "Neutral"
    DELAY = "Delay"
    REJECT = "Reject"
    DNC = "DNC"


class ConversationDirection(str, Enum):
    INBOUND = "INBOUND"
    OUTBOUND = "OUTBOUND"


class ConversationDeliveryStatus(str, Enum):
    QUEUED = "QUEUED"
    SENT = "SENT"
    DELIVERED = "DELIVERED"
    FAILED = "FAILED"
    UNDELIVERED = "UNDELIVERED"
    OPT_OUT = "OPT OUT"


class ConversationAIIntent(str, Enum):
    INTRO = "intro"
    WHO_IS_THIS = "who_is_this"
    HOW_GOT_NUMBER = "how_got_number"
    INTEREST_DETECTED = "interest_detected"
    ASK_PRICE = "ask_price"
    OFFER_DISCUSSION = "offer_discussion"
    MOTIVATION_DETECTED = "motivation_detected"
    CONDITION_QUESTION = "condition_question"
    NOT_INTERESTED = "not_interested"
    WRONG_NUMBER = "wrong_number"
    DELAY = "delay"
    NEUTRAL = "neutral"
    OTHER = "other"
    TIMELINE_QUESTION = "timeline_question"


# ---------------------------------------------------------------------------
# Conversations table schema (Leads & Conversations base)
# ---------------------------------------------------------------------------

CONVERSATIONS_TABLE = TableDefinition(
    default="Conversations",
    env_vars=("CONVERSATIONS_TABLE",),
    fields={
        # Core status fields
        "STAGE": FieldDefinition(
            default="Stage",
            env_vars=("CONV_STAGE_FIELD",),
            options=tuple(stage.value for stage in ConversationStage),
            fallbacks=("stage",),
        ),
        "PROCESSED_BY": FieldDefinition(
            default="Processed By",
            env_vars=("CONV_PROCESSED_BY_FIELD",),
            options=tuple(proc.value for proc in ConversationProcessor),
            fallbacks=("processed_by",),
        ),
        "INTENT": FieldDefinition(
            default="Intent Detected",
            env_vars=("CONV_INTENT_FIELD",),
            options=tuple(intent.value for intent in ConversationIntent),
            fallbacks=("intent_detected", "Intent"),
        ),
        "DIRECTION": FieldDefinition(
            default="Direction",
            env_vars=("CONV_DIRECTION_FIELD",),
            options=tuple(direction.value for direction in ConversationDirection),
            fallbacks=("direction",),
        ),
        "STATUS": FieldDefinition(
            default="Delivery Status",
            env_vars=("CONV_STATUS_FIELD",),
            options=tuple(status.value for status in ConversationDeliveryStatus),
            fallbacks=("status",),
        ),
        "AI_INTENT": FieldDefinition(
            default="AI Intent",
            env_vars=("CONV_AI_INTENT_FIELD",),
            options=tuple(intent.value for intent in ConversationAIIntent),
            fallbacks=("ai_intent",),
        ),
        # Messaging metadata
        "TEXTGRID_PHONE": FieldDefinition(
            default="TextGrid Phone Number",
            env_vars=("CONV_TO_FIELD", "CONV_TEXTGRID_PHONE_FIELD"),
            fallbacks=("to_number", "To", "DID", "TextGrid Number"),
        ),
        "TEXTGRID_ID": FieldDefinition(
            default="TextGrid ID",
            env_vars=("CONV_TEXTGRID_ID_FIELD",),
            fallbacks=("MessageSid", "message_sid", "sid"),
        ),
        "TEMPLATE_RECORD_ID": FieldDefinition(
            default="Template Record ID",
            env_vars=("CONV_TEMPLATE_RECORD_ID_FIELD",),
            fallbacks=("template_record_id", "Template ID"),
        ),
        "SELLER_PHONE": FieldDefinition(
            default="Seller Phone Number",
            env_vars=("CONV_FROM_FIELD", "CONV_SELLER_PHONE_FIELD"),
            fallbacks=("phone", "Phone", "From", "seller_phone"),
        ),
        "MESSAGE": FieldDefinition(
            default="Message",
            env_vars=("CONV_MESSAGE_FIELD",),
            fallbacks=("Body", "message"),
        ),
        "MESSAGE_SUMMARY": FieldDefinition(
            default="Message Summary (AI)",
            env_vars=("CONV_MESSAGE_SUMMARY_FIELD",),
            fallbacks=("message_summary", "AI Summary"),
        ),
        "RECEIVED_AT": FieldDefinition(
            default="Received Time",
            env_vars=("CONV_RECEIVED_AT_FIELD",),
            fallbacks=("received_at", "Received At"),
        ),
        "PROCESSED_AT": FieldDefinition(
            default="Processed Time",
            env_vars=("CONV_PROCESSED_AT_FIELD",),
            fallbacks=("processed_at", "Processed At"),
        ),
        "SENT_AT": FieldDefinition(
            default="Last Sent Time",
            env_vars=("CONV_SENT_AT_FIELD",),
            fallbacks=("sent_at", "Sent At"),
        ),
        "LAST_REPLY_AT": FieldDefinition(
            default="Last Reply Time",
            env_vars=("CONV_LAST_REPLY_AT_FIELD",),
            fallbacks=("last_reply_time", "Last Reply At"),
        ),
        "LAST_RETRY_AT": FieldDefinition(
            default="Last Retry Time",
            env_vars=("CONV_LAST_RETRY_AT_FIELD",),
            fallbacks=("last_retry_time", "Last Retry At"),
        ),
        "AI_RESPONSE_TRIGGER": FieldDefinition(
            default="AI Response Trigger",
            env_vars=("CONV_AI_RESPONSE_TRIGGER_FIELD",),
            fallbacks=("ai_response_trigger",),
        ),
        # Linkages / identifiers
        "PROSPECT_RECORD_ID": FieldDefinition(
            default="Prospect Record ID",
            env_vars=("CONV_PROSPECT_RECORD_ID_FIELD",),
            fallbacks=("prospect_record_id", "Prospect ID"),
        ),
        "LEAD_RECORD_ID": FieldDefinition(
            default="Lead Record ID",
            env_vars=("CONV_LEAD_RECORD_ID_FIELD", "CONV_LEAD_LINK_FIELD"),
            fallbacks=("lead_record_id", "Lead ID", "lead"),
        ),
        "CAMPAIGN_RECORD_ID": FieldDefinition(
            default="Campaign Record ID",
            env_vars=("CONV_CAMPAIGN_RECORD_ID_FIELD",),
            fallbacks=("campaign_record_id", "Campaign ID"),
        ),
        "CAMPAIGN_LINK": FieldDefinition(
            default="Campaign",
            env_vars=("CONV_CAMPAIGN_LINK_FIELD", "CONV_CAMPAIGN_FIELD"),
            fallbacks=("Campaign", "campaign"),
        ),
        "TEMPLATE_LINK": FieldDefinition(
            default="Template",
            env_vars=("CONV_TEMPLATE_LINK_FIELD",),
            fallbacks=("Template",),
        ),
        "PROSPECT_LINK": FieldDefinition(
            default="Prospect",
            env_vars=("CONV_PROSPECT_LINK_FIELD",),
            fallbacks=("Prospect",),
        ),
        "PROSPECTS_LINK": FieldDefinition(
            default="Prospects",
            env_vars=("CONV_PROSPECTS_LINK_FIELD",),
            fallbacks=("Prospects",),
        ),
        "DRIP_QUEUE_LINK": FieldDefinition(
            default="Drip Queue",
            env_vars=("CONV_DRIP_LINK_FIELD",),
            fallbacks=("Drip Queue",),
        ),
        "LEAD_LINK": FieldDefinition(
            default="Lead",
            env_vars=("CONV_LEAD_LINK_FIELD",),
            fallbacks=("Lead",),
        ),
        "PROPERTY_ID": FieldDefinition(
            default="Property Record ID",
            env_vars=("CONV_PROPERTY_ID_FIELD",),
            fallbacks=("property_record_id", "Property ID"),
        ),
        "CONVERSATION_ID": FieldDefinition(
            default="Conversation ID",
            env_vars=("CONV_PRIMARY_FIELD",),
            fallbacks=("ConversationID",),
        ),
        "RECORD_ID": FieldDefinition(
            default="Record ID",
            env_vars=("CONV_RECORD_ID_FIELD",),
            fallbacks=("Record ID", "record_id"),
        ),
        # Counters / metrics
        "SENT_COUNT": FieldDefinition(
            default="Sent Count",
            env_vars=("CONV_SENT_COUNT_FIELD",),
            fallbacks=("sent_count",),
        ),
        "REPLY_COUNT": FieldDefinition(
            default="Reply Count",
            env_vars=("CONV_REPLY_COUNT_FIELD",),
            fallbacks=("reply_count",),
        ),
        "RETRY_COUNT": FieldDefinition(
            default="Retry Count",
            env_vars=("CONV_RETRY_COUNT_FIELD",),
            fallbacks=("retry_count",),
        ),
        "RETRY_AFTER": FieldDefinition(
            default="Retry After",
            env_vars=("CONV_RETRY_AFTER_FIELD",),
            fallbacks=("retry_after",),
        ),
        "LAST_ERROR": FieldDefinition(
            default="Last Error",
            env_vars=("CONV_LAST_ERROR_FIELD",),
            fallbacks=("last_retry_error", "last_error"),
        ),
        "PERMANENT_FAIL": FieldDefinition(
            default="Permanent Fail Reason",
            env_vars=("CONV_PERM_FAIL_FIELD",),
            fallbacks=("permanent_fail_reason", "perm_fail_reason"),
        ),
        "RESPONSE_TIME_MINUTES": FieldDefinition(
            default="Response Time (Minutes)",
            env_vars=("CONV_RESPONSE_TIME_FIELD",),
            fallbacks=("response_time_minutes", "response_minutes"),
        ),
    },
)


def conversations_field_map() -> Dict[str, str]:
    """
    Convenience accessor mirroring the legacy CONV_FIELDS dict:

        {
            "FROM": "Seller Phone Number",
            "TO": "TextGrid Phone Number",
            ...
        }
    """

    fields = CONVERSATIONS_TABLE.fields
    return {
        "FROM": fields["SELLER_PHONE"].resolve(),
        "TO": fields["TEXTGRID_PHONE"].resolve(),
        "BODY": fields["MESSAGE"].resolve(),
        "STATUS": fields["STATUS"].resolve(),
        "DIRECTION": fields["DIRECTION"].resolve(),
        "TEXTGRID_ID": fields["TEXTGRID_ID"].resolve(),
        "RECEIVED_AT": fields["RECEIVED_AT"].resolve(),
        "INTENT": fields["INTENT"].resolve(),
        "PROCESSED_BY": fields["PROCESSED_BY"].resolve(),
        "SENT_AT": fields["SENT_AT"].resolve(),
        "STAGE": fields["STAGE"].resolve(),
        "AI_INTENT": fields["AI_INTENT"].resolve(),
    }


def conversations_field_candidates(keys: Iterable[str]) -> Dict[str, Tuple[str, ...]]:
    """
    Return candidate names for the requested logical keys. Useful when parsing
    legacy exports where a column might still be named "phone" instead of the
    canonical "Seller Phone Number".
    """

    results: Dict[str, Tuple[str, ...]] = {}
    for key in keys:
        definition = CONVERSATIONS_TABLE.fields[key]
        results[key] = definition.candidates()
    return results


# ---------------------------------------------------------------------------
# Leads table schema (Leads & Conversations base)
# ---------------------------------------------------------------------------


class LeadStatus(str, Enum):
    NEW = "New"
    CONTACTED = "Contacted"
    ACTIVE_COMMUNICATION = "Active Communication"
    LEAD_FOLLOW_UP = "Lead Follow Up"
    RUN_COMPS = "Run Comps"
    MAKE_OFFER = "Make Offer"
    OFFER_FOLLOW_UP = "Offer Follow Up"
    UNDER_CONTRACT = "Under Contract"
    DISPOSITION_STAGE = "Disposition Stage"
    IN_ESCROW = "In Escrow"
    CLOSING_SET = "Closing Set"
    DEAD = "Dead"


LEADS_TABLE = TableDefinition(
    default="Leads",
    env_vars=("LEADS_TABLE",),
    fields={
        "PRIMARY": FieldDefinition(
            default="Lead ID",
            env_vars=("LEAD_PRIMARY_FIELD",),
            fallbacks=("LeadID", "lead_id"),
        ),
        "NAME": FieldDefinition(
            default="Name",
            env_vars=("LEAD_NAME_FIELD",),
            fallbacks=("name",),
        ),
        "STATUS": FieldDefinition(
            default="Lead Status",
            env_vars=("LEAD_STATUS_FIELD",),
            options=tuple(status.value for status in LeadStatus),
            fallbacks=("status", "LeadStatus"),
        ),
        "LAST_DIRECTION": FieldDefinition(
            default="Last Direction",
            env_vars=("LEAD_LAST_DIRECTION_FIELD",),
            options=tuple(direction.value for direction in ConversationDirection),
            fallbacks=("last_direction", "Direction"),
        ),
        "LAST_DELIVERY_STATUS": FieldDefinition(
            default="Last Delivery Status",
            env_vars=("LEAD_LAST_DELIVERY_STATUS_FIELD",),
            options=tuple(status.value for status in ConversationDeliveryStatus),
            fallbacks=("last_delivery_status",),
        ),
        "SOURCE": FieldDefinition(
            default="Source",
            env_vars=("LEAD_SOURCE_FIELD",),
            fallbacks=("source", "Source Name"),
        ),
        "PHONE": FieldDefinition(
            default="phone",
            env_vars=("LEAD_PHONE_FIELD",),
            fallbacks=("Phone", "Seller Phone Number", "Seller Phone"),
        ),
        "SENT_COUNT": FieldDefinition(
            default="Sent Count",
            env_vars=("LEAD_SENT_COUNT_FIELD",),
            fallbacks=("sent_count",),
        ),
        "RESPONSE_MINUTES": FieldDefinition(
            default="Response Time (Minutes)",
            env_vars=("LEAD_RESPONSE_MIN_FIELD",),
            fallbacks=("response_time_minutes", "response_minutes"),
        ),
        "REPLY_COUNT": FieldDefinition(
            default="Reply Count",
            env_vars=("LEAD_REPLY_COUNT_FIELD",),
            fallbacks=("reply_count",),
        ),
        "FAILED_COUNT": FieldDefinition(
            default="Failed Count",
            env_vars=("LEAD_FAILED_COUNT_FIELD",),
            fallbacks=("failed_count",),
        ),
        "DELIVERED_COUNT": FieldDefinition(
            default="Delivered Count",
            env_vars=("LEAD_DELIVERED_COUNT_FIELD",),
            fallbacks=("delivered_count",),
        ),
        "LAST_MESSAGE": FieldDefinition(
            default="Last Message",
            env_vars=("LEAD_LAST_MESSAGE_FIELD",),
            fallbacks=("last_message",),
        ),
        "TEMPLATE_LINK": FieldDefinition(
            default="Template",
            env_vars=("LEAD_TEMPLATE_LINK_FIELD",),
            fallbacks=("Template",),
        ),
        "PROSPECT_LINK": FieldDefinition(
            default="Prospect",
            env_vars=("LEAD_PROSPECT_LINK_FIELD",),
            fallbacks=("Prospect",),
        ),
        "NOTIFICATIONS_LINK": FieldDefinition(
            default="Notifications",
            env_vars=("LEAD_NOTIFICATIONS_LINK_FIELD",),
            fallbacks=("Notifications",),
        ),
        "DEALS_LINK": FieldDefinition(
            default="Deals",
            env_vars=("LEAD_DEALS_LINK_FIELD",),
            fallbacks=("Deals",),
        ),
        "CONVERSATIONS_LINK": FieldDefinition(
            default="Conversations",
            env_vars=("LEAD_CONVERSATIONS_LINK_FIELD",),
            fallbacks=("Conversations",),
        ),
        "CAMPAIGNS_LINK": FieldDefinition(
            default="Campaigns",
            env_vars=("LEAD_CAMPAIGNS_LINK_FIELD",),
            fallbacks=("Campaigns",),
        ),
        "PROSPECT_RECORD_ID": FieldDefinition(
            default="Prospect Record ID",
            env_vars=("LEAD_PROSPECT_RECORD_ID_FIELD",),
            fallbacks=("prospect_record_id",),
        ),
        "RECORD_ID": FieldDefinition(
            default="Record ID",
            env_vars=("LEAD_RECORD_ID_FIELD",),
            fallbacks=("record_id",),
        ),
        "LAST_OUTBOUND": FieldDefinition(
            default="Last Outbound",
            env_vars=("LEAD_LAST_OUTBOUND_FIELD",),
            fallbacks=("last_outbound",),
        ),
        "LAST_INBOUND": FieldDefinition(
            default="Last Inbound",
            env_vars=("LEAD_LAST_INBOUND_FIELD",),
            fallbacks=("last_inbound",),
        ),
        "LAST_ACTIVITY": FieldDefinition(
            default="Last Activity",
            env_vars=("LEAD_LAST_ACTIVITY_FIELD",),
            fallbacks=("last_activity",),
        ),
        "PROPERTY_ID": FieldDefinition(
            default="Property ID",
            env_vars=("LEAD_PROPERTY_ID_FIELD",),
            fallbacks=("property_id", "Property ID (from Prospect)"),
        ),
    },
)


def leads_field_map() -> Dict[str, str]:
    fields = LEADS_TABLE.fields
    return {
        "PRIMARY": fields["PRIMARY"].resolve(),
        "NAME": fields["NAME"].resolve(),
        "STATUS": fields["STATUS"].resolve(),
        "LAST_DIRECTION": fields["LAST_DIRECTION"].resolve(),
        "LAST_DELIVERY_STATUS": fields["LAST_DELIVERY_STATUS"].resolve(),
        "SOURCE": fields["SOURCE"].resolve(),
        "PHONE": fields["PHONE"].resolve(),
        "SENT_COUNT": fields["SENT_COUNT"].resolve(),
        "RESPONSE_MINUTES": fields["RESPONSE_MINUTES"].resolve(),
        "REPLY_COUNT": fields["REPLY_COUNT"].resolve(),
        "FAILED_COUNT": fields["FAILED_COUNT"].resolve(),
        "DELIVERED_COUNT": fields["DELIVERED_COUNT"].resolve(),
        "LAST_MESSAGE": fields["LAST_MESSAGE"].resolve(),
        "TEMPLATE_LINK": fields["TEMPLATE_LINK"].resolve(),
        "PROSPECT_LINK": fields["PROSPECT_LINK"].resolve(),
        "NOTIFICATIONS_LINK": fields["NOTIFICATIONS_LINK"].resolve(),
        "DEALS_LINK": fields["DEALS_LINK"].resolve(),
        "CONVERSATIONS_LINK": fields["CONVERSATIONS_LINK"].resolve(),
        "CAMPAIGNS_LINK": fields["CAMPAIGNS_LINK"].resolve(),
        "PROSPECT_RECORD_ID": fields["PROSPECT_RECORD_ID"].resolve(),
        "RECORD_ID": fields["RECORD_ID"].resolve(),
        "LAST_OUTBOUND": fields["LAST_OUTBOUND"].resolve(),
        "LAST_INBOUND": fields["LAST_INBOUND"].resolve(),
        "LAST_ACTIVITY": fields["LAST_ACTIVITY"].resolve(),
        "PROPERTY_ID": fields["PROPERTY_ID"].resolve(),
    }


def leads_field_candidates(keys: Iterable[str]) -> Dict[str, Tuple[str, ...]]:
    results: Dict[str, Tuple[str, ...]] = {}
    for key in keys:
        definition = LEADS_TABLE.fields[key]
        results[key] = definition.candidates()
    return results


# ---------------------------------------------------------------------------
# Campaigns table schema (Leads & Conversations base)
# ---------------------------------------------------------------------------


class CampaignStatus(str, Enum):
    DRAFT = "Draft"
    SCHEDULED = "Scheduled"
    RUNNING = "Running"
    PAUSED = "Paused"
    COMPLETED = "Completed"


class CampaignMarket(str, Enum):
    LOS_ANGELES = "Los Angeles, CA"
    TAMPA = "Tampa, FL"
    CHARLOTTE = "Charlotte, NC"
    MIAMI = "Miami, FL"
    MINNEAPOLIS = "Minneapolis, MN"
    JACKSONVILLE = "Jacksonville, FL"
    HOUSTON = "Houston, TX"
    DALLAS = "Dallas, TX"
    PHOENIX = "Phoenix, AZ"
    ATLANTA = "Atlanta, GA"
    NASHVILLE = "Nashville, TN"


CAMPAIGNS_TABLE = TableDefinition(
    default="Campaigns",
    env_vars=("CAMPAIGNS_TABLE",),
    fields={
        "PRIMARY": FieldDefinition(
            default="Campaign ID",
            env_vars=("CAMPAIGN_PRIMARY_FIELD",),
            fallbacks=("CampaignID", "campaign_id"),
        ),
        "NAME": FieldDefinition(
            default="Name",
            env_vars=("CAMPAIGN_NAME_FIELD",),
            fallbacks=("name",),
        ),
        "PUBLIC_NAME": FieldDefinition(
            default="Campaign Name",
            env_vars=("CAMPAIGN_PUBLIC_NAME_FIELD",),
            fallbacks=("campaign_name", "Public Name"),
        ),
        "STATUS": FieldDefinition(
            default="Status",
            env_vars=("CAMPAIGN_STATUS_FIELD",),
            options=tuple(status.value for status in CampaignStatus),
            fallbacks=("status",),
        ),
        "MARKET": FieldDefinition(
            default="Market",
            env_vars=("CAMPAIGN_MARKET_FIELD",),
            options=tuple(market.value for market in CampaignMarket),
            fallbacks=("market",),
        ),
        "VIEW_SEGMENT": FieldDefinition(
            default="View/Segment",
            env_vars=("CAMPAIGN_VIEW_FIELD",),
            fallbacks=("View", "Segment", "view_segment"),
        ),
        "TOTAL_SENT": FieldDefinition(
            default="Total Sent",
            env_vars=("CAMPAIGN_TOTAL_SENT_FIELD",),
            fallbacks=("total_sent",),
        ),
        "TOTAL_REPLIES": FieldDefinition(
            default="Total Replies",
            env_vars=("CAMPAIGN_TOTAL_REPLIES_FIELD",),
            fallbacks=("total_replies",),
        ),
        "TOTAL_OPT_OUTS": FieldDefinition(
            default="Total Opt Outs",
            env_vars=("CAMPAIGN_TOTAL_OPTOUTS_FIELD",),
            fallbacks=("total_opt_outs", "Total Opt-outs"),
        ),
        "TOTAL_OFFERS": FieldDefinition(
            default="Total Offers",
            env_vars=("CAMPAIGN_TOTAL_OFFERS_FIELD",),
            fallbacks=("total_offers",),
        ),
        "TOTAL_LEADS": FieldDefinition(
            default="Total Leads",
            env_vars=("CAMPAIGN_TOTAL_LEADS_FIELD",),
            fallbacks=("total_leads",),
        ),
        "TOTAL_FAILED": FieldDefinition(
            default="Total Failed",
            env_vars=("CAMPAIGN_TOTAL_FAILED_FIELD",),
            fallbacks=("total_failed",),
        ),
        "TOTAL_DEALS": FieldDefinition(
            default="Total Deals",
            env_vars=("CAMPAIGN_TOTAL_DEALS_FIELD",),
            fallbacks=("total_deals",),
        ),
        "TOTAL_CONTACTS": FieldDefinition(
            default="Total Contacts",
            env_vars=("CAMPAIGN_TOTAL_CONTACTS_FIELD",),
            fallbacks=("total_contacts",),
        ),
        "LAST_RUN_RESULT": FieldDefinition(
            default="Last Run Result",
            env_vars=("CAMPAIGN_LAST_RUN_RESULT_FIELD",),
            fallbacks=("last_run_result", "Last Result"),
        ),
        "TEMPLATES_LINK": FieldDefinition(
            default="Templates",
            env_vars=("CAMPAIGN_TEMPLATES_LINK_FIELD",),
            fallbacks=("Templates",),
        ),
        "PROSPECTS_LINK": FieldDefinition(
            default="Prospects",
            env_vars=("CAMPAIGN_PROSPECTS_LINK_FIELD",),
            fallbacks=("Prospects",),
        ),
        "NOTIFICATIONS_LINK": FieldDefinition(
            default="Notifications",
            env_vars=("CAMPAIGN_NOTIFICATIONS_LINK_FIELD",),
            fallbacks=("Notifications",),
        ),
        "DRIP_QUEUE_LINK": FieldDefinition(
            default="Drip Queue",
            env_vars=("CAMPAIGN_DRIP_QUEUE_LINK_FIELD",),
            fallbacks=("Drip Queue",),
        ),
        "DEALS_LINK": FieldDefinition(
            default="Deals",
            env_vars=("CAMPAIGN_DEALS_LINK_FIELD",),
            fallbacks=("Deals",),
        ),
        "CONVERSATIONS_LINK": FieldDefinition(
            default="Conversations",
            env_vars=("CAMPAIGN_CONVERSATIONS_LINK_FIELD",),
            fallbacks=("Conversations",),
        ),
        "ASSOCIATED_LEADS_LINK": FieldDefinition(
            default="Associated Leads",
            env_vars=("CAMPAIGN_ASSOCIATED_LEADS_FIELD",),
            fallbacks=("Associated Leads", "Leads"),
        ),
        "START_TIME": FieldDefinition(
            default="Start Time",
            env_vars=("CAMPAIGN_START_TIME_FIELD",),
            fallbacks=("Start", "Start At", "start_time", "Start Date", "Schedule Start"),
        ),
        "END_TIME": FieldDefinition(
            default="End Time",
            env_vars=("CAMPAIGN_END_TIME_FIELD",),
            fallbacks=("End", "End At", "end_time", "End Date", "Schedule End"),
        ),
        "LAST_RUN_AT": FieldDefinition(
            default="Last Run At",
            env_vars=("CAMPAIGN_LAST_RUN_AT_FIELD",),
            fallbacks=("last_run_at", "Last Run"),
        ),
    },
)


def campaign_field_map() -> Dict[str, str]:
    fields = CAMPAIGNS_TABLE.fields
    return {key: field.resolve() for key, field in fields.items()}


def campaign_field_candidates(keys: Iterable[str]) -> Dict[str, Tuple[str, ...]]:
    results: Dict[str, Tuple[str, ...]] = {}
    for key in keys:
        definition = CAMPAIGNS_TABLE.fields[key]
        results[key] = definition.candidates()
    return results


# ---------------------------------------------------------------------------
# Drip Queue table schema
# ---------------------------------------------------------------------------


class DripStatus(str, Enum):
    FAILED = "FAILED"
    QUEUED = "QUEUED"
    READY = "READY"
    SENDING = "SENDING"
    SENT = "SENT"
    DELIVERED = "DELIVERED"
    RETRY = "RETRY"
    THROTTLED = "THROTTLED"
    DNC = "DNC"


class DripProcessor(str, Enum):
    AUTORESPONDER = "Autoresponder"
    AI_CLOSER = "AI Closer"
    MANUAL = "Manual"
    FOLLOW_UP_ENGINE = "Follow-Up Engine"
    SCHEDULER = "Scheduler"
    RE_ENGAGEMENT_BOT = "Re-Engagement Bot"
    CAMPAIGN_ENGINE = "Campaign Engine"


class DripStage(str, Enum):
    DAY_30 = "30"
    DAY_60 = "60"
    DAY_90 = "90"
    COMPLETE = "COMPLETE"


DRIP_QUEUE_TABLE = TableDefinition(
    default="Drip Queue",
    env_vars=("DRIP_QUEUE_TABLE",),
    fields={
        "PRIMARY": FieldDefinition(
            default="Property ID",
            env_vars=("DRIP_PRIMARY_FIELD",),
            fallbacks=("property_id",),
        ),
        "NAME": FieldDefinition(
            default="Name",
            env_vars=("DRIP_NAME_FIELD",),
            fallbacks=("name",),
        ),
        "STATUS": FieldDefinition(
            default="Status",
            env_vars=("DRIP_STATUS_FIELD",),
            options=tuple(status.value for status in DripStatus),
            fallbacks=("status",),
        ),
        "PROCESSOR": FieldDefinition(
            default="Processor",
            env_vars=("DRIP_PROCESSOR_FIELD",),
            options=tuple(proc.value for proc in DripProcessor),
            fallbacks=("processor",),
        ),
        "MARKET": FieldDefinition(
            default="Market",
            env_vars=("DRIP_MARKET_FIELD",),
            options=tuple(market.value for market in CampaignMarket),
            fallbacks=("market",),
        ),
        "DRIP_STAGE": FieldDefinition(
            default="Drip Stage",
            env_vars=("DRIP_STAGE_FIELD",),
            options=tuple(stage.value for stage in DripStage),
            fallbacks=("drip_stage",),
        ),
        "UI": FieldDefinition(
            default="UI",
            env_vars=("DRIP_UI_FIELD",),
            fallbacks=("ui",),
        ),
        "TEXTGRID_PHONE": FieldDefinition(
            default="TextGrid Phone Number",
            env_vars=("DRIP_TEXTGRID_PHONE_FIELD",),
            fallbacks=("textgrid_phone_number", "TextGrid Number"),
        ),
        "SELLER_PHONE": FieldDefinition(
            default="Seller Phone Number",
            env_vars=("DRIP_SELLER_PHONE_FIELD",),
            fallbacks=("phone", "Phone"),
        ),
        "FROM_NUMBER": FieldDefinition(
            default="from_number",
            env_vars=("DRIP_FROM_NUMBER_FIELD",),
            fallbacks=("From Number",),
        ),
        "PROPERTY_ID": FieldDefinition(
            default="Property ID",
            env_vars=("DRIP_PROPERTY_ID_FIELD",),
            fallbacks=("property_id",),
        ),
        "REPLY_COUNT": FieldDefinition(
            default="Reply Count",
            env_vars=("DRIP_REPLY_COUNT_FIELD",),
            fallbacks=("reply_count",),
        ),
        "MESSAGE_PREVIEW": FieldDefinition(
            default="Message Preview",
            env_vars=("DRIP_MESSAGE_PREVIEW_FIELD",),
            fallbacks=("message_preview", "Message"),
        ),
        "LAST_ERROR": FieldDefinition(
            default="Last Error",
            env_vars=("DRIP_LAST_ERROR_FIELD",),
            fallbacks=("last_error",),
        ),
        "TEMPLATE_LINK": FieldDefinition(
            default="Template",
            env_vars=("DRIP_TEMPLATE_LINK_FIELD",),
            fallbacks=("Template",),
        ),
        "PROSPECT_LINK": FieldDefinition(
            default="Prospect",
            env_vars=("DRIP_PROSPECT_LINK_FIELD",),
            fallbacks=("Prospect",),
        ),
        "CAMPAIGN_LINK": FieldDefinition(
            default="Campaign",
            env_vars=("DRIP_CAMPAIGN_LINK_FIELD",),
            fallbacks=("Campaign",),
        ),
        "NEXT_SEND_DATE": FieldDefinition(
            default="Next Send Date",
            env_vars=("DRIP_NEXT_SEND_DATE_FIELD",),
            fallbacks=("next_send_date",),
        ),
        "NEXT_SEND_AT": FieldDefinition(
            default="Next Send At",
            env_vars=("DRIP_NEXT_SEND_AT_FIELD",),
            fallbacks=("next_send_at",),
        ),
        "NEXT_SEND_AT_UTC": FieldDefinition(
            default="next_send_at_utc",
            env_vars=("DRIP_NEXT_SEND_AT_UTC_FIELD",),
            fallbacks=("Next Send At UTC", "Send At UTC", "send_at_utc"),
        ),
        "LAST_SENT": FieldDefinition(
            default="Last Sent",
            env_vars=("DRIP_LAST_SENT_FIELD",),
            fallbacks=("last_sent",),
        ),
        "SENT_AT": FieldDefinition(
            default="sent_at",
            env_vars=("DRIP_SENT_AT_FIELD",),
            fallbacks=("Sent At",),
        ),
        "NUMBER_RECORD_ID": FieldDefinition(
            default="Number Record Id",
            env_vars=("DRIP_NUMBER_RECORD_ID_FIELD",),
            fallbacks=("Number Record ID", "number_record_id"),
        ),
        "SENT_FLAG": FieldDefinition(
            default="SentFlag",
            env_vars=("DRIP_SENT_FLAG_FIELD",),
            fallbacks=("sent_flag",),
        ),
        "FAILED_FLAG": FieldDefinition(
            default="FailedFlag",
            env_vars=("DRIP_FAILED_FLAG_FIELD",),
            fallbacks=("failed_flag",),
        ),
        "DECLINED_FLAG": FieldDefinition(
            default="DeclinedFlag",
            env_vars=("DRIP_DECLINED_FLAG_FIELD",),
            fallbacks=("declined_flag",),
        ),
        "RECORD_ID": FieldDefinition(
            default="Record ID",
            env_vars=("DRIP_RECORD_ID_FIELD",),
            fallbacks=("record_id",),
        ),
    },
)


def drip_field_map() -> Dict[str, str]:
    fields = DRIP_QUEUE_TABLE.fields
    return {key: field.resolve() for key, field in fields.items()}


def drip_field_candidates(keys: Iterable[str]) -> Dict[str, Tuple[str, ...]]:
    results: Dict[str, Tuple[str, ...]] = {}
    for key in keys:
        definition = DRIP_QUEUE_TABLE.fields[key]
        results[key] = definition.candidates()
    return results


# ---------------------------------------------------------------------------
# Templates table schema (Leads & Conversations base)
# ---------------------------------------------------------------------------


class TemplateStage(str, Enum):
    STAGE_1_OWNERSHIP_CONFIRMATION = ConversationStage.STAGE_1_OWNERSHIP_CONFIRMATION.value
    STAGE_2_INTEREST_FEELER = ConversationStage.STAGE_2_INTEREST_FEELER.value
    STAGE_3_PRICE_QUALIFICATION = ConversationStage.STAGE_3_PRICE_QUALIFICATION.value
    STAGE_4_PROPERTY_CONDITION = ConversationStage.STAGE_4_PROPERTY_CONDITION.value
    STAGE_5_MOTIVATION_TIMELINE = ConversationStage.STAGE_5_MOTIVATION_TIMELINE.value
    STAGE_6_OFFER_FOLLOW_UP = ConversationStage.STAGE_6_OFFER_FOLLOW_UP.value
    STAGE_7_CONTRACT_READY = ConversationStage.STAGE_7_CONTRACT_READY.value
    STAGE_8_CONTRACT_SENT = ConversationStage.STAGE_8_CONTRACT_SENT.value
    STAGE_9_CONTRACT_FOLLOW_UP = ConversationStage.STAGE_9_CONTRACT_FOLLOW_UP.value
    OTHER = "Other"


class TemplateCategory(str, Enum):
    INTRO = "Intro"
    FOLLOW_UP = "Follow up"
    POSITIVE = "Positive"
    NEGATIVE = "Negative"
    OPT_OUT = "Opt-Out"
    CLOSING = "Closing"
    OTHER = "Other"
    WRONG_NUMBER = "Wrong Number"
    NEUTRAL_RESPONSE = "Neutral Response"
    INTEREST_DETECTED = "Interest Detected"
    DELAY_BUSY = "Delay / Busy"


TEMPLATES_TABLE = TableDefinition(
    default="Templates",
    env_vars=("TEMPLATES_TABLE",),
    fields={
        "PRIMARY": FieldDefinition(
            default="Template ID",
            env_vars=("TEMPLATE_PRIMARY_FIELD",),
            fallbacks=("template_id",),
        ),
        "NAME": FieldDefinition(
            default="Name",
            env_vars=("TEMPLATE_NAME_FIELD",),
            fallbacks=("name",),
        ),
        "NAME_KEY": FieldDefinition(
            default="Name (Key)",
            env_vars=("TEMPLATE_NAME_KEY_FIELD",),
            fallbacks=("name_key", "Template Name"),
        ),
        "INTERNAL_ID": FieldDefinition(
            default="Internal ID",
            env_vars=("TEMPLATE_INTERNAL_ID_FIELD",),
            fallbacks=("internal_id", "intent"),
        ),
        "STAGE": FieldDefinition(
            default="Stage",
            env_vars=("TEMPLATE_STAGE_FIELD",),
            options=tuple(stage.value if isinstance(stage, Enum) else stage for stage in TemplateStage),
            fallbacks=("stage",),
        ),
        "CATEGORY": FieldDefinition(
            default="Category",
            env_vars=("TEMPLATE_CATEGORY_FIELD",),
            options=tuple(category.value for category in TemplateCategory),
            fallbacks=("category",),
        ),
        "TOTAL_SENDS": FieldDefinition(
            default="Total Sends",
            env_vars=("TEMPLATE_TOTAL_SENDS_FIELD",),
            fallbacks=("total_sends",),
        ),
        "TOTAL_REPLIES": FieldDefinition(
            default="Total Replies",
            env_vars=("TEMPLATE_TOTAL_REPLIES_FIELD",),
            fallbacks=("total_replies",),
        ),
        "TOTAL_OPT_OUTS": FieldDefinition(
            default="Total Opt Outs",
            env_vars=("TEMPLATE_TOTAL_OPTOUTS_FIELD",),
            fallbacks=("total_opt_outs", "Total Opt-outs"),
        ),
        "TOTAL_DELIVERIES": FieldDefinition(
            default="Total Deliveries",
            env_vars=("TEMPLATE_TOTAL_DELIVERIES_FIELD",),
            fallbacks=("total_deliveries",),
        ),
        "TIME_TO_FIRST_REPLY": FieldDefinition(
            default="Time-to-First-Reply (mins)",
            env_vars=("TEMPLATE_TIME_TO_FIRST_REPLY_FIELD",),
            fallbacks=("time_to_first_reply",),
        ),
        "REPLY_DEPTH": FieldDefinition(
            default="Reply Depth",
            env_vars=("TEMPLATE_REPLY_DEPTH_FIELD",),
            fallbacks=("reply_depth",),
        ),
        "POSITIVE_REPLIES": FieldDefinition(
            default="Positive Replies",
            env_vars=("TEMPLATE_POSITIVE_REPLIES_FIELD",),
            fallbacks=("positive_replies",),
        ),
        "OFFERS_SENT": FieldDefinition(
            default="Offers Sent",
            env_vars=("TEMPLATE_OFFERS_SENT_FIELD",),
            fallbacks=("offers_sent",),
        ),
        "NEGATIVE_REPLIES": FieldDefinition(
            default="Negative Replies",
            env_vars=("TEMPLATE_NEGATIVE_REPLIES_FIELD",),
            fallbacks=("negative_replies",),
        ),
        "FAILED_DELIVERIES": FieldDefinition(
            default="Failed Deliveries",
            env_vars=("TEMPLATE_FAILED_DELIVERIES_FIELD",),
            fallbacks=("failed_deliveries",),
        ),
        "DEALS_CLOSED": FieldDefinition(
            default="Deals Closed",
            env_vars=("TEMPLATE_DEALS_CLOSED_FIELD",),
            fallbacks=("deals_closed",),
        ),
        "SEND_TIME_PERFORMANCE": FieldDefinition(
            default="Send Time Performance",
            env_vars=("TEMPLATE_SEND_TIME_PERFORMANCE_FIELD",),
            fallbacks=("send_time_performance",),
        ),
        "PACE_POST_PERFORMANCE": FieldDefinition(
            default="Pace Post Performance",
            env_vars=("TEMPLATE_PACE_POST_PERFORMANCE_FIELD",),
            fallbacks=("pace_post_performance",),
        ),
        "MESSAGE": FieldDefinition(
            default="Message",
            env_vars=("TEMPLATE_MESSAGE_FIELD",),
            fallbacks=("message", "Body"),
        ),
        "MESSAGE_NOTES": FieldDefinition(
            default="Message Notes",
            env_vars=("TEMPLATE_MESSAGE_NOTES_FIELD",),
            fallbacks=("message_notes", "Notes"),
        ),
        "RECORD_ID": FieldDefinition(
            default="Record ID",
            env_vars=("TEMPLATE_RECORD_ID_FIELD",),
            fallbacks=("record_id",),
        ),
        "DRIP_QUEUE_LINK": FieldDefinition(
            default="Drip Queue",
            env_vars=("TEMPLATE_DRIP_QUEUE_LINK_FIELD",),
            fallbacks=("Drip Queue",),
        ),
        "CONVERSATIONS_LINK": FieldDefinition(
            default="Conversations",
            env_vars=("TEMPLATE_CONVERSATIONS_LINK_FIELD",),
            fallbacks=("Conversations",),
        ),
        "CAMPAIGNS_LINK": FieldDefinition(
            default="Campaigns",
            env_vars=("TEMPLATE_CAMPAIGNS_LINK_FIELD",),
            fallbacks=("Campaigns",),
        ),
        "LEADS_LINK": FieldDefinition(
            default="Leads",
            env_vars=("TEMPLATE_LEADS_LINK_FIELD",),
            fallbacks=("Leads",),
        ),
        "LAST_USED_DATE": FieldDefinition(
            default="Last Used Date",
            env_vars=("TEMPLATE_LAST_USED_DATE_FIELD",),
            fallbacks=("last_used_date",),
        ),
    },
)


def template_field_map() -> Dict[str, str]:
    fields = TEMPLATES_TABLE.fields
    return {key: field.resolve() for key, field in fields.items()}


def template_field_candidates(keys: Iterable[str]) -> Dict[str, Tuple[str, ...]]:
    results: Dict[str, Tuple[str, ...]] = {}
    for key in keys:
        definition = TEMPLATES_TABLE.fields[key]
        results[key] = definition.candidates()
    return results


# ---------------------------------------------------------------------------
# Prospects table schema (Leads & Conversations base)
# ---------------------------------------------------------------------------


class ProspectStatus(str, Enum):
    UNMESSAGED = "Unmessaged"
    QUEUED = "Queued"
    MESSAGED = "Messaged"
    REPLIED = "Replied"
    OWNER_VERIFIED = "Owner Verified"
    NOT_INTERESTED = "Not Interested"
    LEAD_CREATED = "Lead Created"
    RUN_COMPS = "Run Comps"
    FOLLOW_UP = "Follow Up"
    OFFER_SENT = "Offer Sent"
    UNDER_CONTRACT = "Under Contract"
    OPT_OUT = "Opt-Out"


class ProspectStage(str, Enum):
    STAGE_1_OWNERSHIP_CHECK = "Stage #1 – Ownership Check"
    STAGE_2_OFFER_INTEREST = "Stage #2 – Offer Interest"
    STAGE_3_PRICE_CONDITION = "Stage #3 – Price/Condition"
    STAGE_4_RUN_COMPS_NUMBERS = "Stage #4 – Run Comps/Numbers"
    STAGE_5_MAKE_OFFER = "Stage #5 – Make Offer"
    STAGE_6_OFFER_FOLLOW_UP = "Stage #6 – Offer Follow Up"
    STAGE_7_SEND_CONTRACT = "Stage #7 – Send Contract"
    STAGE_8_DISPOSITIONS = "Stage #8 – Dispositions"
    STAGE_9_CLOSE_ESCROW = "Stage #9 – Close Escrow"


class ProspectTemperature(str, Enum):
    COLD = "Cold"
    WARM = "Warm"
    HOT = "Hot"
    NOT_SET = "Not Set"


class ProspectDistress(str, Enum):
    HOT = "HOT"
    WARM = "WARM"
    COLD = "COLD"
    DEAD = "DEAD"


class ProspectDirection(str, Enum):
    INBOUND = "Inbound"
    OUTBOUND = "Outbound"


class ProspectIntent(str, Enum):
    NEUTRAL = "neutral"
    WHO_IS_THIS = "who_is_this"
    NOT_OWNER = "not_owner"
    WRONG_NUMBER = "wrong_number"
    DELAY = "delay"
    INTEREST = "interest"
    OFFER_REQUEST = "offer_request"
    PRICE_RESPONSE = "price_response"
    CONDITION_RESPONSE = "condition_response"
    OPTOUT = "optout"


PROSPECTS_TABLE = TableDefinition(
    default="Prospects",
    env_vars=("PROSPECTS_TABLE",),
    fields={
        "PRIMARY": FieldDefinition(
            default="Prospect ID",
            env_vars=("PROSPECT_PRIMARY_FIELD",),
            fallbacks=("prospect_id", "Name"),
        ),
        "NAME": FieldDefinition(
            default="Name",
            env_vars=("PROSPECT_NAME_FIELD",),
            fallbacks=("Prospect Name", "Full Name"),
        ),
        "STATUS": FieldDefinition(
            default="Status",
            env_vars=("PROSPECT_STATUS_FIELD",),
            options=tuple(status.value for status in ProspectStatus),
            fallbacks=("prospect_status",),
        ),
        "STAGE": FieldDefinition(
            default="Stage",
            env_vars=("PROSPECT_STAGE_FIELD",),
            options=tuple(stage.value for stage in ProspectStage),
            fallbacks=("prospect_stage",),
        ),
        "TEMPERATURE": FieldDefinition(
            default="Temperature",
            env_vars=("PROSPECT_TEMPERATURE_FIELD",),
            options=tuple(temp.value for temp in ProspectTemperature),
            fallbacks=("temperature",),
        ),
        "DISTRESS_TIER": FieldDefinition(
            default="Distress Tier",
            env_vars=("PROSPECT_DISTRESS_FIELD",),
            options=tuple(distress.value for distress in ProspectDistress),
            fallbacks=("distress_tier",),
        ),
        "LAST_DIRECTION": FieldDefinition(
            default="Last Direction",
            env_vars=("PROSPECT_LAST_DIRECTION_FIELD",),
            options=tuple(direction.value for direction in ProspectDirection),
            fallbacks=("last_direction",),
        ),
        "INTENT_LAST_DETECTED": FieldDefinition(
            default="Intent Last Detected",
            env_vars=("PROSPECT_INTENT_FIELD",),
            options=tuple(intent.value for intent in ProspectIntent),
            fallbacks=("intent_last_detected",),
        ),
        "MARKET": FieldDefinition(
            default="Market",
            env_vars=("PROSPECT_MARKET_FIELD",),
            fallbacks=("market",),
        ),
        "SYNC_SOURCE": FieldDefinition(
            default="Synced From",
            env_vars=("PROSPECT_SYNC_SOURCE_FIELD",),
            fallbacks=("Sync Source",),
        ),
        "SOURCE_LIST": FieldDefinition(
            default="Source List",
            env_vars=("PROSPECT_SOURCE_LIST_FIELD",),
            fallbacks=("List",),
        ),
        "PROPERTY_TYPE": FieldDefinition(
            default="Property Type",
            env_vars=("PROSPECT_PROPERTY_TYPE_FIELD",),
            fallbacks=("property_type",),
        ),
        "PROPERTY_ADDRESS": FieldDefinition(
            default="Property Address",
            env_vars=("PROSPECT_ADDRESS_FIELD",),
            fallbacks=("Address",),
        ),
        "PROPERTY_CITY": FieldDefinition(
            default="Property City",
            env_vars=("PROSPECT_CITY_FIELD",),
            fallbacks=("City",),
        ),
        "PROPERTY_STATE": FieldDefinition(
            default="Property State",
            env_vars=("PROSPECT_STATE_FIELD",),
            fallbacks=("State",),
        ),
        "PROPERTY_ZIP": FieldDefinition(
            default="Property Zip",
            env_vars=("PROSPECT_ZIP_FIELD",),
            fallbacks=("Zip", "Zip Code"),
        ),
        "PROPERTY_COUNTY": FieldDefinition(
            default="Property County Name",
            env_vars=("PROSPECT_COUNTY_FIELD",),
            fallbacks=("County",),
        ),
        "PROPERTY_ID": FieldDefinition(
            default="Property ID",
            env_vars=("PROSPECT_PROPERTY_ID_FIELD",),
            fallbacks=("property_id", "Property ID (from Linked Owner)"),
        ),
        "OWNER_NAME": FieldDefinition(
            default="Owner Name",
            env_vars=("PROSPECT_OWNER_NAME_FIELD",),
            fallbacks=("Owner", "Owner Full Name"),
        ),
        "OWNER_FIRST_NAME": FieldDefinition(
            default="Owner First Name",
            env_vars=("PROSPECT_OWNER_FIRST_NAME_FIELD",),
            fallbacks=("First Name", "Owner First"),
        ),
        "OWNER_LAST_NAME": FieldDefinition(
            default="Owner Last Name",
            env_vars=("PROSPECT_OWNER_LAST_NAME_FIELD",),
            fallbacks=("Last Name", "Owner Last"),
        ),
        "PHONE_PRIMARY": FieldDefinition(
            default="Phone 1",
            env_vars=("PROSPECT_PHONE1_FIELD",),
            fallbacks=("phone", "Primary Phone"),
        ),
        "PHONE_PRIMARY_LINKED": FieldDefinition(
            default="Phone 1 (from Linked Owner)",
            env_vars=("PROSPECT_PHONE1_LINKED_FIELD",),
            fallbacks=("Phone 1 Linked",),
        ),
        "PHONE_PRIMARY_VERIFIED": FieldDefinition(
            default="Phone 1 Verified",
            env_vars=("PROSPECT_PHONE1_VERIFIED_FIELD",),
            fallbacks=("Phone 1 Ownership Verified",),
        ),
        "PHONE_SECONDARY": FieldDefinition(
            default="Phone 2",
            env_vars=("PROSPECT_PHONE2_FIELD",),
            fallbacks=("Secondary Phone",),
        ),
        "PHONE_SECONDARY_LINKED": FieldDefinition(
            default="Phone 2 (from Linked Owner)",
            env_vars=("PROSPECT_PHONE2_LINKED_FIELD",),
            fallbacks=("Phone 2 Linked",),
        ),
        "PHONE_SECONDARY_VERIFIED": FieldDefinition(
            default="Phone 2 Verified",
            env_vars=("PROSPECT_PHONE2_VERIFIED_FIELD",),
            fallbacks=("Phone 2 Ownership Verified",),
        ),
        "EMAIL": FieldDefinition(
            default="Email",
            env_vars=("PROSPECT_EMAIL_FIELD",),
            fallbacks=("email",),
        ),
        "LAST_MESSAGE": FieldDefinition(
            default="Last Message",
            env_vars=("PROSPECT_LAST_MESSAGE_FIELD",),
            fallbacks=("last_message",),
        ),
        "LAST_ACTIVITY": FieldDefinition(
            default="Last Activity",
            env_vars=("PROSPECT_LAST_ACTIVITY_FIELD",),
            fallbacks=("last_activity",),
        ),
        "LAST_INBOUND": FieldDefinition(
            default="Last Inbound",
            env_vars=("PROSPECT_LAST_INBOUND_FIELD",),
            fallbacks=("last_inbound",),
        ),
        "LAST_OUTBOUND": FieldDefinition(
            default="Last Outbound",
            env_vars=("PROSPECT_LAST_OUTBOUND_FIELD",),
            fallbacks=("last_outbound",),
        ),
        "LEAD_LINK": FieldDefinition(
            default="Leads",
            env_vars=("PROSPECT_LEADS_LINK_FIELD",),
            fallbacks=("Lead", "Lead Link"),
        ),
        "DRIP_QUEUE_LINK": FieldDefinition(
            default="Drip Queue",
            env_vars=("PROSPECT_DRIP_LINK_FIELD",),
            fallbacks=("Drip Queue",),
        ),
        "CONVERSATIONS_LINK": FieldDefinition(
            default="Conversations",
            env_vars=("PROSPECT_CONVERSATIONS_LINK_FIELD",),
            fallbacks=("Conversations",),
        ),
        "CAMPAIGNS_LINK": FieldDefinition(
            default="Campaigns",
            env_vars=("PROSPECT_CAMPAIGNS_LINK_FIELD",),
            fallbacks=("Campaigns",),
        ),
        "RECORD_ID": FieldDefinition(
            default="Record ID",
            env_vars=("PROSPECT_RECORD_ID_FIELD",),
            fallbacks=("record_id",),
        ),
    },
)


def prospects_field_map() -> Dict[str, str]:
    fields = PROSPECTS_TABLE.fields
    return {key: field.resolve() for key, field in fields.items()}


def prospects_field_candidates(keys: Iterable[str]) -> Dict[str, Tuple[str, ...]]:
    results: Dict[str, Tuple[str, ...]] = {}
    for key in keys:
        definition = PROSPECTS_TABLE.fields[key]
        results[key] = definition.candidates()
    return results


# ---------------------------------------------------------------------------
# Deals table schema (Leads & Conversations base)
# ---------------------------------------------------------------------------


class DealStage(str, Enum):
    IN_NEGOTIATION = "In Negotiation"
    UNDER_CONTRACT = "Under Contract"
    IN_ESCROW = "In Escrow"
    CLOSING_SCHEDULED = "Closing Scheduled"
    CLOSED = "Closed"
    DEAD = "Dead"


class DealAcquisitionMethod(str, Enum):
    WHOLESALE = "Wholesale"
    NOVATION = "Novation"
    CREATIVE_FINANCE = "Creative Finance"
    SELLER_FINANCE = "Seller Finance"
    FLIP = "Flip"
    BUY_AND_HOLD = "Buy & Hold"


class DealMarketMomentum(str, Enum):
    HOT = "Hot"
    WARM = "Warm"
    COLD = "Cold"


DEALS_TABLE = TableDefinition(
    default="Deals",
    env_vars=("DEALS_TABLE",),
    fields={
        "PRIMARY": FieldDefinition(
            default="Deal ID",
            env_vars=("DEAL_PRIMARY_FIELD",),
            fallbacks=("deal_id", "Name"),
        ),
        "NAME": FieldDefinition(
            default="Name",
            env_vars=("DEAL_NAME_FIELD",),
            fallbacks=("Deal Name", "name"),
        ),
        "ZILLOW_LINK": FieldDefinition(
            default="Zillow Link",
            env_vars=("DEAL_ZILLOW_LINK_FIELD",),
            fallbacks=("zillow_link",),
        ),
        "MARKET_MOMENTUM": FieldDefinition(
            default="Market Momentum",
            env_vars=("DEAL_MARKET_MOMENTUM_FIELD",),
            options=tuple(momentum.value for momentum in DealMarketMomentum),
            fallbacks=("market_momentum",),
        ),
        "DEAL_STAGE": FieldDefinition(
            default="Deal Stage",
            env_vars=("DEAL_STAGE_FIELD",),
            options=tuple(stage.value for stage in DealStage),
            fallbacks=("deal_stage",),
        ),
        "ACQUISITION_METHOD": FieldDefinition(
            default="Acquisition Method",
            env_vars=("DEAL_ACQUISITION_METHOD_FIELD",),
            options=tuple(method.value for method in DealAcquisitionMethod),
            fallbacks=("acquisition_method",),
        ),
        "PROSPECT": FieldDefinition(
            default="Prospect",
            env_vars=("DEAL_PROSPECT_FIELD",),
            fallbacks=("prospect",),
        ),
        "BUYER": FieldDefinition(
            default="Buyer",
            env_vars=("DEAL_BUYER_FIELD",),
            fallbacks=("buyer",),
        ),
        "AI_CONFIDENCE": FieldDefinition(
            default="AI Confidence (%)",
            env_vars=("DEAL_AI_CONFIDENCE_FIELD",),
            fallbacks=("ai_confidence",),
        ),
        "NOTES": FieldDefinition(
            default="Notes",
            env_vars=("DEAL_NOTES_FIELD",),
            fallbacks=("notes",),
        ),
        "AI_NOTES": FieldDefinition(
            default="Deal Intelligence Notes (AI)",
            env_vars=("DEAL_AI_NOTES_FIELD",),
            fallbacks=("deal_intelligence_notes",),
        ),
        "PROSPECTS_LINK": FieldDefinition(
            default="Prospects",
            env_vars=("DEAL_PROSPECTS_LINK_FIELD",),
            fallbacks=("Prospects",),
        ),
        "LEAD_LINK": FieldDefinition(
            default="Lead",
            env_vars=("DEAL_LEAD_LINK_FIELD",),
            fallbacks=("Lead",),
        ),
        "CAMPAIGN_LINK": FieldDefinition(
            default="Campaign",
            env_vars=("DEAL_CAMPAIGN_LINK_FIELD",),
            fallbacks=("Campaign",),
        ),
        "RECORD_ID": FieldDefinition(
            default="Record ID",
            env_vars=("DEAL_RECORD_ID_FIELD",),
            fallbacks=("record_id",),
        ),
        "PROFIT_MARGIN": FieldDefinition(
            default="Profit Margin",
            env_vars=("DEAL_PROFIT_MARGIN_FIELD",),
            fallbacks=("profit_margin",),
        ),
        "DAYS_TO_CLOSE": FieldDefinition(
            default="Days to Close",
            env_vars=("DEAL_DAYS_TO_CLOSE_FIELD",),
            fallbacks=("days_to_close",),
        ),
        "CONTRACT_DATE": FieldDefinition(
            default="Contract Date",
            env_vars=("DEAL_CONTRACT_DATE_FIELD",),
            fallbacks=("contract_date",),
        ),
        "CLOSE_DATE": FieldDefinition(
            default="Close Date",
            env_vars=("DEAL_CLOSE_DATE_FIELD",),
            fallbacks=("close_date",),
        ),
        "ASSIGNMENT_FEE": FieldDefinition(
            default="Assignment Fee",
            env_vars=("DEAL_ASSIGNMENT_FEE_FIELD",),
            fallbacks=("assignment_fee",),
        ),
        "AI_PREDICTED_PROFIT": FieldDefinition(
            default="AI Predicted Profit ($)",
            env_vars=("DEAL_AI_PREDICTED_PROFIT_FIELD",),
            fallbacks=("ai_predicted_profit",),
        ),
        "ACQUISITION_PRICE": FieldDefinition(
            default="Acquisition Price",
            env_vars=("DEAL_ACQUISITION_PRICE_FIELD",),
            fallbacks=("acquisition_price",),
        ),
        "PURCHASE_AGREEMENT": FieldDefinition(
            default="Purchase Agreement / Assignment Agreement",
            env_vars=("DEAL_PURCHASE_AGREEMENT_FIELD",),
            fallbacks=("purchase_agreement",),
        ),
    },
)


def deals_field_map() -> Dict[str, str]:
    fields = DEALS_TABLE.fields
    return {key: field.resolve() for key, field in fields.items()}


def deals_field_candidates(keys: Iterable[str]) -> Dict[str, Tuple[str, ...]]:
    results: Dict[str, Tuple[str, ...]] = {}
    for key in keys:
        definition = DEALS_TABLE.fields[key]
        results[key] = definition.candidates()
    return results


# ---------------------------------------------------------------------------
# Campaign Manager table schema (Campaign Control base)
# ---------------------------------------------------------------------------


class CampaignManagerStatus(str, Enum):
    ACTIVE = "Active"
    PAUSED = "Paused"
    COMPLETED = "Completed"


class CampaignManagerMarket(str, Enum):
    MIAMI = "Miami, FL"
    ORLANDO = "Orlando, FL"
    TAMPA = "Tampa, FL"
    CHARLOTTE = "Charlotte, NC"
    HOUSTON = "Houston, TX"
    ATLANTA = "Atlanta, GA"
    PHOENIX = "Phoenix, AZ"
    NASHVILLE = "Nashville, TN"
    DALLAS = "Dallas, TX"


class CampaignManagerAlertLevel(str, Enum):
    NORMAL = "Normal"
    CRITICAL = "Critical"


class CampaignManagerStrategy(str, Enum):
    INBOUND = "Inbound"
    OUTBOUND = "Outbound"


CAMPAIGN_MANAGER_TABLE = TableDefinition(
    default="Campaigns Manager",
    env_vars=("CAMPAIGN_MANAGER_TABLE",),
    fields={
        "PRIMARY": FieldDefinition(
            default="Campaign ID",
            env_vars=("CAMPAIGN_MANAGER_PRIMARY_FIELD",),
            fallbacks=("campaign_id", "Name"),
        ),
        "NAME": FieldDefinition(
            default="Campaign Name",
            env_vars=("CAMPAIGN_MANAGER_NAME_FIELD",),
            fallbacks=("name",),
        ),
        "STATUS": FieldDefinition(
            default="Status",
            env_vars=("CAMPAIGN_MANAGER_STATUS_FIELD",),
            options=tuple(status.value for status in CampaignManagerStatus),
            fallbacks=("status",),
        ),
        "MARKET": FieldDefinition(
            default="Market",
            env_vars=("CAMPAIGN_MANAGER_MARKET_FIELD",),
            options=tuple(market.value for market in CampaignManagerMarket),
            fallbacks=("market",),
        ),
        "AI_ALERT_YN": FieldDefinition(
            default="AI Alerts Y/N",
            env_vars=("CAMPAIGN_MANAGER_ALERT_TOGGLE_FIELD",),
            options=("On", "Off"),
            fallbacks=("ai_alerts",),
        ),
        "AI_ALERT_LEVEL": FieldDefinition(
            default="AI Alert Level",
            env_vars=("CAMPAIGN_MANAGER_ALERT_LEVEL_FIELD",),
            options=tuple(level.value for level in CampaignManagerAlertLevel),
            fallbacks=("ai_alert_level",),
        ),
        "AI_STRATEGY_TAG": FieldDefinition(
            default="AI Strategy Tag",
            env_vars=("CAMPAIGN_MANAGER_STRATEGY_FIELD",),
            options=tuple(strategy.value for strategy in CampaignManagerStrategy),
            fallbacks=("ai_strategy_tag",),
        ),
        "TEMPLATE_CAMPAIGN_ID": FieldDefinition(
            default="Template Campaign ID",
            env_vars=("CAMPAIGN_MANAGER_TEMPLATE_ID_FIELD",),
            fallbacks=("template_campaign_id",),
        ),
        "GO_LIVE_COUNT": FieldDefinition(
            default="GoLive (count)",
            env_vars=("CAMPAIGN_MANAGER_GOLIVE_COUNT_FIELD",),
            fallbacks=("golive_count",),
        ),
        "AI_CONVERSATIONS_ASSIGNED": FieldDefinition(
            default="AI Conversations Assigned",
            env_vars=("CAMPAIGN_MANAGER_AI_CONVOS_FIELD",),
            fallbacks=("ai_conversations_assigned",),
        ),
        "OPT_OUT_RATE": FieldDefinition(
            default="Opt Out Rate",
            env_vars=("CAMPAIGN_MANAGER_OPT_OUT_RATE_FIELD",),
            fallbacks=("opt_out_rate",),
        ),
        "TOTAL_SENT": FieldDefinition(
            default="Total Sent",
            env_vars=("CAMPAIGN_MANAGER_TOTAL_SENT_FIELD",),
            fallbacks=("total_sent",),
        ),
        "TOTAL_REPLIES": FieldDefinition(
            default="Total Replies",
            env_vars=("CAMPAIGN_MANAGER_TOTAL_REPLIES_FIELD",),
            fallbacks=("total_replies",),
        ),
        "TOTAL_PROSPECTS": FieldDefinition(
            default="Total Prospects",
            env_vars=("CAMPAIGN_MANAGER_TOTAL_PROSPECTS_FIELD",),
            fallbacks=("total_prospects",),
        ),
        "TOTAL_OPT_INS": FieldDefinition(
            default="Total Opt-Ins",
            env_vars=("CAMPAIGN_MANAGER_TOTAL_OPTINS_FIELD",),
            fallbacks=("total_opt_ins",),
        ),
        "TOTAL_LEADS": FieldDefinition(
            default="Total Leads",
            env_vars=("CAMPAIGN_MANAGER_TOTAL_LEADS_FIELD",),
            fallbacks=("total_leads",),
        ),
        "TOTAL_DEALS": FieldDefinition(
            default="Total Deals",
            env_vars=("CAMPAIGN_MANAGER_TOTAL_DEALS_FIELD",),
            fallbacks=("total_deals",),
        ),
        "TOTAL_FAILS": FieldDefinition(
            default="Total Failed",
            env_vars=("CAMPAIGN_MANAGER_TOTAL_FAILS_FIELD",),
            fallbacks=("total_failed",),
        ),
        "TOTAL_ESCALATES": FieldDefinition(
            default="Total Escalates",
            env_vars=("CAMPAIGN_MANAGER_TOTAL_ESCALATES_FIELD",),
            fallbacks=("total_escalates",),
        ),
        "AI_CAMPAIGN_SCORE": FieldDefinition(
            default="AI Campaign Score",
            env_vars=("CAMPAIGN_MANAGER_AI_SCORE_FIELD",),
            fallbacks=("ai_campaign_score",),
        ),
        "SUGGESTED_NEXT_ACTION": FieldDefinition(
            default="Suggested Next Action (AI)",
            env_vars=("CAMPAIGN_MANAGER_NEXT_ACTION_FIELD",),
            fallbacks=("suggested_next_action",),
        ),
        "CAMPAIGN_HEALTH_SUMMARY": FieldDefinition(
            default="Campaign Health Summary (AI)",
            env_vars=("CAMPAIGN_MANAGER_HEALTH_SUMMARY_FIELD",),
            fallbacks=("campaign_health_summary",),
        ),
        "AI_STRATEGY_SUMMARY": FieldDefinition(
            default="AI Strategy Summary",
            env_vars=("CAMPAIGN_MANAGER_STRATEGY_SUMMARY_FIELD",),
            fallbacks=("ai_strategy_summary",),
        ),
        "AI_ALERT_NOTES": FieldDefinition(
            default="AI Alert Notes",
            env_vars=("CAMPAIGN_MANAGER_ALERT_NOTES_FIELD",),
            fallbacks=("ai_alert_notes",),
        ),
        "OPT_OUTS_LINK": FieldDefinition(
            default="Opt Outs",
            env_vars=("CAMPAIGN_MANAGER_OPT_OUTS_LINK_FIELD",),
            fallbacks=("Opt Outs",),
        ),
        "NUMBERS_LINK": FieldDefinition(
            default="Numbers",
            env_vars=("CAMPAIGN_MANAGER_NUMBERS_LINK_FIELD",),
            fallbacks=("Numbers",),
        ),
        "LAST_AI_MESSAGE": FieldDefinition(
            default="Last AI Message",
            env_vars=("CAMPAIGN_MANAGER_LAST_AI_MESSAGE_FIELD",),
            fallbacks=("last_ai_message",),
        ),
        "ROI": FieldDefinition(
            default="ROI",
            env_vars=("CAMPAIGN_MANAGER_ROI_FIELD",),
            fallbacks=("roi",),
        ),
        "REVENUE_PER_SEND": FieldDefinition(
            default="Revenue/Send Rate",
            env_vars=("CAMPAIGN_MANAGER_REVENUE_PER_SEND_FIELD",),
            fallbacks=("revenue_per_send",),
        ),
        "ACTIVE_TIME_HRS": FieldDefinition(
            default="Active Time (hrs)",
            env_vars=("CAMPAIGN_MANAGER_ACTIVE_TIME_FIELD",),
            fallbacks=("active_time_hrs",),
        ),
        "TOTAL_PROFIT": FieldDefinition(
            default="Total Profit",
            env_vars=("CAMPAIGN_MANAGER_TOTAL_PROFIT_FIELD",),
            fallbacks=("total_profit",),
        ),
        "TOTAL_EXPENSE": FieldDefinition(
            default="Total Expense",
            env_vars=("CAMPAIGN_MANAGER_TOTAL_EXPENSE_FIELD",),
            fallbacks=("total_expense",),
        ),
        "AI_ALERT_COUNT": FieldDefinition(
            default="AI Alert Count",
            env_vars=("CAMPAIGN_MANAGER_ALERT_COUNT_FIELD",),
            fallbacks=("ai_alert_count",),
        ),
    },
)


def campaign_manager_field_map() -> Dict[str, str]:
    fields = CAMPAIGN_MANAGER_TABLE.fields
    return {key: field.resolve() for key, field in fields.items()}


def campaign_manager_field_candidates(keys: Iterable[str]) -> Dict[str, Tuple[str, ...]]:
    results: Dict[str, Tuple[str, ...]] = {}
    for key in keys:
        definition = CAMPAIGN_MANAGER_TABLE.fields[key]
        results[key] = definition.candidates()
    return results
