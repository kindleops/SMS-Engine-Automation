"""Authoritative Airtable schema definitions used across the SMS engine.

This module mirrors the canonical field names and select options described in
``README2.md``.  Centralising them here guarantees that every integration point
logs data using the exact strings Airtable expects.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


@dataclass(frozen=True)
class ConversationsSchema:
    """Field names + enumerations for the ``Conversations`` table."""

    stage: str = "Stage"
    processed_by: str = "Processed By"
    intent_detected: str = "Intent Detected"
    direction: str = "Direction"
    delivery_status: str = "Delivery Status"
    ai_intent: str = "AI Intent"
    textgrid_phone_number: str = "TextGrid Phone Number"
    textgrid_id: str = "TextGrid ID"
    template_record_id: str = "Template Record ID"
    seller_phone_number: str = "Seller Phone Number"
    prospect_record_id: str = "Prospect Record ID"
    lead_record_id: str = "Lead Record ID"
    campaign_record_id: str = "Campaign Record ID"
    message_summary: str = "Message Summary (AI)"
    message_long: str = "Message Long text"
    received_time: str = "Received Time"
    processed_time: str = "Processed Time"
    last_sent_time: str = "Last Sent Time"
    last_retry_time: str = "Last Retry Time"

    link_lead: str = "Lead"
    link_prospect: str = "Prospect"
    link_campaign: str = "Campaign"
    link_template: str = "Template"

    allowed_stages: Sequence[str] = (
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

    allowed_processed_by: Sequence[str] = (
        "Campaign Runner",
        "Autoresponder",
        "AI: Phi-3 Mini",
        "AI: Phi-3 Medium",
        "AI: GPT-4o",
        "AI: Mistral 7B",
        "AI: Gemma 2",
        "Manual / Human",
    )

    allowed_intents: Sequence[str] = (
        "Positive",
        "Neutral",
        "Delay",
        "Reject",
        "DNC",
    )

    allowed_directions: Sequence[str] = ("INBOUND", "OUTBOUND")

    allowed_delivery_statuses: Sequence[str] = (
        "QUEUED",
        "SENT",
        "DELIVERED",
        "FAILED",
        "UNDELIVERED",
        "OPT OUT",
    )

    allowed_ai_intents: Sequence[str] = (
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


CONVERSATIONS = ConversationsSchema()


@dataclass(frozen=True)
class LeadsSchema:
    """Field names + enumerations for the ``Leads`` table."""

    phone: str = "Phone"
    lead_status: str = "Lead Status"
    campaigns: str = "Campaigns"
    conversations: str = "Conversations"
    deals: str = "Deals"
    delivered_count: str = "Delivered Count"
    failed_count: str = "Failed Count"
    last_activity: str = "Last Activity"
    last_delivery_status: str = "Last Delivery Status"
    last_direction: str = "Last Direction"
    last_inbound: str = "Last Inbound"
    last_message: str = "Last Message"
    last_outbound: str = "Last Outbound"
    reply_count: str = "Reply Count"
    response_time: str = "Response Time (Minutes)"
    sent_count: str = "Sent Count"
    source: str = "Source"
    prospect_link: str = "Prospect"

    allowed_statuses: Sequence[str] = (
        "NEW",
        "CONTACTED",
        "ACTIVE COMMUNICATION",
        "LEAD FOLLOW UP",
        "WARM COMPS?",
        "MAKE OFFER",
        "OFFER FOLLOW UP",
        "UNDER CONTRACT",
        "DISPOSITION STAGE",
        "IN ESCROW",
        "CLOSING SET",
        "DEAD",
    )


LEADS = LeadsSchema()


@dataclass(frozen=True)
class ProspectsSchema:
    """Minimal field references for ``Prospects`` table interactions."""

    phone: str = "Phone"
    stage: str = "Stage"
    lead_link: str = "Lead"
    last_inbound: str = "Last Inbound"
    reply_count: str = "Reply Count"


PROSPECTS = ProspectsSchema()


DEFAULT_STAGE = CONVERSATIONS.allowed_stages[0]
PROMOTION_STAGE = "STAGE 3 - PRICE QUALIFICATION"


def ensure_stage(stage: str | None) -> str:
    """Return a valid stage value, defaulting to ``DEFAULT_STAGE``."""

    if stage and stage in CONVERSATIONS.allowed_stages:
        return stage
    return DEFAULT_STAGE


def ensure_processed_by(actor: str | None) -> str:
    """Return a valid Processed By value with a safe default."""

    if actor and actor in CONVERSATIONS.allowed_processed_by:
        return actor
    return "Manual / Human"


def ensure_delivery_status(status: str | None) -> str:
    """Return a valid Delivery Status string."""

    if status and status in CONVERSATIONS.allowed_delivery_statuses:
        return status
    return "SENT"

