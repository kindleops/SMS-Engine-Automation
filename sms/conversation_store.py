"""Shared Airtable helpers for Conversations/Leads/Prospects operations."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, Optional, Tuple

from sms.airtable_schema import (
    CONVERSATIONS,
    LEADS,
    PROSPECTS,
    ensure_delivery_status,
    ensure_processed_by,
    ensure_stage,
)
from sms.tables import get_convos, get_leads, get_prospects


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _digits_only(phone: Optional[str]) -> str:
    return re.sub(r"\D", "", phone or "")


def normalize_phone(phone: Optional[str]) -> Optional[str]:
    """Return an E.164-ish phone number (US-biased fallback)."""

    digits = _digits_only(phone)
    if not digits:
        return None
    if len(digits) == 10:
        return "+1" + digits
    if len(digits) == 11 and digits.startswith("1"):
        return "+" + digits
    if phone and phone.startswith("+"):
        return "+" + digits
    return "+" + digits


def last10(phone: Optional[str]) -> Optional[str]:
    digits = _digits_only(phone)
    return digits[-10:] if len(digits) >= 10 else None


@lru_cache(maxsize=1)
def _conversation_table():
    return get_convos()


@lru_cache(maxsize=1)
def _leads_table():
    return get_leads()


@lru_cache(maxsize=1)
def _prospects_table():
    return get_prospects()


def _safe_create(tbl, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not tbl:
        return None
    body = {k: v for k, v in payload.items() if v not in (None, "", [], {})}
    if not body:
        return None
    try:
        return tbl.create(body)
    except Exception as exc:  # pragma: no cover - network failure path
        print(f"⚠️ Airtable create failed: {exc}")
        return None


def _safe_update(tbl, record_id: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not tbl or not record_id:
        return None
    body = {k: v for k, v in payload.items() if v not in (None, "", [], {})}
    if not body:
        return None
    try:
        return tbl.update(record_id, body)
    except Exception as exc:  # pragma: no cover - network failure path
        print(f"⚠️ Airtable update failed: {exc}")
        return None


def _find_record_by_phone(tbl, phone: Optional[str]) -> Optional[Dict[str, Any]]:
    if not tbl or not phone:
        return None
    want = last10(phone)
    if not want:
        return None
    try:
        for record in tbl.all():
            fields = record.get("fields", {}) or {}
            for candidate in (
                "Phone",
                "phone",
                "Seller Phone Number",
                "Mobile",
                "Cell",
                "Primary Phone",
                "Phone 1 (from Linked Owner)",
                "Phone 2 (from Linked Owner)",
            ):
                if last10(fields.get(candidate)) == want:
                    return record
    except Exception as exc:  # pragma: no cover - network failure path
        print(f"⚠️ Failed to scan Airtable rows: {exc}")
    return None


def _find_conversation_by_sid(sid: Optional[str]) -> Optional[Dict[str, Any]]:
    tbl = _conversation_table()
    if not tbl or not sid:
        return None
    try:
        for record in tbl.all():
            fields = record.get("fields", {}) or {}
            if str(fields.get(CONVERSATIONS.textgrid_id) or "") == sid:
                return record
            if str(fields.get("TextGrid ID") or "") == sid:
                return record
    except Exception as exc:  # pragma: no cover - network failure path
        print(f"⚠️ Failed to scan Conversations for SID {sid}: {exc}")
    return None


_LOCAL_IDEMPOTENCY_CACHE: set[str] = set()


def upsert_conversation(payload: Dict[str, Any], textgrid_id: Optional[str]) -> Optional[str]:
    """Create/update a Conversations row, enforcing idempotency on TextGrid ID."""

    tbl = _conversation_table()
    if tbl:
        existing = _find_conversation_by_sid(textgrid_id)
        if existing:
            _safe_update(tbl, existing["id"], payload)
            return existing["id"]
        created = _safe_create(tbl, payload)
        return (created or {}).get("id")

    # Local fallback: prevent duplicates during tests without Airtable
    if textgrid_id:
        if textgrid_id in _LOCAL_IDEMPOTENCY_CACHE:
            return None
        _LOCAL_IDEMPOTENCY_CACHE.add(textgrid_id)
    return None


def find_or_create_prospect(phone: str) -> Optional[Dict[str, Any]]:
    tbl = _prospects_table()
    if not phone:
        return None
    existing = _find_record_by_phone(tbl, phone)
    if existing:
        return existing
    payload = {
        PROSPECTS.phone: normalize_phone(phone),
        PROSPECTS.stage: CONVERSATIONS.allowed_stages[0],
        PROSPECTS.reply_count: 0,
        PROSPECTS.last_inbound: utcnow_iso(),
    }
    return _safe_create(tbl, payload)


def resolve_contact_links(phone: Optional[str]) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """Return (lead_record, prospect_record) for a seller phone."""

    normalized = normalize_phone(phone)
    lead_record = _find_record_by_phone(_leads_table(), normalized)
    if lead_record:
        return lead_record, None

    prospect_record = find_or_create_prospect(normalized) if normalized else None
    return None, prospect_record


def promote_to_lead(phone: str, *, source: str, campaign_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Create or fetch a lead for the seller phone number."""

    tbl = _leads_table()
    if not phone or not tbl:
        return None

    normalized = normalize_phone(phone)
    existing = _find_record_by_phone(tbl, normalized)
    if existing:
        return existing

    fields: Dict[str, Any] = {
        LEADS.phone: normalized,
        LEADS.lead_status: "ACTIVE COMMUNICATION",
        LEADS.source: source,
        LEADS.reply_count: 0,
        LEADS.sent_count: 0,
        LEADS.last_activity: utcnow_iso(),
        LEADS.last_inbound: utcnow_iso(),
        LEADS.last_direction: "INBOUND",
    }
    if campaign_id:
        fields[LEADS.campaigns] = [campaign_id]

    created = _safe_create(tbl, fields)
    return created


def update_conversation_links(
    conversation_id: Optional[str],
    *,
    lead: Optional[Dict[str, Any]] = None,
    prospect: Optional[Dict[str, Any]] = None,
    textgrid_id: Optional[str] = None,
):
    tbl = _conversation_table()
    if not tbl or not conversation_id:
        return

    payload: Dict[str, Any] = {}
    if lead:
        payload.update(
            {
                CONVERSATIONS.lead_record_id: lead.get("id"),
                CONVERSATIONS.link_lead: [lead.get("id")],
                CONVERSATIONS.prospect_record_id: None,
                CONVERSATIONS.link_prospect: [],
            }
        )
    elif prospect:
        payload.update(
            {
                CONVERSATIONS.prospect_record_id: prospect.get("id"),
                CONVERSATIONS.link_prospect: [prospect.get("id")],
                CONVERSATIONS.lead_record_id: None,
                CONVERSATIONS.link_lead: [],
            }
        )

    if textgrid_id:
        payload[CONVERSATIONS.textgrid_id] = textgrid_id

    _safe_update(tbl, conversation_id, payload)


def update_lead_activity(
    lead: Optional[Dict[str, Any]],
    *,
    body: str,
    direction: str,
    delivery_status: str,
    reply_increment: bool = False,
    send_increment: bool = False,
    status_changed: bool = True,
):
    if not lead:
        return
    tbl = _leads_table()
    if not tbl:
        return

    lead_id = lead.get("id")
    fields = lead.get("fields", {}) if lead else {}
    reply_count = int(fields.get(LEADS.reply_count, 0) or 0)
    sent_count = int(fields.get(LEADS.sent_count, 0) or 0)
    delivered_count = int(fields.get(LEADS.delivered_count, 0) or 0)
    failed_count = int(fields.get(LEADS.failed_count, 0) or 0)

    patch: Dict[str, Any] = {
        LEADS.last_activity: utcnow_iso(),
        LEADS.last_message: (body or "")[:500],
        LEADS.last_direction: direction,
        LEADS.last_delivery_status: delivery_status,
    }
    if direction == "INBOUND":
        patch[LEADS.last_inbound] = utcnow_iso()
        if reply_increment:
            patch[LEADS.reply_count] = reply_count + 1
    else:
        patch[LEADS.last_outbound] = utcnow_iso()
        if send_increment:
            patch[LEADS.sent_count] = sent_count + 1

    if status_changed:
        if delivery_status == "DELIVERED":
            patch[LEADS.delivered_count] = delivered_count + 1
        elif delivery_status in {"FAILED", "UNDELIVERED"}:
            patch[LEADS.failed_count] = failed_count + 1

    _safe_update(tbl, lead_id, patch)


def base_conversation_payload(
    *,
    seller_phone: Optional[str],
    textgrid_phone: Optional[str],
    body: str,
    direction: str,
    delivery_status: str,
    processed_by: Optional[str],
    stage: Optional[str],
    intent_detected: Optional[str],
    ai_intent: Optional[str],
    textgrid_id: Optional[str],
    campaign_id: Optional[str] = None,
    template_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Create a normalised payload ready for ``upsert_conversation``."""

    payload = {
        CONVERSATIONS.seller_phone_number: normalize_phone(seller_phone),
        CONVERSATIONS.textgrid_phone_number: normalize_phone(textgrid_phone),
        CONVERSATIONS.message_long: body,
        CONVERSATIONS.message_summary: (body or "")[:255],
        CONVERSATIONS.direction: direction,
        CONVERSATIONS.delivery_status: ensure_delivery_status(delivery_status),
        CONVERSATIONS.processed_by: ensure_processed_by(processed_by),
        CONVERSATIONS.stage: ensure_stage(stage),
        CONVERSATIONS.intent_detected: intent_detected if intent_detected in CONVERSATIONS.allowed_intents else "Neutral",
        CONVERSATIONS.ai_intent: ai_intent if ai_intent in CONVERSATIONS.allowed_ai_intents else "other",
        CONVERSATIONS.textgrid_id: textgrid_id,
        CONVERSATIONS.received_time: utcnow_iso(),
    }

    if direction == "OUTBOUND":
        payload[CONVERSATIONS.last_sent_time] = utcnow_iso()

    if campaign_id:
        payload.update({
            CONVERSATIONS.campaign_record_id: campaign_id,
            CONVERSATIONS.link_campaign: [campaign_id],
        })

    if template_id:
        payload.update({
            CONVERSATIONS.template_record_id: template_id,
            CONVERSATIONS.link_template: [template_id],
        })

    return payload

