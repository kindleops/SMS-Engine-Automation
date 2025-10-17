"""Campaign automation aligned with the README specification."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

from . import spec
from .datastore import (
    CONNECTOR,
    REPOSITORY,
    create_conversation,
    ensure_prospect_or_lead,
)
from .dispatcher import DISPATCHER, OutboundMessage


@dataclass
class Campaign:
    id: str
    fields: Dict[str, object]

    @property
    def status(self) -> str:
        return str(self.fields.get(spec.CAMPAIGN_FIELDS.status, "")).strip()

    @property
    def start_time(self) -> Optional[datetime]:
        raw = self.fields.get(spec.CAMPAIGN_FIELDS.start_time)
        if isinstance(raw, datetime):
            return raw
        return None

    @property
    def end_time(self) -> Optional[datetime]:
        raw = self.fields.get(spec.CAMPAIGN_FIELDS.end_time)
        if isinstance(raw, datetime):
            return raw
        return None


def _eligible_campaigns() -> List[Campaign]:
    campaigns = []
    for record in REPOSITORY.all_campaigns():
        campaign = Campaign(id=record.get("id"), fields=record.get("fields", {}))
        if campaign.status != "Running":
            continue
        now = spec.utc_now()
        if campaign.start_time and now < campaign.start_time:
            continue
        if campaign.end_time and now > campaign.end_time:
            continue
        campaigns.append(campaign)
    return campaigns


def _queue_message(
    campaign: Campaign,
    phone: str,
    body: str,
    from_number: str,
    lead_id: Optional[str],
    prospect_id: Optional[str],
) -> Dict[str, object]:
    message_fields = {
        spec.CONVERSATION_FIELDS.seller_phone: phone,
        spec.CONVERSATION_FIELDS.textgrid_phone: from_number,
        spec.CONVERSATION_FIELDS.direction: "OUTBOUND",
        spec.CONVERSATION_FIELDS.delivery_status: "QUEUED",
        spec.CONVERSATION_FIELDS.message_body: body,
        spec.CONVERSATION_FIELDS.stage: campaign.fields.get(spec.CONVERSATION_FIELDS.stage),
        spec.CONVERSATION_FIELDS.processed_by: "Campaign Runner",
        spec.CONVERSATION_FIELDS.campaign_link: [campaign.id],
        spec.CONVERSATION_FIELDS.campaign_record_id: campaign.id,
        spec.CONVERSATION_FIELDS.last_sent_time: spec.iso_now(),
    }

    if lead_id:
        message_fields[spec.CONVERSATION_FIELDS.lead_link] = [lead_id]
    elif prospect_id:
        message_fields[spec.CONVERSATION_FIELDS.prospect_link] = [prospect_id]

    record = create_conversation(None, message_fields)

    outbound = OutboundMessage(
        to_number=phone,
        from_number=from_number,
        body=body,
        campaign_id=campaign.id,
        metadata={"conversation_id": record.get("id")},
    )
    DISPATCHER.queue(outbound)
    return record


def run_campaigns() -> Dict[str, object]:
    results = {"campaigns": 0, "messages": 0}
    campaigns = _eligible_campaigns()
    for campaign in campaigns:
        results["campaigns"] += 1
        recipients = campaign.fields.get("Prospects", [])
        if not isinstance(recipients, list):
            continue
        prospects_handle = CONNECTOR.prospects()
        for prospect_id in recipients:
            prospect = prospects_handle.table.get(prospect_id) if prospects_handle else None
            if not prospect:
                continue
            phone = prospect.get("fields", {}).get(spec.CONVERSATION_FIELDS.seller_phone)
            if not phone:
                continue
            lead, prospect_record = ensure_prospect_or_lead(phone)
            body = prospect.get("fields", {}).get("Message") or "Hello! We'd love to chat about your property."
            from_number = campaign.fields.get(spec.CONVERSATION_FIELDS.textgrid_phone) or spec.normalize_phone("+15551234567")
            lead_id = lead["id"] if lead else None
            prospect_id_value = prospect_record["id"] if prospect_record else None
            _queue_message(campaign, phone, body, str(from_number), lead_id, prospect_id_value)
            results["messages"] += 1
    return results

