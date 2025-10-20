"""Promote interested conversations into Leads."""

from __future__ import annotations

from typing import Dict, Optional, Set

from sms.airtable_schema import CONVERSATIONS_TABLE, conversations_field_map
from sms.datastore import CONNECTOR, promote_to_lead, update_record
from sms.runtime import get_logger

logger = get_logger(__name__)

CONV_FIELDS = conversations_field_map()
CONV_FIELD_NAMES = CONVERSATIONS_TABLE.field_names()

CONV_SELLER_PHONE_FIELD = CONV_FIELDS["FROM"]
CONV_INTENT_FIELD = CONV_FIELDS["INTENT"]
CONV_STAGE_FIELD = CONV_FIELD_NAMES["STAGE"]
CONV_LEAD_LINK_FIELD = CONV_FIELD_NAMES.get("LEAD_LINK", "Lead")
CONV_LEAD_RECORD_FIELD = CONV_FIELD_NAMES.get("LEAD_RECORD_ID", "Lead Record ID")

INTEREST_INTENTS = {"followup_yes", "interest", "price_response", "condition_response"}


def run(limit: Optional[int] = None) -> Dict[str, int]:
    handle = CONNECTOR.conversations()
    records = handle.table.all()  # type: ignore[attr-defined]
    promoted = 0
    seen_leads: Set[str] = set()
    for record in records:
        fields = record.get("fields", {}) or {}
        if fields.get(CONV_LEAD_LINK_FIELD):
            continue

        phone = fields.get(CONV_SELLER_PHONE_FIELD)
        if not phone:
            continue

        intent = str(fields.get(CONV_INTENT_FIELD) or "").strip().lower()
        if intent not in INTEREST_INTENTS:
            continue

        lead_id, _ = promote_to_lead(str(phone), source="Lead Promoter", conversation_fields=fields)
        if not lead_id or lead_id in seen_leads:
            continue

        update_record(
            handle,
            record["id"],
            {
                CONV_LEAD_LINK_FIELD: [lead_id],
                CONV_LEAD_RECORD_FIELD: lead_id,
            },
        )
        promoted += 1
        seen_leads.add(lead_id)
        if limit is not None and promoted >= limit:
            break

    logger.info("Lead promoter linked %s conversations to leads", promoted)
    return {"promoted": promoted}


if __name__ == "__main__":  # pragma: no cover - manual execution helper
    print(run())
