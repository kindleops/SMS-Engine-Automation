"""Backfill missing Conversation ↔ Lead/Prospect links using datastore lookups."""

from __future__ import annotations

from typing import Dict, Optional

from sms.airtable_schema import CONVERSATIONS_TABLE, conversations_field_map
from sms.datastore import CONNECTOR, ensure_prospect_or_lead, update_record
from sms.runtime import get_logger

logger = get_logger(__name__)

CONV_FIELDS = conversations_field_map()
CONV_FIELD_NAMES = CONVERSATIONS_TABLE.field_names()

CONV_SELLER_PHONE_FIELD = CONV_FIELDS["FROM"]
CONV_LEAD_LINK_FIELD = CONV_FIELD_NAMES.get("LEAD_LINK", "Lead")
CONV_LEAD_RECORD_FIELD = CONV_FIELD_NAMES.get("LEAD_RECORD_ID", "Lead Record ID")
CONV_PROSPECT_LINK_FIELD = CONV_FIELD_NAMES.get("PROSPECT_LINK", "Prospect")
CONV_PROSPECT_RECORD_FIELD = CONV_FIELD_NAMES.get("PROSPECT_RECORD_ID", "Prospect Record ID")


def run(limit: Optional[int] = None) -> Dict[str, int]:
    handle = CONNECTOR.conversations()
    records = handle.table.all()  # type: ignore[attr-defined]
    linked = 0
    for record in records:
        fields = record.get("fields", {}) or {}
        if fields.get(CONV_LEAD_LINK_FIELD) or fields.get(CONV_PROSPECT_LINK_FIELD):
            continue

        phone = fields.get(CONV_SELLER_PHONE_FIELD)
        if not phone:
            continue

        lead, prospect = ensure_prospect_or_lead(str(phone))
        updates = {}
        if lead:
            updates[CONV_LEAD_LINK_FIELD] = [lead["id"]]
            updates[CONV_LEAD_RECORD_FIELD] = lead["id"]
        elif prospect:
            updates[CONV_PROSPECT_LINK_FIELD] = [prospect["id"]]
            updates[CONV_PROSPECT_RECORD_FIELD] = prospect["id"]

        if updates:
            update_record(handle, record["id"], updates)
            linked += 1
            if limit is not None and linked >= limit:
                break

    logger.info("Autolinker linked %s conversations", linked)
    return {"linked": linked}


if __name__ == "__main__":  # pragma: no cover - manual execution helper
    print(run())
