"""Worker that backfills Conversation ↔ Prospect/Lead links."""

from __future__ import annotations

from typing import Dict, Optional

from .. import spec
from ..datastore import ensure_prospect_or_lead, update_conversation, CONNECTOR


def run(limit: Optional[int] = None) -> Dict[str, int]:
    handle = CONNECTOR.conversations()
    records = handle.table.all()
    linked = 0
    for record in records:
        fields = record.get("fields", {})
        if fields.get(spec.CONVERSATION_FIELDS.lead_link) or fields.get(spec.CONVERSATION_FIELDS.prospect_link):
            continue
        phone = fields.get(spec.CONVERSATION_FIELDS.seller_phone)
        if not phone:
            continue
        lead, prospect = ensure_prospect_or_lead(str(phone))
        updates = {}
        if lead:
            updates[spec.CONVERSATION_FIELDS.lead_link] = [lead["id"]]
        elif prospect:
            updates[spec.CONVERSATION_FIELDS.prospect_link] = [prospect["id"]]
        if updates:
            update_conversation(record["id"], updates)
            linked += 1
            if limit is not None and linked >= limit:
                break
    return {"linked": linked}


if __name__ == "__main__":
    print(run())

