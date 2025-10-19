"""Batch classifier for inbound conversations."""

from __future__ import annotations

from typing import Dict, Optional

from .. import autoresponder, spec
from ..datastore import CONNECTOR, update_conversation


def run(limit: Optional[int] = None) -> Dict[str, int]:
    handle = CONNECTOR.conversations()
    records = handle.table.all()
    classified = 0
    for record in records:
        fields = record.get("fields", {})
        if fields.get(spec.CONVERSATION_FIELDS.direction) != "INBOUND":
            continue
        body = fields.get(spec.CONVERSATION_FIELDS.message_body)
        if not body:
            continue
        classification = autoresponder.classify_intent(str(body))
        updates = {
            spec.CONVERSATION_FIELDS.intent_detected: classification.intent_detected,
            spec.CONVERSATION_FIELDS.ai_intent: classification.ai_intent,
            spec.CONVERSATION_FIELDS.message_summary: classification.summary,
        }
        if classification.stage:
            updates[spec.CONVERSATION_FIELDS.stage] = classification.stage
        update_conversation(record["id"], updates)
        classified += 1
        if limit is not None and classified >= limit:
            break
    return {"classified": classified}


if __name__ == "__main__":
    print(run())

