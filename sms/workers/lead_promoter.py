"""Promote eligible prospects into leads."""

from __future__ import annotations

from typing import Dict, Optional

from .. import spec
from ..datastore import CONNECTOR, promote_if_needed


def run(limit: Optional[int] = None) -> Dict[str, int]:
    handle = CONNECTOR.conversations()
    records = handle.table.all()
    promoted = 0
    for record in records:
        fields = record.get("fields", {})
        phone = fields.get(spec.CONVERSATION_FIELDS.seller_phone)
        if not phone:
            continue
        stage = fields.get(spec.CONVERSATION_FIELDS.stage)
        intent = fields.get(spec.CONVERSATION_FIELDS.intent_detected)
        ai_intent = fields.get(spec.CONVERSATION_FIELDS.ai_intent)
        if not spec.should_promote(intent, ai_intent, stage):
            continue
        lead = promote_if_needed(str(phone), fields, stage)
        if lead:
            promoted += 1
            if limit is not None and promoted >= limit:
                break
    return {"promoted": promoted}


if __name__ == "__main__":
    print(run())

