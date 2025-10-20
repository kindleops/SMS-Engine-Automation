"""Classify inbound conversations using the autoresponder's intent detector."""

from __future__ import annotations

from typing import Dict, Optional

from sms.autoresponder import STAGE_MAP, classify_intent
from sms.airtable_schema import CONVERSATIONS_TABLE, conversations_field_map
from sms.datastore import CONNECTOR, update_record
from sms.runtime import get_logger

logger = get_logger(__name__)

CONV_FIELDS = conversations_field_map()
CONV_FIELD_NAMES = CONVERSATIONS_TABLE.field_names()

CONV_DIRECTION_FIELD = CONV_FIELDS["DIRECTION"]
CONV_BODY_FIELD = CONV_FIELDS["BODY"]
CONV_INTENT_FIELD = CONV_FIELDS["INTENT"]
CONV_STAGE_FIELD = CONV_FIELD_NAMES.get("STAGE", "Stage")
CONV_PROCESSED_AT_FIELD = CONV_FIELD_NAMES.get("PROCESSED_AT", "Processed Time")


def run(limit: Optional[int] = None) -> Dict[str, int]:
    handle = CONNECTOR.conversations()
    records = handle.table.all()  # type: ignore[attr-defined]
    classified = 0
    for record in records:
        fields = record.get("fields", {}) or {}
        if str(fields.get(CONV_DIRECTION_FIELD) or "").upper() not in {"IN", "INBOUND"}:
            continue
        body = fields.get(CONV_BODY_FIELD)
        if not body:
            continue
        intent = classify_intent(str(body))
        stage = STAGE_MAP.get(intent)
        updates = {
            CONV_INTENT_FIELD: intent,
        }
        if stage:
            updates[CONV_STAGE_FIELD] = stage
        if updates:
            update_record(handle, record["id"], updates)
            classified += 1
            if limit is not None and classified >= limit:
                break

    logger.info("Intent worker classified %s conversations", classified)
    return {"classified": classified}


if __name__ == "__main__":  # pragma: no cover - manual run helper
    print(run())
