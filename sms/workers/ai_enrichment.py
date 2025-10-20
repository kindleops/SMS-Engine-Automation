"""Generate lightweight message summaries for Conversations lacking AI output."""

from __future__ import annotations

from typing import Dict, Optional

from sms.airtable_schema import CONVERSATIONS_TABLE, conversations_field_map
from sms.datastore import CONNECTOR, update_record
from sms.runtime import get_logger

logger = get_logger(__name__)

CONV_FIELDS = conversations_field_map()
CONV_FIELD_NAMES = CONVERSATIONS_TABLE.field_names()

CONV_BODY_FIELD = CONV_FIELDS["BODY"]
CONV_SUMMARY_FIELD = CONV_FIELD_NAMES.get("MESSAGE_SUMMARY", "Message Summary (AI)")


def _summarise(body: str) -> str:
    body = body.strip()
    if len(body) <= 120:
        return body
    return body[:117] + "..."


def run(limit: Optional[int] = None) -> Dict[str, int]:
    handle = CONNECTOR.conversations()
    records = handle.table.all()  # type: ignore[attr-defined]
    enriched = 0
    for record in records:
        fields = record.get("fields", {}) or {}
        message = fields.get(CONV_BODY_FIELD)
        if not message:
            continue
        if fields.get(CONV_SUMMARY_FIELD):
            continue
        update_record(handle, record["id"], {CONV_SUMMARY_FIELD: _summarise(str(message))})
        enriched += 1
        if limit is not None and enriched >= limit:
            break

    logger.info("AI enrichment summarised %s conversations", enriched)
    return {"enriched": enriched}


if __name__ == "__main__":  # pragma: no cover - manual helper
    print(run())
