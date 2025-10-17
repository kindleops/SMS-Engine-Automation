"""Worker that enriches conversations with lightweight AI summaries."""

from __future__ import annotations

from typing import Dict, Optional

from .. import spec
from ..datastore import CONNECTOR, update_conversation


def _summarise(body: str) -> str:
    body = body.strip()
    if len(body) <= 120:
        return body
    return body[:117] + "..."


def run(limit: Optional[int] = None) -> Dict[str, int]:
    handle = CONNECTOR.conversations()
    records = handle.table.all()
    enriched = 0
    for record in records:
        fields = record.get("fields", {})
        message = fields.get(spec.CONVERSATION_FIELDS.message_body)
        summary = fields.get(spec.CONVERSATION_FIELDS.message_summary)
        if not message or summary:
            continue
        new_summary = _summarise(str(message))
        update_conversation(record["id"], {spec.CONVERSATION_FIELDS.message_summary: new_summary})
        enriched += 1
        if limit is not None and enriched >= limit:
            break
    return {"enriched": enriched}


if __name__ == "__main__":
    print(run())

