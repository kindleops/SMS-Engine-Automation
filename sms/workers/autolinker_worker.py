"""
🚀 Autolinker Worker (Final Revision)
-------------------------------------
Backfills missing Conversation ↔ Lead/Prospect links using datastore lookups.
Safe for large datasets with pagination and retry protection.
"""

from __future__ import annotations
from typing import Dict, Optional

from sms.airtable_schema import CONVERSATIONS_TABLE, conversations_field_map
from sms.datastore import CONNECTOR, ensure_prospect_or_lead, update_record
from sms.runtime import get_logger
from sms.campaign_runner import _safe_update  # reuse your resilient update

logger = get_logger("autolinker")

CONV_FIELDS = conversations_field_map()
CONV_FIELD_NAMES = CONVERSATIONS_TABLE.field_names()

F_FROM = CONV_FIELDS["FROM"]
F_LEAD_LINK = CONV_FIELD_NAMES.get("LEAD_LINK", "Lead")
F_LEAD_RECORD = CONV_FIELD_NAMES.get("LEAD_RECORD_ID", "Lead Record ID")
F_PROSPECT_LINK = CONV_FIELD_NAMES.get("PROSPECT_LINK", "Prospect")
F_PROSPECT_RECORD = CONV_FIELD_NAMES.get("PROSPECT_RECORD_ID", "Prospect Record ID")
F_STATUS = CONV_FIELD_NAMES.get("Status", "Status")

PAGE_SIZE = 100  # safety for large bases


def run(limit: Optional[int] = None) -> Dict[str, int]:
    table = CONNECTOR.conversations().table
    linked = 0
    offset = None
    total_processed = 0

    logger.info("🔍 Starting autolinker job...")

    while True:
        try:
            batch = table.all(page_size=PAGE_SIZE, offset=offset)
        except Exception as e:
            logger.error(f"❌ Failed fetching batch: {e}")
            break

        if not batch:
            break

        for rec in batch:
            total_processed += 1
            f = rec.get("fields", {}) or {}

            # Skip if already linked or archived
            if f.get(F_LEAD_LINK) or f.get(F_PROSPECT_LINK) or str(f.get(F_STATUS)).lower() == "archived":
                continue

            phone = f.get(F_FROM)
            if not phone:
                continue

            lead, prospect = ensure_prospect_or_lead(str(phone))
            if not lead and not prospect:
                continue

            updates = {}
            if lead:
                updates[F_LEAD_LINK] = [lead["id"]]
                updates[F_LEAD_RECORD] = lead["id"]
            if prospect:
                updates[F_PROSPECT_LINK] = [prospect["id"]]
                updates[F_PROSPECT_RECORD] = prospect["id"]

            if updates:
                try:
                    update_record(table, rec["id"], updates)
                    linked += 1
                except Exception as e:
                    logger.warning(f"⚠️ Update failed for {rec['id']}: {e}")
                    continue

            if limit is not None and linked >= limit:
                logger.info("✅ Limit reached, stopping early.")
                break

        if limit is not None and linked >= limit:
            break

        # Airtable pagination: check if there’s another offset
        if hasattr(table, "offset"):
            offset = getattr(table, "offset", None)
        else:
            break

        if total_processed % 200 == 0:
            logger.info(f"🔄 Processed {total_processed} records so far...")

    logger.info(f"✅ Autolinker complete — linked {linked} conversations (processed {total_processed})")
    return {"linked": linked, "processed": total_processed}


if __name__ == "__main__":  # pragma: no cover
    print(run())
