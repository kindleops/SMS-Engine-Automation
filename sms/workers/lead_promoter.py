"""
🚀 Bulletproof Lead Promotion Worker
------------------------------------
Promotes interested conversations into Leads.

Features:
 • Formula-based query to limit fetch size
 • Skips already linked leads
 • Resilient Airtable updates with retry
 • De-duplication via in-memory set
 • Structured metrics & duration tracking
"""

from __future__ import annotations

import time
from typing import Dict, Optional, Set

from sms.airtable_schema import CONVERSATIONS_TABLE, conversations_field_map
from sms.datastore import CONNECTOR, promote_to_lead, update_record
from sms.runtime import get_logger

logger = get_logger("lead_promoter")

# Field maps
CONV_FIELDS = conversations_field_map()
CONV_FIELD_NAMES = CONVERSATIONS_TABLE.field_names()

# Column aliases
F_SELLER_PHONE = CONV_FIELDS["FROM"]
F_INTENT = CONV_FIELDS["INTENT"]
F_STAGE = CONV_FIELD_NAMES.get("STAGE", "Stage")
F_LEAD_LINK = CONV_FIELD_NAMES.get("LEAD_LINK", "Lead")
F_LEAD_RECORD = CONV_FIELD_NAMES.get("LEAD_RECORD_ID", "Lead Record ID")

# Define what counts as "interest"
INTEREST_INTENTS = {"followup_yes", "interest", "price_response", "condition_response"}


# ---------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------
def _safe_update(table, rid: str, payload: Dict[str, str], retries: int = 3):
    """Retry-safe Airtable update with exponential backoff."""
    delay = 0.5
    for i in range(retries):
        try:
            update_record(table, rid, payload)
            return True
        except Exception as e:
            msg = str(e)
            if "422" in msg or "429" in msg:
                time.sleep(delay)
                delay *= 2
                continue
            logger.warning(f"⚠️ Update failed ({i+1}/{retries}) for {rid}: {msg}")
            time.sleep(delay)
    logger.error(f"❌ Gave up updating record {rid}")
    return False


# ---------------------------------------------------------------
# Main Worker
# ---------------------------------------------------------------
def run(limit: Optional[int] = None) -> Dict[str, int]:
    """Promote interested conversations into Leads."""
    start_time = time.time()
    handle = CONNECTOR.conversations()

    # Formula: only pull inbound conversations that show interest and have no lead yet
    formula = (
        "AND("
        "NOT({Lead}),"
        "OR("
        "{Intent}='followup_yes',"
        "{Intent}='interest',"
        "{Intent}='price_response',"
        "{Intent}='condition_response'"
        ")"
        ")"
    )

    try:
        records = handle.table.all(formula=formula)  # type: ignore[attr-defined]
    except Exception as e:
        logger.error(f"❌ Failed to fetch conversations: {e}")
        return {"promoted": 0, "error": str(e)}

    promoted = 0
    skipped = 0
    errors = 0
    seen_leads: Set[str] = set()

    for record in records:
        fields = record.get("fields", {}) or {}
        if fields.get(F_LEAD_LINK):
            skipped += 1
            continue

        phone = fields.get(F_SELLER_PHONE)
        if not phone:
            skipped += 1
            continue

        intent = str(fields.get(F_INTENT) or "").strip().lower()
        if intent not in INTEREST_INTENTS:
            skipped += 1
            continue

        try:
            lead_id, _ = promote_to_lead(
                str(phone),
                source="Lead Promoter",
                conversation_fields=fields,
            )
        except Exception as e:
            logger.warning(f"⚠️ promote_to_lead failed for {phone}: {e}")
            errors += 1
            continue

        if not lead_id or lead_id in seen_leads:
            skipped += 1
            continue

        ok = _safe_update(
            handle,
            record["id"],
            {
                F_LEAD_LINK: [lead_id],
                F_LEAD_RECORD: lead_id,
            },
        )
        if ok:
            promoted += 1
            seen_leads.add(lead_id)

        if limit is not None and promoted >= limit:
            break

    duration = round(time.time() - start_time, 2)
    logger.info(
        f"✅ Lead promoter complete — promoted={promoted}, skipped={skipped}, errors={errors}, duration={duration}s"
    )

    return {
        "promoted": promoted,
        "skipped": skipped,
        "errors": errors,
        "duration_sec": duration,
    }


# ---------------------------------------------------------------
# Manual Execution
# ---------------------------------------------------------------
if __name__ == "__main__":  # pragma: no cover - manual run helper
    print(run())
