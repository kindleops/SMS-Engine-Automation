"""
🚀 Bulletproof Intent Classification Worker
-------------------------------------------
Classifies inbound conversations using the autoresponder's intent detector.

Features:
 • Skips already classified conversations
 • Formula-based filtering for speed
 • Resilient Airtable updates with retry
 • Structured logging and progress tracking
 • Optional record limit for batch runs
"""

from __future__ import annotations

import time
from typing import Dict, Optional

from sms.autoresponder import STAGE_MAP, classify_intent
from sms.airtable_schema import CONVERSATIONS_TABLE, conversations_field_map
from sms.datastore import CONNECTOR, update_record
from sms.runtime import get_logger

logger = get_logger("intent_worker")

# Field maps
CONV_FIELDS = conversations_field_map()
CONV_FIELD_NAMES = CONVERSATIONS_TABLE.field_names()

# Columns
F_DIRECTION = CONV_FIELDS["DIRECTION"]
F_BODY = CONV_FIELDS["BODY"]
F_INTENT = CONV_FIELDS["INTENT"]
F_STAGE = CONV_FIELD_NAMES.get("STAGE", "Stage")
F_PROCESSED_AT = CONV_FIELD_NAMES.get("PROCESSED_AT", "Processed Time")


# ---------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------
def _safe_update(table, rid: str, payload: Dict[str, str], retries: int = 3):
    """Safe Airtable update with retry + exponential backoff."""
    delay = 0.5
    for i in range(retries):
        try:
            update_record(table, rid, payload)
            return True
        except Exception as e:
            msg = str(e)
            if "429" in msg or "422" in msg:
                time.sleep(delay)
                delay *= 2
                continue
            logger.warning(f"⚠️ Update failed ({i + 1}/{retries}) for {rid}: {msg}")
            time.sleep(delay)
    logger.error(f"❌ Gave up updating record {rid}")
    return False


# ---------------------------------------------------------------
# Main Worker
# ---------------------------------------------------------------
def run(limit: Optional[int] = None) -> Dict[str, int]:
    """Classify inbound messages with autoresponder intent."""
    start_time = time.time()
    handle = CONNECTOR.conversations()

    # Formula: only pull inbound messages with a body but no intent yet
    formula = "AND({Direction}='INBOUND', {Body} != '', NOT({Intent}))"

    try:
        records = handle.table.all(formula=formula)  # type: ignore[attr-defined]
    except Exception as e:
        logger.error(f"❌ Failed to fetch conversations: {e}")
        return {"classified": 0, "error": str(e)}

    classified = 0
    skipped = 0
    errors = 0

    for record in records:
        fields = record.get("fields", {}) or {}
        body = fields.get(F_BODY)
        if not body:
            skipped += 1
            continue

        # Optional safety: skip if already classified
        if fields.get(F_INTENT):
            skipped += 1
            continue

        try:
            intent = classify_intent(str(body))
            stage = STAGE_MAP.get(intent)
        except Exception as e:
            logger.warning(f"⚠️ Failed to classify record {record.get('id')}: {e}")
            errors += 1
            continue

        updates = {F_INTENT: intent}
        if stage:
            updates[F_STAGE] = stage
        if not updates:
            skipped += 1
            continue

        ok = _safe_update(handle, record["id"], updates)
        if ok:
            classified += 1

        if limit is not None and classified >= limit:
            break

    duration = round(time.time() - start_time, 2)
    logger.info(f"✅ Intent worker complete — classified={classified}, skipped={skipped}, errors={errors}, duration={duration}s")

    return {
        "classified": classified,
        "skipped": skipped,
        "errors": errors,
        "duration_sec": duration,
    }


# ---------------------------------------------------------------
# Manual Execution
# ---------------------------------------------------------------
if __name__ == "__main__":  # pragma: no cover - for manual runs
    print(run())
