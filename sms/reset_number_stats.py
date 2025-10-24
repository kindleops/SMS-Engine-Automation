"""
🔁 reset_daily_stats.py (v3.1 — Telemetry Edition)
──────────────────────────────────────────────────
Resets all daily number stats with flexible field mapping.
Adds:
 - Structured logging
 - KPI + Run telemetry
 - Cron-ready output
"""

from __future__ import annotations
import os, traceback, re
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

from sms.runtime import get_logger
log = get_logger("reset_daily_stats")

try:
    from sms.logger import log_run
except Exception:
    def log_run(*_a, **_k): pass

try:
    from sms.kpi_logger import log_kpi
except Exception:
    def log_kpi(*_a, **_k): pass


# -------------------------------
# Airtable setup
# -------------------------------
_PyTable = _PyApi = None
try:
    from pyairtable import Table as _PyTable
except Exception:
    pass
try:
    from pyairtable import Api as _PyApi
except Exception:
    pass

def _make_table(api_key: Optional[str], base_id: Optional[str], table_name: str):
    if not (api_key and base_id):
        return None
    try:
        if _PyTable: return _PyTable(api_key, base_id, table_name)
        if _PyApi: return _PyApi(api_key).table(base_id, table_name)
    except Exception:
        log.error("Failed to init Airtable table", exc_info=True)
    return None


# -------------------------------
# ENV / CONFIG
# -------------------------------
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
CONTROL_BASE = os.getenv("CAMPAIGN_CONTROL_BASE")
NUMBERS_TABLE = os.getenv("NUMBERS_TABLE", "Numbers")
DAILY_LIMIT_DEFAULT = int(os.getenv("DAILY_LIMIT", "750"))

# Fields
FIELD_NUMBER = os.getenv("NUMBERS_FIELD_NUMBER", "Number")
FIELD_LAST_USED = os.getenv("NUMBERS_FIELD_LAST_USED", "Last Used")
FIELD_REMAINING = os.getenv("NUMBERS_FIELD_REMAINING", "Remaining")
FIELD_SENT = os.getenv("NUMBERS_FIELD_SENT", "Sent")
FIELD_DELIVERED = os.getenv("NUMBERS_FIELD_DELIVERED", "Delivered")
FIELD_FAILED = os.getenv("NUMBERS_FIELD_FAILED", "Failed")
FIELD_COUNT = os.getenv("NUMBERS_FIELD_COUNT", "Count")
FIELD_DAILY_RESET_CAP = os.getenv("NUMBERS_FIELD_DAILY_RESET", "Daily Reset")

SYNONYMS = {
    "Sent": ["Sent Today"],
    "Delivered": ["Delivered Today"],
    "Failed": ["Failed Today"],
    "Remaining": ["Remaining Today"],
}


# -------------------------------
# Helpers
# -------------------------------
def _today_iso() -> str:
    return datetime.now(timezone.utc).date().isoformat()

def _auto_field_map(tbl) -> Dict[str, str]:
    try:
        sample = tbl.all(max_records=1)
        keys = list(sample[0].get("fields", {}).keys()) if sample else []
    except Exception:
        keys = []
    return {re.sub(r"[^a-z0-9]+", "", k.lower()): k for k in keys}

def _existing_only(tbl, patch: Dict[str, Any]) -> Dict[str, Any]:
    amap = _auto_field_map(tbl)
    out = {}
    for k, v in patch.items():
        ak = amap.get(re.sub(r"[^a-z0-9]+", "", k.lower()))
        if ak: out[ak] = v
    return out

def _cap_for_row(f: Dict[str, Any]) -> int:
    try:
        val = f.get(FIELD_DAILY_RESET_CAP)
        return int(val) if val not in (None, "") else DAILY_LIMIT_DEFAULT
    except Exception:
        return DAILY_LIMIT_DEFAULT


# -------------------------------
# Core
# -------------------------------
def reset_daily_stats():
    if not (AIRTABLE_API_KEY and CONTROL_BASE):
        log.warning("⚠️ Missing Airtable credentials.")
        return {"ok": False, "error": "no_airtable"}

    try:
        tbl = _make_table(AIRTABLE_API_KEY, CONTROL_BASE, NUMBERS_TABLE)
        rows = tbl.all() if tbl else []
    except Exception as e:
        log.error("Failed to load Numbers table", exc_info=True)
        return {"ok": False, "error": str(e)}

    today = _today_iso()
    updated, errors = 0, []

    for rec in rows or []:
        f = rec.get("fields", {})
        label = f.get(FIELD_NUMBER) or rec.get("id")
        cap = _cap_for_row(f)

        patch = {
            FIELD_SENT: 0,
            FIELD_DELIVERED: 0,
            FIELD_FAILED: 0,
            FIELD_REMAINING: cap,
            FIELD_COUNT: 0,
            FIELD_LAST_USED: today,
        }

        for k, alts in SYNONYMS.items():
            val = 0 if k != "Remaining" else cap
            for alt in alts:
                patch[alt] = val

        try:
            safe = _existing_only(tbl, patch)
            if not safe:
                log.warning(f"No matching fields for {label}, skipped")
                continue
            tbl.update(rec["id"], safe)
            updated += 1
        except Exception as e:
            errors.append({"number": label, "error": str(e)})
            log.error(f"Failed to reset {label}", exc_info=True)

    # Telemetry
    log_run("RESET_DAILY_STATS", processed=updated, breakdown={"errors": len(errors)})
    log_kpi("NUMBERS_STATS_RESET", updated)
    log.info(f"✅ Reset complete — updated={updated}, errors={len(errors)}")

    return {"ok": True, "date": today, "updated": updated, "errors": errors}


if __name__ == "__main__":
    reset_daily_stats()
