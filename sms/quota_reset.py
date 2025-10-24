"""
🔄 quota_reset.py (v3.1 — Telemetry Edition)
──────────────────────────────────────────────
Resets all DID quotas daily.
Adds:
 - log_run + log_kpi telemetry
 - structured logging
 - graceful error handling
"""

from __future__ import annotations
import os, traceback, re
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from sms.runtime import get_logger

log = get_logger("quota_reset")

try:
    from sms.logger import log_run
except Exception:

    def log_run(*_a, **_k):
        pass


try:
    from sms.kpi_logger import log_kpi
except Exception:

    def log_kpi(*_a, **_k):
        pass


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
        if _PyTable:
            return _PyTable(api_key, base_id, table_name)
        if _PyApi:
            return _PyApi(api_key).table(base_id, table_name)
    except Exception:
        log.error("Failed to init Airtable table", exc_info=True)
    return None


# -------------------------------
# ENV
# -------------------------------
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
CONTROL_BASE = os.getenv("CAMPAIGN_CONTROL_BASE")
NUMBERS_TABLE = os.getenv("NUMBERS_TABLE", "Numbers")
DAILY_LIMIT_DEFAULT = int(os.getenv("DAILY_LIMIT", "750"))

# Field names
F_SENT_TODAY = "Sent Today"
F_DELIV_TODAY = "Delivered Today"
F_FAILED_TODAY = "Failed Today"
F_OPTOUT_TODAY = "Opt-Outs Today"
F_REMAINING = "Remaining"
F_LAST_USED = "Last Used"
F_DAILY_RESET_CAP = "Daily Reset"
F_NUMBER = "Number"


# -------------------------------
# Helpers
# -------------------------------
def _today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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
        if ak:
            out[ak] = v
    return out


def _cap_for_row(f: Dict[str, Any]) -> int:
    try:
        cap = f.get(F_DAILY_RESET_CAP)
        return int(cap) if cap not in (None, "") else DAILY_LIMIT_DEFAULT
    except Exception:
        return DAILY_LIMIT_DEFAULT


def _init_table():
    if not (AIRTABLE_API_KEY and CONTROL_BASE):
        log.warning("⚠️ Missing Airtable credentials for quota_reset")
        return None
    return _make_table(AIRTABLE_API_KEY, CONTROL_BASE, NUMBERS_TABLE)


# -------------------------------
# Core
# -------------------------------
def reset_daily_quotas():
    tbl = _init_table()
    if not tbl:
        return {"ok": False, "error": "no_airtable"}

    today = _today()
    updated, errors = 0, []

    try:
        rows = tbl.all()
    except Exception as e:
        log.error("Failed to read Numbers table", exc_info=True)
        return {"ok": False, "error": str(e), "updated": 0}

    for r in rows or []:
        f = r.get("fields", {})
        number_label = f.get(F_NUMBER) or r.get("id")
        cap = _cap_for_row(f)
        patch = {
            F_SENT_TODAY: 0,
            F_DELIV_TODAY: 0,
            F_FAILED_TODAY: 0,
            F_OPTOUT_TODAY: 0,
            F_REMAINING: cap,
            F_LAST_USED: today,
        }

        try:
            safe_patch = _existing_only(tbl, patch)
            if not safe_patch:
                log.warning(f"No matching fields for {number_label}, skipped")
                continue
            tbl.update(r["id"], safe_patch)
            updated += 1
        except Exception as e:
            errors.append({"number": number_label, "error": str(e)})
            log.error(f"Failed to reset {number_label}", exc_info=True)

    # Telemetry
    log_run("QUOTA_RESET", processed=updated, breakdown={"errors": len(errors)})
    log_kpi("NUMBERS_RESET", updated)
    log.info(f"✅ Reset complete — updated={updated}, errors={len(errors)}")

    return {"ok": True, "date": today, "updated": updated, "errors": errors}


if __name__ == "__main__":
    reset_daily_quotas()
