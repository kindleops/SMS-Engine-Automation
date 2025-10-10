# sms/quota_reset.py
from __future__ import annotations

import os
import traceback
from datetime import datetime, timezone
from typing import Optional, Dict, Any

# -------------------------------
# pyairtable compatibility layer
# -------------------------------
_PyTable = None
_PyApi   = None
try:
    from pyairtable import Table as _PyTable  # v1 style
except Exception:
    _PyTable = None
try:
    from pyairtable import Api as _PyApi      # v2 style
except Exception:
    _PyApi = None

def _make_table(api_key: Optional[str], base_id: Optional[str], table_name: str):
    """
    Works with both pyairtable styles. Returns a Table-like object
    exposing .all(...), .get(...), .create(...), .update(...), or None.
    """
    if not (api_key and base_id):
        return None
    try:
        if _PyTable:
            return _PyTable(api_key, base_id, table_name)
        if _PyApi:
            return _PyApi(api_key).table(base_id, table_name)
    except Exception:
        traceback.print_exc()
    return None

# -------------------------------
# ENV / CONFIG
# -------------------------------
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
CONTROL_BASE     = os.getenv("CAMPAIGN_CONTROL_BASE")
NUMBERS_TABLE    = os.getenv("NUMBERS_TABLE", "Numbers")

# Default daily cap if the row doesn’t have its own “Daily Reset” value
DAILY_LIMIT_DEFAULT = int(os.getenv("DAILY_LIMIT", "750"))

# Field names we’ll *attempt* to write if they exist
F_SENT_TODAY      = "Sent Today"
F_DELIV_TODAY     = "Delivered Today"
F_FAIL_TODAY      = "Failed Today"
F_FAILED_TODAY    = "Failed Today"
F_OPTOUT_TODAY    = "Opt-Outs Today"
F_REMAINING       = "Remaining"
F_LAST_USED       = "Last Used"
F_DAILY_RESET_CAP = "Daily Reset"
F_NUMBER          = "Number"

# -------------------------------
# Helpers
# -------------------------------
def _today_date_str() -> str:
    return datetime.now(timezone.utc).date().isoformat()

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _auto_field_map(tbl) -> Dict[str, str]:
    """normalized(lower, nospace) -> actual Airtable field name present on this table."""
    import re
    def _norm(s: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", s.strip().lower())

    keys = []
    try:
        page = tbl.all(max_records=1)
        keys = list((page[0].get("fields", {}) if page else {}).keys())
    except Exception:
        pass
    return {_norm(k): k for k in keys}

def _existing_only(tbl, patch: Dict[str, Any]) -> Dict[str, Any]:
    """Keep only keys that already exist on the table (prevents 422 UNKNOWN_FIELD_NAME)."""
    import re
    amap = _auto_field_map(tbl)
    out: Dict[str, Any] = {}
    for k, v in patch.items():
        nk = re.sub(r"[^a-z0-9]+", "", k.strip().lower())
        ak = amap.get(nk)
        if ak:
            out[ak] = v
    return out

def _cap_for_row(fields: Dict[str, Any]) -> int:
    try:
        per_row = fields.get(F_DAILY_RESET_CAP)
        if per_row is None or per_row == "":
            return DAILY_LIMIT_DEFAULT
        return int(per_row)
    except Exception:
        return DAILY_LIMIT_DEFAULT

def _init_numbers_table():
    if not (AIRTABLE_API_KEY and CONTROL_BASE):
        print("⚠️ quota_reset: Missing Airtable env (AIRTABLE_API_KEY / CAMPAIGN_CONTROL_BASE)")
        return None
    tbl = _make_table(AIRTABLE_API_KEY, CONTROL_BASE, NUMBERS_TABLE)
    if not tbl:
        print("❌ quota_reset: Failed to init Numbers table client")
    return tbl

# -------------------------------
# Public: reset daily quotas
# -------------------------------
def reset_daily_quotas():
    """
    Reset daily counters for every number row (only touching fields that exist):
      - Sent Today, Delivered Today, Failed Today, Opt-Outs Today → 0
      - Remaining → row's 'Daily Reset' (or DAILY_LIMIT_DEFAULT)
      - Last Used → today (date or timestamp accepted)
    Returns a summary dict.
    """
    tbl = _init_numbers_table()
    if not tbl:
        return {"ok": False, "error": "Airtable not configured"}

    today_date = _today_date_str()
    updated, errors = 0, []

    try:
        rows = tbl.all()
    except Exception as e:
        traceback.print_exc()
        return {"ok": False, "error": f"Failed to read Numbers: {e}", "updated": 0, "errors": []}

    for r in rows or []:
        f = r.get("fields", {})
        number_label = f.get(F_NUMBER) or r.get("id") or "UNKNOWN"

        # Determine per-row cap
        cap = _cap_for_row(f)

        # Prepare patch; we’ll filter to existing fields right before update
        patch = {
            F_SENT_TODAY: 0,
            F_DELIV_TODAY: 0,
            F_FAILED_TODAY: 0,
            F_OPTOUT_TODAY: 0,
            F_REMAINING: cap,
            # Prefer date-only if your field is a Date; ISO timestamp also works for DateTime fields
            F_LAST_USED: today_date,
        }

        try:
            safe_patch = _existing_only(tbl, patch)
            if not safe_patch:
                # Table has none of the expected fields; skip quietly
                print(f"⚠️ quota_reset: No matching fields on row for {number_label}, skipped")
                continue

            tbl.update(r["id"], safe_patch)
            updated += 1
            print(f"🔄 Reset quota for {number_label} → Remaining={cap}")

        except Exception as e:
            err = str(e)
            errors.append({"number": number_label, "error": err})
            print(f"❌ Failed to reset {number_label}: {err}")
            traceback.print_exc()

    summary = {
        "ok": True,
        "date": today_date,
        "updated": updated,
        "errors": errors,
    }
    print(f"✅ Reset complete | Date: {today_date} | Updated: {updated} | Errors: {len(errors)}")
    return summary

if __name__ == "__main__":
    reset_daily_quotas()
