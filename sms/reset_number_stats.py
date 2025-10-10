# sms/reset_daily_stats.py
from __future__ import annotations

import os
import traceback
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

# ----------------------------------
# pyairtable compatibility wrapper
# ----------------------------------
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
    Returns a Table-like object exposing .all() / .update() across pyairtable versions.
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

# ----------------------------------
# ENV / CONFIG
# ----------------------------------
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
CONTROL_BASE     = os.getenv("CAMPAIGN_CONTROL_BASE")
NUMBERS_TABLE    = os.getenv("NUMBERS_TABLE", "Numbers")

# Default daily cap if a row does not define "Daily Reset"
DAILY_LIMIT_DEFAULT = int(os.getenv("DAILY_LIMIT", "750"))

# Primary (env-overridable) field names
FIELD_NUMBER        = os.getenv("NUMBERS_FIELD_NUMBER", "Number")
FIELD_MARKET        = os.getenv("NUMBERS_FIELD_MARKET", "Market")
FIELD_LAST_USED     = os.getenv("NUMBERS_FIELD_LAST_USED", "Last Used")
FIELD_COUNT         = os.getenv("NUMBERS_FIELD_COUNT", "Count")              # optional
FIELD_REMAINING     = os.getenv("NUMBERS_FIELD_REMAINING", "Remaining")
FIELD_SENT          = os.getenv("NUMBERS_FIELD_SENT", "Sent")
FIELD_DELIVERED     = os.getenv("NUMBERS_FIELD_DELIVERED", "Delivered")
FIELD_FAILED        = os.getenv("NUMBERS_FIELD_FAILED", "Failed")

# Common synonyms we’ll also try to write (only if those fields exist)
SYNONYMS = {
    "Sent": ["Sent Today"],
    "Delivered": ["Delivered Today"],
    "Failed": ["Failed Today"],
    "Remaining": ["Remaining Today"],
}

# Optional per-row cap field
FIELD_DAILY_RESET_CAP = os.getenv("NUMBERS_FIELD_DAILY_RESET", "Daily Reset")

# ----------------------------------
# Helpers
# ----------------------------------
def _today_iso_date() -> str:
    return datetime.now(timezone.utc).date().isoformat()

def _auto_field_map(tbl) -> Dict[str, str]:
    """normalized(lower, nospace) -> actual Airtable field name present on this table."""
    import re
    def _norm(s: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", s.strip().lower())

    keys: List[str] = []
    try:
        page = tbl.all(max_records=1)
        keys = list((page[0].get("fields", {}) if page else {}).keys())
    except Exception:
        pass
    return {_norm(k): k for k in keys}

def _existing_only(tbl, patch: Dict[str, Any]) -> Dict[str, Any]:
    """Keep only keys that already exist on the table (avoid 422 UNKNOWN_FIELD_NAME)."""
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
        per_row = fields.get(FIELD_DAILY_RESET_CAP)
        if per_row is None or per_row == "":
            return DAILY_LIMIT_DEFAULT
        return int(per_row)
    except Exception:
        return DAILY_LIMIT_DEFAULT

def _numbers_table():
    if not (AIRTABLE_API_KEY and CONTROL_BASE):
        print("⚠️ reset_daily_stats: Missing AIRTABLE_API_KEY or CAMPAIGN_CONTROL_BASE")
        return None
    tbl = _make_table(AIRTABLE_API_KEY, CONTROL_BASE, NUMBERS_TABLE)
    if not tbl:
        print("❌ reset_daily_stats: Failed to init Numbers table client")
    return tbl

# ----------------------------------
# Core
# ----------------------------------
def reset_daily_stats() -> Dict[str, Any]:
    """
    Reset Numbers counters safely:
      - Prefer writing env-configured fields (e.g., Sent/Delivered/Failed/Remaining/Count).
      - Also try common synonyms (e.g., Sent Today, Delivered Today, Failed Today).
      - Set Remaining to each row's 'Daily Reset' if present else DAILY_LIMIT_DEFAULT.
      - Set Last Used to today's date (YYYY-MM-DD).
      - Only updates fields that exist on the table.
    """
    tbl = _numbers_table()
    if not tbl:
        return {"ok": False, "error": "Airtable not configured", "updated": 0, "errors": []}

    today_date = _today_iso_date()
    updated, errors = 0, []

    try:
        rows = tbl.all()
    except Exception as e:
            traceback.print_exc()
            return {"ok": False, "error": f"Failed to fetch Numbers: {e}", "updated": 0, "errors": []}

    for rec in rows or []:
        f = rec.get("fields", {})
        label = f.get(FIELD_NUMBER) or rec.get("id") or "UNKNOWN"

        cap = _cap_for_row(f)

        # Build a generous patch including synonyms; _existing_only will prune.
        patch = {
            FIELD_COUNT: 0,                     # optional "Count"
            FIELD_REMAINING: cap,
            FIELD_SENT: 0,
            FIELD_DELIVERED: 0,
            FIELD_FAILED: 0,
            FIELD_LAST_USED: today_date,        # date works for Date or DateTime fields
        }
        # Synonym writes (will be pruned if fields don’t exist)
        for k, alts in SYNONYMS.items():
            base = {
                "Sent": 0,
                "Delivered": 0,
                "Failed": 0,
                "Remaining": cap,
            }.get(k, None)
            if base is None:
                continue
            for alt in alts:
                patch[alt] = base

        try:
            safe_patch = _existing_only(tbl, patch)
            if not safe_patch:
                print(f"⚠️ reset_daily_stats: No matching fields on row for {label}, skipped")
                continue

            tbl.update(rec["id"], safe_patch)
            updated += 1
            print(f"🔄 Reset stats for {label} → Remaining={cap}")

        except Exception as e:
            err = str(e)
            errors.append({"number": label, "error": err})
            print(f"❌ Failed reset for {label}: {err}")
            traceback.print_exc()

    print(f"✅ Daily number stats reset complete | Updated: {updated} | Errors: {len(errors)}")
    return {"ok": True, "updated": updated, "errors": errors, "date": today_date}

if __name__ == "__main__":
    reset_daily_stats()
