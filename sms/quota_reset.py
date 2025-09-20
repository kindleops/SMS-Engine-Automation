import os
from datetime import datetime, timezone
from pyairtable import Table
import traceback

# --- ENV CONFIG ---
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
CONTROL_BASE     = os.getenv("CAMPAIGN_CONTROL_BASE")
NUMBERS_TABLE    = os.getenv("NUMBERS_TABLE", "Numbers")

DAILY_LIMIT = int(os.getenv("DAILY_LIMIT", "750"))

# --- Fields ---
FIELD_NUMBER          = "Number"
FIELD_LAST_USED       = "Last Used"
FIELD_SENT_TODAY      = "Sent Today"
FIELD_DELIVERED_TODAY = "Delivered Today"
FIELD_FAILED_TODAY    = "Failed Today"
FIELD_OPTOUTS_TODAY   = "Opt-Outs Today"
FIELD_REMAINING       = "Remaining"

# --- Helpers ---
def _today():
    return datetime.now(timezone.utc).date().isoformat()

def _init_table():
    if not (AIRTABLE_API_KEY and CONTROL_BASE):
        print("⚠️ Missing Airtable env for Numbers table")
        return None
    try:
        return Table(AIRTABLE_API_KEY, CONTROL_BASE, NUMBERS_TABLE)
    except Exception:
        print("❌ Failed to init Numbers table")
        traceback.print_exc()
        return None

# --- Reset Logic ---
def reset_daily_quotas():
    """
    Reset all daily counters for every number row:
      - Sent Today, Delivered Today, Failed Today, Opt-Outs Today → 0
      - Remaining → DAILY_LIMIT
      - Last Used → today
    """
    tbl = _init_table()
    if not tbl:
        return {"ok": False, "error": "Airtable env missing"}

    today = _today()
    updated, errors = 0, []

    try:
        rows = tbl.all()
        for r in rows:
            try:
                tbl.update(r["id"], {
                    FIELD_SENT_TODAY: 0,
                    FIELD_DELIVERED_TODAY: 0,
                    FIELD_FAILED_TODAY: 0,
                    FIELD_OPTOUTS_TODAY: 0,
                    FIELD_REMAINING: DAILY_LIMIT,
                    FIELD_LAST_USED: today,
                })
                updated += 1
            except Exception as e:
                errors.append({"number": r.get("fields", {}).get(FIELD_NUMBER), "error": str(e)})
                print(f"❌ Failed to reset {r.get('fields', {}).get(FIELD_NUMBER)}: {e}")
                traceback.print_exc()

        return {"ok": True, "date": today, "updated": updated, "errors": errors}

    except Exception as e:
        print("❌ Error in reset_daily_quotas:", e)
        traceback.print_exc()
        return {"ok": False, "error": str(e), "date": today, "updated": updated, "errors": errors}