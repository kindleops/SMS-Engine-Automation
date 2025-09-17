import os
from datetime import datetime, timezone
from pyairtable import Table
import traceback

# --- ENV CONFIG ---
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
CONTROL_BASE = os.getenv("CAMPAIGN_CONTROL_BASE") or os.getenv("AIRTABLE_CAMPAIGN_CONTROL_BASE_ID")
NUMBERS_TABLE = os.getenv("NUMBERS_TABLE", "Numbers")

DAILY_LIMIT = int(os.getenv("DAILY_LIMIT", "750"))

# --- Helpers ---
def _iso_date():
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
    Ensure each number has 1 quota row for today:
      - If missing → create row (Count=0, Remaining=DAILY_LIMIT).
      - If exists → reset Count/Remaining.
    """
    tbl = _init_table()
    if not tbl:
        return {"ok": False, "error": "Airtable env missing"}

    today = _iso_date()
    created, updated = 0, 0

    try:
        rows = tbl.all()
        numbers_seen = {}

        # Build {number: (id, market)} for dedupe
        for r in rows:
            f = r.get("fields", {})
            num = f.get("Number")
            if num and num not in numbers_seen:
                numbers_seen[num] = f.get("Market")

        for number, market in numbers_seen.items():
            formula = f"AND({{Number}}='{number}', {{Last Used}}='{today}')"
            today_rows = tbl.all(formula=formula)

            if not today_rows:
                try:
                    tbl.create({
                        "Name": f"{number} - {today}",  # primary field unique
                        "Number": number,
                        "Market": market,
                        "Last Used": today,
                        "Count": 0,
                        "Remaining": DAILY_LIMIT,
                    })
                    created += 1
                except Exception as e:
                    print(f"❌ Failed to create row for {number}: {e}")
                    traceback.print_exc()
            else:
                try:
                    rec = today_rows[0]
                    tbl.update(rec["id"], {
                        "Count": 0,
                        "Remaining": DAILY_LIMIT,
                        "Last Used": today,
                    })
                    updated += 1
                except Exception as e:
                    print(f"❌ Failed to update row for {number}: {e}")
                    traceback.print_exc()

        return {"ok": True, "date": today, "created": created, "updated": updated}

    except Exception as e:
        print("❌ Error in reset_daily_quotas:", e)
        traceback.print_exc()
        return {"ok": False, "error": str(e), "date": today, "created": created, "updated": updated}