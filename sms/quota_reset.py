# sms/quota_reset.py
import os
from datetime import datetime, timezone
from pyairtable import Table

# ENV
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
CAMPAIGN_CONTROL_BASE = (
    os.getenv("CAMPAIGN_CONTROL_BASE")
    or os.getenv("AIRTABLE_CAMPAIGN_CONTROL_BASE_ID")
)
NUMBERS_TABLE = os.getenv("NUMBERS_TABLE", "Numbers")

DAILY_LIMIT = int(os.getenv("DAILY_LIMIT", "750"))

def _iso_date():
    # Store date only (Airtable “Date” field recommended as Date type, time disabled)
    return datetime.now(timezone.utc).date().isoformat()

def reset_daily_quotas():
    """
    For every distinct Number in Numbers table, ensure there's a row for TODAY.
    If today's row is missing, create it with Count=0, Remaining=DAILY_LIMIT.
    If it exists, reset Count to 0 and Remaining back to DAILY_LIMIT.
    """
    if not (AIRTABLE_API_KEY and CAMPAIGN_CONTROL_BASE):
        return {"ok": False, "error": "Airtable env missing"}

    tbl = Table(AIRTABLE_API_KEY, CAMPAIGN_CONTROL_BASE, NUMBERS_TABLE)
    today = _iso_date()

    # 1) Pull all rows (you can scope this to a view like “Active Numbers” if you prefer)
    rows = tbl.all()
    # Build a set of known numbers (and remember their latest Market if present)
    by_number = {}
    for r in rows:
        f = r.get("fields", {})
        num = f.get("Number")
        if not num:
            continue
        # Prefer a specific “Market” if you have it
        if num not in by_number:
            by_number[num] = f.get("Market")

    created = 0
    updated = 0

    for number, market in by_number.items():
        # 2) Does a row for TODAY already exist for this number?
        # NOTE: filterByFormula must escape quotes; we assume Number is a plain string like +18325551234
        formula = f"AND({{Number}}='{number}', {{Date}}='{today}')"
        today_rows = tbl.all(formula=formula)

        if not today_rows:
            # Create fresh daily row
            tbl.create({
                "Number": number,
                "Market": market,
                "Date": today,
                "Count": 0,
                "Remaining": DAILY_LIMIT,
            })
            created += 1
        else:
            # Normalize today’s row to Count=0, Remaining=limit
            rec = today_rows[0]
            tbl.update(rec["id"], {"Count": 0, "Remaining": DAILY_LIMIT})
            updated += 1

    return {"ok": True, "created": created, "updated": updated, "date": today}