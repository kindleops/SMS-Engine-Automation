# sms/number_pools.py
from datetime import datetime
from pyairtable import Table
import os

# --- Airtable Setup ---
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
PERF_BASE = os.getenv("AIRTABLE_PERFORMANCE_BASE_ID") or os.getenv("PERFORMANCE_BASE")
NUMBERS_TABLE = "Numbers"

numbers_tbl = Table(AIRTABLE_API_KEY, PERF_BASE, NUMBERS_TABLE)

MARKET_NUMBERS = {
    "houston": ["+17135551234", "+12815552345", "+18325553456", "+13465554567"],
    "phoenix": ["+16025551234", "+14805552345", "+16235553456"],
    "tampa": ["+18135551234", "+17275552345", "+18135553456"],
}

rotation_index = {m: 0 for m in MARKET_NUMBERS}
quota_tracker = {}
DAILY_LIMIT = 750

def _reset_if_new_day(number: str, market: str):
    today = datetime.now().date().isoformat()
    if number not in quota_tracker:
        quota_tracker[number] = {"date": today, "count": 0}
    elif quota_tracker[number]["date"] != today:
        quota_tracker[number] = {"date": today, "count": 0}

    # Sync with Airtable
    try:
        records = numbers_tbl.all(formula=f"AND(Number='{number}', Date='{today}')")
        if records:
            rec = records[0]
            quota_tracker[number]["count"] = rec["fields"].get("Count", 0)
        else:
            rec = numbers_tbl.create({"Number": number, "Market": market, "Date": today, "Count": 0})
            quota_tracker[number]["count"] = 0
    except Exception as e:
        print(f"⚠️ Failed to sync Airtable for {number}: {e}")

def _update_airtable_count(number: str):
    today = quota_tracker[number]["date"]
    count = quota_tracker[number]["count"]
    try:
        recs = numbers_tbl.all(formula=f"AND(Number='{number}', Date='{today}')")
        if recs:
            numbers_tbl.update(recs[0]["id"], {"Count": count})
    except Exception as e:
        print(f"⚠️ Failed to update Airtable for {number}: {e}")

def get_next_number(market: str) -> str:
    numbers = MARKET_NUMBERS.get(market.lower())
    if not numbers:
        raise ValueError(f"No number pool for market: {market}")

    for _ in range(len(numbers)):
        idx = rotation_index[market]
        number = numbers[idx]
        rotation_index[market] = (idx + 1) % len(numbers)

        _reset_if_new_day(number, market)
        if quota_tracker[number]["count"] < DAILY_LIMIT:
            quota_tracker[number]["count"] += 1
            _update_airtable_count(number)
            return number

    raise RuntimeError(f"🚨 All numbers in {market} hit quota ({DAILY_LIMIT})")

# sms/number_pools.py (patched)

DAILY_LIMIT = 750

def _reset_if_new_day(number: str, market: str):
    today = datetime.now().date().isoformat()
    if number not in quota_tracker:
        quota_tracker[number] = {"date": today, "count": 0}
    elif quota_tracker[number]["date"] != today:
        quota_tracker[number] = {"date": today, "count": 0}

    # Sync with Airtable
    try:
        records = numbers_tbl.all(formula=f"AND(Number='{number}', Date='{today}')")
        if records:
            rec = records[0]
            quota_tracker[number]["count"] = rec["fields"].get("Count", 0)
            remaining = DAILY_LIMIT - quota_tracker[number]["count"]
            numbers_tbl.update(rec["id"], {"Remaining": remaining})
        else:
            rec = numbers_tbl.create({
                "Number": number,
                "Market": market,
                "Date": today,
                "Count": 0,
                "Remaining": DAILY_LIMIT
            })
            quota_tracker[number]["count"] = 0
    except Exception as e:
        print(f"⚠️ Failed to sync Airtable for {number}: {e}")

def _update_airtable_count(number: str):
    today = quota_tracker[number]["date"]
    count = quota_tracker[number]["count"]
    remaining = DAILY_LIMIT - count
    try:
        recs = numbers_tbl.all(formula=f"AND(Number='{number}', Date='{today}')")
        if recs:
            numbers_tbl.update(recs[0]["id"], {"Count": count, "Remaining": remaining})
    except Exception as e:
        print(f"⚠️ Failed to update Airtable for {number}: {e}")