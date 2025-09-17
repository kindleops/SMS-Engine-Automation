import os
from datetime import datetime, timezone
from pyairtable import Table

# --- Airtable Setup ---
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
CONTROL_BASE = os.getenv("CAMPAIGN_CONTROL_BASE") or os.getenv("AIRTABLE_CAMPAIGN_CONTROL_BASE_ID")
NUMBERS_TABLE = os.getenv("NUMBERS_TABLE", "Numbers")

# Field names (configurable to avoid UNKNOWN_FIELD_NAME)
FIELD_NUMBER    = os.getenv("NUMBERS_FIELD_NUMBER", "Number")
FIELD_MARKET    = os.getenv("NUMBERS_FIELD_MARKET", "Market")
FIELD_LAST_USED = os.getenv("NUMBERS_FIELD_LAST_USED", "Last Used")
FIELD_COUNT     = os.getenv("NUMBERS_FIELD_COUNT", "Count")
FIELD_REMAINING = os.getenv("NUMBERS_FIELD_REMAINING", "Remaining")

if not AIRTABLE_API_KEY or not CONTROL_BASE:
    raise RuntimeError("⚠️ Missing Airtable config for Numbers table")

numbers_tbl = Table(AIRTABLE_API_KEY, CONTROL_BASE, NUMBERS_TABLE)

# --- Static Pools (fallback) ---
MARKET_NUMBERS = {
    "houston": ["+17135551234", "+12815552345", "+18325553456", "+13465554567"],
    "phoenix": ["+16025551234", "+14805552345", "+16235553456"],
    "tampa":   ["+18135551234", "+17275552345", "+18135553456"],
}

rotation_index = {m: 0 for m in MARKET_NUMBERS}
quota_tracker = {}
DAILY_LIMIT = int(os.getenv("DAILY_LIMIT", "750"))

# --- Helpers ---
def _today() -> str:
    return datetime.now(timezone.utc).date().isoformat()

def _reset_if_new_day(number: str, market: str):
    today = _today()
    if number not in quota_tracker or quota_tracker[number]["date"] != today:
        quota_tracker[number] = {"date": today, "count": 0}

    try:
        formula = (
            f"AND({{{FIELD_NUMBER}}}='{number}', "
            f"DATETIME_FORMAT({{{FIELD_LAST_USED}}}, 'YYYY-MM-DD')='{today}')"
        )
        records = numbers_tbl.all(formula=formula)
        if records:
            rec = records[0]
            count = rec["fields"].get(FIELD_COUNT, 0)
            quota_tracker[number]["count"] = count
            remaining = DAILY_LIMIT - count
            numbers_tbl.update(rec["id"], {FIELD_REMAINING: remaining})
        else:
            rec = numbers_tbl.create({
                FIELD_NUMBER: number,
                FIELD_MARKET: market,
                FIELD_LAST_USED: today,
                FIELD_COUNT: 0,
                FIELD_REMAINING: DAILY_LIMIT,
            })
            quota_tracker[number]["count"] = 0
    except Exception as e:
        print(f"⚠️ Failed to sync Airtable for {number}: {e}")

def _update_airtable_count(number: str):
    today = quota_tracker[number]["date"]
    count = quota_tracker[number]["count"]
    remaining = DAILY_LIMIT - count
    try:
        formula = (
            f"AND({{{FIELD_NUMBER}}}='{number}', "
            f"DATETIME_FORMAT({{{FIELD_LAST_USED}}}, 'YYYY-MM-DD')='{today}')"
        )
        recs = numbers_tbl.all(formula=formula)
        if recs:
            numbers_tbl.update(recs[0]["id"], {FIELD_COUNT: count, FIELD_REMAINING: remaining})
        else:
            # Fallback: re-create if Airtable row missing
            numbers_tbl.create({
                FIELD_NUMBER: number,
                FIELD_MARKET: "UNKNOWN",
                FIELD_LAST_USED: today,
                FIELD_COUNT: count,
                FIELD_REMAINING: remaining,
            })
    except Exception as e:
        print(f"⚠️ Failed to update Airtable for {number}: {e}")

# --- Public API ---
def get_next_number(market: str) -> dict:
    """Rotate numbers for a market, enforce quotas, sync to Airtable."""
    m = market.lower()
    numbers = MARKET_NUMBERS.get(m)
    if not numbers:
        raise ValueError(f"🚨 No number pool for market: {market}")

    for _ in range(len(numbers)):
        idx = rotation_index[m]
        number = numbers[idx]
        rotation_index[m] = (idx + 1) % len(numbers)

        _reset_if_new_day(number, m)

        if quota_tracker[number]["count"] < DAILY_LIMIT:
            quota_tracker[number]["count"] += 1
            _update_airtable_count(number)

            count = quota_tracker[number]["count"]
            remaining = DAILY_LIMIT - count
            print(f"📞 {m.upper()} → {number} | Used {count}/{DAILY_LIMIT}, Remaining {remaining}")
            return {"market": m, "number": number, "count": count, "remaining": remaining}

    raise RuntimeError(f"🚨 All numbers in {market} hit quota ({DAILY_LIMIT})")