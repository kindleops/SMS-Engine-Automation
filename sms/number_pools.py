import os
import random
from datetime import datetime, timezone

try:
    from pyairtable import Table
except ImportError:
    Table = None

# --- Env Config ---
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
CONTROL_BASE     = os.getenv("CAMPAIGN_CONTROL_BASE")
NUMBERS_TABLE    = os.getenv("NUMBERS_TABLE", "Numbers")
DAILY_LIMIT      = int(os.getenv("DAILY_LIMIT", "750"))

# --- Field Names ---
FIELD_NUMBER          = "Number"
FIELD_MARKET          = "Market"
FIELD_LAST_USED       = "Last Used"

FIELD_SENT_TODAY      = "Sent Today"
FIELD_DELIVERED_TODAY = "Delivered Today"
FIELD_FAILED_TODAY    = "Failed Today"
FIELD_OPTOUTS_TODAY   = "Opt-Outs Today"

FIELD_SENT_TOTAL      = "Sent Total"
FIELD_DELIVERED_TOTAL = "Delivered Total"
FIELD_FAILED_TOTAL    = "Failed Total"
FIELD_OPTOUTS_TOTAL   = "Opt-Outs Total"

FIELD_REMAINING       = "Remaining"


# --- Helpers ---
def _today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


# --- Airtable Mode ---
if AIRTABLE_API_KEY and CONTROL_BASE and Table:

    numbers_tbl = Table(AIRTABLE_API_KEY, CONTROL_BASE, NUMBERS_TABLE)

    def _reset_daily_if_needed(record: dict) -> dict:
        """Reset counters if Last Used != today."""
        today = _today()
        last_used = record["fields"].get(FIELD_LAST_USED)
        if not last_used or last_used[:10] != today:
            updates = {
                FIELD_SENT_TODAY: 0,
                FIELD_DELIVERED_TODAY: 0,
                FIELD_FAILED_TODAY: 0,
                FIELD_OPTOUTS_TODAY: 0,
                FIELD_LAST_USED: today,
                FIELD_REMAINING: DAILY_LIMIT,
            }
            numbers_tbl.update(record["id"], updates)
            record["fields"].update(updates)
        return record

    def _increment_field(record_id: str, field: str, total_field: str, inc: int = 1):
        rec = numbers_tbl.get(record_id)
        rec = _reset_daily_if_needed(rec)

        daily_val = rec["fields"].get(field, 0) + inc
        total_val = rec["fields"].get(total_field, 0) + inc

        updates = {field: daily_val, total_field: total_val}
        if field == FIELD_SENT_TODAY:
            updates[FIELD_REMAINING] = max(0, DAILY_LIMIT - daily_val)
            updates[FIELD_LAST_USED] = _today()

        numbers_tbl.update(record_id, updates)

    def _find_record(number: str) -> dict:
        recs = numbers_tbl.all(formula=f"{{{FIELD_NUMBER}}}='{number}'")
        if not recs:
            raise RuntimeError(f"🚨 Number {number} not found in Airtable")
        return recs[0]

    # --- Public Increment APIs ---
    def increment_sent(number: str):      _increment_field(_find_record(number)["id"], FIELD_SENT_TODAY, FIELD_SENT_TOTAL)
    def increment_delivered(number: str): _increment_field(_find_record(number)["id"], FIELD_DELIVERED_TODAY, FIELD_DELIVERED_TOTAL)
    def increment_failed(number: str):    _increment_field(_find_record(number)["id"], FIELD_FAILED_TODAY, FIELD_FAILED_TOTAL)
    def increment_opt_out(number: str):   _increment_field(_find_record(number)["id"], FIELD_OPTOUTS_TODAY, FIELD_OPTOUTS_TOTAL)

    # --- Rotation Logic ---
    def get_from_number(market: str) -> str:
        """Select the healthiest number for a given market (quota-aware)."""
        formula = f"SEARCH(LOWER('{market}'), LOWER({{{FIELD_MARKET}}}))"
        recs = numbers_tbl.all(formula=formula)
        if not recs:
            raise RuntimeError(f"🚨 No numbers found for market '{market}'")

        recs = [_reset_daily_if_needed(r) for r in recs]
        available = [r for r in recs if r["fields"].get(FIELD_REMAINING, DAILY_LIMIT) > 0]
        if not available:
            raise RuntimeError(f"🚨 All numbers in {market} exhausted (limit {DAILY_LIMIT})")

        # sort by Sent Today, pick among least-used with random tie-break
        available.sort(key=lambda r: r["fields"].get(FIELD_SENT_TODAY, 0))
        least_used = available[0]["fields"].get(FIELD_SENT_TODAY, 0)
        candidates = [r for r in available if r["fields"].get(FIELD_SENT_TODAY, 0) == least_used]
        choice = random.choice(candidates)
        return choice["fields"][FIELD_NUMBER]


# --- Mock/Fallback Mode ---
else:
    print("⚠️ No Airtable config detected → using MOCK number pool")

    def increment_sent(number: str):      print(f"[MOCK] increment_sent({number})")
    def increment_delivered(number: str): print(f"[MOCK] increment_delivered({number})")
    def increment_failed(number: str):    print(f"[MOCK] increment_failed({number})")
    def increment_opt_out(number: str):   print(f"[MOCK] increment_opt_out({number})")

    def get_from_number(market: str) -> str:
        dummy = f"+1999999{random.randint(1000,9999)}"
        print(f"[MOCK] get_from_number({market}) → {dummy}")
        return dummy