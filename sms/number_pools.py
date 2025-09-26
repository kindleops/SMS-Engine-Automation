# sms/number_pools.py
import os
import random
from datetime import datetime, timezone
from functools import lru_cache

try:
    from pyairtable import Table
except ImportError:
    Table = None

# --- Env Config ---
DAILY_LIMIT = int(os.getenv("DAILY_LIMIT", "750"))

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


def _today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


# --- Lazy Airtable Client ---
@lru_cache(maxsize=1)
def get_numbers_tbl():
    """Return Airtable Numbers table client, or None if not configured."""
    api_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("CAMPAIGN_CONTROL_BASE")
    table_name = os.getenv("NUMBERS_TABLE", "Numbers")

    if not (api_key and base_id and Table):
        print("⚠️ NumberPools: No Airtable config → using MOCK mode")
        return None
    try:
        return Table(api_key, base_id, table_name)
    except Exception as e:
        print(f"❌ NumberPools: failed to init Airtable table → {e}")
        return None


def _reset_daily_if_needed(record: dict) -> dict:
    tbl = get_numbers_tbl()
    if not tbl:
        return record
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
        tbl.update(record["id"], updates)
        record["fields"].update(updates)
    return record


def _increment_field(record_id: str, field: str, total_field: str, inc: int = 1):
    tbl = get_numbers_tbl()
    if not tbl:
        print(f"[MOCK] _increment_field({record_id}, {field}, {total_field}, +{inc})")
        return

    rec = tbl.get(record_id)
    rec = _reset_daily_if_needed(rec)

    daily_val = rec["fields"].get(field, 0) + inc
    total_val = rec["fields"].get(total_field, 0) + inc

    updates = {field: daily_val, total_field: total_val}
    if field == FIELD_SENT_TODAY:
        updates[FIELD_REMAINING] = max(0, DAILY_LIMIT - daily_val)
        updates[FIELD_LAST_USED] = _today()

    tbl.update(record_id, updates)


def _find_record(number: str) -> dict:
    tbl = get_numbers_tbl()
    if not tbl:
        print(f"[MOCK] _find_record({number}) → returning fake record")
        return {"id": "mock_id", "fields": {FIELD_NUMBER: number}}
    recs = tbl.all(formula=f"{{{FIELD_NUMBER}}}='{number}'")
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
    tbl = get_numbers_tbl()
    if not tbl:
        dummy = f"+1999999{random.randint(1000,9999)}"
        print(f"[MOCK] get_from_number({market}) → {dummy}")
        return dummy

    formula = f"SEARCH(LOWER('{market}'), LOWER({{{FIELD_MARKET}}}))"
    recs = tbl.all(formula=formula)
    if not recs:
        raise RuntimeError(f"🚨 No numbers found for market '{market}'")

    recs = [_reset_daily_if_needed(r) for r in recs]
    available = [r for r in recs if r["fields"].get(FIELD_REMAINING, DAILY_LIMIT) > 0]
    if not available:
        raise RuntimeError(f"🚨 All numbers in {market} exhausted (limit {DAILY_LIMIT})")

    available.sort(key=lambda r: r["fields"].get(FIELD_SENT_TODAY, 0))
    least_used = available[0]["fields"].get(FIELD_SENT_TODAY, 0)
    candidates = [r for r in available if r["fields"].get(FIELD_SENT_TODAY, 0) == least_used]
    choice = random.choice(candidates)
    return choice["fields"][FIELD_NUMBER]