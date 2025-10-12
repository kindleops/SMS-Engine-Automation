import os, traceback
from datetime import datetime, timezone
from pyairtable import Table


def tznow_iso():
    return datetime.now(timezone.utc).isoformat()


def get_table(base_env: str, table_name: str) -> Table | None:
    key = os.getenv("AIRTABLE_API_KEY")
    base = os.getenv(base_env)
    if not key or not base:
        print(f"⚠️ Missing Airtable config → {base_env}")
        return None
    try:
        return Table(key, base, table_name)
    except Exception:
        traceback.print_exc()
        return None


def remap_existing_only(table: Table, payload: dict) -> dict:
    try:
        one = table.all(max_records=1)
        keys = set(one[0].get("fields", {}).keys()) if one else set()
    except Exception:
        keys = set()
    if not keys:  # optimistic if table is empty
        return payload
    return {k: v for k, v in payload.items() if k in keys}
