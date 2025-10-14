# sms/health_strict.py
import os
from fastapi import HTTPException
from pyairtable import Table
from datetime import datetime, timezone

def strict_health(
    mode: str = "prospects",
    api_key: str | None = None,
    base_id: str | None = None,
) -> dict:
    valid_modes = {"prospects", "leads", "inbounds"}
    if mode not in valid_modes:
        raise HTTPException(status_code=400, detail={"ok": False, "error": f"Invalid mode '{mode}'"})

    api_key = api_key or os.getenv("AIRTABLE_API_KEY")
    base_id = base_id or os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
    if not api_key or not base_id:
        raise HTTPException(status_code=500, detail={"ok": False, "errors": ["Missing Airtable API key or base ID"]})

    # 👇 table names from env with safe defaults
    PROSPECTS_TABLE = os.getenv("PROSPECTS_TABLE", "Prospects")
    LEADS_TABLE     = os.getenv("LEADS_TABLE", "Leads")
    INBOUNDS_TABLE  = os.getenv("INBOUNDS_TABLE", os.getenv("CONVERSATIONS_TABLE", "Conversations"))

    table_name_map = {
        "prospects": PROSPECTS_TABLE,
        "leads":     LEADS_TABLE,
        "inbounds":  INBOUNDS_TABLE,   # <-- the important bit
    }
    table_name = table_name_map[mode]

    try:
        t = Table(api_key, base_id, table_name)
        t.all(max_records=1)  # probe
        return {"ok": True, "mode": mode, "table": table_name, "timestamp": datetime.now(timezone.utc).isoformat()}
    except Exception as err:
        raise HTTPException(status_code=500, detail={"ok": False, "errors": [f"{table_name}: {err}"]})