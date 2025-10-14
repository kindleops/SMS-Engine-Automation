import os
from fastapi import HTTPException
from pyairtable import Table
from datetime import datetime, timezone

def strict_health(
    mode: str = "prospects",
    api_key: str | None = None,
    base_id: str | None = None,
) -> dict:
    """
    Safe Airtable health check.
    - Never crashes on missing tables or network errors.
    - Returns HTTP 200 with detailed info instead of raising 500.
    """

    valid_modes = {"prospects", "leads", "inbounds"}
    if mode not in valid_modes:
        # 400 only if bad parameter
        raise HTTPException(status_code=400, detail={"ok": False, "error": f"Invalid mode '{mode}'"})

    api_key = api_key or os.getenv("AIRTABLE_API_KEY")
    base_id = base_id or os.getenv("LEADS_CONVOS_BASE")

    if not api_key or not base_id:
        # Missing creds → return 200 with ok=False (so Render doesn't kill deploy)
        return {
            "ok": False,
            "mode": mode,
            "error": "Missing Airtable API key or base ID",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    try:
        tbl_name = mode.capitalize()  # "Prospects", "Leads", "Inbounds"
        table = Table(api_key, base_id, tbl_name)
        rows = table.all(max_records=1)
        count = len(rows)
        return {
            "ok": True,
            "mode": mode,
            "table": tbl_name,
            "records_found": count,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    except Exception as e:
        # Don’t raise 500s — log and return diagnostic info
        return {
            "ok": False,
            "mode": mode,
            "error": str(e),
            "table": mode.capitalize(),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }