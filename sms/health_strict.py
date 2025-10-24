# sms/health_strict.py
"""
Strict Health Check
-------------------
Ensures Airtable connectivity for each engine mode.
Integrates with datastore for unified health status.
"""

import traceback
from datetime import datetime, timezone
from fastapi import HTTPException
from sms.datastore import CONNECTOR


def strict_health(mode: str = "prospects") -> dict:
    valid_modes = {"prospects", "leads", "inbounds"}
    if mode not in valid_modes:
        raise HTTPException(status_code=400, detail={"ok": False, "error": f"Invalid mode '{mode}'"})

    table_map = {
        "prospects": CONNECTOR.prospects,
        "leads": CONNECTOR.leads,
        "inbounds": CONNECTOR.conversations,
    }
    tbl_func = table_map[mode]
    try:
        handle = tbl_func()
        table = getattr(handle, "table", None)
        table_name = getattr(handle, "table_name", "unknown") if handle else "unknown"
        if not table:
            raise ValueError("Table unavailable or misconfigured")
        try:
            table.all(max_records=1)
        except Exception as err:
            return {
                "ok": False,
                "mode": mode,
                "table": table_name,
                "error": str(err),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        return {
            "ok": True,
            "mode": mode,
            "table": table_name,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    except HTTPException:
        raise
    except Exception as err:
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail={"ok": False, "errors": [f"{mode} health check failed: {err}"]},
        )
