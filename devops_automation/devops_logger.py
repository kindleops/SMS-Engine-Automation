import os, traceback
from .common import tznow_iso, get_table, remap_existing_only

DEVOPS_BASE = "DEVOPS_BASE"

def log_devops(event_type: str, service: str, payload, status="OK", severity="Info"):
    tbl = get_table(DEVOPS_BASE, "System Logs")
    if not tbl:
        return
    try:
        row = {
            "Timestamp": tznow_iso(),
            "Source": [service] if service else None,
            "Event Type": event_type,
            "Message / Payload": str(payload)[:10000],
            "Severity": severity,
            "Outcome": "✅" if status == "OK" else "❌",
        }
        tbl.create(remap_existing_only(tbl, {k: v for k, v in row.items() if v is not None}))
    except Exception:
        traceback.print_exc()