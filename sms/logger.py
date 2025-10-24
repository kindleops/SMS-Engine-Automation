# sms/logger.py
"""
Run Logger
----------
Lightweight utility to log automation runs (campaigns, inbound, autoresponder, etc.)
to the 'Logs' table in the Performance base.
"""

from __future__ import annotations
import traceback
from datetime import datetime, timezone
from typing import Dict, Optional
from sms.runtime import get_logger
from sms.datastore import CONNECTOR

logger = get_logger("run_logger")

# -----------------------------
# Helpers
# -----------------------------
def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _norm(s: str) -> str:
    return "".join(ch for ch in s.lower() if ch.isalnum())

def _auto_map(tbl) -> Dict[str, str]:
    try:
        one = tbl.all(max_records=1)
        keys = list(one[0].get("fields", {}).keys()) if one else []
    except Exception:
        keys = []
    return {_norm(k): k for k in keys}

def _remap(tbl, data: Dict) -> Dict:
    amap = _auto_map(tbl)
    return {amap.get(_norm(k), k): v for k, v in data.items() if amap.get(_norm(k))}

# -----------------------------
# Public API
# -----------------------------
def log_run(
    run_type: str,
    processed: int = 0,
    breakdown: dict | str | None = None,
    status: str = "OK",
    campaign: str | None = None,
    extra: dict | None = None,
) -> Dict:
    """
    Log a system run into the Performance > Logs table.
    Example:
        log_run("CAMPAIGN_RUN", processed=500)
    """
    tbl = CONNECTOR.performance_logs()
    if not tbl:
        logger.warning(f"⚠️ Skipping log_run for {run_type} — table unavailable.")
        return {"ok": False, "error": "Performance Logs table unavailable"}

    record = {
        "Type": run_type,
        "Processed": processed,
        "Breakdown": str(breakdown or {}),
        "Status": status,
        "Timestamp": _utcnow_iso(),
    }
    if campaign:
        record["Campaign"] = campaign
    if extra:
        record.update(extra)

    try:
        tbl.create(_remap(tbl, record))
        logger.info(f"📝 Logged run: {run_type} | {status} | processed={processed}")
        return {"ok": True, "action": "created", "type": run_type, "status": status}
    except Exception as e:
        logger.error(f"❌ log_run failed: {run_type} — {e}", exc_info=True)
        return {"ok": False, "error": str(e)}
