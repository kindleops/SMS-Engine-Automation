# ops/webhooks.py
from __future__ import annotations

import os
import traceback
from typing import Any, Dict, List, Optional

# Optional system stats
try:
    import psutil  # optional
except Exception:  # pragma: no cover
    psutil = None  # type: ignore

# Optional Airtable client (v2)
try:
    from pyairtable import Api  # type: ignore
except Exception:  # pragma: no cover
    Api = None  # type: ignore


def _get_airtable_table(table_name: str):
    """
    Lazy, safe table getter. Returns None if env or client missing.
    Works with pyairtable v2: Api(key).table(base_id, table_name)
    """
    try:
        key = os.getenv("AIRTABLE_API_KEY")
        base = os.getenv("DEVOPS_BASE") or os.getenv("AIRTABLE_DEVOPS_BASE_ID")
        if not (Api and key and base):
            return None
        return Api(key).table(base, table_name)
    except Exception:
        traceback.print_exc()
        return None


def system_stats() -> Dict[str, Any]:
    """
    Lightweight system metrics. Never crashes if psutil is absent.
    """
    if not psutil:
        return {
            "ok": True,
            "psutil_available": False,
            "note": "psutil not installed; returning minimal stats",
        }
    try:
        vm = psutil.virtual_memory()
        cpu = psutil.cpu_percent(interval=0.0)  # non-blocking snapshot
        return {
            "ok": True,
            "psutil_available": True,
            "cpu_percent": cpu,
            "mem_percent": vm.percent,
            "mem_used_mb": round(vm.used / (1024 * 1024), 1),
            "mem_total_mb": round(vm.total / (1024 * 1024), 1),
        }
    except Exception:
        traceback.print_exc()
        return {"ok": False, "error": "stats_failed"}


def devops_log(event: str, **fields) -> Dict[str, Any]:
    """
    Best-effort: write a row to DevOps Logs table (if configured).
    Never raises; returns status dict.
    """
    tbl = _get_airtable_table("Logs")
    if not tbl:
        return {"ok": False, "skipped": "devops_base_or_client_missing"}

    payload = {
        "Event": event,
        "Payload": {k: v for k, v in fields.items() if v is not None},
    }
    try:
        rec = tbl.create(payload)
        return {"ok": True, "id": rec.get("id")}
    except Exception:
        traceback.print_exc()
        return {"ok": False, "error": "create_failed"}


def recent_logs(limit: int = 50) -> List[Dict[str, Any]]:
    """
    Fetch recent devops logs (if table configured). Never raises.
    """
    tbl = _get_airtable_table("Logs")
    if not tbl:
        return []
    try:
        rows = tbl.all(max_records=max(1, min(limit, 100)))
        return rows or []
    except Exception:
        traceback.print_exc()
        return []