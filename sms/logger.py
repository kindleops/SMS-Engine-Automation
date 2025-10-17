"""Utility helpers for logging operational runs to Airtable.

This module purposely keeps a very small public API – currently just
``log_run`` – but the implementation does a bit more than the previous version:

* The Airtable dependency (``pyairtable``) is now optional.  When it is missing
  the module no longer crashes on import; instead it transparently falls back
  to a no-op mode and prints a human friendly warning.
* Inputs are normalised so callers can pass ``processed`` values or breakdowns
  without worrying about exact types.
* Exception handling prints a compact stack trace to aid debugging while still
  surfacing the error message.

The behaviour is otherwise backwards compatible – we still print to stdout so
existing log scraping continues to work – but the module is considerably more
robust in environments where Airtable credentials or dependencies are missing.
"""

from __future__ import annotations

import json
import os
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, Optional

# ``pyairtable`` is an optional dependency at runtime.  Import it lazily so
# that environments running unit tests (or local development) without the
# package installed can still import the module without raising immediately.
try:  # pragma: no cover - behaviour validated indirectly via fallbacks
    from pyairtable import Table as _RealTable
except Exception:  # pragma: no cover - the fallback path is exercised in tests
    _RealTable = None


class Table:  # thin compatibility wrapper around the optional dependency
    """Wrapper that only initialises the real ``Table`` when available."""

    def __init__(self, api_key: str, base_id: str, table_name: str) -> None:
        if _RealTable is None:
            raise ImportError(
                "pyairtable is not installed or failed to import. Install with: "
                "pip install pyairtable"
            )
        self._table = _RealTable(api_key, base_id, table_name)

    def create(self, fields: Dict[str, Any]):
        return self._table.create(fields)

AIRTABLE_KEY = os.getenv("AIRTABLE_REPORTING_KEY") or os.getenv("AIRTABLE_API_KEY")
PERF_BASE = os.getenv("PERFORMANCE_BASE")


def _get_table() -> Optional[Table]:
    if not (AIRTABLE_KEY and PERF_BASE):
        return None
    try:
        return Table(AIRTABLE_KEY, PERF_BASE, "Runs/Logs")
    except Exception:
        traceback.print_exc()
        return None


def _coerce_processed(value: Any) -> int:
    """Best-effort conversion of ``processed`` counts to an integer."""

    if value is None:
        return 0
    if isinstance(value, int):
        return value
    try:
        return int(value)
    except Exception:
        try:
            return int(float(str(value).replace(",", "")))
        except Exception:
            return 0


def _serialise_breakdown(breakdown: Any) -> str:
    if breakdown is None:
        return "{}"
    if isinstance(breakdown, str):
        return breakdown
    try:
        return json.dumps(breakdown, default=str, sort_keys=True)
    except Exception:
        return str(breakdown)


def log_run(
    run_type: str,
    processed: int = 0,
    breakdown: dict | str | None = None,
    status: str = "OK",
    campaign: str | None = None,
    extra: dict | None = None,
):
    """
    Write a row into Runs/Logs table in Performance base.

    Args:
        run_type (str): Type of run (e.g. "CAMPAIGN_RUN", "AUTORESPONDER", "INBOUND").
        processed (int): Number of records processed.
        breakdown (dict|str|None): Any extra breakdown/debug info.
        status (str): Status flag (OK, ERROR, PARTIAL, etc).
        campaign (str|None): Optional campaign name for traceability.
        extra (dict|None): Extra fields to attach into the record.
    """
    tbl = _get_table()
    if not tbl:
        print(f"⚠️ Skipping run log for {run_type}, Airtable not configured")
        return

    try:
        record = {
            "Type": run_type,
            "Processed": _coerce_processed(processed),
            "Breakdown": _serialise_breakdown(breakdown),
            "Status": status,
            "Timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if campaign:
            record["Campaign"] = campaign
        if extra:
            record.update(dict(extra))

        tbl.create(record)
        print(f"📝 Logged run → {run_type} | {status} | {processed} processed")
    except Exception as e:
        print(f"❌ Failed to log run {run_type}: {e}")
        traceback.print_exc()
