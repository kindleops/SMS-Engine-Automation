# sms/tables.py
"""
🏗️ Airtable Connection Manager (v2.1 – Telemetry Edition)
────────────────────────────────────────────────────────────
Centralized table initialization and connection factory for all Airtable bases.
Fully compatible with existing modules (scheduler, retry runner, dispatcher, etc.)
and now includes runtime telemetry helpers (summary, ping_all, templates table).
"""

from __future__ import annotations

import os
import traceback
from functools import lru_cache
from typing import Any, Optional

# Optional import (avoid NameError if pyairtable isn't installed)
try:
    from pyairtable import Table as _AirTable
except Exception:
    _AirTable = None

# Optional imports (for telemetry/logging integration)
try:
    from sms.logger import log_run
except Exception:
    log_run = None

try:
    from sms.spec import CONVERSATION_FIELDS, LEAD_FIELDS
except Exception:
    CONVERSATION_FIELDS = None
    LEAD_FIELDS = None


# ---------------------------------------------------------------------------
# ENV helpers and synonym registries
# ---------------------------------------------------------------------------
_KEY_SYNONYMS = {
    "AIRTABLE_ACQUISITIONS_KEY": ["AIRTABLE_API_KEY"],
    "AIRTABLE_COMPLIANCE_KEY": ["AIRTABLE_API_KEY"],
    "AIRTABLE_REPORTING_KEY": ["AIRTABLE_API_KEY"],
}

_BASE_SYNONYMS = {
    "AIRTABLE_LEADS_CONVOS_BASE_ID": ["LEADS_CONVOS_BASE"],
    "AIRTABLE_CAMPAIGN_CONTROL_BASE_ID": ["CAMPAIGN_CONTROL_BASE"],
    "AIRTABLE_PERFORMANCE_BASE_ID": ["PERFORMANCE_BASE"],
}

_TABLE_SYNONYMS = {
    "DRIP_TABLE": ["DRIP_QUEUE_TABLE"],
    "CONVERSATIONS_TABLE": [],
    "LEADS_TABLE": [],
    "PROSPECTS_TABLE": [],
    "CAMPAIGNS_TABLE": [],
    "NUMBERS_TABLE": [],
    "KPIS_TABLE": ["KPIS_TABLE_NAME"],
    "RUNS_TABLE": ["RUNS_TABLE_NAME"],
    "TEMPLATES_TABLE": [],
}

VERBOSE = os.getenv("TABLES_VERBOSE", "0").lower() in ("1", "true", "yes")


def _env_first(name: str, extra: list[str] | None = None, fallback: str | None = None) -> Optional[str]:
    """Return first non-empty value among name, its synonyms, and fallback."""
    candidates = [name]
    if extra:
        candidates.extend(extra)
    for n in candidates:
        v = os.getenv(n)
        if v:
            return v
    return os.getenv(fallback) if fallback else None


def _resolve_key(api_key_env: str) -> Optional[str]:
    synonyms = _KEY_SYNONYMS.get(api_key_env, [])
    return _env_first(api_key_env, synonyms, fallback="AIRTABLE_API_KEY")


def _resolve_base(base_id_env: str) -> Optional[str]:
    synonyms = _BASE_SYNONYMS.get(base_id_env, [])
    return _env_first(base_id_env, synonyms)


def _resolve_table_name(table_name_env: str, default_table: str) -> str:
    synonyms = _TABLE_SYNONYMS.get(table_name_env, [])
    return _env_first(table_name_env, synonyms) or default_table


# ---------------------------------------------------------------------------
# Core Table Factory
# ---------------------------------------------------------------------------
def get_table(api_key_env: str, base_id_env: str, table_name_env: str, default_table: str) -> Any | None:
    """
    Safely initialize a pyairtable Table.

    Returns:
        pyairtable.Table instance or None if env/deps are incomplete.
    """
    key = _resolve_key(api_key_env)
    base = _resolve_base(base_id_env)
    table_name = _resolve_table_name(table_name_env, default_table)

    if not _AirTable:
        if VERBOSE:
            print("⚠️ tables.py: pyairtable not installed → returning None (MOCK mode).")
        return None

    if not (key and base):
        if VERBOSE:
            print(f"⚠️ Missing Airtable env for {default_table} (base={base_id_env}, key={api_key_env})")
        return None

    try:
        tbl = _AirTable(key, base, table_name)
        return tbl
    except Exception:
        print(f"❌ tables.py: failed to init Table({table_name}) for base env '{base_id_env}'")
        traceback.print_exc()
        return None


# ---------------------------------------------------------------------------
# Cached Shortcuts for Common Tables
# ---------------------------------------------------------------------------
@lru_cache(maxsize=None)
def get_convos(table_name: str = "Conversations") -> Any | None:
    return get_table("AIRTABLE_ACQUISITIONS_KEY", "AIRTABLE_LEADS_CONVOS_BASE_ID", "CONVERSATIONS_TABLE", table_name)


@lru_cache(maxsize=None)
def get_leads(table_name: str = "Leads") -> Any | None:
    return get_table("AIRTABLE_ACQUISITIONS_KEY", "AIRTABLE_LEADS_CONVOS_BASE_ID", "LEADS_TABLE", table_name)


@lru_cache(maxsize=None)
def get_prospects(table_name: str = "Prospects") -> Any | None:
    return get_table("AIRTABLE_ACQUISITIONS_KEY", "AIRTABLE_LEADS_CONVOS_BASE_ID", "PROSPECTS_TABLE", table_name)


@lru_cache(maxsize=None)
def get_drip(table_name: str = "Drip Queue") -> Any | None:
    return get_table("AIRTABLE_ACQUISITIONS_KEY", "AIRTABLE_LEADS_CONVOS_BASE_ID", "DRIP_TABLE", table_name)


@lru_cache(maxsize=None)
def get_campaigns(table_name: str = "Campaigns") -> Any | None:
    return get_table("AIRTABLE_COMPLIANCE_KEY", "AIRTABLE_CAMPAIGN_CONTROL_BASE_ID", "CAMPAIGNS_TABLE", table_name)


@lru_cache(maxsize=None)
def get_numbers(table_name: str = "Numbers") -> Any | None:
    return get_table("AIRTABLE_COMPLIANCE_KEY", "AIRTABLE_CAMPAIGN_CONTROL_BASE_ID", "NUMBERS_TABLE", table_name)


@lru_cache(maxsize=None)
def get_kpis(table_name: str = "KPIs") -> Any | None:
    return get_table("AIRTABLE_REPORTING_KEY", "AIRTABLE_PERFORMANCE_BASE_ID", "KPIS_TABLE", table_name)


@lru_cache(maxsize=None)
def get_runs(table_name: str = "Logs") -> Any | None:
    return get_table("AIRTABLE_REPORTING_KEY", "AIRTABLE_PERFORMANCE_BASE_ID", "RUNS_TABLE", table_name)


@lru_cache(maxsize=None)
def get_templates(table_name: str = "Templates") -> Any | None:
    """Templates table (Leads/Convos base) — used by status handler."""
    return get_table("AIRTABLE_ACQUISITIONS_KEY", "AIRTABLE_LEADS_CONVOS_BASE_ID", "TEMPLATES_TABLE", table_name)


# ---------------------------------------------------------------------------
# Telemetry Utilities
# ---------------------------------------------------------------------------
def summary() -> dict[str, dict[str, str]]:
    """Return a snapshot of resolved Airtable environment connections."""
    return {
        "Acquisitions (Leads/Convos)": {
            "Base": _resolve_base("AIRTABLE_LEADS_CONVOS_BASE_ID") or "<missing>",
            "Key": _resolve_key("AIRTABLE_ACQUISITIONS_KEY") or "<missing>",
        },
        "Campaign Control": {
            "Base": _resolve_base("AIRTABLE_CAMPAIGN_CONTROL_BASE_ID") or "<missing>",
            "Key": _resolve_key("AIRTABLE_COMPLIANCE_KEY") or "<missing>",
        },
        "Performance": {
            "Base": _resolve_base("AIRTABLE_PERFORMANCE_BASE_ID") or "<missing>",
            "Key": _resolve_key("AIRTABLE_REPORTING_KEY") or "<missing>",
        },
    }


def ping_all(verbose: bool = True) -> dict[str, bool]:
    """
    Attempt to call `.all(max_records=0)` on each configured table.
    Returns a dict of {table_name: success_flag}.
    """
    results = {}
    table_map = {
        "Conversations": get_convos,
        "Leads": get_leads,
        "Prospects": get_prospects,
        "Drip Queue": get_drip,
        "Campaigns": get_campaigns,
        "Numbers": get_numbers,
        "Templates": get_templates,
        "KPIs": get_kpis,
        "Runs": get_runs,
    }
    for name, getter in table_map.items():
        tbl = getter()
        ok = False
        try:
            if tbl:
                tbl.all(max_records=0)
                ok = True
        except Exception:
            traceback.print_exc()
        results[name] = ok
        if verbose:
            print(f"✅ {name}" if ok else f"⚠️ {name} failed or not configured")
    return results


# ---------------------------------------------------------------------------
# CLI / Self-Check Execution
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("🔍 Running Airtable connection self-check...")
    print("📊 Environment Summary:", summary())
    results = ping_all(verbose=True)
    success_count = sum(1 for v in results.values() if v)
    print(f"🏁 Connection check complete → {success_count}/{len(results)} OK")

    # Optional structured run logging (if logger available)
    if log_run:
        try:
            log_run("AIRTABLE_PING", processed=success_count, breakdown=results)
        except Exception:
            traceback.print_exc()
