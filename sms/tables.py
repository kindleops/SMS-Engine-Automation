# sms/tables.py
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


# --------- ENV helpers (with synonyms you’ve used elsewhere) ---------
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
    "KPIS_TABLE": ["KPIS_TABLE_NAME"],  # sometimes referenced as KPIS_TABLE_NAME
    "RUNS_TABLE": ["RUNS_TABLE_NAME"],
}

VERBOSE = os.getenv("TABLES_VERBOSE", "0") in ("1", "true", "yes")


def _env_first(name: str, extra: list[str] | None = None, fallback: str | None = None) -> Optional[str]:
    """Return first non-empty value among name, its synonyms, and final fallback."""
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
    # Always last-resort fallback to AIRTABLE_API_KEY
    return _env_first(api_key_env, synonyms, fallback="AIRTABLE_API_KEY")


def _resolve_base(base_id_env: str) -> Optional[str]:
    synonyms = _BASE_SYNONYMS.get(base_id_env, [])
    return _env_first(base_id_env, synonyms)


def _resolve_table_name(table_name_env: str, default_table: str) -> str:
    synonyms = _TABLE_SYNONYMS.get(table_name_env, [])
    return _env_first(table_name_env, synonyms) or default_table


# --------- Core factory ---------
def get_table(api_key_env: str, base_id_env: str, table_name_env: str, default_table: str) -> Any | None:
    """
    Safely initialize a pyairtable Table.

    Args:
        api_key_env: ENV var for a table-specific API key
                     (falls back through synonyms → AIRTABLE_API_KEY).
        base_id_env: ENV var for the Airtable base ID
                     (accepts synonyms like LEADS_CONVOS_BASE).
        table_name_env: ENV var for the table name (accepts synonyms).
        default_table: fallback name if table env not set.

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
        # Optional ping (cheap) — comment out if you want zero calls here:
        # _ = tbl.all(max_records=0)
        return tbl
    except Exception:
        print(f"❌ tables.py: failed to init Table({table_name}) for base env '{base_id_env}'")
        traceback.print_exc()
        return None


# --------- Cached shortcuts for common tables ---------
@lru_cache(maxsize=None)
def get_convos(table_name: str = "Conversations") -> Any | None:
    """Conversations table (Leads/Convos base)."""
    return get_table(
        "AIRTABLE_ACQUISITIONS_KEY",
        "AIRTABLE_LEADS_CONVOS_BASE_ID",
        "CONVERSATIONS_TABLE",
        table_name,
    )


@lru_cache(maxsize=None)
def get_leads(table_name: str = "Leads") -> Any | None:
    """Leads table (Leads/Convos base)."""
    return get_table(
        "AIRTABLE_ACQUISITIONS_KEY",
        "AIRTABLE_LEADS_CONVOS_BASE_ID",
        "LEADS_TABLE",
        table_name,
    )


@lru_cache(maxsize=None)
def get_prospects(table_name: str = "Prospects") -> Any | None:
    """Prospects table (Leads/Convos base)."""
    return get_table(
        "AIRTABLE_ACQUISITIONS_KEY",
        "AIRTABLE_LEADS_CONVOS_BASE_ID",
        "PROSPECTS_TABLE",
        table_name,
    )


@lru_cache(maxsize=None)
def get_drip(table_name: str = "Drip Queue") -> Any | None:
    """Drip Queue table (Leads/Convos base). Accepts DRIP_TABLE or DRIP_QUEUE_TABLE."""
    return get_table(
        "AIRTABLE_ACQUISITIONS_KEY",
        "AIRTABLE_LEADS_CONVOS_BASE_ID",
        "DRIP_TABLE",  # synonyms include DRIP_QUEUE_TABLE
        table_name,
    )


@lru_cache(maxsize=None)
def get_campaigns(table_name: str = "Campaigns") -> Any | None:
    """Campaigns table (Campaign Control base)."""
    return get_table(
        "AIRTABLE_COMPLIANCE_KEY",
        "AIRTABLE_CAMPAIGN_CONTROL_BASE_ID",
        "CAMPAIGNS_TABLE",
        table_name,
    )


@lru_cache(maxsize=None)
def get_numbers(table_name: str = "Numbers") -> Any | None:
    """Numbers table (Campaign Control base)."""
    return get_table(
        "AIRTABLE_COMPLIANCE_KEY",
        "AIRTABLE_CAMPAIGN_CONTROL_BASE_ID",
        "NUMBERS_TABLE",
        table_name,
    )


@lru_cache(maxsize=None)
def get_kpis(table_name: str = "KPIs") -> Any | None:
    """KPIs table (Performance base). Accepts KPIS_TABLE or KPIS_TABLE_NAME."""
    return get_table(
        "AIRTABLE_REPORTING_KEY",
        "AIRTABLE_PERFORMANCE_BASE_ID",
        "KPIS_TABLE",  # synonyms include KPIS_TABLE_NAME
        table_name,
    )


@lru_cache(maxsize=None)
def get_runs(table_name: str = "Runs/Logs") -> Any | None:
    """Runs/Logs table (Performance base). Accepts RUNS_TABLE or RUNS_TABLE_NAME."""
    return get_table(
        "AIRTABLE_REPORTING_KEY",
        "AIRTABLE_PERFORMANCE_BASE_ID",
        "RUNS_TABLE",  # synonyms include RUNS_TABLE_NAME
        table_name,
    )
