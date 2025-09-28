# sms/airtable_client.py
import os
import traceback
from pyairtable import Table
from typing import Optional


def _table(
    api_key: Optional[str], base_id: Optional[str], name: str
) -> Optional[Table]:
    """
    Safely initialize a pyairtable.Table.
    Returns None if env vars missing, instead of raising immediately.
    """
    if not api_key or not base_id:
        print(
            f"[AirtableClient] ⚠️ Missing config for '{name}' "
            f"(api_key={bool(api_key)}, base={bool(base_id)})"
        )
        return None
    try:
        return Table(api_key, base_id, name)
    except Exception:
        print(f"[AirtableClient] ❌ Failed to init table: {name}")
        traceback.print_exc()
        return None


# ── Base-level getters ──────────────────────────────────────────────
def get_leads_table(name: str) -> Optional[Table]:
    """Leads/Conversations base (inbound/outbound logs, leads)."""
    base = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
    key = os.getenv("AIRTABLE_ACQUISITIONS_KEY") or os.getenv("AIRTABLE_API_KEY")
    return _table(key, base, name)


def get_campaigns_table(name: str) -> Optional[Table]:
    """Campaign Control base (Numbers, Campaigns, Opt-Outs)."""
    base = os.getenv("CAMPAIGN_CONTROL_BASE") or os.getenv(
        "AIRTABLE_CAMPAIGN_CONTROL_BASE_ID"
    )
    key = os.getenv("AIRTABLE_COMPLIANCE_KEY") or os.getenv("AIRTABLE_API_KEY")
    return _table(key, base, name)


def get_performance_table(name: str) -> Optional[Table]:
    """Performance base (Runs/Logs, KPIs)."""
    base = os.getenv("AIRTABLE_PERFORMANCE_BASE_ID") or os.getenv("PERFORMANCE_BASE")
    key = os.getenv("AIRTABLE_REPORTING_KEY") or os.getenv("AIRTABLE_API_KEY")
    return _table(key, base, name)


# ── Shortcuts ──────────────────────────────────────────────
def get_convos() -> Optional[Table]:
    return get_leads_table(os.getenv("CONVERSATIONS_TABLE", "Conversations"))


def get_leads() -> Optional[Table]:
    return get_leads_table(os.getenv("LEADS_TABLE", "Leads"))


def get_prospects() -> Optional[Table]:
    return get_leads_table(os.getenv("PROSPECTS_TABLE", "Prospects"))


def get_campaigns() -> Optional[Table]:
    return get_campaigns_table(os.getenv("CAMPAIGNS_TABLE", "Campaigns"))


def get_numbers() -> Optional[Table]:
    return get_campaigns_table(os.getenv("NUMBERS_TABLE", "Numbers"))


def get_optouts() -> Optional[Table]:
    return get_campaigns_table(os.getenv("OPTOUTS_TABLE", "Opt-Outs"))


def get_kpis() -> Optional[Table]:
    return get_performance_table(os.getenv("KPIS_TABLE", "KPIs"))


def get_runs() -> Optional[Table]:
    return get_performance_table(os.getenv("RUNS_TABLE", "Runs/Logs"))
