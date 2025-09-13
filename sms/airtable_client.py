import os
from pyairtable import Table
import traceback

def _table(api_key: str | None, base_id: str | None, name: str) -> Table | None:
    """
    Safely initialize a pyairtable.Table.
    Returns None if env vars missing, instead of raising immediately.
    """
    if not api_key or not base_id:
        print(f"⚠️ Missing Airtable config for table '{name}' (api_key={bool(api_key)}, base={bool(base_id)})")
        return None
    try:
        return Table(api_key, base_id, name)
    except Exception:
        print(f"❌ Failed to init Airtable table: {name}")
        traceback.print_exc()
        return None


def get_leads_table(name: str) -> Table | None:
    """
    Conversations base: inbound/outbound SMS logs.
    Uses Acquisitions key if set, otherwise defaults to AIRTABLE_API_KEY.
    """
    base = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
    key  = os.getenv("AIRTABLE_ACQUISITIONS_KEY") or os.getenv("AIRTABLE_API_KEY")
    return _table(key, base, name)


def get_campaigns_table(name: str) -> Table | None:
    """
    Campaign Control base: Numbers, Campaigns, Opt-Outs.
    Uses Compliance key if set, otherwise defaults to AIRTABLE_API_KEY.
    """
    base = os.getenv("CAMPAIGN_CONTROL_BASE") or os.getenv("AIRTABLE_CAMPAIGN_CONTROL_BASE_ID")
    key  = os.getenv("AIRTABLE_COMPLIANCE_KEY") or os.getenv("AIRTABLE_API_KEY")
    return _table(key, base, name)


def get_performance_table(name: str) -> Table | None:
    """
    Performance base: Runs/Logs, KPIs.
    Uses Reporting key if set, otherwise defaults to AIRTABLE_API_KEY.
    """
    base = os.getenv("AIRTABLE_PERFORMANCE_BASE_ID") or os.getenv("PERFORMANCE_BASE")
    key  = os.getenv("AIRTABLE_REPORTING_KEY") or os.getenv("AIRTABLE_API_KEY")
    return _table(key, base, name)