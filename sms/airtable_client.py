# sms/airtable_client.py
import os
from pyairtable import Table

def _table(api_key: str | None, base_id: str | None, name: str) -> Table:
    if not api_key or not base_id:
        raise RuntimeError(f"Missing Airtable config for table '{name}'. api_key or base_id is empty.")
    return Table(api_key, base_id, name)

def get_leads_table(name: str) -> Table:
    base = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
    key  = os.getenv("AIRTABLE_ACQUISITIONS_KEY") or os.getenv("AIRTABLE_API_KEY")
    return _table(key, base, name)

def get_campaigns_table(name: str) -> Table:
    base = os.getenv("CAMPAIGN_CONTROL_BASE") or os.getenv("AIRTABLE_CAMPAIGN_CONTROL_BASE_ID")
    key  = os.getenv("AIRTABLE_COMPLIANCE_KEY") or os.getenv("AIRTABLE_API_KEY")
    return _table(key, base, name)

def get_performance_table(name: str) -> Table:
    base = os.getenv("AIRTABLE_PERFORMANCE_BASE_ID") or os.getenv("PERFORMANCE_BASE")
    key  = os.getenv("AIRTABLE_REPORTING_KEY") or os.getenv("AIRTABLE_API_KEY")
    return _table(key, base, name)