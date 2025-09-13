import os
from pyairtable import Table


def get_table(api_key_env: str, base_id_env: str, table_name_env: str, default_table: str):
    """
    Safely initialize a pyairtable Table.
    Falls back to AIRTABLE_API_KEY if a specific key is not provided.
    Returns None if required env vars are missing.
    """
    key = os.getenv(api_key_env) or os.getenv("AIRTABLE_API_KEY")
    base = os.getenv(base_id_env)
    table_name = os.getenv(table_name_env, default_table)

    if not (key and base):
        print(f"⚠️ Missing Airtable env for {default_table} (base={base_id_env}, key={api_key_env})")
        return None

    return Table(key, base, table_name)


# ── Shortcuts ──────────────────────────────────────────────
def get_convos(table_name: str = "Conversations"):
    return get_table("AIRTABLE_ACQUISITIONS_KEY", "AIRTABLE_LEADS_CONVOS_BASE_ID", "CONVERSATIONS_TABLE", table_name)


def get_leads(table_name: str = "Leads"):
    return get_table("AIRTABLE_ACQUISITIONS_KEY", "AIRTABLE_LEADS_CONVOS_BASE_ID", "LEADS_TABLE", table_name)


def get_campaigns(table_name: str = "Campaigns"):
    return get_table("AIRTABLE_COMPLIANCE_KEY", "AIRTABLE_CAMPAIGN_CONTROL_BASE_ID", "CAMPAIGNS_TABLE", table_name)


def get_numbers(table_name: str = "Numbers"):
    return get_table("AIRTABLE_COMPLIANCE_KEY", "AIRTABLE_CAMPAIGN_CONTROL_BASE_ID", "NUMBERS_TABLE", table_name)


def get_kpis(table_name: str = "KPIs"):
    return get_table("AIRTABLE_REPORTING_KEY", "AIRTABLE_PERFORMANCE_BASE_ID", "KPIS_TABLE", table_name)


def get_runs(table_name: str = "Runs/Logs"):
    return get_table("AIRTABLE_REPORTING_KEY", "AIRTABLE_PERFORMANCE_BASE_ID", "RUNS_TABLE", table_name)