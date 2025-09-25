import os
from pyairtable import Table


def get_table(api_key_env: str, base_id_env: str, table_name_env: str, default_table: str):
    """
    Safely initialize a pyairtable Table.
    - api_key_env: ENV var for a table-specific API key (falls back to AIRTABLE_API_KEY).
    - base_id_env: ENV var for the Airtable base ID.
    - table_name_env: ENV var for the table name.
    - default_table: fallback name if env not set.

    Returns:
        pyairtable.Table or None if env is incomplete.
    """
    key = os.getenv(api_key_env) or os.getenv("AIRTABLE_API_KEY")
    base = os.getenv(base_id_env)
    table_name = os.getenv(table_name_env, default_table)

    if not (key and base):
        print(f"⚠️ Missing Airtable env for {default_table} "
              f"(base={base_id_env}, key={api_key_env})")
        return None

    return Table(key, base, table_name)


# ── Shortcuts for common tables ──────────────────────────────

def get_convos(table_name: str = "Conversations"):
    """Conversations table (Leads/Convos base)."""
    return get_table("AIRTABLE_ACQUISITIONS_KEY", "AIRTABLE_LEADS_CONVOS_BASE_ID",
                     "CONVERSATIONS_TABLE", table_name)


def get_leads(table_name: str = "Leads"):
    """Leads table (Leads/Convos base)."""
    return get_table("AIRTABLE_ACQUISITIONS_KEY", "AIRTABLE_LEADS_CONVOS_BASE_ID",
                     "LEADS_TABLE", table_name)


def get_prospects(table_name: str = "Prospects"):
    """Prospects table (Leads/Convos base)."""
    return get_table("AIRTABLE_ACQUISITIONS_KEY", "AIRTABLE_LEADS_CONVOS_BASE_ID",
                     "PROSPECTS_TABLE", table_name)


def get_drip(table_name: str = "Drip Queue"):
    """Drip Queue table (Leads/Convos base)."""
    return get_table("AIRTABLE_ACQUISITIONS_KEY", "AIRTABLE_LEADS_CONVOS_BASE_ID",
                     "DRIP_TABLE", table_name)


def get_campaigns(table_name: str = "Campaigns"):
    """Campaigns table (Campaign Control base)."""
    return get_table("AIRTABLE_COMPLIANCE_KEY", "AIRTABLE_CAMPAIGN_CONTROL_BASE_ID",
                     "CAMPAIGNS_TABLE", table_name)


def get_numbers(table_name: str = "Numbers"):
    """Numbers table (Campaign Control base)."""
    return get_table("AIRTABLE_COMPLIANCE_KEY", "AIRTABLE_CAMPAIGN_CONTROL_BASE_ID",
                     "NUMBERS_TABLE", table_name)


def get_kpis(table_name: str = "KPIs"):
    """KPIs table (Performance base)."""
    return get_table("AIRTABLE_REPORTING_KEY", "AIRTABLE_PERFORMANCE_BASE_ID",
                     "KPIS_TABLE", table_name)


def get_runs(table_name: str = "Runs/Logs"):
    """Runs/Logs table (Performance base)."""
    return get_table("AIRTABLE_REPORTING_KEY", "AIRTABLE_PERFORMANCE_BASE_ID",
                     "RUNS_TABLE", table_name)