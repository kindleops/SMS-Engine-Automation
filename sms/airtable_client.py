# sms/airtable_client.py
import os
from pyairtable import Table

# ---- API Keys (fallback to AIRTABLE_API_KEY if a per-base key is missing)
ACQ_KEY  = os.getenv("AIRTABLE_ACQUISITIONS_KEY") or os.getenv("AIRTABLE_API_KEY")  # Leads & Conversations base
DISPO_KEY = os.getenv("AIRTABLE_DISPO_KEY")       or os.getenv("AIRTABLE_API_KEY")  # Campaign Control base
PERF_KEY  = os.getenv("AIRTABLE_REPORTING_KEY")   or os.getenv("AIRTABLE_API_KEY")  # Performance base

# ---- Base IDs
LEADS_CONVOS_BASE     = os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
CAMPAIGN_CONTROL_BASE = os.getenv("AIRTABLE_CAMPAIGN_CONTROL_BASE_ID")
PERFORMANCE_BASE      = os.getenv("AIRTABLE_PERFORMANCE_BASE_ID")

def leads_table(name: str) -> Table:
    """Tables in Leads & Conversations base."""
    return Table(ACQ_KEY, LEADS_CONVOS_BASE, name)

def campaign_table(name: str) -> Table:
    """Tables in Campaign Control base."""
    return Table(DISPO_KEY, CAMPAIGN_CONTROL_BASE, name)

def perf_table(name: str) -> Table:
    """Tables in Performance base."""
    return Table(PERF_KEY, PERFORMANCE_BASE, name)