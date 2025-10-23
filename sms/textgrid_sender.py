"""
=======================================================================
 📡  AIRTABLE SCHEMA — FINAL VERIFIED BUILD
=======================================================================
Central definition for all Airtable table mappings, field names,
and enum constants used across the REI SMS Engine.

Key Fix:
--------
✅ Corrected SMS send direction for TextGrid integration
   → FROM = "TextGrid Number"      (our sending DID / 10DLC)
   → TO   = "Seller Phone Number"  (prospect’s number)

Used by:
---------
- sms.textgrid_sender
- sms.outbound_batcher
- sms.autoresponder
- sms.retry_runner
- sms.metrics_tracker
=======================================================================
"""

from __future__ import annotations
from enum import Enum
from typing import Dict


# =====================================================================
# ENUM DEFINITIONS
# =====================================================================

class ConversationDirection(str, Enum):
    """Defines message direction: Inbound vs Outbound."""
    INBOUND = "Inbound"
    OUTBOUND = "Outbound"


class ConversationDeliveryStatus(str, Enum):
    """Defines delivery state of messages."""
    QUEUED = "Queued"
    SENT = "Sent"
    FAILED = "Failed"
    DELIVERED = "Delivered"
    RECEIVED = "Received"


class ConversationProcessor(str, Enum):
    """Tracks which system component processed the message."""
    CAMPAIGN_RUNNER = "Campaign Runner"
    AUTORESPONDER = "Autoresponder"
    MANUAL = "Manual"


# =====================================================================
# CONVERSATIONS TABLE FIELD MAP
# =====================================================================

def conversations_field_map() -> Dict[str, str]:
    """
    Returns canonical Airtable field names for the Conversations table.

    ⚙️ Correct direction mapping for TextGrid:
        FROM = our TextGrid Number (sending DID)
        TO   = Seller Phone Number (prospect / lead)
    """
    return {
        "FROM": "TextGrid Number",          # ✅ From: your 10DLC sending number
        "TO": "Seller Phone Number",        # ✅ To: seller’s / prospect’s number
        "BODY": "Message",                  # SMS body
        "DIRECTION": "Direction",           # Inbound / Outbound
        "STATUS": "Delivery Status",        # Sent / Failed / Delivered
        "SENT_AT": "Sent At",               # Timestamp
        "TEXTGRID_ID": "Message SID",       # Returned SID from TextGrid
        "PROCESSED_BY": "Processed By",     # Campaign Runner / Autoresponder / Manual
    }


# =====================================================================
# LEADS, CAMPAIGNS, AND KPI SCHEMAS (EXTENSIBLE)
# =====================================================================

def leads_field_map() -> Dict[str, str]:
    """
    Defines canonical field names for the Leads table.
    Extend as needed for future AI tagging or scoring.
    """
    return {
        "NAME": "Seller Name",
        "PHONE": "Seller Phone Number",
        "EMAIL": "Email",
        "PROPERTY": "Property Address",
        "MARKET": "Market",
        "SOURCE": "Source",
        "STATUS": "Lead Status",
        "NOTES": "Notes",
        "LAST_TOUCHED": "Last Touched",
    }


def campaigns_field_map() -> Dict[str, str]:
    """Defines canonical field names for the Campaigns control table."""
    return {
        "NAME": "Campaign Name",
        "STATUS": "Status",
        "MARKET": "Market",
        "START": "Start Date",
        "END": "End Date",
        "DAILY_LIMIT": "Daily Limit",
        "TOTAL_SENT": "Total Sent",
        "LAST_RUN": "Last Run",
    }


def kpi_field_map() -> Dict[str, str]:
    """Defines canonical field names for KPI tracking."""
    return {
        "CAMPAIGN": "Campaign",
        "DATE": "Date",
        "MARKET": "Market",
        "METRIC": "Metric",
        "VALUE": "Value",
        "HEALTH": "Health",
        "SCORE": "Score",
    }


# =====================================================================
# TABLE ACCESS HELPERS (USED BY OTHER MODULES)
# =====================================================================

class CONVERSATIONS_TABLE:
    """
    Provides default field names for linked relationships
    used in create_conversation() and textgrid_sender logging.
    """
    @staticmethod
    def field_names() -> Dict[str, str]:
        return {
            "LEAD_LINK": "Lead",            # Linked Lead record
            "TEMPLATE_LINK": "Template",    # Linked Template
            "CAMPAIGN_LINK": "Campaign",    # Linked Campaign
        }


class LEADS_TABLE:
    """Placeholder for leads schema resolution."""
    @staticmethod
    def field_names() -> Dict[str, str]:
        return leads_field_map()


class CAMPAIGNS_TABLE:
    """Placeholder for campaigns schema resolution."""
    @staticmethod
    def field_names() -> Dict[str, str]:
        return campaigns_field_map()


class KPIS_TABLE:
    """Placeholder for KPI schema resolution."""
    @staticmethod
    def field_names() -> Dict[str, str]:
        return kpi_field_map()


# =====================================================================
# SELF-TEST (OPTIONAL LOCAL VALIDATION)
# =====================================================================

if __name__ == "__main__":
    print("✅ Conversations Map:", conversations_field_map())
    print("✅ Leads Map:", leads_field_map())
    print("✅ Campaigns Map:", campaigns_field_map())
    print("✅ KPI Map:", kpi_field_map())

