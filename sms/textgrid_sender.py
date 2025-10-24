"""
=======================================================================
 📡  AIRTABLE SCHEMA — FINAL OPTIMIZED BUILD
=======================================================================
Central definition for all Airtable table mappings, field names,
and enum constants used across the REI SMS Engine.

Key Improvements:
-----------------
✅ Canonical direction mapping for TextGrid (FROM vs TO)
✅ Added FIELD_ALIAS for unified lookups
✅ Added DELIVERY_STATUS_MAP normalization
✅ Added TABLE_NAMES constants for consistency
✅ Added default_conversation_payload() helper for DRY record creation
✅ Added __all__ for clean imports

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
# FIELD MAPS
# =====================================================================


def conversations_field_map() -> Dict[str, str]:
    """
    Canonical Airtable field names for the Conversations table.

    ⚙️ Correct TextGrid direction mapping:
        FROM = our TextGrid Number (sending DID)
        TO   = Seller Phone Number (prospect / lead)
    """
    return {
        "FROM": "TextGrid Number",  # ✅ From: your 10DLC sending number
        "TO": "Seller Phone Number",  # ✅ To: seller’s / prospect’s number
        "BODY": "Message",  # SMS body
        "DIRECTION": "Direction",  # Inbound / Outbound
        "STATUS": "Delivery Status",  # Sent / Failed / Delivered
        "SENT_AT": "Sent At",  # Timestamp
        "TEXTGRID_ID": "Message SID",  # Returned SID from TextGrid
        "PROCESSED_BY": "Processed By",  # Campaign Runner / Autoresponder / Manual
    }


def leads_field_map() -> Dict[str, str]:
    """Defines canonical field names for the Leads table."""
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
# CONSTANTS / ALIASES
# =====================================================================

FIELD_ALIAS = {
    "from": "TextGrid Number",
    "to": "Seller Phone Number",
    "body": "Message",
    "status": "Delivery Status",
    "sent_at": "Sent At",
    "textgrid_id": "Message SID",
    "processor": "Processed By",
}

TABLE_NAMES = {
    "CONVERSATIONS": "Conversations",
    "LEADS": "Leads",
    "CAMPAIGNS": "Campaigns",
    "KPIS": "KPIs",
}

DELIVERY_STATUS_MAP = {
    "queued": ConversationDeliveryStatus.QUEUED,
    "sending": ConversationDeliveryStatus.SENT,
    "sent": ConversationDeliveryStatus.SENT,
    "delivered": ConversationDeliveryStatus.DELIVERED,
    "success": ConversationDeliveryStatus.DELIVERED,
    "received": ConversationDeliveryStatus.RECEIVED,
    "failed": ConversationDeliveryStatus.FAILED,
    "undelivered": ConversationDeliveryStatus.FAILED,
    "error": ConversationDeliveryStatus.FAILED,
    "blocked": ConversationDeliveryStatus.FAILED,
    "expired": ConversationDeliveryStatus.FAILED,
}


# =====================================================================
# TABLE ACCESS HELPERS
# =====================================================================


class CONVERSATIONS_TABLE:
    """Default linked field names used in create_conversation()."""

    @staticmethod
    def field_names() -> Dict[str, str]:
        return {
            "LEAD_LINK": "Lead",
            "TEMPLATE_LINK": "Template",
            "CAMPAIGN_LINK": "Campaign",
        }


class LEADS_TABLE:
    """Schema wrapper for leads table."""

    @staticmethod
    def field_names() -> Dict[str, str]:
        return leads_field_map()


class CAMPAIGNS_TABLE:
    """Schema wrapper for campaigns table."""

    @staticmethod
    def field_names() -> Dict[str, str]:
        return campaigns_field_map()


class KPIS_TABLE:
    """Schema wrapper for KPIs table."""

    @staticmethod
    def field_names() -> Dict[str, str]:
        return kpi_field_map()


# =====================================================================
# DEFAULT PAYLOAD HELPER
# =====================================================================


def default_conversation_payload(
    from_number: str,
    to_number: str,
    body: str,
    processor: ConversationProcessor = ConversationProcessor.CAMPAIGN_RUNNER,
) -> Dict[str, str]:
    """
    Provides a canonical payload structure for new outbound conversation records.
    Ensures consistent naming across all sender modules.
    """
    f = conversations_field_map()
    return {
        f["FROM"]: from_number,
        f["TO"]: to_number,
        f["BODY"]: body,
        f["DIRECTION"]: ConversationDirection.OUTBOUND.value,
        f["STATUS"]: ConversationDeliveryStatus.QUEUED.value,
        f["PROCESSED_BY"]: processor.value,
    }


# =====================================================================
# EXPORTS
# =====================================================================

__all__ = [
    "ConversationDirection",
    "ConversationDeliveryStatus",
    "ConversationProcessor",
    "FIELD_ALIAS",
    "TABLE_NAMES",
    "DELIVERY_STATUS_MAP",
    "conversations_field_map",
    "leads_field_map",
    "campaigns_field_map",
    "kpi_field_map",
    "CONVERSATIONS_TABLE",
    "LEADS_TABLE",
    "CAMPAIGNS_TABLE",
    "KPIS_TABLE",
    "default_conversation_payload",
]


# =====================================================================
# SELF-TEST (LOCAL VALIDATION)
# =====================================================================

if __name__ == "__main__":
    print("✅ Conversations Map:", conversations_field_map())
    print("✅ Leads Map:", leads_field_map())
    print("✅ Campaigns Map:", campaigns_field_map())
    print("✅ KPI Map:", kpi_field_map())
    sample = default_conversation_payload("+18334445555", "+14015556666", "Test message")
    print("🧠 Sample Payload:", sample)
