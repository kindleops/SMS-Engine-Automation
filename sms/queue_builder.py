# sms/queue_builder.py
"""
📦 Drip Queue Builder — Datastore Integration Build
────────────────────────────────────────────
Pulls leads for active campaigns and creates Drip Queue
records in Airtable via CONNECTOR abstraction layer.
"""

from __future__ import annotations
import traceback
from datetime import datetime, timezone
from typing import Dict, Any
from sms.runtime import get_logger
from sms.datastore import CONNECTOR
from sms.airtable_schema import DripStatus

log = get_logger("queue_builder")


def build_campaign_queue(campaign_record: Dict[str, Any], limit: int = 500) -> int:
    """
    For the given campaign Airtable record, find matching leads and push them to Drip Queue.
    """
    try:
        leads_tbl = CONNECTOR.leads()
        drip_tbl = CONNECTOR.drip_queue()

        if not leads_tbl or not drip_tbl:
            log.error("❌ Missing leads or drip queue table connection.")
            return 0

        campaign_id = campaign_record.get("id")
        campaign_fields = campaign_record.get("fields", {})
        campaign_name = campaign_fields.get("Campaign Name", "Unknown")

        # ✅ Pull all leads linked to this campaign
        all_leads = leads_tbl.all()
        leads = [
            l for l in all_leads
            if campaign_id in {str(x) for x in (l["fields"].get("Campaign") or [])}
        ]

        if not leads:
            log.info(f"ℹ️ No leads found for campaign {campaign_name}")
            return 0

        queued = 0
        now = datetime.now(timezone.utc).isoformat()

        for lead in leads[:limit]:
            f = lead.get("fields", {})
            phone = f.get("Seller Phone Number")
            msg = f.get("Message") or f.get("Template Message") or ""
            market = f.get("Market", "")
            prop = f.get("Property Address", "")

            if not phone or not msg:
                continue

            payload = {
                "Seller Phone Number": phone,
                "Message": msg,
                "Market": market,
                "Property Address": prop,
                "Status": DripStatus.READY.value,
                "Campaign": [campaign_id],
                "Queued At": now,
            }

            try:
                drip_tbl.create(payload)
                queued += 1
            except Exception as e:
                log.warning(f"⚠️ Failed to create drip record for {phone}: {e}")

        log.info(f"✅ Queued {queued} messages → campaign {campaign_name}")
        return queued

    except Exception as e:
        log.error(f"❌ build_campaign_queue failed: {e}")
        traceback.print_exc()
        return 0