"""
📦 Queue Builder — Prospect-Based Final Version
──────────────────────────────────────────────────────
Builds SMS queue from active campaigns and linked prospects.
"""

from __future__ import annotations
import traceback
from datetime import datetime, timezone
from typing import Any, Dict

from sms.runtime import get_logger
from sms.datastore import CONNECTOR, list_records, update_record, _compact
from sms.airtable_schema import DripStatus

log = get_logger("queue_builder")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def build_campaign_queue(campaign: Dict[str, Any], limit: int = 500) -> int:
    """Queue messages for an active campaign into Drip Queue (Prospect-based)."""
    try:
        # ── Get Airtable table handles
        prospects_tbl = CONNECTOR.prospects()
        drip_tbl = CONNECTOR.drip_queue()

        # ── Fetch prospects (safe)
        prospects = list_records(prospects_tbl, max_records=limit)
        if not prospects:
            log.info("⚠️ No prospects found for campaign.")
            return 0

        # ── Campaign fields
        f = campaign.get("fields", {}) or {}
        campaign_id = campaign.get("id")
        campaign_name = f.get("Campaign Name", "Unnamed Campaign")
        textgrid_num = f.get("TextGrid Phone Number") or f.get("From Number")
        message_body = f.get("Message") or f.get("Body")

        if not message_body or not textgrid_num:
            log.warning(f"⚠️ Campaign {campaign_name} missing message or TextGrid number.")
            return 0

        queued = 0
        now = _utcnow()

        for prospect in prospects:
            pf = prospect.get("fields", {}) or {}

            # ── Match only prospects linked to this campaign (if applicable)
            linked_campaigns = pf.get("Campaign") or pf.get("Campaigns") or []
            if linked_campaigns and campaign_id not in linked_campaigns:
                continue

            # ── Get seller phone
            seller_num = (
                pf.get("Seller Phone Number")
                or pf.get("Phone")
                or pf.get("Primary Phone")
                or pf.get("Mobile")
                or pf.get("Owner Phone")
            )
            if not seller_num:
                continue

            # ── Prepare payload
            drip_payload = _compact({
                "Campaign": [campaign_id],
                "Campaign Name": campaign_name,
                "Prospect": [prospect.get("id")],
                "Seller Phone Number": seller_num,
                "TextGrid Phone Number": textgrid_num,
                "Message": message_body,
                "Status": DripStatus.QUEUED.value,
                "Next Send At": now.isoformat(),
            })

            # ── Create or update queue entry
            try:
                update_record(drip_tbl, None, drip_payload)
                queued += 1
            except Exception as e:
                log.warning(f"⚠️ Failed to queue {seller_num}: {e}")

        log.info(f"✅ Queued {queued} messages for campaign → {campaign_name}")
        return queued

    except Exception as e:
        traceback.print_exc()
        log.error(f"❌ build_campaign_queue failed: {e}")
        return 0