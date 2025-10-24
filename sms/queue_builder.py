"""
📦 Queue Builder — Market-Based with Linked Templates & Numbers
──────────────────────────────────────────────────────────────
Pulls message text from linked Template + sending number from Numbers table by Market.
"""

from __future__ import annotations
import traceback, random
from datetime import datetime, timezone
from typing import Any, Dict
from sms.runtime import get_logger
from sms.datastore import CONNECTOR, list_records, update_record, _compact
from sms.airtable_schema import DripStatus

log = get_logger("queue_builder")

def _utcnow():
    return datetime.now(timezone.utc)

def _fetch_template_message(template_links: list[str]) -> str | None:
    """Fetch message text from linked template(s)."""
    if not template_links:
        return None
    try:
        templates_tbl = CONNECTOR.templates()
        for rec in list_records(templates_tbl, max_records=20):
            if rec.get("id") in template_links:
                f = rec.get("fields", {}) or {}
                return f.get("Message") or f.get("Body") or f.get("Template Text")
    except Exception:
        traceback.print_exc()
    return None

def _fetch_market_number(market_name: str) -> str | None:
    """Fetch active sending number for this market."""
    if not market_name:
        return None
    try:
        numbers_tbl = CONNECTOR.numbers()
        for rec in list_records(numbers_tbl, max_records=50):
            f = rec.get("fields", {}) or {}
            if (f.get("Market") == market_name or f.get("Market Name") == market_name) and str(f.get("Active")).lower() in {"1","true","yes"}:
                return f.get("Number") or f.get("Phone") or f.get("TextGrid Number")
    except Exception:
        traceback.print_exc()
    return None

def build_campaign_queue(campaign: Dict[str, Any], limit: int = 1000) -> int:
    """Build drip queue entries for a given campaign based on Prospects."""
    try:
        prospects_tbl = CONNECTOR.prospects()
        drip_tbl = CONNECTOR.drip_queue()

        f = campaign.get("fields", {}) or {}
        campaign_id = campaign.get("id")
        campaign_name = f.get("Campaign Name", "Unnamed Campaign")
        market = f.get("Market")
        linked_templates = f.get("Template") or f.get("Templates") or []

        # 🔹 Resolve message text from linked Template
        message_body = _fetch_template_message(linked_templates)

        # 🔹 Resolve sending number by Market
        textgrid_num = _fetch_market_number(market)

        if not message_body:
            log.warning(f"⚠️ Campaign {campaign_name} missing linked template message text.")
            return 0
        if not textgrid_num:
            log.warning(f"⚠️ Campaign {campaign_name} missing active number for Market '{market}'.")
            return 0

        prospects = list_records(prospects_tbl, max_records=limit)
        if not prospects:
            log.info("⚠️ No prospects found.")
            return 0

        queued = 0
        now = _utcnow()

        for p in prospects:
            pf = p.get("fields", {}) or {}

            # Optional: skip if not same market
            if market and str(pf.get("Market")) != str(market):
                continue

            seller_num = (
                pf.get("Phone 1 (from Linked Owner)")
                or pf.get("Seller Phone Number")
                or pf.get("Phone")
                or pf.get("Primary Phone")
            )
            if not seller_num:
                continue

            drip_payload = _compact({
                "Campaign": [campaign_id],
                "Campaign Name": campaign_name,
                "Prospect": [p.get("id")],
                "Seller Phone Number": seller_num,
                "TextGrid Phone Number": textgrid_num,
                "Message": message_body,
                "Status": DripStatus.QUEUED.value,
                "Next Send At": (now).isoformat(),
            })

            try:
                update_record(drip_tbl, None, drip_payload)
                queued += 1
            except Exception as e:
                log.warning(f"⚠️ Failed to queue message for {seller_num}: {e}")

        log.info(f"✅ Queued {queued} messages for campaign → {campaign_name}")
        return queued

    except Exception as e:
        traceback.print_exc()
        log.error(f"❌ build_campaign_queue failed: {e}")
        return 0