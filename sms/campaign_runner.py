# sms/queue_builder.py
from __future__ import annotations

import random
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from sms.runtime import get_logger
from sms.datastore import CONNECTOR
from sms.airtable_schema import DripStatus
from sms.runtime import normalize_phone

log = get_logger("queue_builder")

# UI icons (exact set you confirmed)
STATUS_ICON = {
    "QUEUED": "⏳",
    "Sending…": "🔄",
    "Sent": "✅",
    "Retry": "🔁",
    "Throttled": "🕒",
    "Failed": "❌",
    "DNC": "⛔",
}

QUIET_TZ = ZoneInfo("America/Chicago")

# --- Helpers -------------------------------------------------

def _ct_now_iso_naive() -> str:
    """Current time in CT, naive ISO (matches how your Airtable UI fields are stored)."""
    return datetime.now(QUIET_TZ).replace(tzinfo=None).isoformat(timespec="seconds")

def _ct_future_iso_naive(min_s: int = 2, max_s: int = 12) -> str:
    dt = datetime.now(QUIET_TZ) + timedelta(seconds=random.randint(min_s, max_s))
    return dt.replace(tzinfo=None).isoformat(timespec="seconds")

def _first(items: List[Any]) -> Optional[Any]:
    return items[0] if isinstance(items, list) and items else None

def _get_campaign_fields(camp: Dict[str, Any]) -> Dict[str, Any]:
    return (camp or {}).get("fields", {}) or {}

def _get_template_body(templates_table, template_id: str) -> Optional[str]:
    """Read best-effort body from Templates. We try common field names."""
    try:
        rec = templates_table.get(template_id)
    except Exception as e:
        log.warning(f"Template read failed: {e}")
        return None
    f = (rec or {}).get("fields", {}) or {}
    # Common body field names we’ve seen
    for key in ("Body", "Message", "Text", "Template", "Content"):
        v = f.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None

def _prospect_phone(fields: Dict[str, Any]) -> Optional[str]:
    # You told me your primary column is exactly this:
    phone = fields.get("Phone 1 (from Linked Owner)")
    # fallbacks just in case some records have other columns populated
    if not phone:
        phone = fields.get("Phone") or fields.get("Primary Phone") or fields.get("Mobile")
    if not phone:
        return None
    normalized = normalize_phone(str(phone))
    return normalized or str(phone)

def _prospect_property_id(fields: Dict[str, Any]) -> Optional[str]:
    return fields.get("Property ID") or fields.get("Property") or fields.get("PropertyId")

def _prospect_market(fields: Dict[str, Any]) -> Optional[str]:
    return fields.get("Market") or fields.get("market") or fields.get("Market Name")

def _safe_create_drip(drip_table, payload: Dict[str, Any]) -> bool:
    """
    Create a Drip Queue record using only columns we *know* exist in your base:
      - Campaign (link)
      - Prospect (link)  ← optional, we try both "Prospect" and "Prospects"
      - Seller Phone Number
      - TextGrid Phone Number  ← optional (outbound batcher can auto-backfill)
      - Message  ← final body string
      - Market
      - Property ID
      - Status
      - UI
      - Next Send Date (CT naive ISO)
    If a column doesn't exist, Airtable will reject; so we defensively try with/without Prospect link.
    """
    base = {
        "Campaign": payload.get("Campaign"),
        "Seller Phone Number": payload.get("Seller Phone Number"),
        "TextGrid Phone Number": payload.get("TextGrid Phone Number"),
        "Message": payload.get("Message"),
        "Market": payload.get("Market"),
        "Property ID": payload.get("Property ID"),
        "Status": payload.get("Status"),
        "UI": payload.get("UI"),
        "Next Send Date": payload.get("Next Send Date"),
    }

    # First try with "Prospect" link (if present)
    prospect_link = payload.get("Prospect")
    if prospect_link:
        with_prospect = dict(base)
        with_prospect["Prospect"] = prospect_link
        try:
            drip_table.create(with_prospect)
            return True
        except Exception as e:
            log.debug(f"Create with Prospect failed (will retry without/with Prospects): {e}")

    # Try with "Prospects" link plural
    prospects_link = payload.get("Prospects")
    if prospects_link:
        with_prospects = dict(base)
        with_prospects["Prospects"] = prospects_link
        try:
            drip_table.create(with_prospects)
            return True
        except Exception as e:
            log.debug(f"Create with Prospects failed (will retry base only): {e}")

    # Final fallback: create without prospect link
    try:
        drip_table.create(base)
        return True
    except Exception as e:
        log.error(f"Airtable create failed [Drip Queue]: {e}")
        return False

# --- Core ----------------------------------------------------

def build_campaign_queue(campaign: Dict[str, Any], limit: int = 10000) -> int:
    """
    Queue messages from Prospects to Drip Queue.

    Priority:
      1) If Campaign has linked Prospects, use those
      2) else, filter Prospects by Campaign.Market

    Message body is resolved from the *first linked Template* on the Campaign.
    TextGrid Phone Number is *optional here* — your outbound batcher can auto-backfill by market.
    """
    drip_handle = CONNECTOR.drip_queue()
    camp_fields = _get_campaign_fields(campaign)
    campaign_id = campaign.get("id")
    campaign_name = camp_fields.get("Name") or camp_fields.get("Campaign Name") or "Unnamed Campaign"

    # Template → body
    templates_handle = CONNECTOR.templates().table
    template_link = _first(camp_fields.get("Templates") or [])
    if not template_link:
        log.warning(f"⚠️ Campaign {campaign_name} has no linked Template; skipping.")
        return 0
    body = _get_template_body(templates_handle, template_link)
    if not body:
        log.warning(f"⚠️ Campaign {campaign_name} template has no body; skipping.")
        return 0

    # Prospects source
    prospects_handle = CONNECTOR.prospects().table
    linked_prospects = camp_fields.get("Prospects") or camp_fields.get("Prospect")
    market = camp_fields.get("Market") or camp_fields.get("market") or camp_fields.get("Market Name")

    prospects: List[Dict[str, Any]] = []

    if linked_prospects:
        # hydrate linked prospects
        ids = [pid for pid in linked_prospects if isinstance(pid, str)]
        for pid in ids:
            try:
                rec = prospects_handle.get(pid)
                if rec:
                    prospects.append(rec)
            except Exception as e:
                log.debug(f"Prospect fetch failed ({pid}): {e}")
    else:
        # fallback by market
        if not market:
            log.warning(f"⚠️ Campaign {campaign_name} missing Market and no linked Prospects; skipping.")
            return 0
        try:
            prospects = prospects_handle.all(formula=f"{{Market}}='{market}'")
        except Exception as e:
            log.error(f"Prospects query by Market failed: {e}")
            return 0

    if not prospects:
        log.info(f"⚠️ No prospects found for campaign → {campaign_name}")
        return 0

    # Optional TextGrid number is *not required* (outbound will backfill).
    # If you *do* want to set it here from the Numbers table by market,
    # add that logic; otherwise leave it blank.
    textgrid_number: Optional[str] = None

    queued = 0
    for p in prospects[: max(1, int(limit))]:
        pf = (p or {}).get("fields", {}) or {}
        phone = _prospect_phone(pf)
        if not phone:
            continue

        property_id = _prospect_property_id(pf)
        p_market = _prospect_market(pf) or market

        payload = {
            "Campaign": [campaign_id] if campaign_id else None,
            # We try both singular and plural; _safe_create_drip() will handle gracefully
            "Prospect": [p.get("id")] if p.get("id") else None,
            "Prospects": [p.get("id")] if p.get("id") else None,
            "Seller Phone Number": phone,
            # Leave blank → outbound_batcher can AUTO_BACKFILL_FROM_NUMBER by Market
            "TextGrid Phone Number": textgrid_number,
            "Message": body,
            "Market": p_market,
            "Property ID": property_id,
            "Status": DripStatus.QUEUED.value,   # exact status token your system expects
            "UI": STATUS_ICON["QUEUED"],         # ⏳
            "Next Send Date": _ct_future_iso_naive(2, 12),
        }

        if _safe_create_drip(drip_handle.table, payload):
            queued += 1

    log.info(f"✅ Queued {queued} messages for campaign → {campaign_name}")
    return queued
