# sms/campaign_runner.py
from __future__ import annotations

import random
import traceback
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from sms.runtime import get_logger, normalize_phone
from sms.datastore import CONNECTOR
from sms.airtable_schema import DripStatus

log = get_logger("campaign_runner")
QUIET_TZ = ZoneInfo("America/Chicago")

# UI icons you specified
STATUS_ICON = {
    "QUEUED": "⏳",
    "Sending…": "🔄",
    "Sent": "✅",
    "Retry": "🔁",
    "Throttled": "🕒",
    "Failed": "❌",
    "DNC": "⛔",
}

# ─────────────────────────────── time helpers ───────────────────────────────

def _ct_future_iso_naive(min_s: int = 2, max_s: int = 12) -> str:
    """Return a Central Time naive ISO string a few seconds in the future."""
    dt = datetime.now(QUIET_TZ) + timedelta(seconds=random.randint(min_s, max_s))
    return dt.replace(tzinfo=None).isoformat(timespec="seconds")


# ─────────────────────────────── Airtable helpers ───────────────────────────

def _get_campaigns_table():
    try:
        return CONNECTOR.campaigns().table
    except Exception as e:
        log.error(f"❌ Campaigns table fetch failed: {e}")
        return None

def _get_templates_table():
    try:
        return CONNECTOR.templates().table
    except Exception as e:
        log.error(f"❌ Templates table fetch failed: {e}")
        return None

def _get_prospects_table():
    try:
        return CONNECTOR.prospects().table
    except Exception as e:
        log.error(f"❌ Prospects table fetch failed: {e}")
        return None

def _get_numbers_table():
    try:
        return CONNECTOR.numbers().table
    except Exception as e:
        log.error(f"❌ Numbers table fetch failed: {e}")
        return None

def _get_drip_table():
    try:
        return CONNECTOR.drip_queue().table
    except Exception as e:
        log.error(f"❌ Drip Queue table fetch failed: {e}")
        return None


# ─────────────────────────────── business helpers ───────────────────────────

def _activate_scheduled_campaigns(campaigns_tbl) -> int:
    """
    Flip campaigns from 'Scheduled' -> 'Active' when {Start Time} <= now.
    Returns number of campaigns activated.
    """
    if not campaigns_tbl:
        return 0

    activated = 0
    now_ct = datetime.now(QUIET_TZ)

    try:
        # Pull scheduled that actually have a Start Time value
        scheduled = campaigns_tbl.all(formula="AND({Status}='Scheduled', {Start Time}!='')")
    except Exception as e:
        log.error(f"Failed to list scheduled campaigns: {e}")
        return 0

    for camp in (scheduled or []):
        fields = camp.get("fields", {}) or {}
        start_raw = fields.get("Start Time")
        if not start_raw:
            continue
        try:
            # Accept both naive and offset ISO
            try:
                start_dt = datetime.fromisoformat(str(start_raw))
            except Exception:
                start_dt = now_ct  # be lenient if malformed; let it activate

            # If the stored value has no tz, treat it as CT
            if start_dt.tzinfo is None:
                start_dt = start_dt.replace(tzinfo=QUIET_TZ)

            if start_dt <= now_ct:
                campaigns_tbl.update(camp["id"], {"Status": "Active"})
                activated += 1
                log.info(f"✅ Activated campaign → {fields.get('Name') or camp.get('id')}")
        except Exception as e:
            log.warning(f"Activation check failed for {fields.get('Name')}: {e}")

    return activated


def _fetch_active_campaigns(campaigns_tbl) -> List[Dict[str, Any]]:
    """
    Only campaigns with Status = 'Active' run.
    """
    if not campaigns_tbl:
        return []
    try:
        return campaigns_tbl.all(formula="{Status}='Active'")
    except Exception as e:
        log.error(f"❌ Failed to fetch active campaigns: {e}")
        return []


def _get_template_body(templates_tbl, template_id: str) -> Optional[str]:
    """
    Pull a text body from common template fields.
    """
    if not (templates_tbl and template_id):
        return None
    try:
        rec = templates_tbl.get(template_id)
        f = (rec or {}).get("fields", {}) or {}
        for key in ("Body", "Message", "Text", "Template", "Content"):
            v = f.get(key)
            if isinstance(v, str) and v.strip():
                return v.strip()
    except Exception as e:
        log.warning(f"Template read failed ({template_id}): {e}")
    return None


def _resolve_textgrid_number(numbers_tbl, market: Optional[str]) -> Optional[str]:
    """
    Get TextGrid number by market. Tries common column names.
    """
    if not (numbers_tbl and market):
        return None
    try:
        # simple exact match on single select text
        recs = numbers_tbl.all(formula=f"{{Market}}='{market}'")
        if recs:
            fields = recs[0].get("fields", {}) or {}
            return (
                fields.get("TextGrid Number")
                or fields.get("TextGrid Phone Number")
                or fields.get("Number")
                or fields.get("Phone")
            )
    except Exception as e:
        log.warning(f"TextGrid lookup failed for market '{market}': {e}")
    return None


def _fill_placeholders(template_text: str, pf: Dict[str, Any]) -> str:
    """
    Replace {Placeholders} with prospect field values.
    We do a simple .replace for any string field present in pf.
    Also include a few common synthetic aliases.
    """
    if not template_text:
        return template_text

    # Derive a few common aliases
    derived: Dict[str, str] = {}
    # First
    derived["First"] = (
        pf.get("First")
        or pf.get("First Name")
        or (pf.get("Owner First Name") if isinstance(pf.get("Owner First Name"), str) else None)
        or (pf.get("Name").split(" ")[0] if isinstance(pf.get("Name"), str) and pf.get("Name") else None)
        or ""
    )
    # Address
    derived["Address"] = pf.get("Address") or pf.get("Property Address") or pf.get("Street Address") or ""
    # Property City
    derived["Property City"] = pf.get("Property City") or pf.get("City") or pf.get("Mailing City") or ""

    # Build source map: raw fields + derived aliases
    source: Dict[str, str] = {}
    for k, v in pf.items():
        if isinstance(v, str):
            source[k] = v
    for k, v in derived.items():
        source[k] = v if isinstance(v, str) else ""

    msg = template_text
    # Replace any {Key} present in source
    for k, v in source.items():
        msg = msg.replace(f"{{{k}}}", v or "")

    return msg


def _drip_has_existing(drip_tbl, campaign_id: str, prospect_id: str, template_id: Optional[str]) -> bool:
    """
    Best-effort dedupe: if a row already exists for (Campaign, Prospect[, Template]),
    skip creating another. For linked-record fields, use SEARCH on ARRAYJOIN.
    """
    if not drip_tbl:
        return False
    try:
        parts = [
            f"SEARCH('{campaign_id}', ARRAYJOIN({{Campaign}}))",
            f"SEARCH('{prospect_id}', ARRAYJOIN({{Prospect}}))",
        ]
        if template_id:
            parts.append(f"SEARCH('{template_id}', ARRAYJOIN({{Template}}))")
        formula = f"AND({', '.join(parts)})"
        recs = drip_tbl.all(formula=formula, max_records=1)
        return bool(recs)
    except Exception as e:
        log.debug(f"Dedupe check failed (camp={campaign_id}, prospect={prospect_id}): {e}")
        return False


# ─────────────────────────────── core queue builder ──────────────────────────

def _build_campaign_queue(campaign: Dict[str, Any], limit: int | str = "ALL") -> int:
    """
    Queue messages for an *Active* campaign using its **linked Prospects only**.
    - Template body is filled with Prospect placeholders.
    - Market from Prospect fills Drip Queue (not used for filtering).
    - TextGrid number pulled from Numbers by market.
    - Drip Queue links: Campaign, Prospect, Template.
    - Dedupes by (Campaign, Prospect, Template).
    """
    drip_tbl = _get_drip_table()
    templates_tbl = _get_templates_table()
    prospects_tbl = _get_prospects_table()
    numbers_tbl = _get_numbers_table()

    if not (drip_tbl and templates_tbl and prospects_tbl):
        log.error("Required tables unavailable for queue build.")
        return 0

    camp_fields = (campaign or {}).get("fields", {}) or {}
    campaign_id = campaign.get("id")
    campaign_name = camp_fields.get("Name") or "Unnamed Campaign"

    # Template (require exactly one; we use the first if multiple)
    template_ids = camp_fields.get("Templates") or []
    template_id = template_ids[0] if template_ids else None
    if not template_id:
        log.warning(f"⚠️ Campaign {campaign_name} has no linked Template; skipping.")
        return 0

    body = _get_template_body(templates_tbl, template_id)
    if not body:
        log.warning(f"⚠️ Campaign {campaign_name} template has no body; skipping.")
        return 0

    # Prospects (linked only; no market fallback)
    linked_prospects: List[str] = camp_fields.get("Prospects") or []
    if not linked_prospects:
        log.warning(f"⚠️ Campaign {campaign_name} has 0 linked Prospects; skipping.")
        return 0

    # Cap
    if limit == "ALL":
        target_ids = linked_prospects
    else:
        try:
            n = max(1, int(limit))
            target_ids = linked_prospects[:n]
        except Exception:
            target_ids = linked_prospects

    queued = 0

    for pid in target_ids:
        try:
            prec = prospects_tbl.get(pid)
            pf = (prec or {}).get("fields", {}) or {}

            # Seller phone
            phone_raw = (
                pf.get("Phone 1 (from Linked Owner)")
                or pf.get("Phone")
                or pf.get("Primary Phone")
                or pf.get("Mobile")
                or ""
            )
            phone = normalize_phone(str(phone_raw)) or str(phone_raw)
            if not phone:
                continue

            # Message with placeholders
            filled_msg = _fill_placeholders(body, pf)

            # Market (display-only) + Property ID
            market = pf.get("Market") or pf.get("market") or pf.get("Market Name")
            property_id = pf.get("Property ID") or pf.get("Property") or pf.get("PropertyId")

            # TextGrid number from Numbers by market
            textgrid_number = _resolve_textgrid_number(numbers_tbl, market)

            # Dedupe (Campaign + Prospect + Template)
            if campaign_id and _drip_has_existing(drip_tbl, campaign_id, pid, template_id):
                # Already queued/sent before — skip
                continue

            payload = {
                "Campaign": [campaign_id] if campaign_id else None,
                "Prospect": [pid],
                "Template": [template_id],
                "Seller Phone Number": phone,
                "TextGrid Phone Number": textgrid_number,
                "Message": filled_msg,
                "Market": market,
                "Property ID": property_id,
                "Status": DripStatus.QUEUED.value,   # your enum token
                "UI": STATUS_ICON["QUEUED"],         # ⏳
                "Next Send Date": _ct_future_iso_naive(2, 12),
            }

            drip_tbl.create(payload)
            queued += 1

        except Exception as e:
            log.error(f"Queue insert failed for {campaign_name} (prospect {pid}): {e}")
            log.debug(traceback.format_exc())

    log.info(f"✅ Queued {queued} messages for campaign → {campaign_name}")
    return queued


# ─────────────────────────────── public runner ───────────────────────────────

def run_campaigns(limit: int | str = "ALL", send_after_queue: bool = True) -> Dict[str, Any]:
    """
    Main entry (used by FastAPI endpoint and CLI).
    1) Activate any Scheduled campaigns whose Start Time has arrived.
    2) Fetch Active campaigns.
    3) Build queues (deduped).
    4) Optionally kick outbound sends immediately.
    """
    log.info(f"🚀 Starting Campaign Runner — limit={limit}, send_after_queue={send_after_queue}")

    campaigns_tbl = _get_campaigns_table()
    if not campaigns_tbl:
        return {"ok": False, "error": "Campaigns table unavailable"}

    try:
        activated = _activate_scheduled_campaigns(campaigns_tbl)
    except Exception as e:
        log.warning(f"Scheduled activation pass failed: {e}")
        activated = 0

    active_campaigns = _fetch_active_campaigns(campaigns_tbl)
    if not active_campaigns:
        log.info("⚠️ No active campaigns found.")
        return {"ok": True, "queued": 0, "activated": activated, "note": "No active campaigns found."}

    total_queued = 0
    processed = 0
    errors: List[str] = []

    for camp in active_campaigns:
        try:
            q = _build_campaign_queue(camp, limit)
            total_queued += q
            processed += 1
        except Exception as e:
            errors.append(str(e))
            log.error(f"Campaign queue failed: {e}")
            log.debug(traceback.format_exc())

    result: Dict[str, Any] = {
        "ok": True,
        "activated": activated,
        "processed": processed,
        "queued": total_queued,
        "errors": errors,
        "timestamp": datetime.utcnow().isoformat(),
    }

    if send_after_queue and total_queued > 0:
        try:
            from sms.outbound_batcher import send_batch
            send_batch(limit=500)
            result["send_after_queue"] = True
        except Exception as e:
            log.warning(f"Send after queue failed: {e}")
            result["send_after_queue"] = False
            result["send_error"] = str(e)

    log.info(f"✅ Campaign Runner complete → {total_queued} queued across {processed} campaigns (activated {activated})")
    return result


# ─────────────────────────────── async shim / CLI ───────────────────────────

async def run_campaigns_main(limit: int | str = "ALL", send_after_queue: bool = True):
    import asyncio
    return await asyncio.to_thread(run_campaigns, limit, send_after_queue)

if __name__ == "__main__":
    print(run_campaigns("ALL", True))
