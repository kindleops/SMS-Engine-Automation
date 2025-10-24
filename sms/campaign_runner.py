from __future__ import annotations
import random, traceback
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from sms.runtime import get_logger, normalize_phone
from sms.datastore import CONNECTOR
from sms.airtable_schema import DripStatus

log = get_logger("campaign_runner")
QUIET_TZ = ZoneInfo("America/Chicago")

STATUS_ICON = {
    "QUEUED": "⏳",
    "Sending…": "🔄",
    "Sent": "✅",
    "Retry": "🔁",
    "Throttled": "🕒",
    "Failed": "❌",
    "DNC": "⛔",
}

# ─────────────────────────────── helpers ───────────────────────────────
def _ct_future_iso_naive(min_s: int = 2, max_s: int = 12) -> str:
    dt = datetime.now(QUIET_TZ) + timedelta(seconds=random.randint(min_s, max_s))
    return dt.replace(tzinfo=None).isoformat(timespec="seconds")

def _get_template_body(templates_table, template_id: str) -> Optional[str]:
    try:
        rec = templates_table.get(template_id)
    except Exception as e:
        log.warning(f"Template read failed: {e}")
        return None
    f = (rec or {}).get("fields", {}) or {}
    for key in ("Body", "Message", "Text", "Template", "Content"):
        v = f.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None

def _safe_create_drip(drip_table, payload: Dict[str, Any]) -> bool:
    base = {k: payload.get(k) for k in [
        "Campaign", "Seller Phone Number", "TextGrid Phone Number", "Message",
        "Market", "Property ID", "Status", "UI", "Next Send Date"
    ]}
    for key in ("Prospect", "Prospects"):
        if payload.get(key):
            base[key] = payload[key]
            try:
                drip_table.create(base)
                return True
            except Exception:
                base.pop(key, None)
    try:
        drip_table.create(base)
        return True
    except Exception as e:
        log.error(f"Airtable create failed [Drip Queue]: {e}")
        return False

# ─────────────────────────────── queue builder ───────────────────────────────
def _build_campaign_queue(campaign: Dict[str, Any], limit: int = 10000) -> int:
    drip_handle = CONNECTOR.drip_queue()
    camp_fields = (campaign or {}).get("fields", {}) or {}
    campaign_id = campaign.get("id")
    campaign_name = camp_fields.get("Name") or camp_fields.get("Campaign Name") or "Unnamed Campaign"

    templates_handle = CONNECTOR.templates().table
    template_link = (camp_fields.get("Templates") or [None])[0]
    if not template_link:
        log.warning(f"⚠️ Campaign {campaign_name} has no linked Template; skipping.")
        return 0
    body = _get_template_body(templates_handle, template_link)
    if not body:
        log.warning(f"⚠️ Campaign {campaign_name} template has no body; skipping.")
        return 0

    prospects_handle = CONNECTOR.prospects().table
    linked_prospects = camp_fields.get("Prospects") or camp_fields.get("Prospect")
    market = camp_fields.get("Market") or camp_fields.get("market") or camp_fields.get("Market Name")

    prospects: List[Dict[str, Any]] = []
    if linked_prospects:
        for pid in linked_prospects:
            try:
                rec = prospects_handle.get(pid)
                if rec:
                    prospects.append(rec)
            except Exception as e:
                log.debug(f"Prospect fetch failed ({pid}): {e}")
    elif market:
        try:
            prospects = prospects_handle.all(formula=f"{{Market}}='{market}'")
        except Exception as e:
            log.error(f"Prospects query by Market failed: {e}")
            return 0
    else:
        log.warning(f"⚠️ Campaign {campaign_name} missing Market and no linked Prospects; skipping.")
        return 0

    if not prospects:
        log.info(f"⚠️ No prospects found for campaign → {campaign_name}")
        return 0

    queued = 0
    for p in prospects[: max(1, int(limit))]:
        pf = (p or {}).get("fields", {}) or {}
        phone = pf.get("Phone 1 (from Linked Owner)") or pf.get("Phone") or pf.get("Primary Phone") or pf.get("Mobile")
        if not phone:
            continue
        normalized = normalize_phone(str(phone)) or str(phone)
        payload = {
            "Campaign": [campaign_id],
            "Prospect": [p.get("id")],
            "Prospects": [p.get("id")],
            "Seller Phone Number": normalized,
            "Message": body,
            "Market": pf.get("Market") or market,
            "Property ID": pf.get("Property ID"),
            "Status": DripStatus.QUEUED.value,
            "UI": STATUS_ICON["QUEUED"],
            "Next Send Date": _ct_future_iso_naive(2, 12),
        }
        if _safe_create_drip(drip_handle.table, payload):
            queued += 1
    log.info(f"✅ Queued {queued} messages for campaign → {campaign_name}")
    return queued

# ─────────────────────────────── campaign runner ───────────────────────────────
def _get_active_campaigns_table():
    try:
        return CONNECTOR.campaigns().table
    except Exception as e:
        log.error(f"❌ Campaigns table fetch failed: {e}")
        return None

def _fetch_active_campaigns(table) -> List[Dict[str, Any]]:
    if not table:
        return []
    try:
        # Only include campaigns explicitly marked Active, Scheduled, or Running
        formula = "OR({Status}='Active', {Status}='Scheduled', {Status}='Running')"
        active = table.all(formula=formula)
        log.info(f"📊 Found {len(active)} eligible campaigns (Active/Scheduled).")
        if not active:
            log.info("⚠️ No active campaigns matched filter.")
        else:
            for c in active:
                name = c.get("fields", {}).get("Name") or "Unnamed Campaign"
                status = c.get("fields", {}).get("Status")
                log.info(f"✅ Queued candidate → {name} [Status={status}]")
        return active
    except Exception as e:
        log.error(f"❌ Failed to fetch active campaigns: {e}")
        return []

def run_campaigns(limit="ALL", send_after_queue: bool = True) -> Dict[str, Any]:
    log.info(f"🚀 Starting Campaign Runner — limit={limit}, send_after_queue={send_after_queue}")
    total_queued = 0
    campaigns_processed = 0
    errors: List[str] = []

    campaigns_table = _get_active_campaigns_table()
    active_campaigns = _fetch_active_campaigns(campaigns_table)

    if not active_campaigns:
        log.info("⚠️ No active campaigns found.")
        return {"ok": True, "queued": 0, "note": "No active campaigns found."}

    for camp in active_campaigns:
        try:
            status = (camp.get("fields") or {}).get("Status", "")
            name = (camp.get("fields") or {}).get("Name", "Unnamed Campaign")
            if status not in ("Active", "Scheduled", "Running"):
                log.info(f"⏸️ Skipping paused/inactive campaign → {name} [Status={status}]")
                continue

            queued = _build_campaign_queue(camp, 10000 if limit == "ALL" else int(limit))
            total_queued += queued
            campaigns_processed += 1

            # update Last Run timestamp in Airtable
            try:
                campaigns_table.update(camp["id"], {"Last Run": datetime.utcnow().isoformat()})
            except Exception as e:
                log.debug(f"Last Run update failed for {name}: {e}")

        except Exception as e:
            err = f"Campaign queue failed: {e}"
            log.error(err)
            log.debug(traceback.format_exc())
            errors.append(str(e))

    result = {
        "ok": True,
        "processed": campaigns_processed,
        "queued": total_queued,
        "errors": errors,
        "timestamp": datetime.utcnow().isoformat(),
    }

    if send_after_queue:
        try:
            from sms.outbound_batcher import send_batch
            send_batch(limit=500)
            result["send_after_queue"] = True
        except Exception as e:
            result["send_after_queue"] = False
            result["send_error"] = str(e)
            log.warning(f"Send after queue failed: {e}")

    log.info(f"✅ Campaign Runner complete → {total_queued} queued across {campaigns_processed} campaigns")
    return result

async def run_campaigns_main(limit="ALL", send_after_queue=True):
    import asyncio
    return await asyncio.to_thread(run_campaigns, limit, send_after_queue)

if __name__ == "__main__":
    print(run_campaigns("ALL", True))
