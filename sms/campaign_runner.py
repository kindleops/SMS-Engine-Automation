# sms/campaign_runner.py
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
    """Pull the text body from a linked template."""
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


def _render_template_body(template: str, prospect_fields: Dict[str, Any], campaign_fields: Dict[str, Any]) -> str:
    """Render placeholders in message body using both Prospect + Campaign fields."""
    body = str(template)
    # attempt to split full name if only one field is present
    full_name = (
        prospect_fields.get("Owner Name")
        or prospect_fields.get("Full Name")
        or prospect_fields.get("Name")
        or ""
    )
    first = prospect_fields.get("Owner First") or prospect_fields.get("First Name") or (full_name.split(" ")[0] if full_name else "")
    last = prospect_fields.get("Owner Last") or prospect_fields.get("Last Name") or (" ".join(full_name.split(" ")[1:]) if full_name and len(full_name.split(" ")) > 1 else "")
    replacements = {
        "{First}": first,
        "{Last}": last,
        "{Address}": prospect_fields.get("Property Address") or prospect_fields.get("Address") or "",
        "{Property City}": prospect_fields.get("Property City") or prospect_fields.get("City") or "",
        "{Market}": campaign_fields.get("Market") or "",
    }
    for key, val in replacements.items():
        body = body.replace(key, str(val).strip())
    return body.strip()


def _safe_create_drip(drip_table, payload: Dict[str, Any]) -> bool:
    """Create Drip Queue record safely with fallback logic."""
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


# ─────────────────────────────── campaign queue builder ───────────────────────────────
def _build_campaign_queue(campaign: Dict[str, Any], limit: int = 10000) -> int:
    drip_handle = CONNECTOR.drip_queue()
    camp_fields = (campaign or {}).get("fields", {}) or {}
    campaign_id = campaign.get("id")
    campaign_name = camp_fields.get("Name") or camp_fields.get("Campaign Name") or "Unnamed Campaign"
    status = str(camp_fields.get("Status") or "").strip().lower()

    # 🧠 Skip inactive campaigns
    if status in ("paused", "inactive", "stopped", "complete", "completed"):
        log.info(f"⏸️ Skipping paused/inactive campaign → {campaign_name}")
        return 0

    templates_handle = CONNECTOR.templates().table
    template_link = (camp_fields.get("Templates") or [None])[0]
    if not template_link:
        log.warning(f"⚠️ Campaign {campaign_name} has no linked Template; skipping.")
        return 0

    body = _get_template_body(templates_handle, template_link)
    if not body:
        log.warning(f"⚠️ Campaign {campaign_name} template has no body; skipping.")
        return 0

    # 🔗 Only linked prospects — no market fallback
    prospects_handle = CONNECTOR.prospects().table
    linked_prospects = camp_fields.get("Prospects") or camp_fields.get("Prospect")
    if not linked_prospects:
        log.warning(f"⚠️ Campaign {campaign_name} has no linked Prospects; skipping.")
        return 0

    # 📞 Use campaign TextGrid number for outbound
    textgrid_number = camp_fields.get("TextGrid Phone Number") or None
    if not textgrid_number:
        log.warning(f"⚠️ Campaign {campaign_name} missing TextGrid number — will still queue, outbound will backfill.")

    # hydrate linked prospects
    prospects: List[Dict[str, Any]] = []
    for pid in linked_prospects:
        try:
            rec = prospects_handle.get(pid)
            if rec:
                prospects.append(rec)
        except Exception as e:
            log.debug(f"Prospect fetch failed ({pid}): {e}")

    if not prospects:
        log.info(f"⚠️ No valid prospect records found for campaign → {campaign_name}")
        return 0

    queued = 0
    for p in prospects[: max(1, int(limit))]:
        pf = (p or {}).get("fields", {}) or {}
        phone = pf.get("Phone 1 (from Linked Owner)") or pf.get("Phone") or pf.get("Primary Phone") or pf.get("Mobile")
        if not phone:
            continue
        normalized = normalize_phone(str(phone)) or str(phone)
        msg_body = _render_template_body(body, pf, camp_fields)

        payload = {
            "Campaign": [campaign_id],
            "Prospect": [p.get("id")],
            "Prospects": [p.get("id")],
            "Seller Phone Number": normalized,
            "TextGrid Phone Number": textgrid_number,
            "Message": msg_body,
            "Market": camp_fields.get("Market"),
            "Property ID": pf.get("Property ID"),
            "Status": DripStatus.QUEUED.value,
            "UI": STATUS_ICON["QUEUED"],
            "Next Send Date": _ct_future_iso_naive(2, 12),
        }

        if _safe_create_drip(drip_handle.table, payload):
            queued += 1

    log.info(f"✅ Queued {queued} messages for campaign → {campaign_name}")
    return queued


# ─────────────────────────────── main runner ───────────────────────────────
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
        # Only fetch active campaigns (exclude paused/completed)
        return table.all(formula="NOT({Status}='Paused')")
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
            queued = _build_campaign_queue(camp, 10000 if limit == "ALL" else int(limit))
            total_queued += queued
            campaigns_processed += 1
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
