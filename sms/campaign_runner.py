from __future__ import annotations
import random, re, traceback
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
def _ct_future_iso_naive(min_s: int = 2, max_s: int = 15) -> str:
    dt = datetime.now(QUIET_TZ) + timedelta(seconds=random.randint(min_s, max_s))
    return dt.replace(tzinfo=None).isoformat(timespec="seconds")

def _get_template_body(templates_table, template_id: str) -> Optional[str]:
    try:
        rec = templates_table.get(template_id)
        f = (rec or {}).get("fields", {}) or {}
        for key in ("Body", "Message", "Text", "Template", "Content"):
            v = f.get(key)
            if isinstance(v, str) and v.strip():
                return v.strip()
    except Exception as e:
        log.warning(f"Template read failed: {e}")
    return None

def _robust_create_drip(drip_tbl, payload: Dict[str, Any]) -> bool:
    """Attempt creation, retry once if market select fails."""
    try:
        drip_tbl.create(payload)
        return True
    except Exception as e:
        msg = str(e)
        if "INVALID_MULTIPLE_CHOICE_OPTIONS" in msg:
            mkt = payload.pop("Market", None)
            log.warning(f"⚠️ Market select rejected ({mkt}); retrying without Market.")
            try:
                drip_tbl.create(payload)
                return True
            except Exception as e2:
                log.error(f"Airtable create failed [Drip Queue]: {e2}")
                return False
        log.error(f"Airtable create failed [Drip Queue]: {e}")
        return False

def _eligible_campaigns(table):
    """Return campaigns ready to run (Active or Scheduled and past Start Time)."""
    now = datetime.now(QUIET_TZ).isoformat(timespec="seconds")
    formula = (
        "OR("
        "{Status}='Active',"
        "AND({Status}='Scheduled', {Start Time}!='', IS_BEFORE({Start Time}, '" + now + "'))"
        ")"
    )
    try:
        return table.all(formula=formula, page_size=100)
    except Exception as e:
        log.error(f"❌ Failed to fetch campaigns: {e}")
        return []

# ─────────────────────────────── queue builder ───────────────────────────────
def _build_campaign_queue(campaign: Dict[str, Any], limit: int = 10000) -> int:
    camp_fields = (campaign or {}).get("fields", {}) or {}
    campaign_id = campaign.get("id")
    campaign_name = camp_fields.get("Name") or "Unnamed Campaign"
    log.info(f"➡️ Queuing campaign: {campaign_name}")

    # Linked prospects
    prospects_handle = CONNECTOR.prospects().table
    linked_prospects = camp_fields.get("Prospects") or []
    if not linked_prospects:
        log.warning(f"⚠️ Campaign {campaign_name} has no linked prospects; skipping.")
        return 0

    # Templates
    templates_handle = CONNECTOR.templates().table
    template_links = camp_fields.get("Templates") or camp_fields.get("Template") or []
    if not template_links:
        log.warning(f"⚠️ Campaign {campaign_name} has no linked templates; skipping.")
        return 0

    # Fetch all template bodies
    templates = []
    for tid in template_links:
        body = _get_template_body(templates_handle, tid)
        if body:
            templates.append((tid, body))
    if not templates:
        log.warning(f"⚠️ Campaign {campaign_name} templates invalid; skipping.")
        return 0

    drip_tbl = CONNECTOR.drip_queue().table
    numbers_tbl = CONNECTOR.numbers().table

    # Fetch TextGrid numbers for this campaign’s market
    camp_market = camp_fields.get("Market") or ""
    tg_numbers = []
    if camp_market:
        try:
            tg_numbers = [
                rec.get("fields", {}).get("Phone Number")
                for rec in numbers_tbl.all(formula=f"{{Market}}='{camp_market}'")
                if rec.get("fields", {}).get("Phone Number")
            ]
        except Exception as e:
            log.warning(f"⚠️ Could not fetch TextGrid numbers: {e}")
    if not tg_numbers:
        log.warning(f"⚠️ No TextGrid numbers found for {camp_market}")

    queued = 0
    for idx, pid in enumerate(linked_prospects[:limit]):
        try:
            p = prospects_handle.get(pid)
            pf = (p or {}).get("fields", {}) or {}
            phone = pf.get("Phone 1 (from Linked Owner)") or pf.get("Phone") or pf.get("Mobile")
            if not phone:
                continue
            phone_norm = normalize_phone(str(phone)) or str(phone)

            # 🔹 Extract clean first name
            full_name = (
                pf.get("Owner Name")
                or pf.get("Owner")
                or pf.get("Name")
                or ""
            ).strip()
            first_name = ""
            if full_name:
                parts = re.split(r"\s+", full_name)
                if parts:
                    raw_first = parts[0]
                    first_name = re.sub(r"[^A-Za-z]", "", raw_first).title()

            # 🔹 Alternate templates round-robin
            tmpl_id, tmpl_body = templates[idx % len(templates)]

            # 🔹 Replace placeholders
            msg = (
                tmpl_body
                .replace("{First}", first_name)
                .replace("{Address}", pf.get("Property Address") or "")
                .replace("{Property City}", pf.get("Property City") or "")
            )

            # 🔹 Assign rotating TextGrid number
            tg_number = tg_numbers[idx % len(tg_numbers)] if tg_numbers else None

            # 🔹 Determine Market from prospect (to sync)
            prospect_market = pf.get("Market") or camp_market
            if prospect_market and not "," in prospect_market:
                # normalize to exact option style: e.g. "Minneapolis, MN"
                if "Minneapolis" in prospect_market:
                    prospect_market = "Minneapolis, MN"

            payload: Dict[str, Any] = {
                "Campaign": [campaign_id],
                "Prospect": [pid],
                "Seller Phone Number": phone_norm,
                "TextGrid Phone Number": tg_number,
                "Message": msg,
                "Market": prospect_market,
                "Property ID": pf.get("Property ID") or pf.get("Property") or pf.get("PropertyId"),
                "Status": DripStatus.QUEUED.value,
                "UI": STATUS_ICON["QUEUED"],
                "Next Send Date": _ct_future_iso_naive(3, 25),
                "Template": [tmpl_id],
            }

            if _robust_create_drip(drip_tbl, payload):
                queued += 1
        except Exception as e:
            log.error(f"Queue insert failed for {campaign_name} ({pid}): {e}")
            log.debug(traceback.format_exc())
    log.info(f"✅ Queued {queued} messages for campaign → {campaign_name}")
    return queued

# ─────────────────────────────── main runner ───────────────────────────────
def run_campaigns(limit="ALL", send_after_queue: bool = True) -> Dict[str, Any]:
    log.info(f"🚀 Campaign Runner — limit={limit}, send_after_queue={send_after_queue}")

    total_queued = 0
    total_processed = 0
    errors: List[str] = []

    try:
        camp_tbl = CONNECTOR.campaigns().table
        campaigns = _eligible_campaigns(camp_tbl)
    except Exception as e:
        log.error(f"❌ Failed to fetch campaigns: {e}")
        return {"ok": False, "error": str(e)}

    if not campaigns:
        log.info("⚠️ No due/active campaigns found.")
        return {"ok": True, "queued": 0}

    per_camp_limit = 10000 if limit == "ALL" else int(limit)
    for camp in campaigns:
        try:
            q = _build_campaign_queue(camp, per_camp_limit)
            total_queued += q
            total_processed += 1
        except Exception as e:
            errors.append(str(e))
            log.error(f"❌ Campaign failed: {e}")
            log.debug(traceback.format_exc())

    result = {
        "ok": True,
        "processed": total_processed,
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
            log.warning(f"⚠️ Send batch failed: {e}")
            result["send_after_queue"] = False
            result["send_error"] = str(e)

    log.info(f"🏁 Done — {total_queued} queued across {total_processed} campaigns.")
    return result

if __name__ == "__main__":
    print(run_campaigns("ALL", True))
