"""
Campaign Runner — REI SMS Engine
------------------------------------------------
Scans Airtable Campaigns, auto-activates scheduled ones,
queues outbound messages into the Drip Queue,
and triggers send batches if `send_after_queue=True`.
"""

from __future__ import annotations
import traceback
from datetime import datetime, timezone
from typing import Any, Dict

from sms.tables import get_campaigns, get_leads, get_drip
from sms.runtime import get_logger
from sms.templates import get_template
from sms.textgrid_sender import queue_message

log = get_logger("campaign_runner")


# ───────────────────────────────────────────────
def _within_window(fields: dict) -> bool:
    """Return True if campaign is within its Start/End window."""
    now = datetime.now(timezone.utc)
    try:
        start = fields.get("Start Date")
        end = fields.get("End Date")
        if start and datetime.fromisoformat(start) > now:
            return False
        if end and datetime.fromisoformat(end) < now:
            return False
        return True
    except Exception:
        return True


# ───────────────────────────────────────────────
def _queue_for_campaign(camp: dict, limit: int) -> int:
    """Queue messages for leads under a campaign."""
    cid = camp.get("id")
    f = camp.get("fields", {})
    name = f.get("Campaign Name", cid)

    leads_tbl = get_leads()
    drip_tbl = get_drip()
    if not (leads_tbl and drip_tbl):
        log.warning(f"⚠️ Missing Airtable tables for campaign {name}")
        return 0

    linked_leads = f.get("Leads") or []
    if not linked_leads:
        log.info(f"ℹ️ No linked leads for campaign {name}")
        return 0

    queued = 0
    for lead_id in linked_leads[:limit]:
        try:
            lead = leads_tbl.get(lead_id)
            lf = lead.get("fields", {})
            msg = get_template("intro", lf)
            queue_message(lf, msg, campaign_id=cid)
            queued += 1
        except Exception as e:
            log.warning(f"⚠️ Lead queue failed in {name}: {e}")
            traceback.print_exc()
    log.info(f"✅ Queued {queued} messages for {name}")
    return queued


# ───────────────────────────────────────────────
def run_campaigns(limit: Any = 50, send_after_queue: bool = True) -> Dict[str, Any]:
    """
    Auto-activate scheduled campaigns, queue active ones, and optionally trigger send_batch().
    """
    try:
        camp_tbl = get_campaigns()
        if not camp_tbl:
            return {"ok": False, "error": "Campaigns table unavailable"}

        campaigns = camp_tbl.all()
        if not campaigns:
            return {"ok": False, "error": "No campaigns found"}

        total_processed = 0

        for camp in campaigns:
            f = camp.get("fields", {})
            cid = camp.get("id")
            status = str(f.get("Status", "")).lower()

            # ── Activate scheduled ones ──
            if status == "scheduled" and _within_window(f):
                try:
                    camp_tbl.update(cid, {"Status": "Active"})
                    log.info(f"⏰ Activated scheduled campaign → {f.get('Campaign Name')}")
                    status = "active"
                except Exception as e:
                    log.warning(f"⚠️ Failed to activate campaign {cid}: {e}")

            # ── Process active campaigns ──
            if status in ("active", "running") and _within_window(f):
                total_processed += _queue_for_campaign(camp, limit)

        # ── Trigger outbound send batch ──
        if send_after_queue:
            try:
                from sms.outbound_batcher import send_batch
                send_batch(limit=limit)
            except Exception as e:
                log.warning(f"⚠️ send_batch failed: {e}")
                traceback.print_exc()

        return {"ok": True, "processed": total_processed, "queued": total_processed}

    except Exception as e:
        traceback.print_exc()
        return {"ok": False, "error": str(e)}


def run_campaigns_sync(limit: Any = 50, send_after_queue: bool = True) -> Dict[str, Any]:
    return run_campaigns(limit=limit, send_after_queue=send_after_queue)


if __name__ == "__main__":
    print(run_campaigns(limit=3))
