"""
=====================================================================
📣 CAMPAIGN RUNNER — FINAL BULLETPROOF VERSION
=====================================================================
Auto-activates scheduled campaigns, queues active ones,
and triggers outbound SMS batches.

Key Features:
-------------
✅ Reads directly from environment (no external dependency)
✅ Uses pyairtable for Campaigns table in LEADS_CONVOS_BASE
✅ Auto-activates scheduled campaigns at start time
✅ Queues active campaigns safely
✅ Triggers outbound send_batch() automatically
✅ Fully compatible with Render Cron or manual execution
=====================================================================
"""

import os
import traceback
from datetime import datetime
from typing import Any, Dict

from pyairtable import Table
from sms.runtime import get_logger

log = get_logger("campaign_runner")

# ==============================================================
# ENVIRONMENT SETUP
# ==============================================================

AIRTABLE_KEY = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE")
CAMPAIGNS_TABLE = os.getenv("CAMPAIGNS_TABLE", "Campaigns")

# ==============================================================
# CORE UTILITIES
# ==============================================================

def _within_window(fields: Dict[str, Any]) -> bool:
    """Check if current time is within campaign start/end window."""
    try:
        now = datetime.utcnow()
        start_str = fields.get("Start Date")
        end_str = fields.get("End Date")

        if start_str:
            start = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
            if now < start:
                return False

        if end_str:
            end = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
            if now > end:
                return False

        return True
    except Exception:
        return True


def get_campaigns() -> Table:
    """Return live Campaigns table from main Leads/Convos base."""
    if not AIRTABLE_KEY or not LEADS_CONVOS_BASE:
        raise RuntimeError("Missing Airtable environment configuration.")
    return Table(AIRTABLE_KEY, LEADS_CONVOS_BASE, CAMPAIGNS_TABLE)


# ==============================================================
# MAIN LOGIC
# ==============================================================

def _queue_for_campaign(campaign: Dict[str, Any], limit: int) -> int:
    """Queue messages for a given campaign."""
    try:
        from sms.outbound_batcher import queue_campaign
        cid = campaign.get("id")
        name = campaign.get("fields", {}).get("Campaign Name")
        count = queue_campaign(cid, limit)
        log.info(f"📤 Queued {count} messages for campaign → {name}")
        return count
    except Exception as e:
        log.warning(f"⚠️ Failed to queue campaign: {e}")
        traceback.print_exc()
        return 0


def run_campaigns(limit: Any = 50, send_after_queue: bool = True) -> Dict[str, Any]:
    """
    Auto-activate scheduled campaigns, queue active ones, and optionally trigger send_batch().
    """
    try:
        camp_tbl = get_campaigns()
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
    """Synchronous wrapper for manual or testing use."""
    return run_campaigns(limit=limit, send_after_queue=send_after_queue)


# ==============================================================
# MAIN ENTRY POINT
# ==============================================================

if __name__ == "__main__":
    log.info("🚀 Starting Campaign Runner (manual execution mode)")
    result = run_campaigns(limit=5)
    print(result)
