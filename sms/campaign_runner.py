"""
⚡ Campaign Runner — Final 2025 Build
────────────────────────────────────────────
• Reads campaigns from Campaign Control Base
• Queues messages into Drip Queue in Leads/Convos Base
• Logs metrics in Performance Base
• Auto-activates scheduled campaigns
• Handles quiet hours + time window + error isolation
"""

from __future__ import annotations
import traceback
from datetime import datetime, timezone

from sms.runtime import get_logger
from sms.airtable import get_table
from sms.outbound_batcher import send_batch
from sms.metrics_tracker import record_campaign_metric

log = get_logger("campaign_runner")


# ============================================================
# Helpers for multi-base access
# ============================================================
def get_campaigns():
    """Return Campaigns table from Campaign Control base."""
    try:
        tbl = get_table("CAMPAIGN_CONTROL_BASE", "Campaigns")
        log.info("✅ Connected to Campaign Control base → Campaigns")
        return tbl
    except Exception as e:
        log.error(f"❌ Failed to connect to Campaign Control base: {e}")
        return None


def get_drip_queue():
    """Return Drip Queue table from Leads/Convos base."""
    try:
        tbl = get_table("LEADS_CONVOS_BASE", "Drip Queue")
        log.info("✅ Connected to Leads/Convos base → Drip Queue")
        return tbl
    except Exception as e:
        log.error(f"❌ Failed to connect to Leads/Convos base: {e}")
        return None


def get_performance():
    """Return KPI/Performance table."""
    try:
        tbl = get_table("PERFORMANCE_BASE", "KPIs")
        log.info("✅ Connected to Performance base → KPIs")
        return tbl
    except Exception as e:
        log.error(f"⚠️ Performance logging unavailable: {e}")
        return None


# ============================================================
# Time & window helpers
# ============================================================
def _within_window(fields: dict) -> bool:
    """Check if current UTC time is inside Start/End window."""
    now = datetime.now(timezone.utc)
    start = fields.get("Start Date")
    end = fields.get("End Date")

    try:
        start_dt = datetime.fromisoformat(start.replace("Z", "+00:00")) if start else None
        end_dt = datetime.fromisoformat(end.replace("Z", "+00:00")) if end else None
    except Exception:
        start_dt = end_dt = None

    if start_dt and now < start_dt:
        return False
    if end_dt and now > end_dt:
        return False
    return True


# ============================================================
# Campaign processing
# ============================================================
def _queue_for_campaign(camp: dict, limit: int = 500) -> int:
    """Push messages for this campaign into the drip queue."""
    try:
        name = camp["fields"].get("Campaign Name", "Unknown")
        cid = camp["id"]
        status = str(camp["fields"].get("Status", "")).lower()

        drip_tbl = get_drip_queue()
        if not drip_tbl:
            log.warning("⚠️ No drip queue table available.")
            return 0

        # --- queue logic ---
        from sms.queue_builder import build_campaign_queue  # local helper module
        queued = build_campaign_queue(campaign_id=cid, limit=limit)
        log.info(f"📤 Queued {queued} messages for campaign → {name}")
        record_campaign_metric(name, "Queued", queued)
        return queued

    except Exception as e:
        log.warning(f"⚠️ Failed to queue campaign: {e}")
        traceback.print_exc()
        return 0


# ============================================================
# Main runner
# ============================================================
def run_campaigns(limit: int = 500, send_after_queue: bool = True) -> dict:
    """Activate scheduled campaigns, queue actives, optionally send."""
    try:
        log.info("🚀 Starting Campaign Runner (Render/Manual mode)")
        camp_tbl = get_campaigns()
        if not camp_tbl:
            return {"ok": False, "error": "campaigns_table_unavailable"}

        records = camp_tbl.all()
        if not records:
            return {"ok": True, "queued": 0, "note": "no_campaigns"}

        total = 0
        for camp in records:
            f = camp.get("fields", {})
            cid = camp.get("id")
            name = f.get("Campaign Name", "Unknown")
            status = str(f.get("Status", "")).lower()

            # Auto-activate scheduled ones
            if status == "scheduled" and _within_window(f):
                try:
                    camp_tbl.update(cid, {"Status": "Active"})
                    log.info(f"⏰ Activated scheduled campaign → {name}")
                    status = "active"
                except Exception as e:
                    log.warning(f"⚠️ Could not activate campaign {name}: {e}")

            # Process actives
            if status in ("active", "running") and _within_window(f):
                total += _queue_for_campaign(camp, limit)

        # Trigger outbound send
        if send_after_queue:
            try:
                result = send_batch(limit=limit)
                log.info(f"📦 send_batch complete: {result}")
            except Exception as e:
                log.warning(f"⚠️ send_batch failed: {e}")
                traceback.print_exc()

        log.info(f"✅ Campaign cycle complete — total queued: {total}")
        return {"ok": True, "queued": total}

    except Exception as e:
        traceback.print_exc()
        return {"ok": False, "error": str(e)}


# ============================================================
# Entry
# ============================================================
if __name__ == "__main__":
    print(run_campaigns(limit=5))
