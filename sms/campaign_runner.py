"""
🚀 Campaign Runner — Datastore-Integrated Final Version
────────────────────────────────────────────────────────
Activates scheduled campaigns, queues eligible leads,
and triggers outbound sending automatically.
"""

from __future__ import annotations
import traceback
from datetime import datetime, timezone
from typing import Any, Dict

from sms.runtime import get_logger
from sms.datastore import CONNECTOR, list_records, update_record
from sms.queue_builder import build_campaign_queue
from sms.outbound_batcher import send_batch

log = get_logger("campaign_runner")


# ============================================================
# Helpers
# ============================================================

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _within_window(f: Dict[str, Any]) -> bool:
    """Check if campaign is within start/end date window."""
    try:
        start_str = f.get("Start Date")
        end_str = f.get("End Date")
        now = _utcnow()

        start_ok = not start_str or datetime.fromisoformat(start_str) <= now
        end_ok = not end_str or datetime.fromisoformat(end_str) >= now
        return start_ok and end_ok
    except Exception:
        return True


# ============================================================
# Core
# ============================================================

def run_campaigns(limit: int = 500, send_after_queue: bool = True) -> Dict[str, Any]:
    """Activate scheduled campaigns, queue leads, and send messages."""
    try:
        camp_handle = CONNECTOR.campaigns()
        campaigns = list_records(camp_handle)
        if not campaigns:
            log.warning("⚠️ No campaigns found.")
            return {"ok": False, "error": "No campaigns found"}

        total_processed = 0

        for camp in campaigns:
            f = camp.get("fields", {}) or {}
            cid = camp.get("id")
            name = f.get("Campaign Name", "Unnamed Campaign")
            status = str(f.get("Status", "")).strip().lower()

            # ── Activate scheduled ──
            if status == "scheduled" and _within_window(f):
                try:
                    update_record(camp_handle, cid, {"Status": "Active"})
                    log.info(f"⏰ Activated scheduled campaign → {name}")
                    status = "active"
                except Exception as e:
                    log.warning(f"⚠️ Failed to activate campaign {cid}: {e}")

            # ── Process active campaigns ──
            if status in ("active", "running") and _within_window(f):
                try:
                    queued = build_campaign_queue(camp, limit)
                    total_processed += queued
                    log.info(f"📤 Queued {queued} messages for campaign → {name}")
                except Exception as e:
                    log.warning(f"⚠️ Failed to queue campaign {name}: {e}")
                    traceback.print_exc()

        # ── Trigger outbound send batch ──
        if send_after_queue:
            try:
                res = send_batch(limit=limit)
                log.info(f"📦 Outbound batch triggered → {res}")
            except Exception as e:
                log.warning(f"⚠️ send_batch failed: {e}")
                traceback.print_exc()

        log.info(f"✅ Campaign runner complete — total processed={total_processed}")
        return {"ok": True, "processed": total_processed, "queued": total_processed}

    except Exception as e:
        traceback.print_exc()
        log.error(f"❌ run_campaigns failed: {e}")
        return {"ok": False, "error": str(e)}


# ============================================================
# CLI Entrypoint
# ============================================================

if __name__ == "__main__":
    log.info("🚀 Starting Campaign Runner (manual execution mode)")
    print(run_campaigns(limit=500))
