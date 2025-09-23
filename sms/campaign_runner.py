import os, traceback
from datetime import datetime, timezone
from pyairtable import Table
import json

from sms.outbound_batcher import send_batch, format_template
from sms.metrics_tracker import update_metrics
from sms.retry_runner import run_retry   # 🔁 retry handler

# --- Airtable Config ---
API_KEY = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE")
PERFORMANCE_BASE = os.getenv("PERFORMANCE_BASE")

CAMPAIGNS_TABLE = "Campaigns"
TEMPLATES_TABLE = "Templates"
LEADS_TABLE = "Leads"
DRIP_QUEUE_TABLE = "Drip Queue"

# Airtable clients
campaigns = Table(API_KEY, LEADS_CONVOS_BASE, CAMPAIGNS_TABLE)
templates = Table(API_KEY, LEADS_CONVOS_BASE, TEMPLATES_TABLE)
leads_tbl = Table(API_KEY, LEADS_CONVOS_BASE, LEADS_TABLE)
drip = Table(API_KEY, LEADS_CONVOS_BASE, DRIP_QUEUE_TABLE)

runs = Table(API_KEY, PERFORMANCE_BASE, "Runs/Logs") if PERFORMANCE_BASE else None
kpis = Table(API_KEY, PERFORMANCE_BASE, "KPIs") if PERFORMANCE_BASE else None


def utcnow():
    return datetime.now(timezone.utc)


def run_campaigns(limit: str | int = 1, retry_limit: int = 3):
    """
    Auto-runs scheduled campaigns with full lifecycle:
    - Queues leads into Drip Queue
    - Runs outbound batch + retries
    - Tracks KPIs + marks campaigns Completed
    """

    now = utcnow()
    now_iso = now.isoformat()

    # Convert "ALL" → large int
    if isinstance(limit, str) and limit.upper() == "ALL":
        limit = 9999

    try:
        scheduled = campaigns.all(formula="{Status}='Scheduled'")
    except Exception:
        traceback.print_exc()
        return {"ok": False, "error": "Failed to fetch campaigns"}

    processed, results = 0, []

    for camp in scheduled:
        if processed >= limit:
            break

        f = camp["fields"]
        cid = camp["id"]
        name = f.get("Name", "Unnamed Campaign")

        # --- Handle Start/End Time ---
        start_dt = datetime.fromisoformat(f["Start Time"].replace("Z", "+00:00")) if f.get("Start Time") else None
        end_dt   = datetime.fromisoformat(f["End Time"].replace("Z", "+00:00")) if f.get("End Time") else None

        if not start_dt or now < start_dt:
            continue
        if end_dt and now > end_dt:
            # Mark expired campaigns as Completed
            try:
                campaigns.update(cid, {"Status": "Completed", "Last Run At": now_iso})
            except Exception:
                traceback.print_exc()
            continue

        # --- Template ---
        template_id = (f.get("Template") or [None])[0]
        if not template_id:
            print(f"⚠️ Campaign {name} missing template, skipping")
            continue

        tmpl = templates.get(template_id)
        template_text = tmpl["fields"].get("Message", "")
        if not template_text:
            print(f"⚠️ Template {template_id} empty, skipping")
            continue

        # --- Leads ---
        view = f.get("View/Segment")
        try:
            lead_records = leads_tbl.all(view=view) if view else leads_tbl.all()
        except Exception:
            traceback.print_exc()
            continue

        total_leads = len(lead_records)
        queued = 0

        # --- Queue Leads ---
        for lead in lead_records:
            lf = lead["fields"]
            phone = lf.get("phone")
            if not phone:
                continue

            personalized_text = format_template(template_text, lf)

            try:
                drip.create({
                    "Leads": [lead["id"]],
                    "Campaign": [cid],
                    "Template": [template_id],
                    "phone": phone,
                    "message_preview": personalized_text,
                    "status": "QUEUED",
                    "from_number": None,  # TODO: integrate pick_number() if needed
                    "next_send_date": now_iso
                })
                queued += 1
            except Exception as e:
                print(f"❌ Failed to queue {phone}: {e}")
                traceback.print_exc()

        # --- Batch Send ---
        batch_result = send_batch(limit=500)

        # --- Retry Loop ---
        retry_result = {}
        if batch_result.get("total_sent", 0) < queued:
            for attempt in range(retry_limit):
                retry_result = run_retry(limit=100, view="Failed Sends")
                if retry_result.get("retried", 0) == 0:
                    break

        # --- Progress ---
        sent_so_far = f.get("Messages Sent", 0) + batch_result.get("total_sent", 0) + retry_result.get("retried", 0)
        completed = sent_so_far >= total_leads

        # --- Update Campaign ---
        try:
            campaigns.update(cid, {
                "Status": "Completed" if completed else "Running",
                "Queued Leads": queued,
                "Messages Sent": sent_so_far,
                "Completion %": round(sent_so_far / total_leads * 100, 2) if total_leads else 0,
                "Last Run Result": json.dumps({
                    "Queued": queued,
                    "Sent": batch_result.get("total_sent", 0),
                    "Retries": retry_result.get("retried", 0),
                    "Completed": completed
                }),
                "Last Run At": now_iso
            })
        except Exception:
            traceback.print_exc()

        # --- Log to Performance ---
        if runs:
            try:
                run_record = runs.create({
                    "Type": "CAMPAIGN_RUN",
                    "Campaign": name,
                    "Processed": sent_so_far,
                    "Breakdown": json.dumps({
                        "initial": batch_result,
                        "retries": retry_result
                    }),
                    "Timestamp": now_iso
                })
                batch_result["run_id"] = run_record["id"]
            except Exception:
                traceback.print_exc()

        if kpis:
            try:
                kpis.create({
                    "Campaign": name,
                    "Metric": "OUTBOUND_SENT",
                    "Value": sent_so_far,
                    "Date": now.date().isoformat()
                })
            except Exception:
                traceback.print_exc()

        results.append({
            "campaign": name,
            "queued": queued,
            "sent": sent_so_far,
            "completed": completed,
            "retries": retry_result.get("retried", 0)
        })
        processed += 1

    try:
        update_metrics()
    except Exception:
        traceback.print_exc()

    return {"ok": True, "processed": processed, "results": results}