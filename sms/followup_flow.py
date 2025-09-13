import os
from datetime import datetime, timedelta, timezone
from sms.textgrid_sender import send_message
from sms.airtable_client import get_leads_table

# --- Config ---
DRIP_QUEUE_TABLE = os.getenv("DRIP_QUEUE_TABLE", "Drip Queue")

# --- Follow-Up Flow ---
def run_followups():
    """
    Process today's follow-up messages from Drip Queue.
    - Finds rows where next_send_date = today
    - Sends SMS based on stage (30, 60, 90)
    - Updates stage, next_send_date, and last_sent
    """
    queue = get_leads_table(DRIP_QUEUE_TABLE)
    if not queue:
        return {"ok": False, "error": "Drip Queue table not configured"}

    today = datetime.now(timezone.utc).date().isoformat()
    records = queue.all(formula=f"DATETIME_FORMAT({{next_send_date}}, 'YYYY-MM-DD') = '{today}'")

    sent_count = 0

    for r in records:
        f = r.get("fields", {})
        phone   = f.get("phone")
        lead_id = f.get("lead_id")
        stage   = f.get("drip_stage", 30)
        address = f.get("address", "your property")
        owner   = f.get("owner_name", "Owner")

        if not phone:
            print(f"⚠️ Skipping record {r['id']} (missing phone)")
            continue

        # Pick template by stage
        if stage == 30:
            body = f"Hi {owner}, just checking back — are you open to an offer on {address}?"
            next_stage = 60
        elif stage == 60:
            body = f"Hey {owner}, circling back on {address}. Any change in timing?"
            next_stage = 90
        elif stage == 90:
            body = f"Hi {owner}, wanted to see if now’s a better time to chat about {address}."
            next_stage = "COMPLETE"
        else:
            print(f"⚠️ Unknown stage {stage} for record {r['id']} → skipping")
            continue

        try:
            send_message(phone, body)
        except Exception as e:
            print(f"❌ Failed to send SMS to {phone}: {e}")
            continue

        updates = {
            "last_sent": datetime.now(timezone.utc).isoformat(),
            "drip_stage": next_stage
        }

        if isinstance(next_stage, int):
            updates["next_send_date"] = str((datetime.now(timezone.utc) + timedelta(days=30)).date())

        try:
            queue.update(r["id"], updates)
            print(f"📩 Drip {stage} → {phone}: {body}")
            sent_count += 1
        except Exception as e:
            print(f"❌ Failed to update Airtable for record {r['id']}: {e}")

    print(f"✅ Follow-up flow complete — sent {sent_count} messages")
    return {"ok": True, "sent": sent_count}