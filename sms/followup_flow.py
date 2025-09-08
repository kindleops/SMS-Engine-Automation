import os
from datetime import datetime, timedelta
from pyairtable import Table
from sms.textgrid_sender import send_message

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
DRIP_QUEUE_TABLE = os.getenv("DRIP_QUEUE_TABLE", "Drip Queue")

queue = Table(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, DRIP_QUEUE_TABLE)

def run_followups():
    today = datetime.utcnow().date().isoformat()
    records = queue.all(formula=f"DATETIME_FORMAT({{next_send_date}}, 'YYYY-MM-DD') = '{today}'")

    sent_count = 0

    for r in records:
        f = r.get("fields", {})
        phone = f.get("phone")
        lead_id = f.get("lead_id")
        stage = f.get("drip_stage", 30)
        address = f.get("address", "your property")
        owner = f.get("owner_name", "Owner")

        if not phone:
            continue

        # Choose template by stage
        if stage == 30:
            body = f"Hi {owner}, just checking back — are you open to an offer on {address}?"
            next_stage = 60
        elif stage == 60:
            body = f"Hey {owner}, circling back on {address}. Any change in timing?"
            next_stage = 90
        else:
            body = f"Hi {owner}, wanted to see if now’s a better time to chat about {address}."
            next_stage = "COMPLETE"

        send_message(phone, body)

        updates = {
            "last_sent": str(datetime.utcnow()),
            "drip_stage": next_stage
        }
        if isinstance(next_stage, int):
            updates["next_send_date"] = str((datetime.utcnow() + timedelta(days=30)).date())

        queue.update(r["id"], updates)
        sent_count += 1

        print(f"📩 Drip {stage} → {phone}: {body}")

    print(f"✅ Follow-up flow complete — sent {sent_count} messages")
    return {"sent": sent_count}

if __name__ == "__main__":
    run_followups()