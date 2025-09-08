import os, random
from datetime import datetime
from pyairtable import Table
from textgrid_sender import send_message
from templates import ownership_templates

# Env vars
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")

# Default tables/views (can override per run)
DEFAULT_PROPERTIES_TABLE = os.getenv("PROPERTIES_TABLE", "Properties")
DEFAULT_CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")
DEFAULT_SEND_VIEW = os.getenv("SEND_VIEW", "Send View")

def run_batch(
    limit=100,
    table_name=DEFAULT_PROPERTIES_TABLE,
    view=DEFAULT_SEND_VIEW,
    conversations_table=DEFAULT_CONVERSATIONS_TABLE
):
    properties = Table(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, table_name)
    convos = Table(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, conversations_table)

    records = properties.all(view=view)[:limit]
    sent_count, skipped = 0, 0

    for r in records:
        f = r.get("fields", {})
        phone = f.get("phone")
        owner = f.get("owner_name", "Owner")
        address = f.get("address", "your property")
        lead_id = f.get("property_id")

        if not phone:
            skipped += 1
            continue

        try:
            # Pick random ownership template
            template = random.choice(ownership_templates)
            body = template.format(First=owner, Address=address)

            # Send SMS via Textgrid
            send_message(phone, body)

            # Log to Conversations
            convos.create({
                "lead_id": lead_id,
                "owner_name": owner,
                "phone": phone,
                "direction": "OUT",
                "message": body,
                "status": "SENT",
                "timestamp": str(datetime.utcnow())
            })

            print(f"✅ Sent to {phone}: {body}")
            sent_count += 1

        except Exception as e:
            print(f"❌ Error sending to {phone}: {e}")
            skipped += 1

    print(f"📊 Outbound batch complete — Sent: {sent_count}, Skipped: {skipped}")
    return {"sent": sent_count, "skipped": skipped}

if __name__ == "__main__":
    run_batch(limit=50)