import os
from datetime import datetime, timezone
from pyairtable import Table
from sms.textgrid_sender import send_message

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
PROPERTIES_TABLE = os.getenv("PROPERTIES_TABLE", "Properties")
CAMPAIGNS_TABLE = os.getenv("CAMPAIGNS_TABLE", "Campaigns")

properties = Table(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, PROPERTIES_TABLE)
campaigns = Table(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, CAMPAIGNS_TABLE)

def get_campaigns():
    """Fetch campaign configs from Airtable Campaigns table."""
    return campaigns.all()

def send_batch():
    """Send outbound SMS in batches across all campaigns."""
    all_campaigns = get_campaigns()
    if not all_campaigns:
        return {"error": "❌ No campaigns defined in Airtable"}

    results = []
    total_sent = 0

    for camp in all_campaigns:
        fields = camp.get("fields", {})
        view_name = fields.get("view_name")
        batch_limit = int(fields.get("batch_limit", 50))

        if not view_name:
            continue

        # Grab records from this campaign's Airtable view
        records = properties.all(view=view_name)[:batch_limit]

        sent_count = 0
        for r in records:
            phone = r["fields"].get("phone")
            owner = r["fields"].get("owner_name", "Owner")
            address = r["fields"].get("address", "your property")

            if not phone:
                continue

            body = f"Hi {owner}, quick question—are you the owner of {address}?"

            send_message(phone, body)
            sent_count += 1
            total_sent += 1

            results.append({
                "campaign": view_name,
                "phone": phone,
                "message": body,
                "sent_at": datetime.now(timezone.utc).isoformat()
            })

        # Update metadata for campaign
        campaigns.update(camp["id"], {
            "last_sent_at": datetime.now(timezone.utc).isoformat(),
            "last_index": sent_count
        })

        print(f"✅ Sent {sent_count} messages from {view_name}")

    return {
        "status": f"✅ Outbound batches sent across {len(all_campaigns)} campaigns",
        "total_sent": total_sent,
        "results": results
    }