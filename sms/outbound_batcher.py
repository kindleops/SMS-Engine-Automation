# sms/outbound_batcher.py
import os
from datetime import datetime, timezone
from pyairtable import Table
from sms.textgrid_sender import send_message

# --- ENV CONFIG ---
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_LEADS_CONVO_BASE_ID = os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
CAMPAIGN_CONTROL_BASE = os.getenv("AIRTABLE_CAMPAIGN_CONTROL_BASE_ID")

# --- Airtable Tables ---
convos = Table(AIRTABLE_API_KEY, AIRTABLE_LEADS_CONVO_BASE_ID, "Conversations")
leads = Table(AIRTABLE_API_KEY, AIRTABLE_LEADS_CONVO_BASE_ID, "Leads")
campaigns = Table(AIRTABLE_API_KEY, CAMPAIGN_CONTROL_BASE, "Campaigns")
numbers = Table(AIRTABLE_API_KEY, CAMPAIGN_CONTROL_BASE, "Numbers")

def get_campaigns():
    """Fetch campaign configs from Airtable."""
    return campaigns.all()

def send_batch():
    """Send outbound SMS in batches across all campaigns, saving campaign_id in Conversations."""
    all_campaigns = get_campaigns()
    if not all_campaigns:
        return {"error": "❌ No campaigns defined in Airtable"}

    results = []
    total_sent = 0

    for camp in all_campaigns:
        fields = camp.get("fields", {})
        campaign_id = camp["id"]
        view_name = fields.get("view_name")
        batch_limit = int(fields.get("batch_limit", 50))
        from_number = fields.get("from_number")

        if not view_name or not from_number:
            continue

        # Fetch leads from Leads table by view
        records = leads.all(view=view_name)[:batch_limit]

        sent_count = 0
        for r in records:
            phone = r["fields"].get("phone")
            owner = r["fields"].get("owner_name", "Owner")
            address = r["fields"].get("address", "your property")

            if not phone:
                continue

            body = f"Hi {owner}, quick question—are you the owner of {address}?"

            # --- Send SMS via TextGrid ---
            send_message(phone, body, from_number=from_number)

            # --- Log into Conversations with campaign link ---
            convos.create({
                "phone": phone,
                "to_number": from_number,
                "message": body,
                "direction": "OUT",
                "status": "SENT",
                "campaign_id": campaign_id,
                "sent_at": datetime.now(timezone.utc).isoformat()
            })

            sent_count += 1
            total_sent += 1

            results.append({
                "campaign": view_name,
                "campaign_id": campaign_id,
                "phone": phone,
                "message": body,
                "sent_at": datetime.now(timezone.utc).isoformat()
            })

        # --- Update Campaign metadata ---
        campaigns.update(camp["id"], {
            "last_sent_at": datetime.now(timezone.utc).isoformat(),
            "last_sent_count": sent_count
        })

        print(f"✅ Sent {sent_count} messages from campaign {view_name} ({campaign_id})")

    return {
        "status": f"✅ Outbound batches sent across {len(all_campaigns)} campaigns",
        "total_sent": total_sent,
        "results": results
    }