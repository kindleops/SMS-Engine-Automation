# sms/outbound_batcher.py
import os
from datetime import datetime
from pyairtable import Table
from sms.textgrid_sender import send_message

# --- Airtable Config ---
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")

LEADS_CONVOS_BASE = os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")  # Leads & Conversations Base
CAMPAIGN_CONTROL_BASE = os.getenv("AIRTABLE_CAMPAIGN_CONTROL_BASE_ID")  # Campaign Control Base

# --- Airtable Tables ---
leads = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, "Leads")
convos = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, "Conversations")

campaigns = Table(AIRTABLE_API_KEY, CAMPAIGN_CONTROL_BASE, "Campaigns")
numbers = Table(AIRTABLE_API_KEY, CAMPAIGN_CONTROL_BASE, "Numbers")


def get_campaigns():
    """Fetch campaign configs from Airtable (Campaign Control base)."""
    return campaigns.all()


def send_batch():
    """Send outbound SMS in batches across all campaigns."""
    all_campaigns = get_campaigns()
    if not all_campaigns:
        return {"error": "❌ No campaigns defined in Campaigns table"}

    results = []
    total_sent = 0

    for camp in all_campaigns:
        fields = camp.get("fields", {})
        campaign_name = fields.get("Name") or fields.get("Campaign Name")
        view_name = fields.get("view_name")  # Optional: can be stored in Campaigns
        batch_limit = int(fields.get("batch_limit", 50))

        if not view_name:
            print(f"⚠️ Campaign {campaign_name} has no view_name defined, skipping.")
            continue

        # Pull leads from the Leads table (Leads & Conversations base)
        records = leads.all(view=view_name)[:batch_limit]

        sent_count = 0
        for r in records:
            phone = r["fields"].get("phone")
            owner = r["fields"].get("owner_name", "Owner")
            address = r["fields"].get("address", "your property")

            if not phone:
                continue

            body = f"Hi {owner}, quick question—are you the owner of {address}?"

            # Send SMS
            send_message(phone, body)
            sent_count += 1
            total_sent += 1

            results.append({
                "campaign": campaign_name,
                "phone": phone,
                "message": body,
                "sent_at": str(datetime.utcnow())
            })

        # Update campaign metadata in Campaigns table
        campaigns.update(camp["id"], {
            "last_sent_at": str(datetime.utcnow()),
            "last_sent_count": sent_count
        })

        print(f"✅ Sent {sent_count} messages from campaign {campaign_name}")

    return {
        "status": f"✅ Outbound batches sent across {len(all_campaigns)} campaigns",
        "total_sent": total_sent,
        "results": results
    }