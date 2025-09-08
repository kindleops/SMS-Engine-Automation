import os
from datetime import datetime
from pyairtable import Table
from sms.textgrid_sender import send_message

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")

# accept both naming styles
LEADS_CONVOS_BASE = (
    os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
    or os.getenv("LEADS_CONVOS_BASE")
)
CAMPAIGN_CONTROL_BASE = (
    os.getenv("AIRTABLE_CAMPAIGN_CONTROL_BASE_ID")
    or os.getenv("CAMPAIGN_CONTROL_BASE")
)

missing = [k for k, v in {
    "AIRTABLE_API_KEY": AIRTABLE_API_KEY,
    "LEADS_CONVOS_BASE": LEADS_CONVOS_BASE,
    "CAMPAIGN_CONTROL_BASE": CAMPAIGN_CONTROL_BASE,
}.items() if not v]
if missing:
    raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

# tables
leads        = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, "Leads")
convos       = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, "Conversations")
campaigns    = Table(AIRTABLE_API_KEY, CAMPAIGN_CONTROL_BASE, "Campaigns")
numbers      = Table(AIRTABLE_API_KEY, CAMPAIGN_CONTROL_BASE, "Numbers")

def get_campaigns():
    return campaigns.all()

def send_batch():
    all_campaigns = get_campaigns()
    if not all_campaigns:
        return {"error": "❌ No campaigns defined in Airtable"}

    results, total_sent = [], 0
    for camp in all_campaigns:
        fields = camp.get("fields", {})
        view_name   = fields.get("view_name")            # Airtable view in Leads table
        batch_limit = int(fields.get("batch_limit", 50))

        if not view_name:
            continue

        records = leads.all(view=view_name)[:batch_limit]
        sent_count = 0
        for r in records:
            f = r.get("fields", {})
            phone   = f.get("phone")
            owner   = f.get("owner_name", "Owner")
            address = f.get("address", "your property")
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
                "sent_at": datetime.utcnow().isoformat()
            })

        campaigns.update(camp["id"], {
            "last_sent_at": datetime.utcnow().isoformat(),
            "last_index": sent_count
        })
        print(f"✅ Sent {sent_count} from view '{view_name}'")

    return {"status": "ok", "total_sent": total_sent, "results": results}