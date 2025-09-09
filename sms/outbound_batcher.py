# sms/outbound_batcher.py
import os
from datetime import datetime, timezone
from sms.textgrid_sender import send_message
from sms.airtable_client import get_leads_table, get_campaigns_table

CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")
LEADS_TABLE         = os.getenv("LEADS_TABLE", "Leads")
CAMPAIGNS_TABLE     = os.getenv("CAMPAIGNS_TABLE", "Campaigns")

def get_campaigns():
    tbl = get_campaigns_table(CAMPAIGNS_TABLE)
    return tbl.all()

def send_batch():
    try:
        leads_tbl   = get_leads_table(LEADS_TABLE)
        convos_tbl  = get_leads_table(CONVERSATIONS_TABLE)
        camp_tbl    = get_campaigns_table(CAMPAIGNS_TABLE)
    except Exception as e:
        # Don’t crash the server; return a clear payload
        return {"error": f"Airtable config error: {e}"}

    all_campaigns = camp_tbl.all()
    if not all_campaigns:
        return {"error": "No campaigns defined in Airtable"}

    results, total_sent = [], 0
    for camp in all_campaigns:
        fields = camp.get("fields", {})
        view_name   = fields.get("view_name")
        batch_limit = int(fields.get("batch_limit", 50))
        if not view_name:
            continue

        # pull from Leads by view
        records = leads_tbl.all(view=view_name)[:batch_limit]
        sent_count = 0

        for r in records:
            f = r.get("fields", {})
            phone   = f.get("Phone") or f.get("phone")
            owner   = f.get("Owner Name") or f.get("owner_name") or "Owner"
            address = f.get("Address") or f.get("address") or "your property"
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
                "sent_at": datetime.now(timezone.utc).isoformat(),
            })

        camp_tbl.update(camp["id"], {
            "last_sent_at": datetime.now(timezone.utc).isoformat(),
            "last_index": sent_count,
        })

    return {
        "status": f"Sent across {len(all_campaigns)} campaigns",
        "total_sent": total_sent,
        "results": results,
    }