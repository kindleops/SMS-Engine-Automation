import os
from datetime import datetime, timezone
from sms.airtable_client import leads_table, campaign_table
from sms.textgrid_sender import send_message

# Airtable tables
leads_tbl   = leads_table("Leads")
convos_tbl  = leads_table("Conversations")
campaigns   = campaign_table("Campaigns")
numbers_tbl = campaign_table("Numbers")

DEFAULT_BATCH_LIMIT = int(os.getenv("DEFAULT_BATCH_LIMIT", "50"))

def get_campaigns():
    return campaigns.all()

def pick_number_for_campaign(campaign_id: str) -> str | None:
    # Choose any Active number linked to this campaign
    recs = numbers_tbl.all()
    for r in recs:
        f = r.get("fields", {})
        linked = f.get("Campaign")
        status = (f.get("Status") or "").lower()
        if status == "active" and (
            (isinstance(linked, list) and campaign_id in linked)
            or linked == campaign_id
        ):
            return f.get("Number") or f.get("number")
    return None

def send_batch():
    all_campaigns = get_campaigns()
    if not all_campaigns:
        return {"error": "❌ No campaigns defined in Airtable"}

    results, total_sent = [], 0

    for camp in all_campaigns:
        cf = camp.get("fields", {})
        campaign_id = camp.get("id")
        view_name   = cf.get("view_name") or cf.get("View Name") or cf.get("view")
        batch_limit = int(cf.get("batch_limit") or DEFAULT_BATCH_LIMIT)

        if not view_name:
            continue

        from_number = pick_number_for_campaign(campaign_id)  # optional
        records = leads_tbl.all(view=view_name)[:batch_limit]

        sent_count = 0
        for r in records:
            f = r["fields"]
            phone   = f.get("phone") or f.get("Phone")
            owner   = f.get("owner_name") or f.get("Owner") or "Owner"
            address = f.get("address") or f.get("Address") or "your property"
            if not phone:
                continue

            body = f"Hi {owner}, quick question—are you the owner of {address}?"
            send_message(phone, body, from_number=from_number)

            # Log an outbound in Conversations (optional but nice to have)
            convos_tbl.create({
                "phone": phone,
                "to_number": from_number,
                "message": body,
                "direction": "OUT",
                "status": "SENT",
                "timestamp": datetime.now(timezone.utc).isoformat()
            })

            sent_count += 1
            total_sent += 1
            results.append({
                "campaign": view_name,
                "phone": phone,
                "message": body,
                "from": from_number,
                "sent_at": datetime.now(timezone.utc).isoformat()
            })

        campaigns.update(camp["id"], {
            "last_sent_at": datetime.now(timezone.utc).isoformat(),
            "last_count": sent_count
        })
        print(f"✅ Sent {sent_count} from {view_name}")

    return {"status": f"✅ Sent across {len(all_campaigns)} campaigns",
            "total_sent": total_sent, "results": results}