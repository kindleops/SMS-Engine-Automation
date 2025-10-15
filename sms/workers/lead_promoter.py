# sms/workers/lead_promoter.py
import os, time, re
from datetime import datetime, timezone
from sms.tables import get_table

STAGE_OK = {"Stage 2 - Offer Interest","Stage 3 - Price/Condition","Stage 4 - Run Comps/Numbers","Stage 5 - Make Offer",
            "Stage 6 - Send Contract","Stage 7 - Dispositions","Stage 8 - Close Escrow"}

def iso_now(): return datetime.now(timezone.utc).isoformat()
def last10(s): return re.sub(r"[^0-9]","",str(s or ""))[-10:]

def run():
    P = get_table("AIRTABLE_API_KEY","LEADS_CONVOS_BASE","PROSPECTS_TABLE","Prospects")
    L = get_table("AIRTABLE_API_KEY","LEADS_CONVOS_BASE","LEADS_TABLE","Leads")

    # cache leads by phone10 to prevent dups
    lead_by_phone = {}
    for r in L.all():
        f = r.get("fields", {})
        p = last10(f.get("Phone") or f.get("phone") or f.get("Mobile"))
        if p and p not in lead_by_phone:
            lead_by_phone[p] = r["id"]

    created, linked = 0, 0
    for p in P.all():
        f = p.get("fields", {})
        if f.get("Lead"):  # already linked
            continue
        stage = f.get("Stage") or f.get("stage")
        if stage not in STAGE_OK:
            continue

        phone = last10(f.get("Phone") or f.get("Phone 1 (from Linked Owner)") or f.get("Mobile"))
        if not phone:
            continue

        lead_id = lead_by_phone.get(phone)
        if not lead_id:
            # create new lead
            new = L.create({
                "Phone": phone,
                "Lead Status": "Warm",
                "Source": f.get("Sync Source") or "Prospect Promotion",
                "Reply Count": int(f.get("Reply Count") or 0),
                "Last Inbound": f.get("Last Inbound") or iso_now(),
                "Last Activity": iso_now(),
            })
            lead_id = new["id"]
            lead_by_phone[phone] = lead_id
            created += 1

        # link prospect → lead (and mirror ID text field if present)
        P.update(p["id"], {"Lead": [lead_id], "Lead ID": lead_id})
        linked += 1
        time.sleep(0.1)

    print(f"[lead_promoter] created={created} linked={linked}")

if __name__ == "__main__":
    run()