import os, time
from sms.tables import get_table as _get

DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

# === Tables ===
C = _get("AIRTABLE_API_KEY", "LEADS_CONVOS_BASE", "CONVERSATIONS_TABLE", "Conversations")
P = _get("AIRTABLE_API_KEY", "LEADS_CONVOS_BASE", "PROSPECTS_TABLE", "Prospects")


def normalize(phone: str):
    if not phone:
        return None
    return "".join(c for c in str(phone) if c.isdigit())[-10:]  # last 10 digits for matching


# === Build prospect maps ===
pros_phone, pros_lead, pros_id = {}, {}, {}
print("Building prospect maps...")

for r in P.all():
    f = r.get("fields", {})
    rid = f.get("Record ID") or r["id"]
    if not rid:
        continue

    # phone 1 and 2
    for k in ["Phone 1 (from Linked Owner)", "Phone 2 (from Linked Owner)"]:
        v = f.get(k)
        if v and isinstance(v, str):
            norm = normalize(v)
            if norm:
                pros_phone[norm] = rid

    # Lead ID link
    lead_id = f.get("Lead ID")
    if lead_id:
        pros_lead[str(lead_id).strip()] = rid

    pros_id[rid] = rid

print(f"✅ Prospect maps built | phone keys={len(pros_phone)}, lead-id keys={len(pros_lead)}")

# === Process conversations ===
rows = C.all()
linked, already, scanned = 0, 0, len(rows)

for r in rows:
    f = r.get("fields", {})
    patch = {}
    rid = r["id"]

    # skip if already linked
    if f.get("Prospect") and isinstance(f["Prospect"], list) and f["Prospect"]:
        already += 1
        continue

    # extract possible phone and lead ID
    phone_fields = [f.get(k) for k in ["Phone", "Phone10", "phone", "From", "To"] if f.get(k)]
    phone = None
    for p in phone_fields:
        n = normalize(p)
        if n:
            phone = n
            break

    lead_id = f.get("Lead Record ID") or f.get("Lead ID")

    # attempt match
    match = None
    if phone and phone in pros_phone:
        match = pros_phone[phone]
    elif lead_id and str(lead_id) in pros_lead:
        match = pros_lead[str(lead_id)]

    if match:
        patch["Prospect"] = [match]
        patch["Prospect Record ID"] = match
        linked += 1
        if DRY_RUN:
            print(f"[DRY] Linked Conversation {rid} → Prospect {match} (phone={phone}, lead={lead_id})")
        else:
            try:
                C.update(rid, patch)
                time.sleep(0.25)
            except Exception as e:
                print("⚠️", rid, e)

print("\n=== Summary ===")
print(f"Conversations scanned: {scanned}")
print(f"Linked: {linked}")
print(f"Already linked: {already}")
print(f"DRY_RUN={DRY_RUN}")
