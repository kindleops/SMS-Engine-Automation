import os
from datetime import datetime
from pyairtable import Table
from textgrid_sender import send_message
from templates import followup_yes, followup_no, followup_wrong

# Airtable setup
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")

CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")
LEADS_TABLE = os.getenv("LEADS_TABLE", "Leads")
UNPROCESSED_VIEW = os.getenv("UNPROCESSED_VIEW", "Unprocessed Inbounds")

convos = Table(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, CONVERSATIONS_TABLE)
leads = Table(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, LEADS_TABLE)

def run_autoresponder(limit=50, view=UNPROCESSED_VIEW):
    records = convos.all(view=view)[:limit]
    processed = 0
    skipped = 0

    for r in records:
        try:
            fields = r.get("fields", {})
            msg = fields.get("message", "").lower()
            phone = fields.get("phone")

            if not phone:
                skipped += 1
                continue

            # Classification rules
            if any(x in msg for x in ["wrong", "not me", "who"]):
                reply, intent = followup_wrong, "WRONG"
            elif any(x in msg for x in ["stop", "no thanks", "not interested"]):
                reply, intent = followup_no, "NO"
            elif any(x in msg for x in ["yes", "offer", "price", "how much", "maybe"]):
                reply, intent = followup_yes, "YES"
            elif any(x in msg for x in ["later", "not now", "check back"]):
                reply, intent = "Totally fine—I’ll check back later. If timing changes, text me sooner.", "LATER"
            else:
                reply, intent = "Thanks for the response. Are you the owner and open to an offer if the numbers work?", "UNKNOWN"

            # Send reply
            send_message(phone, reply)

            # Update Conversations
            convos.update(r["id"], {
                "status": f"PROCESSED-{intent}",
                "processed_at": str(datetime.utcnow())
            })

            # Update Leads
            leads.update_by_fields({"phone": phone}, {"intent": intent})

            print(f"🤖 Reply sent to {phone}: {intent} → {reply}")
            processed += 1

        except Exception as e:
            print(f"❌ Error processing record {r.get('id')}: {e}")
            skipped += 1
            continue

    print(f"📊 Autoresponder finished — processed {processed}, skipped {skipped}")
    return {"processed": processed, "skipped": skipped}

if __name__ == "__main__":
    run_autoresponder()