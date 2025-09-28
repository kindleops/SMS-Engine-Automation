import os
import traceback
from datetime import datetime, timezone
from fastapi import APIRouter, Request, HTTPException
from pyairtable import Table

from sms.number_pools import increment_delivered, increment_failed, increment_opt_out

router = APIRouter()

# --- ENV CONFIG ---
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
BASE_ID = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")

CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")
LEADS_TABLE = os.getenv("LEADS_TABLE", "Leads")
PROSPECTS_TABLE = os.getenv("PROSPECTS_TABLE", "Prospects")

# --- Field Mappings ---
FROM_FIELD = os.getenv("CONV_FROM_FIELD", "phone")
TO_FIELD = os.getenv("CONV_TO_FIELD", "to_number")
MSG_FIELD = os.getenv("CONV_MESSAGE_FIELD", "message")
STATUS_FIELD = os.getenv("CONV_STATUS_FIELD", "status")
DIR_FIELD = os.getenv("CONV_DIRECTION_FIELD", "direction")
TG_ID_FIELD = os.getenv("CONV_TEXTGRID_ID_FIELD", "TextGrid ID")
RECEIVED_AT = os.getenv("CONV_RECEIVED_AT_FIELD", "received_at")
SENT_AT = os.getenv("CONV_SENT_AT_FIELD", "sent_at")
PROCESSED_BY = os.getenv("CONV_PROCESSED_BY_FIELD", "processed_by")

# Airtable clients
convos = (
    Table(AIRTABLE_API_KEY, BASE_ID, CONVERSATIONS_TABLE) if AIRTABLE_API_KEY else None
)
leads = Table(AIRTABLE_API_KEY, BASE_ID, LEADS_TABLE) if AIRTABLE_API_KEY else None
prospects = (
    Table(AIRTABLE_API_KEY, BASE_ID, PROSPECTS_TABLE) if AIRTABLE_API_KEY else None
)

# --- Field mapping (Prospects → Leads) ---
FIELD_MAP = {
    "phone": "phone",
    "Property ID": "Property ID",  # 🔑 join key
    "Owner Name": "Owner Name",
    "Address": "Address",
    "Market": "Market",
    "Sync Source": "Synced From",
    "List": "Source List",
    "Property Type": "Property Type",
}


# --- Helpers ---
def iso_timestamp():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def promote_prospect_to_lead(phone_number: str, source="Inbound"):
    """Promote Prospects → Leads, carrying Property ID forward."""
    if not phone_number:
        return None, None
    try:
        # Already a Lead?
        existing = leads.all(formula=f"{{phone}}='{phone_number}'")
        if existing:
            lead = existing[0]
            return lead["id"], lead["fields"].get("Property ID")

        # Prospect match?
        fields, property_id = {}, None
        prospect = (
            prospects.all(formula=f"{{phone}}='{phone_number}'") if prospects else []
        )
        if prospect:
            p_fields = prospect[0]["fields"]
            fields = {
                leads_col: p_fields.get(prospects_col)
                for prospects_col, leads_col in FIELD_MAP.items()
            }
            property_id = p_fields.get("Property ID")

        # Create new Lead
        new_lead = leads.create(
            {
                **fields,
                "phone": phone_number,
                "Lead Status": "New",
                "Source": source,
                "Reply Count": 0,
                "Last Inbound": iso_timestamp(),
            }
        )
        print(f"✨ Promoted {phone_number} → Lead")
        return new_lead["id"], property_id

    except Exception as e:
        print(f"⚠️ Prospect promotion failed for {phone_number}: {e}")
    return None, None


def update_lead_activity(
    lead_id: str, body: str, direction: str, reply_increment: bool = False
):
    """Update activity metrics for Leads."""
    if not leads or not lead_id:
        return
    try:
        lead = leads.get(lead_id)
        reply_count = lead["fields"].get("Reply Count", 0)
        updates = {
            "Last Activity": iso_timestamp(),
            "Last Direction": direction,
            "Last Message": (body or "")[:500],
        }
        if reply_increment:
            updates["Reply Count"] = reply_count + 1
            updates["Last Inbound"] = iso_timestamp()
        if direction == "OUT":
            updates["Last Outbound"] = iso_timestamp()

        leads.update(lead_id, updates)
    except Exception as e:
        print(f"⚠️ Failed to update lead activity: {e}")


def log_conversation(payload: dict):
    """Wrapper for safe logging into Conversations table."""
    if not convos:
        return
    try:
        convos.create(payload)
    except Exception as log_err:
        print(f"⚠️ Failed to log to Conversations: {log_err}")


# --- Inbound SMS ---
@router.post("/inbound")
async def inbound_handler(request: Request):
    try:
        data = await request.form()
        from_number = data.get("From")
        to_number = data.get("To")
        body = data.get("Body")
        msg_id = data.get("MessageSid")

        if not from_number or not body:
            raise HTTPException(status_code=400, detail="Missing From or Body")

        print(f"📥 Inbound SMS from {from_number}: {body}")

        lead_id, property_id = promote_prospect_to_lead(from_number)

        payload = {
            FROM_FIELD: from_number,
            TO_FIELD: to_number,
            MSG_FIELD: body,
            STATUS_FIELD: "UNPROCESSED",
            DIR_FIELD: "IN",
            TG_ID_FIELD: msg_id,
            RECEIVED_AT: iso_timestamp(),
        }
        if lead_id:
            payload["lead_id"] = [lead_id]
        if property_id:
            payload["Property ID"] = property_id

        log_conversation(payload)

        if lead_id:
            update_lead_activity(lead_id, body, "IN", reply_increment=True)

        return {"ok": True}

    except Exception as e:
        print("❌ Inbound webhook error:")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# --- Opt-Out Handler ---
@router.post("/optout")
async def optout_handler(request: Request):
    try:
        data = await request.form()
        from_number = data.get("From")
        body = (data.get("Body") or "").lower()

        if "stop" in body or "unsubscribe" in body or "quit" in body:
            print(f"🚫 Opt-out from {from_number}")
            increment_opt_out(from_number)

            lead_id, property_id = promote_prospect_to_lead(
                from_number, source="Opt-Out"
            )
            if lead_id:
                update_lead_activity(lead_id, body, "IN")

            payload = {
                FROM_FIELD: from_number,
                MSG_FIELD: body,
                STATUS_FIELD: "OPTOUT",
                DIR_FIELD: "IN",
                RECEIVED_AT: iso_timestamp(),
                PROCESSED_BY: "OptOut Handler",
            }
            if lead_id:
                payload["lead_id"] = [lead_id]
            if property_id:
                payload["Property ID"] = property_id

            log_conversation(payload)

        return {"ok": True}

    except Exception as e:
        print("❌ Opt-out webhook error:")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# --- Delivery Status ---
@router.post("/status")
async def status_handler(request: Request):
    try:
        data = await request.form()
        msg_id = data.get("MessageSid")
        status = data.get("MessageStatus")
        to = data.get("To")
        from_num = data.get("From")

        print(f"📡 Delivery receipt for {to} [{status}]")

        # Update number metrics
        if status == "delivered":
            increment_delivered(from_num)
        elif status in ("failed", "undelivered"):
            increment_failed(from_num)

        # Update conversation record
        if convos and msg_id:
            try:
                convos.update_by_fields(
                    {TG_ID_FIELD: msg_id}, {STATUS_FIELD: status.upper()}
                )
            except Exception as log_err:
                print(f"⚠️ Failed to update delivery status in Conversations: {log_err}")

        # Update lead metrics
        if leads and to:
            try:
                results = leads.all(formula=f"{{phone}}='{to}'")
                if results:
                    lead = results[0]
                    lead_id = lead["id"]

                    delivered_count = lead["fields"].get("Delivered Count", 0)
                    failed_count = lead["fields"].get("Failed Count", 0)

                    updates = {
                        "Last Activity": iso_timestamp(),
                        "Last Delivery Status": status.upper(),
                    }
                    if status == "delivered":
                        updates["Delivered Count"] = delivered_count + 1
                    elif status in ("failed", "undelivered"):
                        updates["Failed Count"] = failed_count + 1

                    leads.update(lead_id, updates)
            except Exception as e:
                print(f"⚠️ Failed to update lead delivery metrics: {e}")

        return {"ok": True}

    except Exception as e:
        print("❌ Status webhook error:")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
