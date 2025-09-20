import os
from flask import Flask, request, jsonify
from datetime import datetime, timezone
from pyairtable import Table

from sms.number_pools import (
    increment_delivered,
    increment_failed,
    increment_opt_out,
)

# --- Env Config ---
AIRTABLE_API_KEY    = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE   = os.getenv("LEADS_CONVOS_BASE")
CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")

FROM_FIELD   = os.getenv("CONV_FROM_FIELD", "phone")
TO_FIELD     = os.getenv("CONV_TO_FIELD", "to_number")
MSG_FIELD    = os.getenv("CONV_MESSAGE_FIELD", "message")
STATUS_FIELD = os.getenv("CONV_STATUS_FIELD", "status")
DIR_FIELD    = os.getenv("CONV_DIRECTION_FIELD", "direction")
TG_ID_FIELD  = os.getenv("CONV_TEXTGRID_ID_FIELD", "TextGrid ID")
RECEIVED_AT  = os.getenv("CONV_RECEIVED_AT_FIELD", "received_at")

app = Flask(__name__)

@app.route("/inbound", methods=["POST"])
def inbound():
    data = request.form.to_dict() or request.json or {}
    print(f"📥 Inbound webhook received: {data}")

    from_num = data.get("From")
    to_num = data.get("To")
    body = data.get("Body", "").strip()
    status = data.get("MessageStatus")  # delivery status callback
    msg_id = data.get("MessageSid")

    # --- Delivery receipts ---
    if status:
        if status.lower() == "delivered":
            increment_delivered(to_num)
        elif status.lower() == "failed":
            increment_failed(to_num)

    # --- Opt-out handling ---
    if body and body.lower() in {"stop", "unsubscribe", "cancel"}:
        increment_opt_out(to_num)
        status = "OPTOUT"

    # --- Log into Airtable ---
    if AIRTABLE_API_KEY and LEADS_CONVOS_BASE:
        try:
            convos = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, CONVERSATIONS_TABLE)
            convos.create({
                FROM_FIELD: from_num,
                TO_FIELD: to_num,
                MSG_FIELD: body,
                STATUS_FIELD: status or "RECEIVED",
                DIR_FIELD: "IN",
                TG_ID_FIELD: msg_id,
                RECEIVED_AT: datetime.now(timezone.utc).isoformat(),
            })
        except Exception as log_err:
            print(f"⚠️ Airtable log failed: {log_err}")

    return jsonify({"ok": True})