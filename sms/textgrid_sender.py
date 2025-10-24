# sms/textgrid_sender.py
"""
📡 TextGrid Sender — Final Optimized Version
Handles outbound SMS delivery through TextGrid API and logs conversations to Airtable.
"""

import os
import requests
from datetime import datetime, timezone
from sms.airtable_schema import (
    default_conversation_payload,
    TABLE_NAMES,
    ConversationProcessor,
)
from sms.tables import get_table

TEXTGRID_ACCOUNT_SID = os.getenv("TEXTGRID_ACCOUNT_SID")
TEXTGRID_AUTH_TOKEN = os.getenv("TEXTGRID_AUTH_TOKEN")

API_BASE = "https://api.textgrid.com/v1"

def _send_via_textgrid(from_number: str, to_number: str, body: str) -> dict:
    """Low-level API call to TextGrid"""
    url = f"{API_BASE}/Messages.json"
    auth = (TEXTGRID_ACCOUNT_SID, TEXTGRID_AUTH_TOKEN)
    data = {"From": from_number, "To": to_number, "Body": body}
    resp = requests.post(url, data=data, auth=auth, timeout=10)
    resp.raise_for_status()
    return resp.json()

def send_message(from_number: str, to_number: str, body: str, campaign=None):
    """Send a single SMS immediately via TextGrid + log to Airtable."""
    print(f"📤 Sending SMS → {to_number}: {body[:60]}...")
    msg = _send_via_textgrid(from_number, to_number, body)
    sid = msg.get("sid") or msg.get("messageSid")

    tbl = get_table("AIRTABLE_API_KEY", os.getenv("LEADS_CONVOS_BASE"), TABLE_NAMES["CONVERSATIONS"])
    payload = default_conversation_payload(from_number, to_number, body)
    payload["Message SID"] = sid
    payload["Sent At"] = datetime.now(timezone.utc).isoformat()
    if campaign:
        payload["Campaign"] = [campaign]
    tbl.create(payload)
    return sid

def queue_message(from_number: str, to_number: str, body: str, campaign=None):
    """Compatibility wrapper — alias for send_message()."""
    return send_message(from_number, to_number, body, campaign=campaign)
