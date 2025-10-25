# sms/textgrid_sender.py
"""
📡 TextGrid Sender — Final Optimized Version
Handles outbound SMS delivery through TextGrid API and logs conversations to Airtable.
"""

from __future__ import annotations

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

def _send_via_textgrid(from_number: str, to_number: str, body: str, media_url: str | None = None) -> dict:
    """Low-level API call to TextGrid"""
    url = f"{API_BASE}/Messages.json"
    auth = (TEXTGRID_ACCOUNT_SID, TEXTGRID_AUTH_TOKEN)
    data = {"From": from_number, "To": to_number, "Body": body}
    if media_url:
        data["MediaUrl"] = media_url
    resp = requests.post(url, data=data, auth=auth, timeout=10)
    resp.raise_for_status()
    return resp.json()

def send_message(
    *,
    from_number: str,
    to: str,
    message: str,
    media_url: str | None = None,
    **kwargs,
) -> dict:
    """Send a single SMS immediately via TextGrid + log to Airtable."""
    msg = message if message is not None else kwargs.get("body", "")
    print(f"📤 Sending SMS → {to}: {msg[:60]}...")
    response = _send_via_textgrid(from_number, to, msg, media_url=media_url)
    sid = response.get("sid") or response.get("messageSid") or response.get("id")

    tbl = get_table("AIRTABLE_API_KEY", os.getenv("LEADS_CONVOS_BASE"), TABLE_NAMES["CONVERSATIONS"])
    payload = default_conversation_payload(from_number, to, msg)
    payload["Message SID"] = sid
    payload["Sent At"] = datetime.now(timezone.utc).isoformat()
    campaign = kwargs.get("campaign")
    if not campaign:
        campaign = kwargs.get("campaign_id")
    if campaign:
        payload["Campaign"] = [campaign]
    tbl.create(payload)
    enriched = dict(response)
    if sid:
        enriched.setdefault("sid", sid)
        enriched.setdefault("message_sid", sid)
    status = enriched.get("status") or enriched.get("Status")
    if status:
        enriched.setdefault("status", status)
    else:
        enriched["status"] = "sent"
    enriched.setdefault("from", from_number)
    enriched.setdefault("to", to)
    enriched.setdefault("body", msg)
    return enriched

def queue_message(from_number: str, to_number: str, body: str, campaign=None):
    """Compatibility wrapper — alias for send_message()."""
    return send_message(
        from_number=from_number,
        to=to_number,
        message=body,
        campaign=campaign,
    )
