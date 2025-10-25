# sms/textgrid_sender.py  — fixed for TextGrid Breeze API

import os, base64, requests
from datetime import datetime, timezone
from sms.airtable_schema import default_conversation_payload, TABLE_NAMES
from sms.tables import get_table

TEXTGRID_ACCOUNT_SID = os.getenv("TEXTGRID_ACCOUNT_SID", "").strip()
TEXTGRID_AUTH_TOKEN  = os.getenv("TEXTGRID_AUTH_TOKEN", "").strip()

API_BASE = "https://api.textgrid.com/2010-04-01"

def _auth_header() -> dict:
    if not TEXTGRID_ACCOUNT_SID or not TEXTGRID_AUTH_TOKEN:
        raise RuntimeError("TEXTGRID_ACCOUNT_SID/TEXTGRID_AUTH_TOKEN missing")
    token = base64.b64encode(f"{TEXTGRID_ACCOUNT_SID}:{TEXTGRID_AUTH_TOKEN}".encode()).decode()
    return {"Authorization": f"Bearer {token}"}

def _send_via_textgrid(from_number: str, to_number: str, body: str, media_url: str | None = None) -> dict:
    url = f"{API_BASE}/Accounts/{TEXTGRID_ACCOUNT_SID}/Messages.json"
    payload = {"from": from_number, "to": to_number, "body": body}
    if media_url:
        # docs show mediaUrl is an array for MMS
        payload["mediaUrl"] = [media_url]
    headers = _auth_header() | {"Content-Type": "application/json"}
    resp = requests.post(url, json=payload, headers=headers, timeout=15)
    resp.raise_for_status()
    return resp.json()

def send_message(*, from_number: str, to: str, message: str, media_url: str | None = None, **kwargs) -> dict:
    msg = message if message is not None else kwargs.get("body", "")
    print(f"📤 Sending SMS → {to}: {msg[:60]}...")
    response = _send_via_textgrid(from_number, to, msg, media_url=media_url)

    sid = response.get("sid") or response.get("messageSid") or response.get("id")
    tbl = get_table("AIRTABLE_API_KEY", os.getenv("LEADS_CONVOS_BASE"), TABLE_NAMES["CONVERSATIONS"])
    payload = default_conversation_payload(from_number, to, msg)
    payload["Message SID"] = sid
    payload["Sent At"] = datetime.now(timezone.utc).isoformat()
    campaign = kwargs.get("campaign") or kwargs.get("campaign_id")
    if campaign:
        payload["Campaign"] = [campaign]
    if tbl:
        tbl.create(payload)

    # normalize a few fields
    out = dict(response)
    out.setdefault("sid", sid)
    out.setdefault("from", from_number)
    out.setdefault("to", to)
    out.setdefault("body", msg)
    out.setdefault("status", out.get("status") or "sent")
    return out