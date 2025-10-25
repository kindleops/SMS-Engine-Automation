# sms/textgrid_sender.py
"""
📡 TextGrid Sender — Breeze-spec compliant
- Correct base URL & path: /2010-04-01/Accounts/{sid}/Messages.json
- Bearer auth: base64("{AccountSid}:{AuthToken}")
- JSON payload with body/from/to (+ mediaUrl as list)
"""

from __future__ import annotations
import os, base64, requests
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from sms.airtable_schema import default_conversation_payload, TABLE_NAMES
from sms.tables import get_table

ACCOUNT_SID = os.getenv("TEXTGRID_ACCOUNT_SID", "").strip()
AUTH_TOKEN  = os.getenv("TEXTGRID_AUTH_TOKEN", "").strip()
API_VERSION = os.getenv("TEXTGRID_API_VERSION", "2010-04-01").strip()
API_BASE    = os.getenv("TEXTGRID_API_BASE", f"https://api.textgrid.com/{API_VERSION}").rstrip("/")
STATUS_CALLBACK = os.getenv("TEXTGRID_STATUS_CALLBACK")  # optional

def _bearer_token() -> str:
    auth = f"{ACCOUNT_SID}:{AUTH_TOKEN}".encode("utf-8")
    return base64.b64encode(auth).decode("ascii")

def _headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {_bearer_token()}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

def _send_via_textgrid(*, from_number: str, to_number: str, body: str, media_url: Optional[str]=None) -> Dict[str, Any]:
    if not ACCOUNT_SID or not AUTH_TOKEN:
        raise RuntimeError("TEXTGRID_ACCOUNT_SID / TEXTGRID_AUTH_TOKEN missing")

    url = f"{API_BASE}/Accounts/{ACCOUNT_SID}/Messages.json"
    payload: Dict[str, Any] = {"body": body, "from": from_number, "to": to_number}
    if STATUS_CALLBACK:
        payload["statusCallback"] = STATUS_CALLBACK
    if media_url:
        payload["mediaUrl"] = [media_url]  # MMS expects an array

    resp = requests.post(url, headers=_headers(), json=payload, timeout=15)
    resp.raise_for_status()
    return resp.json()

def send_message(*, from_number: str, to: str, message: str, media_url: Optional[str]=None, **kwargs) -> Dict[str, Any]:
    msg = message if message is not None else kwargs.get("body", "")
    print(f"📤 Sending SMS → {to}: {msg[:60]}...")

    # call TextGrid
    tg = _send_via_textgrid(from_number=from_number, to_number=to, body=msg, media_url=media_url)

    # normalize response
    sid = tg.get("sid") or tg.get("messageSid") or tg.get("id")
    status = tg.get("status") or "sent"
    enriched = {**tg, "sid": sid, "status": status, "from": tg.get("from") or from_number, "to": tg.get("to") or to, "body": msg}

    # Airtable log
    base = os.getenv("LEADS_CONVOS_BASE")
    if base:
        try:
            tbl = get_table("AIRTABLE_API_KEY", base, TABLE_NAMES["CONVERSATIONS"])
            payload = default_conversation_payload(from_number, to, msg)
            if sid: payload["Message SID"] = sid
            payload["Sent At"] = datetime.now(timezone.utc).isoformat()
            campaign = kwargs.get("campaign") or kwargs.get("campaign_id")
            if campaign: payload["Campaign"] = [campaign]
            tbl.create(payload)
        except Exception:
            # non-fatal: keep send result
            pass

    return enriched

def queue_message(from_number: str, to_number: str, body: str, campaign=None):
    return send_message(from_number=from_number, to=to_number, message=body, campaign=campaign)
