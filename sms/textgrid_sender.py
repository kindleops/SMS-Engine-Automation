# sms/textgrid_sender.py
"""
📡 TextGrid Sender — transport-only (MessageProcessor-compatible)
- Matches MessageProcessor v3.1 call signature: send_message(from_number=..., to=..., message=...)
- Uses TextGrid's Twilio-style endpoint that previously worked for you:
  https://api.textgrid.com/2010-04-01/Accounts/{ACCOUNT_SID}/Messages.json
- No Airtable calls here (MessageProcessor handles logging + metrics)
"""

from __future__ import annotations

import os
import requests
from typing import Any, Dict, Optional

# ---- Credentials & endpoint -----------------------------------------------
TEXTGRID_ACCOUNT_SID = os.getenv("TEXTGRID_ACCOUNT_SID")
TEXTGRID_AUTH_TOKEN = os.getenv("TEXTGRID_AUTH_TOKEN")

# Optional override: set TEXTGRID_API_BASE to a full URL if your account uses a different family.
# e.g. TEXTGRID_API_BASE="https://api.textgrid.com/2010-04-01/Accounts/ACxxxxx/Messages.json"
_API_BASE_OVERRIDE = (os.getenv("TEXTGRID_API_BASE") or "").strip()

def _resolve_messages_url() -> str:
    if _API_BASE_OVERRIDE:
        return _API_BASE_OVERRIDE.rstrip("/")
    if not TEXTGRID_ACCOUNT_SID:
        # We still return a placeholder; the send will fail fast with a clear error
        return "https://api.textgrid.com/2010-04-01/Accounts//Messages.json"
    return f"https://api.textgrid.com/2010-04-01/Accounts/{TEXTGRID_ACCOUNT_SID}/Messages.json"

MESSAGES_URL = _resolve_messages_url()

# ---- Low-level HTTP --------------------------------------------------------
def _send_via_textgrid(from_number: str, to_number: str, body: str, media_url: Optional[str] = None) -> Dict[str, Any]:
    if not (TEXTGRID_ACCOUNT_SID and TEXTGRID_AUTH_TOKEN):
        raise RuntimeError("missing_textgrid_credentials")

    data: Dict[str, Any] = {
        "From": from_number,
        "To": to_number,
        "Body": body,
    }
    if media_url:
        data["MediaUrl"] = media_url

    resp = requests.post(MESSAGES_URL, data=data, auth=(TEXTGRID_ACCOUNT_SID, TEXTGRID_AUTH_TOKEN), timeout=15)
    resp.raise_for_status()

    try:
        return resp.json()  # TextGrid returns JSON compatible with Twilio style
    except Exception:
        # Fallback if content-type isn't JSON
        return {"raw": resp.text}

# ---- Public API (used by MessageProcessor) ---------------------------------
def send_message(
    *,
    from_number: str,
    to: str,
    message: str,
    media_url: Optional[str] = None,
    **kwargs,
) -> Dict[str, Any]:
    """
    Transport-only sender. Returns a normalized dict with at least: status, sid (if any).
    MessageProcessor handles logging, KPIs, retries, etc.
    """
    # Be forgiving if caller passed body=... (legacy)
    msg = message if message is not None else kwargs.get("body", "")

    if not to or not msg:
        return {"status": "failed", "error": "missing_to_or_body"}

    # Nice console trace (kept from your previous script)
    print(f"📤 Sending SMS → {to}: {msg[:60]}...")

    res = _send_via_textgrid(from_number, to, msg, media_url=media_url)

    # Normalize common fields for the MessageProcessor
    sid = res.get("sid") or res.get("messageSid") or res.get("id")
    status = (res.get("status") or res.get("Status") or "sent").lower()

    normalized = {
        "sid": sid,
        "status": status,       # MessageProcessor checks 'sent'/'delivered'
        "from": from_number,
        "to": to,
        "body": msg,
        "raw": res,
    }
    # If provider didn’t return a status, mark as 'sent' so MP can proceed
    if "status" not in res:
        normalized["status"] = "sent"
    return normalized

# Legacy alias (if anything else calls it)
def queue_message(from_number: str, to_number: str, body: str, campaign=None):
    return send_message(from_number=from_number, to=to_number, message=body, campaign=campaign)