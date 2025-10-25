# sms/textgrid_sender.py
"""
📡 TextGrid Sender — Final Optimized Version
Handles outbound SMS delivery through TextGrid API and best-effort logs to Airtable.
"""

from __future__ import annotations

import os
import requests
from datetime import datetime, timezone

from sms.airtable_schema import (
    default_conversation_payload,
    TABLE_NAMES,
    ConversationProcessor,  # kept for compatibility if referenced elsewhere
)
from sms.tables import get_table  # IMPORTANT: called with env var *names* + default_table

# ──────────────────────────────────────────────────────────────────────────────
# TextGrid config
# ──────────────────────────────────────────────────────────────────────────────
TEXTGRID_ACCOUNT_SID = os.getenv("TEXTGRID_ACCOUNT_SID")
TEXTGRID_AUTH_TOKEN = os.getenv("TEXTGRID_AUTH_TOKEN")

# Default to v1 (Messages.json). You can override with TEXTGRID_API_BASE if needed.
API_BASE = os.getenv("TEXTGRID_API_BASE", "https://api.textgrid.com/v1")


def _send_via_textgrid(from_number: str, to_number: str, body: str, media_url: str | None = None) -> dict:
    """
    Low-level TextGrid API call (v1).
    If you switch to Breeze/v2 later, update API_BASE and the endpoint/params accordingly.
    """
    url = f"{API_BASE}/Messages.json"
    auth = (TEXTGRID_ACCOUNT_SID, TEXTGRID_AUTH_TOKEN)
    data = {"From": from_number, "To": to_number, "Body": body}
    if media_url:
        data["MediaUrl"] = media_url
    resp = requests.post(url, data=data, auth=auth, timeout=10)
    resp.raise_for_status()
    # Some providers return 201 w/o json body; guard for that.
    try:
        return resp.json()
    except Exception:
        return {"status": "queued"}  # minimal fallback


def send_message(
    *,
    from_number: str,
    to: str,
    message: str,
    media_url: str | None = None,
    **kwargs,
) -> dict:
    """
    Send a single SMS via TextGrid + (best-effort) log a Conversation row to Airtable.
    Never let Airtable logging failures break the send path.
    """
    msg = message if message is not None else kwargs.get("body", "")
    print(f"📤 Sending SMS → {to}: {msg[:60]}...")

    # 1) Transport
    response = _send_via_textgrid(from_number, to, msg, media_url=media_url)
    sid = response.get("sid") or response.get("messageSid") or response.get("id")

    # 2) Best-effort Airtable log
    try:
        # NOTE: pass ENV VAR *NAMES* and include default_table to match get_table(...) signature
        conv_table = get_table(
            "AIRTABLE_API_KEY",
            "LEADS_CONVOS_BASE",
            TABLE_NAMES["CONVERSATIONS"],
            default_table=TABLE_NAMES["CONVERSATIONS"],
        )
        if conv_table:
            payload = default_conversation_payload(from_number, to, msg)
            if sid:
                payload["Message SID"] = sid
            payload["Sent At"] = datetime.now(timezone.utc).isoformat()

            campaign = kwargs.get("campaign") or kwargs.get("campaign_id")
            if campaign:
                # Linked record expects an array of record IDs
                payload["Campaign"] = [campaign]

            conv_table.create(payload)
    except Exception as log_exc:
        # Don't convert this into a transport failure
        print(f"[textgrid_sender] Airtable logging skipped: {log_exc}")

    # 3) Normalize/return
    enriched = dict(response) if isinstance(response, dict) else {}
    if sid:
        enriched.setdefault("sid", sid)
        enriched.setdefault("message_sid", sid)
    status = enriched.get("status") or enriched.get("Status") or "sent"
    enriched["status"] = status
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