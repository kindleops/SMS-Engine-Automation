# sms/textgrid_sender.py
"""
📡 TextGrid Sender — Transport + Safe Conversations Logging
- Uses 2010-04-01 TextGrid endpoint (Twilio-style)
- No dependency on sms.tables (avoids signature mismatches)
- Writes to Airtable Conversations with "existing fields only"
- Never crashes sending if Airtable is down/misconfigured
"""

from __future__ import annotations

import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple, List

# --- HTTP client (prefer httpx, fallback to requests) ---
try:
    import httpx  # type: ignore
except Exception:
    httpx = None
try:
    import requests  # type: ignore
except Exception:
    requests = None

# --- Airtable (direct; no project wrappers) ---
try:
    from pyairtable import Table  # type: ignore
except Exception:
    Table = None  # guarded below

# =========================
# ENV / CONFIG
# =========================
ACCOUNT_SID = os.getenv("TEXTGRID_ACCOUNT_SID")
AUTH_TOKEN = os.getenv("TEXTGRID_AUTH_TOKEN")
API_URL = (
    f"https://api.textgrid.com/2010-04-01/Accounts/{ACCOUNT_SID}/Messages.json"
    if ACCOUNT_SID
    else None
)

AIRTABLE_KEY = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")

# Conversations field mapping (try to be flexible; we’ll remap to existing)
FROM_FIELD = os.getenv("CONV_FROM_FIELD", "phone")            # counterparty phone (recipient)
TO_FIELD = os.getenv("CONV_TO_FIELD", "to_number")            # our DID used to send
MSG_FIELD = os.getenv("CONV_MESSAGE_FIELD", "message")
STATUS_FIELD = os.getenv("CONV_STATUS_FIELD", "status")
DIR_FIELD = os.getenv("CONV_DIRECTION_FIELD", "direction")
TG_ID_FIELD = os.getenv("CONV_TEXTGRID_ID_FIELD", "TextGrid ID")
SENT_AT_FIELD = os.getenv("CONV_SENT_AT_FIELD", "sent_at")
PROCESSED_BY_FIELD = os.getenv("CONV_PROCESSED_BY_FIELD", "processed_by")

# Optional links (only written if fields exist)
LEAD_LINK_FIELD = os.getenv("CONV_LEAD_LINK_FIELD", "Lead")
PROPERTY_ID_FIELD = os.getenv("CONV_PROPERTY_ID_FIELD", "Property ID")
TEMPLATE_LINK_FIELD = os.getenv("CONV_TEMPLATE_LINK_FIELD", "Template")
CAMPAIGN_LINK_FIELD = os.getenv("CONV_CAMPAIGN_LINK_FIELD", "Campaign")

DEFAULT_SENDER_LABEL = os.getenv("SENDER_LABEL", "TextGrid Sender")
DRY_RUN = os.getenv("TEXTGRID_DRY_RUN", "0").lower() in ("1", "true", "yes")

# =========================
# Small helpers
# =========================
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def _norm(s: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s).strip().lower()) if s is not None else ""

def _convos_tbl() -> Optional[Any]:
    if not (AIRTABLE_KEY and LEADS_CONVOS_BASE and Table and CONVERSATIONS_TABLE):
        return None
    try:
        return Table(AIRTABLE_KEY, LEADS_CONVOS_BASE, CONVERSATIONS_TABLE)
    except Exception:
        return None

def _safe_create(tbl: Any, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Create with existing fields only to avoid 422s."""
    if not tbl or not payload:
        return None
    try:
        try:
            probe = tbl.all(max_records=1)
            keys = list((probe[0] or {}).get("fields", {}).keys()) if probe else []
        except Exception:
            keys = list(payload.keys())
        lut = {_norm(k): k for k in keys}
        filtered = {lut.get(_norm(k), k): v for k, v in payload.items() if _norm(k) in lut}
        return tbl.create(filtered if filtered else {})
    except Exception:
        return None

def _http_post(url: str, data: Dict[str, Any], auth: Tuple[str, str], timeout: int = 15) -> Dict[str, Any]:
    if DRY_RUN:
        print(f"[DRY RUN] POST {url} data={data}")
        return {"sid": f"SM_fake_{int(time.time())}", "status": "queued"}

    client = httpx or requests
    if client is None:
        raise RuntimeError("No HTTP client available (install httpx or requests).")

    if httpx:
        resp = httpx.post(url, data=data, auth=auth, timeout=timeout)
        if resp.status_code == 429:
            raise RuntimeError(f"429 rate limited; retry_after={resp.headers.get('Retry-After')}")
        resp.raise_for_status()
        try:
            return resp.json()
        except Exception:
            return {"raw": resp.text}

    # requests fallback
    resp = requests.post(url, data=data, auth=auth, timeout=timeout)
    if resp.status_code == 429:
        raise RuntimeError(f"429 rate limited; retry_after={resp.headers.get('Retry-After')}")
    resp.raise_for_status()
    try:
        return resp.json()
    except Exception:
        return {"raw": resp.text}

# =========================
# Core Sender
# =========================
def send_message(
    *,
    from_number: str,
    to: str,
    message: str,
    media_url: Optional[str] = None,
    campaign: Optional[str] = None,
    campaign_id: Optional[str] = None,
    template_id: Optional[str] = None,
    lead_id: Optional[str] = None,
    property_id: Optional[str] = None,
    timeout: int = 15,
) -> Dict[str, Any]:
    """
    Send one SMS via TextGrid and log a Conversations row (best-effort).
    Signature kept compatible with MessageProcessor.
    Returns minimal normalized envelope: {"status": "sent"|"failed", "sid": ..., "raw": ...}
    """
    if not to or not message:
        return {"status": "failed", "error": "missing to/message"}

    if not (ACCOUNT_SID and AUTH_TOKEN and API_URL):
        return {"status": "failed", "error": "textgrid credentials missing"}

    # --- Transport ---
    print(f"📤 Sending SMS → {to}: {message[:60]}...")
    data: Dict[str, Any] = {"To": to, "From": from_number, "Body": message}
    if media_url:
        data["MediaUrl"] = media_url

    try:
        resp = _http_post(API_URL, data=data, auth=(ACCOUNT_SID, AUTH_TOKEN), timeout=timeout)
    except Exception as e:
        # Log FAILED conversation (best-effort), then bubble up
        _log_conversation(
            status="FAILED",
            phone=to,
            from_number=from_number,
            body=message,
            sid=None,
            campaign=campaign or campaign_id,
            template_id=template_id,
            lead_id=lead_id,
            property_id=property_id,
            meta={"error": str(e)},
        )
        raise

    # Normalize provider response
    sid = (resp or {}).get("sid") or (resp or {}).get("messageSid") or (resp or {}).get("id")
    provider_status = str((resp or {}).get("status") or "sent").lower()
    ok = provider_status in {"queued", "accepted", "submitted", "enroute", "sent", "delivered"}

    # --- Conversations log (best-effort) ---
    _log_conversation(
        status="SENT" if ok else "FAILED",
        phone=to,
        from_number=from_number,
        body=message,
        sid=sid,
        campaign=campaign or campaign_id,
        template_id=template_id,
        lead_id=lead_id,
        property_id=property_id,
        meta={"provider_status": provider_status},
    )

    # Final envelope
    out = {"status": "sent" if ok else "failed", "sid": sid, "raw": resp}
    return out

# =========================
# Airtable logging
# =========================
def _log_conversation(
    *,
    status: str,
    phone: str,
    from_number: Optional[str],
    body: str,
    sid: Optional[str],
    campaign: Optional[str],
    template_id: Optional[str],
    lead_id: Optional[str],
    property_id: Optional[str],
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    tbl = _convos_tbl()
    if not tbl:
        # Airtable not configured — silently skip logging
        return

    payload: Dict[str, Any] = {
        FROM_FIELD: phone,
        TO_FIELD: from_number,
        MSG_FIELD: body,
        DIR_FIELD: "OUT",
        STATUS_FIELD: "SENT" if status.upper() == "SENT" else "FAILED",
        SENT_AT_FIELD: _now_iso(),
        PROCESSED_BY_FIELD: DEFAULT_SENDER_LABEL,
        TG_ID_FIELD: sid,
    }
    if campaign and CAMPAIGN_LINK_FIELD:
        payload[CAMPAIGN_LINK_FIELD] = [campaign]
    if template_id and TEMPLATE_LINK_FIELD:
        payload[TEMPLATE_LINK_FIELD] = [template_id]
    if lead_id and LEAD_LINK_FIELD:
        payload[LEAD_LINK_FIELD] = [lead_id]
    if property_id and PROPERTY_ID_FIELD:
        payload[PROPERTY_ID_FIELD] = property_id
    if meta:
        # Merge meta keys that happen to exist in the table (safe_create filters them)
        payload.update(meta)

    _safe_create(tbl, payload)

# Back-compat alias used by some call sites
def queue_message(from_number: str, to_number: str, body: str, campaign=None):
    return send_message(from_number=from_number, to=to_number, message=body, campaign=campaign)