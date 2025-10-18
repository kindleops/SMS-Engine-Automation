# sms/textgrid_sender.py
from __future__ import annotations

import os
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

# --- HTTP client (prefer httpx, fallback to requests) ---
try:
    import httpx
except Exception:
    httpx = None
try:
    import requests  # fallback if httpx missing
except Exception:
    requests = None

# --- Pool DID selector (already resilient) ---
try:
    from sms.number_pools import get_from_number
except Exception:
    get_from_number = None

# --- Lazy Airtable table getters (robust wrappers you added) ---
try:
    from sms.tables import get_convos, get_leads
except Exception:
    get_convos = get_leads = lambda *a, **k: None  # type: ignore

# =========================
# ENV / CONFIG
# =========================
ACCOUNT_SID = os.getenv("TEXTGRID_ACCOUNT_SID")
AUTH_TOKEN = os.getenv("TEXTGRID_AUTH_TOKEN")
BASE_URL = f"https://api.textgrid.com/2010-04-01/Accounts/{ACCOUNT_SID}/Messages.json" if ACCOUNT_SID else None

# Conversations field mapping (defaults align with your base)
FROM_FIELD = os.getenv("CONV_FROM_FIELD", "phone")  # counterparty phone (recipient)
TO_FIELD = os.getenv("CONV_TO_FIELD", "to_number")  # our DID used to send
MSG_FIELD = os.getenv("CONV_MESSAGE_FIELD", "message")
STATUS_FIELD = os.getenv("CONV_STATUS_FIELD", "status")
DIR_FIELD = os.getenv("CONV_DIRECTION_FIELD", "direction")
TG_ID_FIELD = os.getenv("CONV_TEXTGRID_ID_FIELD", "TextGrid ID")
SENT_AT_FIELD = os.getenv("CONV_SENT_AT_FIELD", "sent_at")
PROCESSED_BY = os.getenv("CONV_PROCESSED_BY_FIELD", "processed_by")

# Optional extras we’ll write only if fields exist
LEAD_LINK_FIELD = os.getenv("CONV_LEAD_LINK_FIELD", "lead_id")
PROPERTY_ID_FIELD = os.getenv("CONV_PROPERTY_ID_FIELD", "Property ID")
TEMPLATE_LINK_FLD = os.getenv("CONV_TEMPLATE_LINK_FIELD", "Template")
CAMPAIGN_LINK_FLD = os.getenv("CONV_CAMPAIGN_LINK_FIELD", "Campaign")

DEFAULT_SENDER_LABEL = "TextGrid Sender"
DRY_RUN = os.getenv("TEXTGRID_DRY_RUN", "0").lower() in ("1", "true", "yes")


# =========================
# Small helpers
# =========================
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _digits(s: Any) -> Optional[str]:
    if not isinstance(s, str):
        return None
    import re

    d = "".join(re.findall(r"\d+", s))
    return d if len(d) >= 10 else None


def _safe_table_create(tbl, payload: Dict) -> Optional[Dict]:
    """Create with 'existing fields only' to avoid 422s."""
    if not tbl or not payload:
        return None
    try:
        # Build a field whitelist by peeking at one row (or empty)
        try:
            probe = tbl.all(max_records=1)
            keys = list((probe[0] or {}).get("fields", {}).keys()) if probe else []
        except Exception:
            keys = list(payload.keys())  # optimistic
        norm = {_norm(k): k for k in keys}
        filtered = {}
        for k, v in payload.items():
            mk = norm.get(_norm(k))
            if mk:
                filtered[mk] = v
        return tbl.create(filtered if filtered else {})
    except Exception:
        traceback.print_exc()
        return None


def _safe_table_update(tbl, rec_id: str, patch: Dict) -> Optional[Dict]:
    if not tbl or not rec_id or not patch:
        return None
    try:
        try:
            probe = tbl.get(rec_id)
            keys = list((probe or {}).get("fields", {}).keys()) if probe else []
        except Exception:
            keys = list(patch.keys())
        norm = {_norm(k): k for k in keys}
        filtered = {}
        for k, v in patch.items():
            mk = norm.get(_norm(k))
            if mk:
                filtered[mk] = v
        return tbl.update(rec_id, filtered if filtered else {})
    except Exception:
        traceback.print_exc()
        return None


def _norm(s: Any) -> str:
    import re

    return re.sub(r"[^a-z0-9]+", "", str(s).strip().lower())


def _http_post(url: str, data: Dict[str, Any], auth: Tuple[str, str], timeout: int = 10) -> Dict[str, Any]:
    """POST with httpx or requests, returning parsed JSON or raising."""
    if DRY_RUN:
        print(f"[DRY RUN] POST {url} data={data}")
        # Fake a TextGrid-ish response
        return {"sid": f"SM_fake_{int(time.time())}"}

    if httpx:
        resp = httpx.post(url, data=data, auth=auth, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    if requests:
        resp = requests.post(url, data=data, auth=auth, timeout=timeout)
        resp.raise_for_status()
        try:
            return resp.json()
        except Exception:
            # Some carriers return urlencoded; try to coerce
            return {"raw": resp.text}
    raise RuntimeError("No HTTP client available (install httpx or requests).")


# =========================
# Lead helpers
# =========================
def _find_or_create_lead(phone_number: str, source: str = "Outbound") -> Tuple[Optional[str], Optional[str]]:
    """Ensure every outbound target has a Lead. Returns (lead_id, property_id)."""
    leads_tbl = get_leads()
    if not (leads_tbl and phone_number):
        return None, None
    try:
        # match on exact 'phone'; extend if you keep last10 elsewhere
        recs = leads_tbl.all(formula=f"{{phone}}='{phone_number}'")
        if recs:
            lf = recs[0].get("fields", {})
            return recs[0]["id"], lf.get(PROPERTY_ID_FIELD) or lf.get("Property ID")

        created = _safe_table_create(
            leads_tbl,
            {
                "phone": phone_number,
                "Lead Status": "New",
                "Source": source,
                "Reply Count": 0,
                "Sent Count": 0,
                "Delivered Count": 0,
                "Failed Count": 0,
                "Last Activity": _now_iso(),
            },
        )
        if created:
            print(f"✨ Created Lead for {phone_number}")
            cf = created.get("fields", {}) if isinstance(created, dict) else {}
            return created.get("id"), cf.get(PROPERTY_ID_FIELD) or cf.get("Property ID")
    except Exception:
        traceback.print_exc()
    return None, None


def _update_lead_activity(lead_id: Optional[str], body: str, direction: str, property_id: Optional[str] = None) -> None:
    if not lead_id:
        return
    leads_tbl = get_leads()
    if not leads_tbl:
        return
    try:
        patch = {
            "Last Activity": _now_iso(),
            "Last Direction": direction,
            "Last Message": (body or "")[:500],
        }
        if direction == "OUT":
            patch["Last Outbound"] = _now_iso()
        if property_id:
            patch[PROPERTY_ID_FIELD] = property_id
        _safe_table_update(leads_tbl, lead_id, patch)
    except Exception:
        traceback.print_exc()


# =========================
# Core Sender
# =========================
def send_message(
    to: str,
    body: str,
    from_number: Optional[str] = None,
    market: Optional[str] = None,
    lead_id: Optional[str] = None,
    property_id: Optional[str] = None,
    template_id: Optional[str] = None,
    campaign_id: Optional[str] = None,
    retries: int = 3,
    timeout: int = 10,
) -> Dict[str, Any]:
    """
    Send an SMS via TextGrid and write a Conversations row.
    Returns: {"ok": bool, "sid": str|None, "to": str, "from": str|None, "lead_id": str|None, "property_id": str|None, ...}

    Notes:
      - Conversations mapping uses `phone` (counterparty) and `to_number` (our DID).
      - If Airtable is unavailable, sending still happens; logging is best-effort.
    """
    if not to or not body:
        return {"ok": False, "error": "missing to/body"}

    if not ACCOUNT_SID or not AUTH_TOKEN or not BASE_URL:
        return {"ok": False, "error": "TEXTGRID_ACCOUNT_SID/TEXTGRID_AUTH_TOKEN not configured"}

    # Choose a DID if not provided
    sender = from_number
    if not sender and get_from_number:
        try:
            sender = get_from_number(market or "")
        except Exception as e:
            print(f"⚠️ DID selection failed (market={market}): {e}")
            sender = None
    if not sender:
        return {"ok": False, "error": "no from_number available"}

    # Ensure a Lead exists (optional/best-effort)
    if not lead_id:
        _lead_id, _prop_id = _find_or_create_lead(to, source="Outbound")
        lead_id = lead_id or _lead_id
        property_id = property_id or _prop_id

    # Send with retries (exponential backoff 2^n)
    last_err = None
    msg_id: Optional[str] = None

    for attempt in range(1, max(1, retries) + 1):
        try:
            data = {"To": to, "From": sender, "Body": body}
            resp = _http_post(BASE_URL, data=data, auth=(ACCOUNT_SID, AUTH_TOKEN), timeout=timeout)
            msg_id = (resp or {}).get("sid") or (resp or {}).get("message_sid") or (resp or {}).get("id")
            print(f"📤 OUT → {to} (from {sender}) | {body[:120]}")
            last_err = None
            break
        except Exception as e:
            last_err = str(e)
            print(f"❌ Send attempt {attempt}/{retries} failed → {to}: {last_err}")
            if attempt < retries:
                wait = 2**attempt
                print(f"⏳ retrying in {wait}s...")
                time.sleep(wait)

    # Log to Conversations (best effort) with correct schema mapping
    convos_tbl = get_convos()
    try:
        if convos_tbl:
            rec: Dict[str, Any] = {
                FROM_FIELD: to,  # counterparty phone
                TO_FIELD: sender,  # our DID used to send
                MSG_FIELD: body,
                DIR_FIELD: "OUT",
                STATUS_FIELD: "SENT" if last_err is None else "FAILED",
                SENT_AT_FIELD: _now_iso(),
                PROCESSED_BY: DEFAULT_SENDER_LABEL,
                TG_ID_FIELD: msg_id,
            }
            # optional relations if your schema has them
            if lead_id and LEAD_LINK_FIELD:
                rec[LEAD_LINK_FIELD] = [lead_id]
            if property_id and PROPERTY_ID_FIELD:
                rec[PROPERTY_ID_FIELD] = property_id
            if template_id and TEMPLATE_LINK_FLD:
                # if Template is a linked field, one ID in a list is typical
                rec[TEMPLATE_LINK_FLD] = [template_id]
            if campaign_id and CAMPAIGN_LINK_FLD:
                rec[CAMPAIGN_LINK_FLD] = [campaign_id]

            _safe_table_create(convos_tbl, rec)
    except Exception:
        traceback.print_exc()

    # Update lead activity trail
    try:
        _update_lead_activity(lead_id, body, "OUT", property_id=property_id)
    except Exception:
        traceback.print_exc()

    if last_err is None:
        return {
            "ok": True,
            "sid": msg_id,
            "to": to,
            "from": sender,
            "lead_id": lead_id,
            "property_id": property_id,
        }
    else:
        return {
            "ok": False,
            "error": last_err,
            "sid": msg_id,
            "to": to,
            "from": sender,
            "lead_id": lead_id,
            "property_id": property_id,
        }
