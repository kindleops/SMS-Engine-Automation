# sms/followup_flow.py
"""
Follow-Up Flow (Datastore Refactor)
-----------------------------------
Handles:
  • Scheduling next follow-up after seller response
  • Automatically queuing overdue leads daily/hourly
Fully integrated with datastore + AI autoresponder.
"""

from __future__ import annotations
import random, traceback
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple
from zoneinfo import ZoneInfo

from sms.runtime import get_logger
from sms.datastore import CONNECTOR, update_record, create_record
from sms.airtable_schema import DripStatus

logger = get_logger("followup_flow")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
QUIET_TZ = ZoneInfo("America/Chicago")
STAGES = {
    "ENGAGE": "ENGAGE",
    "NEGOTIATE": "NEGOTIATE",
    "NURTURE_30": "NURTURE_30",
    "NURTURE_60": "NURTURE_60",
    "NURTURE_90": "NURTURE_90",
    "DNC": "DNC",
    "WRONG_NUMBER": "WRONG_NUMBER",
    "ARCHIVE": "ARCHIVE",
}
NURTURE_CHAIN = ["NURTURE_30", "NURTURE_60", "NURTURE_90", "ARCHIVE"]

INTENT_PLAN = {
    "optout": {"stage": STAGES["DNC"], "delay": None, "template": None},
    "followup_wrong": {"stage": STAGES["WRONG_NUMBER"], "delay": None, "template": None},
    "followup_no": {"stage": STAGES["NURTURE_60"], "delay": ("days", 60), "template": "followup_60"},
    "neutral": {"stage": STAGES["NURTURE_30"], "delay": ("days", 30), "template": "followup_30"},
    "intro": {"stage": STAGES["NURTURE_30"], "delay": ("days", 30), "template": "followup_30"},
    "interest": {"stage": STAGES["ENGAGE"], "delay": ("min", 120), "template": "engage_2h"},
    "followup_yes": {"stage": STAGES["ENGAGE"], "delay": ("min", 120), "template": "engage_2h"},
    "price_response": {"stage": STAGES["NEGOTIATE"], "delay": ("min", 30), "template": "negotiate_30m"},
    "condition_response": {"stage": STAGES["NEGOTIATE"], "delay": ("min", 30), "template": "negotiate_30m"},
}
FALLBACK_TEMPLATES = {
    "followup_30": "Hi {First}, circling back — still open to an offer on {Address}?",
    "followup_60": "Hey {First}, quick follow-up on {Address}. Any change in timing?",
    "followup_90": "Hi {First}, checking again on {Address}. Worth a quick chat?",
    "engage_2h": "Great — I’ll run numbers and text back soon. Anything I should know about {Address}?",
    "negotiate_30m": "Thanks! I’ll firm up pricing and reply shortly for {Address}.",
}
STATUS_ICON = {"QUEUED": "⏳", "READY": "⏳", "SENT": "✅", "FAILED": "❌"}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _utcnow() -> datetime: return datetime.now(timezone.utc)
def _ct_naive(dt: Optional[datetime] = None) -> str:
    return (dt or _utcnow()).astimezone(QUIET_TZ).replace(tzinfo=None).isoformat(timespec="seconds")
def _plus_delay(now: datetime, delay: Optional[Tuple[str, int]]) -> datetime:
    if not delay: return now
    typ, val = delay
    return now + timedelta(minutes=val) if typ == "min" else now + timedelta(days=val)
def _escalate(stage: str) -> str:
    if stage not in NURTURE_CHAIN: return "NURTURE_30"
    idx = NURTURE_CHAIN.index(stage)
    return NURTURE_CHAIN[min(idx + 1, len(NURTURE_CHAIN) - 1)]
def _template_message(key: str, context: Dict[str, Any]) -> str:
    raw = FALLBACK_TEMPLATES.get(key, "Checking back on {Address}, {First}.")
    name = context.get("First") or "there"
    addr = context.get("Address") or "your property"
    return raw.format(First=name, Address=addr)

# ---------------------------------------------------------------------------
# Core Scheduling Logic
# ---------------------------------------------------------------------------
def schedule_from_response(
    phone: str,
    intent: str,
    *,
    lead_id: Optional[str] = None,
    market: Optional[str] = None,
    property_id: Optional[str] = None,
    current_stage: Optional[str] = None,
) -> Dict[str, Any]:
    """Trigger next follow-up based on response intent."""
    drip_tbl = CONNECTOR.drip_queue()
    leads_tbl = CONNECTOR.leads()
    if not drip_tbl: return {"ok": False, "error": "Drip table unavailable"}

    plan = INTENT_PLAN.get(intent.lower(), INTENT_PLAN["neutral"])
    next_stage = plan["stage"]
    delay = plan.get("delay")
    template_key = plan.get("template")

    # Escalate for repeated neutral/no
    if intent in ("neutral", "followup_no", "intro") and current_stage in NURTURE_CHAIN:
        next_stage = _escalate(current_stage)
        if next_stage == "NURTURE_90":
            template_key, delay = "followup_90", ("days", 90)

    # Terminal stages (no new drips)
    if next_stage in {"DNC", "WRONG_NUMBER", "ARCHIVE"} or not delay:
        if leads_tbl and lead_id:
            update_record(leads_tbl, lead_id, {"drip_stage": next_stage, "Last Followup": _utcnow().isoformat()})
        return {"ok": True, "queued": 0, "stage": next_stage, "note": "terminal stage"}

    # Schedule new drip
    send_at_utc = _plus_delay(_utcnow(), delay)
    send_at_local = _ct_naive(send_at_utc)
    msg = _template_message(template_key, {"First": "there", "Address": "your property"})

    payload = {
        "Leads": [lead_id] if lead_id else None,
        "Seller Phone Number": phone,
        "Market": market,
        "Property ID": property_id,
        "Message Preview": msg,
        "Status": "QUEUED",
        "Next Send Date": send_at_local,
        "Drip Stage": next_stage,
        "UI": STATUS_ICON["QUEUED"],
    }
    payload = {k: v for k, v in payload.items() if v is not None}

    create_record(drip_tbl, payload)
    if leads_tbl and lead_id:
        update_record(leads_tbl, lead_id, {
            "drip_stage": next_stage,
            "Next Followup Date": send_at_local.split("T")[0],
            "Last Followup": _utcnow().isoformat(),
        })

    return {"ok": True, "queued": 1, "stage": next_stage, "scheduled_local": send_at_local}

# ---------------------------------------------------------------------------
# Daily / Hourly Auto-Followups
# ---------------------------------------------------------------------------
def run_followups(limit: int = 1000) -> Dict[str, Any]:
    """Auto-queue due leads for follow-up."""
    drip_tbl = CONNECTOR.drip_queue()
    leads_tbl = CONNECTOR.leads()
    if not (drip_tbl and leads_tbl):
        return {"ok": False, "queued_from_leads": 0, "error": "Tables unavailable"}

    try:
        leads = leads_tbl.all(max_records=limit)
    except Exception as e:
        traceback.print_exc()
        return {"ok": False, "queued_from_leads": 0, "error": str(e)}

    today = _utcnow().astimezone(QUIET_TZ).strftime("%Y-%m-%d")
    queued = 0

    for r in leads:
        f = r.get("fields", {}) or {}
        nfd = str(f.get("Next Followup Date") or f.get("next_followup_date") or "")
        if not nfd or nfd[:10] > today: continue

        phone = f.get("Phone") or f.get("Seller Phone Number")
        if not phone: continue

        stage = (f.get("drip_stage") or "NURTURE_30").upper()
        key = {
            "NURTURE_30": "followup_30",
            "NURTURE_60": "followup_60",
            "NURTURE_90": "followup_90",
            "ENGAGE": "engage_2h",
            "NEGOTIATE": "negotiate_30m",
        }.get(stage, "followup_30")

        msg = _template_message(key, f)
        now_ct = _ct_naive()

        dq_payload = {
            "Leads": [r["id"]],
            "Seller Phone Number": phone,
            "Market": f.get("Market"),
            "Property ID": f.get("Property ID"),
            "Message Preview": msg,
            "Status": "QUEUED",
            "Next Send Date": now_ct,
            "Drip Stage": stage,
            "UI": STATUS_ICON["QUEUED"],
        }
        create_record(drip_tbl, dq_payload)
        queued += 1

        # Escalate stage
        if stage in NURTURE_CHAIN:
            next_stage = _escalate(stage)
            next_date = (_utcnow() + timedelta(days=30 if stage != "NURTURE_90" else 90)).date().isoformat()
            update_record(leads_tbl, r["id"], {
                "drip_stage": next_stage,
                "Last Followup": _utcnow().isoformat(),
                "Next Followup Date": next_date,
            })
        else:
            update_record(leads_tbl, r["id"], {"Last Followup": _utcnow().isoformat()})

    return {"ok": True, "queued_from_leads": queued}
