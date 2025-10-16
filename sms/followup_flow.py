# sms/followup_flow.py
from __future__ import annotations

import os, re, random, time, traceback
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple, List

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

try:
    from pyairtable import Table
except Exception:
    Table = None  # We’ll guard all usage

# =========================
# ENV / CONFIG
# =========================
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE")

DRIP_TABLE_NAME = os.getenv("DRIP_QUEUE_TABLE", "Drip Queue")
LEADS_TABLE_NAME = os.getenv("LEADS_TABLE", "Leads")
TEMPLATES_TABLE_NAME = os.getenv("TEMPLATES_TABLE", "Templates")

# Local time zone for “naive CT” timestamps shown nicely in Airtable
QUIET_TZ = ZoneInfo("America/Chicago") if ZoneInfo else None

# UI icons to match your app
STATUS_ICON = {"QUEUED": "⏳", "READY": "⏳", "SENDING": "🔄", "SENT": "✅", "DELIVERED": "✅", "FAILED": "❌", "CANCELLED": "❌"}

# -------- Stages (lightweight state machine) --------
# We keep human-readable string stages on Leads (and optionally on Drip rows).
STAGE_ENGAGE = "ENGAGE"  # warm/hot; quick nudges
STAGE_NEGOTIATE = "NEGOTIATE"  # price/condition; quick nudges
STAGE_NURTURE_30 = "NURTURE_30"  # 30-day cadence
STAGE_NURTURE_60 = "NURTURE_60"  # 60-day cadence
STAGE_NURTURE_90 = "NURTURE_90"  # 90-day cadence
STAGE_DNC = "DNC"  # do not contact
STAGE_WRONG = "WRONG_NUMBER"
STAGE_ARCHIVE = "ARCHIVE"

NURTURE_CHAIN = [STAGE_NURTURE_30, STAGE_NURTURE_60, STAGE_NURTURE_90, STAGE_ARCHIVE]

# Intent → (next_stage, delay, template_key)
# (Delays are minutes unless suffixed with _d for days.)
INTENT_PLAN: Dict[str, Dict[str, Any]] = {
    "optout": {"stage": STAGE_DNC, "delay_min": None, "template": None},
    "followup_wrong": {"stage": STAGE_WRONG, "delay_min": None, "template": None},
    "followup_no": {"stage": STAGE_NURTURE_60, "delay_d": 60, "template": "followup_60"},
    "neutral": {"stage": STAGE_NURTURE_30, "delay_d": 30, "template": "followup_30"},
    "intro": {"stage": STAGE_NURTURE_30, "delay_d": 30, "template": "followup_30"},
    "interest": {"stage": STAGE_ENGAGE, "delay_min": 120, "template": "engage_2h"},
    "followup_yes": {"stage": STAGE_ENGAGE, "delay_min": 120, "template": "engage_2h"},
    "price_response": {"stage": STAGE_NEGOTIATE, "delay_min": 30, "template": "negotiate_30m"},
    "condition_response": {"stage": STAGE_NEGOTIATE, "delay_min": 30, "template": "negotiate_30m"},
}

# Fallback copy per template key if Airtable Templates not present
FALLBACK_TEMPLATES = {
    "followup_30": "Hi {First}, circling back — still open to an offer on {Address}?",
    "followup_60": "Hey {First}, quick follow-up on {Address}. Any change in timing?",
    "followup_90": "Hi {First}, checking again on {Address}. Worth a quick chat?",
    "engage_2h": "Great — I’ll run numbers and text back soon. Anything I should know about {Address}?",
    "negotiate_30m": "Thanks! I’ll firm up pricing and reply shortly for {Address}.",
}

PHONE_FIELDS = [
    "phone",
    "Phone",
    "Mobile",
    "Cell",
    "Phone Number",
    "Owner Phone",
    "Owner Phone 1",
    "Owner Phone 2",
    "Phone 1 (from Linked Owner)",
    "Phone 2 (from Linked Owner)",
    "Phone 3 (from Linked Owner)",
]

# Lightweight cache for template lookups
_TEMPLATE_CACHE: Dict[str, Tuple[str, Optional[str], float]] = {}
_TEMPLATE_CACHE_TTL = 5 * 60  # seconds


# =========================
# Time helpers
# =========================
def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def ct_naive(dt_utc: datetime | None = None) -> str:
    """Return naive local CT ISO string (no Z) for Airtable date fields."""
    dt_utc = dt_utc or utcnow()
    if QUIET_TZ:
        local = dt_utc.astimezone(QUIET_TZ).replace(tzinfo=None)
    else:
        local = dt_utc.replace(tzinfo=None)
    return local.isoformat(timespec="seconds")


def plus_delay(now_utc: datetime, *, delay_min: int | None = None, delay_d: int | None = None) -> datetime:
    if delay_min is not None:
        return now_utc + timedelta(minutes=int(delay_min))
    if delay_d is not None:
        return now_utc + timedelta(days=int(delay_d))
    return now_utc  # immediate


def _parse_iso(s: Any) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        return None


# =========================
# Airtable helpers (field-safe)
# =========================
def _norm(s: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", s.strip().lower()) if isinstance(s, str) else str(s)


def _digits_only(v: Any) -> Optional[str]:
    if not isinstance(v, str):
        return None
    ds = "".join(re.findall(r"\d+", v))
    return ds if len(ds) >= 10 else None


def last10(v: Any) -> Optional[str]:
    d = _digits_only(v)
    return d[-10:] if d else None


def _table(name: str) -> Optional[Any]:
    if not (AIRTABLE_API_KEY and LEADS_CONVOS_BASE and Table):
        return None
    try:
        return Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, name)
    except Exception:
        traceback.print_exc()
        return None


def _auto_field_map(tbl: Any) -> Dict[str, str]:
    try:
        one = tbl.all(max_records=1)
        keys = list(one[0].get("fields", {}).keys()) if one else []
    except Exception:
        keys = []
    return {_norm(k): k for k in keys}


def _remap_existing_only(tbl: Any, payload: Dict) -> Dict:
    amap = _auto_field_map(tbl)
    if not amap:
        return dict(payload)
    out = {}
    for k, v in payload.items():
        mk = amap.get(_norm(k))
        if mk:
            out[mk] = v
    return out


def _safe_update(tbl: Any, rec_id: str, patch: Dict) -> Optional[Dict]:
    try:
        data = _remap_existing_only(tbl, patch)
        return tbl.update(rec_id, data) if data else None
    except Exception:
        traceback.print_exc()
        return None


def _fetch_lead_fields(leads: Any, lead_id: Optional[str]) -> Dict[str, Any]:
    if not (leads and lead_id):
        return {}
    try:
        row = leads.get(lead_id)
        if isinstance(row, dict):
            return row.get("fields", {}) or {}
    except Exception:
        traceback.print_exc()
    return {}


def _fetch_drip_snapshot(drip: Any) -> List[Dict[str, Any]]:
    if not drip:
        return []
    try:
        return drip.all(
            fields=[
                "phone",
                "Phone",
                "status",
                "Status",
                "next_send_date",
                "Next Send Date",
            ]
        )
    except Exception:
        traceback.print_exc()
        return []


def _safe_create(tbl: Any, payload: Dict) -> Optional[Dict]:
    try:
        data = _remap_existing_only(tbl, payload)
        return tbl.create(data) if data else None
    except Exception:
        traceback.print_exc()
        return None


# =========================
# Template selection
# =========================
def _personalize(msg: str, row_fields: Dict[str, Any]) -> str:
    name = row_fields.get("Owner Name") or row_fields.get("First") or "there"
    first = name.split()[0] if isinstance(name, str) and name else "there"
    address = row_fields.get("Property Address") or row_fields.get("Address") or "your property"
    try:
        return msg.format(First=first, Address=address)
    except Exception:
        return msg


def _template_by_key(key: Optional[str], row_fields: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    if not key:
        return "", None

    cache_key = key.strip().lower()
    now = time.monotonic()
    cached = _TEMPLATE_CACHE.get(cache_key)
    if cached and now - cached[2] < _TEMPLATE_CACHE_TTL:
        msg, template_id, _ = cached
        return _personalize(msg, row_fields), template_id

    templates = _table(TEMPLATES_TABLE_NAME)
    if templates:
        try:
            rows = templates.all()
            cands = []
            for r in rows:
                f = r.get("fields", {})
                internal = (f.get("Internal ID") or f.get("intent") or "").strip().lower()
                if internal == cache_key:
                    cands.append(r)
            if cands:
                chosen = random.choice(cands)
                msg = (chosen.get("fields", {}) or {}).get("Message") or ""
                result = _personalize(msg, row_fields), chosen["id"]
                _TEMPLATE_CACHE[cache_key] = (msg, chosen["id"], now)
                return result
        except Exception:
            traceback.print_exc()

    # fallback
    raw = FALLBACK_TEMPLATES.get(key, "Just checking back on {Address}, {First}.")
    _TEMPLATE_CACHE[cache_key] = (raw, None, now)
    return _personalize(raw, row_fields), None


# =========================
# Dup guards
# =========================
def _already_queued_today(drip: Any, phone: str, rows: Optional[List[Dict[str, Any]]] = None) -> bool:
    """Prevent multiple follow-ups same day for a phone."""
    try:
        today = datetime.now(timezone.utc)
        if QUIET_TZ:
            today = today.astimezone(QUIET_TZ)
        prefix = today.strftime("%Y-%m-%d")
        p10 = last10(phone)
        dataset = rows if rows is not None else drip.all()
        for r in dataset:
            f = r.get("fields", {})
            ph = f.get("phone") or f.get("Phone")
            if last10(ph) != p10:
                continue
            st = str(f.get("status") or f.get("Status") or "")
            when = f.get("next_send_date") or f.get("Next Send Date") or ""
            if st in ("QUEUED", "SENDING", "SENT", "DELIVERED"):
                if isinstance(when, str) and when.startswith(prefix):
                    return True
        return False
    except Exception:
        traceback.print_exc()
        return False


# =========================
# Stage helpers
# =========================
def _escalate_nurture(stage: str) -> str:
    """NURTURE_30 → _60 → _90 → ARCHIVE"""
    if stage not in NURTURE_CHAIN:
        return STAGE_NURTURE_30
    idx = NURTURE_CHAIN.index(stage)
    return NURTURE_CHAIN[min(idx + 1, len(NURTURE_CHAIN) - 1)]


def _get_phone(f: Dict[str, Any]) -> Optional[str]:
    # prefer verified if present
    p1 = f.get("Phone 1") or f.get("Phone 1 (from Linked Owner)")
    p2 = f.get("Phone 2") or f.get("Phone 2 (from Linked Owner)")
    if f.get("Phone 1 Verified") or f.get("Phone 1 Ownership Verified"):
        d = _digits_only(p1)
        if d:
            return d
    if f.get("Phone 2 Verified") or f.get("Phone 2 Ownership Verified"):
        d = _digits_only(p2)
        if d:
            return d
    for k in PHONE_FIELDS:
        d = _digits_only(f.get(k))
        if d:
            return d
    return None


# =========================
# Public API #1:
# Schedule from seller response (intent)
# =========================
def schedule_from_response(
    phone: str,
    intent: str,
    *,
    lead_id: Optional[str] = None,
    market: Optional[str] = None,
    property_id: Optional[str] = None,
    current_stage: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Call this from autoresponder after you label intent.
    - Sets Lead.drip_stage/Next Followup Date
    - Queues a Drip Queue row at the computed time (CT-naive) with message preview
    Returns a summary dict.
    """
    drip = _table(DRIP_TABLE_NAME)
    leads = _table(LEADS_TABLE_NAME)
    if not drip:
        return {"ok": False, "error": "Drip Queue table not configured"}

    phone_clean = _digits_only(phone) or (phone.strip() if isinstance(phone, str) else None)
    if not phone_clean:
        return {"ok": False, "error": "Phone number required"}

    plan = INTENT_PLAN.get((intent or "").strip().lower(), INTENT_PLAN["neutral"])
    next_stage = plan["stage"]
    delay_min = plan.get("delay_min")
    delay_d = plan.get("delay_d")
    template_key = plan.get("template")

    # Resolve escalation for repeated “no / neutral”
    if intent in ("followup_no", "neutral", "intro") and current_stage in NURTURE_CHAIN:
        next_stage = _escalate_nurture(current_stage)
        # adjust template/delay if we escalated to 90
        if next_stage == STAGE_NURTURE_90:
            template_key, delay_min, delay_d = "followup_90", None, 90

    # Stop stages
    if next_stage in (STAGE_DNC, STAGE_WRONG, STAGE_ARCHIVE) or (delay_min is None and delay_d is None):
        # Just mark the Lead; no new drip
        if leads and lead_id:
            _safe_update(leads, lead_id, {"drip_stage": next_stage, "Last Followup": utcnow().isoformat()})
        return {"ok": True, "queued": 0, "stage": next_stage, "note": "no follow-up queued (terminal stage)"}

    # Compute schedule
    send_at_utc = plus_delay(utcnow(), delay_min=delay_min, delay_d=delay_d)
    send_at_local_str = ct_naive(send_at_utc)

    # Compose message text
    fields_for_pers = {"Owner Name": "there", "Property Address": "your property"}
    lead_fields = _fetch_lead_fields(leads, lead_id)
    if lead_fields:
        fields_for_pers.update(lead_fields)
    msg, template_id = _template_by_key(template_key, fields_for_pers)

    # De-dupe same-day
    drip_snapshot = _fetch_drip_snapshot(drip)
    if _already_queued_today(drip, phone_clean, drip_snapshot):
        queued = 0
    else:
        payload = {
            "Leads": [lead_id] if lead_id else None,
            "phone": phone_clean,
            "Market": market,
            "Property ID": property_id,
            "message_preview": msg,
            "Template": [template_id] if template_id else None,
            "status": "QUEUED",
            "next_send_date": send_at_local_str,  # local CT (naive)
            "drip_stage": next_stage,
            "UI": STATUS_ICON["QUEUED"],
        }
        payload = {k: v for k, v in payload.items() if v is not None}
        _safe_create(drip, payload)
        drip_snapshot.append({"fields": {"phone": phone_clean, "status": "QUEUED", "next_send_date": send_at_local_str}})
        queued = 1

    # Update Lead planning fields
    if leads and lead_id:
        lead_patch = {
            "drip_stage": next_stage,
            "Next Followup Date": send_at_local_str.split("T")[0],  # date-only is fine here
            "Last Followup": utcnow().isoformat(),
        }
        _safe_update(leads, lead_id, lead_patch)

    return {"ok": True, "queued": queued, "stage": next_stage, "scheduled_local": send_at_local_str}


# =========================
# Public API #2:
# Hourly (or daily) job to queue due follow-ups from Leads
# =========================
def run_followups(limit: int = 1000) -> Dict[str, Any]:
    """
    Finds Leads with Next Followup Date <= today (CT) and creates a Drip Queue row if not already queued today.
    This is a safety net to ensure planning-only leads still get queued automatically.
    """
    drip = _table(DRIP_TABLE_NAME)
    leads = _table(LEADS_TABLE_NAME)
    if not (drip and leads):
        return {"ok": False, "queued_from_leads": 0, "errors": ["Tables not configured"]}

    try:
        rows = leads.all(max_records=limit)
    except Exception:
        traceback.print_exc()
        return {"ok": False, "queued_from_leads": 0, "errors": ["Failed to read Leads"]}

    today_ct = datetime.now(timezone.utc)
    if QUIET_TZ:
        today_ct = today_ct.astimezone(QUIET_TZ)
    today_str = today_ct.strftime("%Y-%m-%d")
    queued = 0
    errs: List[str] = []

    drip_snapshot = _fetch_drip_snapshot(drip)

    for lr in rows:
        lf = lr.get("fields", {})
        nfd = lf.get("Next Followup Date") or lf.get("next_followup_date") or lf.get("Followup Date")
        if not nfd:
            continue

        # if nfd is in the past or today
        nfd_dt = _parse_iso(nfd) or _parse_iso(str(nfd) + "T00:00:00Z")
        due = False
        if isinstance(nfd, str) and nfd[:10] <= today_str:  # string compare
            due = True
        elif nfd_dt and nfd_dt.date().isoformat() <= today_str:
            due = True
        if not due:
            continue

        phone = _get_phone(lf)
        if not phone:
            continue
        if _already_queued_today(drip, phone, drip_snapshot):
            continue

        stage = str(lf.get("drip_stage") or "").strip().upper() or STAGE_NURTURE_30
        # pick a template based on stage (maps to internal IDs used earlier)
        key = {
            STAGE_NURTURE_30: "followup_30",
            STAGE_NURTURE_60: "followup_60",
            STAGE_NURTURE_90: "followup_90",
            STAGE_ENGAGE: "engage_2h",
            STAGE_NEGOTIATE: "negotiate_30m",
        }.get(stage, "followup_30")

        msg, template_id = _template_by_key(key, lf)
        now_ct = ct_naive()

        dq_payload = {
            "Leads": [lr["id"]],
            "phone": phone,
            "Market": lf.get("Market"),
            "Property ID": lf.get("Property ID"),
            "message_preview": msg,
            "Template": [template_id] if template_id else None,
            "status": "QUEUED",
            "next_send_date": now_ct,  # queue immediately (outbound_batcher enforces quiet hrs)
            "drip_stage": stage,
            "UI": STATUS_ICON["QUEUED"],
        }
        _safe_create(drip, dq_payload)
        drip_snapshot.append({"fields": {"phone": phone, "status": "QUEUED", "next_send_date": now_ct}})
        queued += 1

        # Plan the next nurture step if in nurture chain
        if stage in (STAGE_NURTURE_30, STAGE_NURTURE_60, STAGE_NURTURE_90):
            next_stage = _escalate_nurture(stage)
            next_date = (utcnow() + timedelta(days=30 if stage != STAGE_NURTURE_90 else 90)).date().isoformat()
            _safe_update(
                leads,
                lr["id"],
                {
                    "drip_stage": next_stage,
                    "Last Followup": utcnow().isoformat(),
                    "Next Followup Date": next_date,
                },
            )
        else:
            _safe_update(leads, lr["id"], {"Last Followup": utcnow().isoformat()})

    return {"ok": True, "queued_from_leads": queued, "errors": errs}
