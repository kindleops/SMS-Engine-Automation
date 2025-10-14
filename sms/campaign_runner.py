# sms/campaign_runner.py
from __future__ import annotations

import os, re, json, random, math, traceback
from datetime import datetime, timezone, timedelta, date
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────
# pyairtable compat (v2: Api, v1: Table)
# ─────────────────────────────────────────────────────────────
_PyApi = None
_PyTable = None
try:
    from pyairtable import Api as _PyApi  # v2
except Exception:
    _PyApi = None
try:
    from pyairtable import Table as _PyTable  # v1
except Exception:
    _PyTable = None

def _make_table(api_key: Optional[str], base_id: Optional[str], table_name: str):
    if not (api_key and base_id and table_name):
        return None
    try:
        if _PyApi:
            return _PyApi(api_key).table(base_id, table_name)
        if _PyTable:
            return _PyTable(api_key, base_id, table_name)
    except Exception:
        traceback.print_exc()
    return None

# ─────────────────────────────────────────────────────────────
# Optional engine hooks
# ─────────────────────────────────────────────────────────────
try:
    from sms.outbound_batcher import send_batch
except Exception:
    def send_batch(*args, **kwargs):
        return {"total_sent": 0}

try:
    from sms.retry_runner import run_retry
except Exception:
    def run_retry(*args, **kwargs):
        return {"retried": 0}

try:
    from sms.metrics_tracker import update_metrics
except Exception:
    def update_metrics(*args, **kwargs):
        return {"ok": True}

# ─────────────────────────────────────────────────────────────
# ENV / CONFIG
# ─────────────────────────────────────────────────────────────
AIRTABLE_KEY = os.getenv("AIRTABLE_API_KEY")

LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
CAMPAIGN_CONTROL_BASE = os.getenv("CAMPAIGN_CONTROL_BASE") or os.getenv("AIRTABLE_CAMPAIGN_CONTROL_BASE_ID")
PERFORMANCE_BASE = os.getenv("PERFORMANCE_BASE") or os.getenv("AIRTABLE_PERFORMANCE_BASE_ID")

PROSPECTS_TABLE = os.getenv("PROSPECTS_TABLE", "Prospects")
CAMPAIGNS_TABLE = os.getenv("CAMPAIGNS_TABLE", "Campaigns")
TEMPLATES_TABLE = os.getenv("TEMPLATES_TABLE", "Templates")
DRIP_QUEUE_TABLE = os.getenv("DRIP_QUEUE_TABLE", "Drip Queue")
NUMBERS_TABLE = os.getenv("NUMBERS_TABLE", "Numbers")

MESSAGES_PER_MIN = max(1, int(os.getenv("MESSAGES_PER_MIN", "20")))
SECONDS_PER_MSG = max(1, int(math.ceil(60.0 / MESSAGES_PER_MIN)))
JITTER_SECONDS = max(0, int(os.getenv("JITTER_SECONDS", "2")))

RATE_PER_NUMBER_PER_MIN = max(
    1, int(os.getenv("RATE_PER_NUMBER_PER_MIN", os.getenv("RATE_MAX_PER_NUMBER_PER_MIN", "20")))
)
SECONDS_PER_NUMBER_MSG = max(1, int(math.ceil(60.0 / RATE_PER_NUMBER_PER_MIN)))

QUIET_TZ = ZoneInfo(os.getenv("QUIET_TZ", "America/Chicago"))
QUIET_START_HOUR = int(os.getenv("QUIET_START_HOUR", os.getenv("QUIET_START_HOUR_LOCAL", "21")))
QUIET_END_HOUR   = int(os.getenv("QUIET_END_HOUR",   os.getenv("QUIET_END_HOUR_LOCAL",   "9")))

RUNNER_SEND_AFTER_QUEUE = os.getenv("RUNNER_SEND_AFTER_QUEUE", "false").lower() in ("1", "true", "yes")
ALLOW_QUEUE_OUTSIDE_HOURS = os.getenv("ALLOW_QUEUE_OUTSIDE_HOURS", "true").lower() in ("1", "true", "yes")
PREQUEUE_BEFORE_START = os.getenv("PREQUEUE_BEFORE_START", "true").lower() in ("1", "true", "yes")
STRICT_CAMPAIGN_ELIGIBILITY = os.getenv("STRICT_CAMPAIGN_ELIGIBILITY", "true").lower() in ("1", "true", "yes")

DEDUPE_HOURS = int(os.getenv("DEDUPE_HOURS", "72"))
DAILY_LIMIT_FALLBACK = int(os.getenv("DAILY_LIMIT", "750"))
DEBUG_CAMPAIGNS = os.getenv("DEBUG_CAMPAIGNS", "false").lower() in ("1", "true", "yes")

# ─────────────────────────────────────────────────────────────
# Statuses / icons
# ─────────────────────────────────────────────────────────────
STATUS_ICON = {
    "QUEUED": "⏳",
    "READY": "⏳",
    "SENDING": "🔄",
    "SENT": "✅",
    "DELIVERED": "✅",
    "FAILED": "❌",
    "CANCELLED": "❌",
}

ALLOWED_STATUSES_RAW = {"scheduled", "running", "ready", "active", ""}  # "" allowed only if permissive
BLOCKED_STATUSES_RAW = {
    "paused", "inactive", "on hold", "hold", "stopped", "stop",
    "complete", "completed", "disabled", "draft", "cancelled", "canceled",
}
ALLOWED_STATUSES = {s.lower() for s in ALLOWED_STATUSES_RAW}
BLOCKED_STATUSES = {s.lower() for s in BLOCKED_STATUSES_RAW}

# Common phone / DNC fields
PHONE_FIELDS = [
    "phone", "Phone", "Mobile", "Cell", "Phone Number", "Primary Phone",
    "Phone 1", "Phone 2", "Phone 3",
    "Owner Phone", "Owner Phone 1", "Owner Phone 2",
    "Phone 1 (from Linked Owner)", "Phone 2 (from Linked Owner)", "Phone 3 (from Linked Owner)",
]
DNC_FIELDS = [
    "DNC", "Do Not Contact", "Do Not Call", "Do Not Text", "Do Not SMS",
    "Opt Out", "Opt-Out", "Unsubscribed", "Unsubscribe", "Stop (SMS)"
]

# ─────────────────────────────────────────────────────────────
# Time / small utils
# ─────────────────────────────────────────────────────────────
def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def iso_now() -> str:
    return utcnow().isoformat()

def _truthy(x: Any) -> bool:
    if isinstance(x, bool):
        return x
    return str(x).strip().lower() in ("1", "true", "yes", "y", "on")

def _norm(s: Any) -> Any:
    return re.sub(r"[^a-z0-9]+", "", s.strip().lower()) if isinstance(s, str) else s

def _digits_only(s: Any) -> Optional[str]:
    if not isinstance(s, str):
        return None
    ds = "".join(re.findall(r"\d+", s))
    return ds if len(ds) >= 10 else None

def last10(s: Any) -> Optional[str]:
    d = _digits_only(s)
    return d[-10:] if d else None

def _field(f: Dict[str, Any], *names: str, default=None):
    if not f:
        return default
    amap = {_norm(k): k for k in f.keys()}
    for n in names:
        k = amap.get(_norm(n))
        if k in f:
            return f.get(k)
    for n in names:
        if n in f:
            return f[n]
    return default

def _get_bool(f: Dict[str, Any], *names: str, default: bool=False) -> bool:
    v = _field(f, *names, default=None)
    if v is None:
        return default
    return _truthy(v)

def _in_quiet_hours(dt_utc: datetime) -> bool:
    local = dt_utc.astimezone(QUIET_TZ)
    return (local.hour >= QUIET_START_HOUR) or (local.hour < QUIET_END_HOUR)

def _shift_to_window(dt_utc: datetime) -> datetime:
    local = dt_utc.astimezone(QUIET_TZ)
    if local.hour >= QUIET_START_HOUR:
        local = (local + timedelta(days=1)).replace(hour=QUIET_END_HOUR, minute=0, second=0, microsecond=0)
    elif local.hour < QUIET_END_HOUR:
        local = local.replace(hour=QUIET_END_HOUR, minute=0, second=0, microsecond=0)
    return local.astimezone(timezone.utc)

def _clamp_future(dt_utc: datetime, min_delta_sec: int = 2) -> datetime:
    floor = utcnow() + timedelta(seconds=min_delta_sec)
    return dt_utc if dt_utc > floor else floor

def _local_naive_iso(dt_utc: datetime) -> str:
    local = dt_utc.astimezone(QUIET_TZ).replace(tzinfo=None)
    return local.isoformat(timespec="seconds")

def _parse_time_maybe_ct(value: Any) -> Optional[datetime]:
    if not value:
        return None
    txt = str(value).strip()
    try:
        if "T" in txt or " " in txt:
            dt = datetime.fromisoformat(txt.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=QUIET_TZ)
            return dt.astimezone(timezone.utc)
        d = date.fromisoformat(txt)
        local = datetime(d.year, d.month, d.day, max(9, QUIET_END_HOUR), 0, 0, tzinfo=QUIET_TZ)
        return local.astimezone(timezone.utc)
    except Exception:
        return None

def _get_time_field(f: Dict[str, Any], *names: str) -> Optional[datetime]:
    for n in names:
        if n in f and f[n]:
            dt = _parse_time_maybe_ct(f[n]);  # direct
            if dt: return dt
    nf = {_norm(k): k for k in f.keys()}
    for n in names:
        k = nf.get(_norm(n))
        if k and f.get(k):
            dt = _parse_time_maybe_ct(f[k])
            if dt: return dt
    return None

# ─────────────────────────────────────────────────────────────
# Base/table auto-anchoring (critical for linked Templates)
# ─────────────────────────────────────────────────────────────
_base_hint: Dict[str, Optional[str]] = {"campaigns": None}

def _probe_table(base_id: Optional[str], table_name: str) -> bool:
    if not (AIRTABLE_KEY and base_id):
        return False
    tbl = _make_table(AIRTABLE_KEY, base_id, table_name)
    if not tbl:
        return False
    try:
        tbl.all(max_records=1)  # type: ignore[attr-defined]
        return True
    except Exception:
        return False

def _choose_campaigns_base() -> Optional[str]:
    """Pick the base that actually has the Campaigns table (prefer Leads/Convos if both)."""
    # If everything is in Leads/Convos, this will be the one.
    order = [LEADS_CONVOS_BASE, CAMPAIGN_CONTROL_BASE]
    for b in order:
        if _probe_table(b, CAMPAIGNS_TABLE):
            return b
    # last resort: whichever responds
    for b in [CAMPAIGN_CONTROL_BASE, LEADS_CONVOS_BASE]:
        if _probe_table(b, CAMPAIGNS_TABLE):
            return b
    return None

@lru_cache(maxsize=None)
def get_campaigns_table():
    if not _base_hint["campaigns"]:
        _base_hint["campaigns"] = _choose_campaigns_base()
        if DEBUG_CAMPAIGNS:
            print(f"[debug] Campaigns base → {_base_hint['campaigns'] or 'None'}")
    return _make_table(AIRTABLE_KEY, _base_hint["campaigns"], CAMPAIGNS_TABLE)

@lru_cache(maxsize=None)
def get_templates_table():
    # Anchor templates to the *same base* as Campaigns
    camp_base = _base_hint.get("campaigns") or _choose_campaigns_base()
    if DEBUG_CAMPAIGNS:
        print(f"[debug] Templates anchored to campaigns base → {camp_base}")
    t = _make_table(AIRTABLE_KEY, camp_base, TEMPLATES_TABLE)
    if t:
        try:
            t.all(max_records=1)  # sanity touch
            return t
        except Exception:
            pass
    # fallback to Leads/Convos just in case (won't match linked IDs if bases differ)
    return _make_table(AIRTABLE_KEY, LEADS_CONVOS_BASE, TEMPLATES_TABLE)

@lru_cache(maxsize=None)
def get_prospects_table():
    # Prospects usually sit with Leads/Convos
    if _probe_table(LEADS_CONVOS_BASE, PROSPECTS_TABLE):
        return _make_table(AIRTABLE_KEY, LEADS_CONVOS_BASE, PROSPECTS_TABLE)
    # else try the campaigns base
    camp_base = _base_hint.get("campaigns") or _choose_campaigns_base()
    return _make_table(AIRTABLE_KEY, camp_base, PROSPECTS_TABLE)

@lru_cache(maxsize=None)
def get_drip_table():
    # Often in Leads/Convos
    if _probe_table(LEADS_CONVOS_BASE, DRIP_QUEUE_TABLE):
        return _make_table(AIRTABLE_KEY, LEADS_CONVOS_BASE, DRIP_QUEUE_TABLE)
    camp_base = _base_hint.get("campaigns") or _choose_campaigns_base()
    return _make_table(AIRTABLE_KEY, camp_base, DRIP_QUEUE_TABLE)

@lru_cache(maxsize=None)
def get_numbers_table():
    # Try campaigns base first so linked markets/etc. align
    camp_base = _base_hint.get("campaigns") or _choose_campaigns_base()
    t = _make_table(AIRTABLE_KEY, camp_base, NUMBERS_TABLE)
    if t and _probe_table(camp_base, NUMBERS_TABLE):
        return t
    # then try the control base
    return _make_table(AIRTABLE_KEY, CAMPAIGN_CONTROL_BASE, NUMBERS_TABLE)

@lru_cache(maxsize=None)
def get_runs_table():
    return _make_table(AIRTABLE_KEY, PERFORMANCE_BASE, "Runs/Logs")

@lru_cache(maxsize=None)
def get_kpis_table():
    return _make_table(AIRTABLE_KEY, PERFORMANCE_BASE, "KPIs")

# ─────────────────────────────────────────────────────────────
# Safe Airtable create/update with schema filtering
# ─────────────────────────────────────────────────────────────
def _auto_field_map(tbl) -> Dict[str, str]:
    try:
        rows = tbl.all(max_records=1)  # type: ignore[attr-defined]
        keys = list(rows[0].get("fields", {}).keys()) if rows else []
    except Exception:
        keys = []
    return {_norm(k): k for k in keys}

def _safe_filter(tbl, payload: Dict) -> Dict:
    amap = _auto_field_map(tbl)
    if not amap:
        return dict(payload)
    out = {}
    for k, v in payload.items():
        mk = amap.get(_norm(k))
        if mk:
            out[mk] = v
    return out

_UNKNOWN_RE   = re.compile(r'Unknown field name:\s*"([^"]+)"', re.I)
_COMPUTED_RE  = re.compile(r'Field\s*"([^"]+)"\s*cannot accept a value because the field is computed', re.I)
_INVALIDVAL_RE= re.compile(r'INVALID_VALUE_FOR_COLUMN.*?Field\s*"([^"]+)"', re.I)

def _safe_create(tbl, payload: Dict):
    if not (tbl and payload):
        return None
    pending = dict(payload)
    for _ in range(8):
        try:
            data = _safe_filter(tbl, pending)
            if not data:
                return None
            return tbl.create(data)  # type: ignore[attr-defined]
        except Exception as e:
            msg = str(e)
            m = _UNKNOWN_RE.search(msg) or _COMPUTED_RE.search(msg) or _INVALIDVAL_RE.search(msg)
            if m:
                pending.pop(m.group(1), None); continue
            traceback.print_exc(); return None
    return None

def _safe_update(tbl, rid: str, payload: Dict):
    if not (tbl and rid and payload):
        return None
    pending = dict(payload)
    for _ in range(8):
        try:
            data = _safe_filter(tbl, pending)
            if not data:
                return None
            return tbl.update(rid, data)  # type: ignore[attr-defined]
        except Exception as e:
            msg = str(e)
            m = _UNKNOWN_RE.search(msg) or _COMPUTED_RE.search(msg) or _INVALIDVAL_RE.search(msg)
            if m:
                pending.pop(m.group(1), None); continue
            traceback.print_exc(); return None
    return None

# ─────────────────────────────────────────────────────────────
# Personalization helpers
# ─────────────────────────────────────────────────────────────
_TITLE_WORDS = {"mr","mrs","ms","miss","dr","prof","sir","madam","rev","capt","cpt","lt","sgt"}
_ORG_HINTS = {"llc","inc","corp","co","company","trust","estates","holdings","hoa","ltd","pllc","llp","pc"}
_BAD_NAME_KEY_HINTS = {
    "city","property","mail","street","zip","state","county","parcel","apn","unit",
    "neighborhood","subdivision","listing","agent","broker","company","business",
    "entity","trust","hoa","llc","inc","corp","co","estate","address"
}

def _looks_org(full: str) -> bool:
    s = _norm(full or ""); return any(hint in s for hint in _ORG_HINTS)

def _clean_token(tok: str) -> str:
    return re.sub(r"[^\w'-]+", "", tok or "").strip()

def _is_initial(tok: str) -> bool:
    t = tok.strip(); return bool(re.fullmatch(r"[A-Za-z]\.?", t))

def _is_person_name_key(key: str) -> bool:
    n = _norm(key or "")
    return ("name" in n) and (not any(bad in n for bad in _BAD_NAME_KEY_HINTS))

def _extract_first_name_natural(full: str) -> Optional[str]:
    if not full: return None
    full = " ".join(str(full).split())
    if _looks_org(full): return None
    if "," in full:
        parts = [p.strip() for p in full.split(",") if p.strip()]
        if len(parts) >= 2: full = parts[1]
    for sep in ("&", "/", "+"):
        if sep in full: full = full.split(sep, 1)[0].strip()
    toks = [_clean_token(t) for t in full.split() if _clean_token(t)]
    if not toks: return None
    while toks and toks[0].lower().rstrip(".") in _TITLE_WORDS: toks.pop(0)
    if not toks: return None
    first = toks[0]
    return first.replace(".", "").upper() if _is_initial(first) else first

def _compose_address(fields: Dict[str, Any]) -> Optional[str]:
    for k in ("Address", "Property Address", "Mailing Address", "Property Full Address", "Address (from Property)"):
        v = fields.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    street = fields.get("Street") or fields.get("Property Street") or fields.get("Mailing Street")
    city   = fields.get("City") or fields.get("Property City") or fields.get("Mailing City") or fields.get("City Name")
    state  = fields.get("State") or fields.get("Property State") or fields.get("Mailing State")
    postal = fields.get("Zip") or fields.get("ZIP") or fields.get("Postal") or fields.get("Property Zip")
    parts = [str(x).strip() for x in (street, city, state) if x]
    if postal: parts.append(str(postal).strip())
    addr = ", ".join([p for p in parts if p]); return addr or None

def _same_letters(a: str, b: str) -> bool:
    ra = re.sub(r"[^a-z]", "", (a or "").lower())
    rb = re.sub(r"[^a-z]", "", (b or "").lower())
    return bool(ra) and ra == rb

def _personalization_ctx(pf: Dict[str, Any]) -> Dict[str, Any]:
    city_candidates = []
    for ck in ("City", "Property City", "Mailing City", "City Name"):
        cv = pf.get(ck)
        if isinstance(cv, str) and cv.strip():
            city_candidates.append(cv.strip())
    preferred = [
        "Owner First Name", "First Name", "Owner 1 First Name", "Owner 2 First Name",
        "Owner Name", "Owner 1 Name", "Owner 2 Name", "Full Name", "Name",
    ]
    first = None
    for k in preferred:
        v = pf.get(k)
        if isinstance(v, str) and v.strip():
            cand = _extract_first_name_natural(v)
            if cand and not any(_same_letters(cand, c) or _same_letters(cand, c.split()[0]) for c in city_candidates):
                first = cand; break
    if not first:
        for k, v in pf.items():
            if not isinstance(v, str) or not v.strip(): continue
            if _is_person_name_key(k):
                cand = _extract_first_name_natural(v)
                if cand and not any(_same_letters(cand, c) or _same_letters(cand, c.split()[0]) for c in city_candidates):
                    first = cand; break
    address = _compose_address(pf)
    friendly_first = first or "there"
    return {"First": friendly_first, "first": friendly_first, "Address": address or "", "address": address or ""}

def _format_template(text: str, ctx: Dict[str, Any]) -> str:
    if not text: return text
    amap = {_norm(k): ("" if v is None else str(v)) for k, v in (ctx or {}).items()}
    def repl(m):
        raw = m.group(1) or m.group(2)
        val = amap.get(_norm(raw))
        return val if val is not None else m.group(0)
    return re.sub(r"\{\{([^}]+)\}\}|\{([^}]+)\}", repl, text)

# ─────────────────────────────────────────────────────────────
# Numbers picking + per-number pacing
# ─────────────────────────────────────────────────────────────
def _supports_market(f: Dict[str, Any], market: Optional[str]) -> bool:
    if not market: return True
    if f.get("Market") == market: return True
    ms = f.get("Markets")
    if isinstance(ms, list): return market in ms
    if isinstance(ms, str) and ms.strip(): return market in [m.strip() for m in ms.split(",")]
    return False

def _to_e164(f: Dict[str, Any]) -> Optional[str]:
    for key in ("Number", "A Number", "Phone", "E164", "Friendly Name"):
        v = f.get(key)
        if isinstance(v, str) and _digits_only(v):
            d = v if v.startswith("+") else "+" + _digits_only(v)
            return d
    return None

class NumberState:
    __slots__ = ("rec_id","e164","remaining","next_time")
    def __init__(self, rec_id: str, e164: str, remaining: int, base_time: datetime):
        self.rec_id = rec_id
        self.e164 = e164
        self.remaining = remaining
        self.next_time = base_time

def _load_number_pool(market: Optional[str], base_time: datetime) -> List[NumberState]:
    nums_tbl = get_numbers_table()
    pool: List[NumberState] = []
    if not nums_tbl:
        return pool
    try:
        rows = nums_tbl.all()  # type: ignore[attr-defined]
    except Exception:
        traceback.print_exc(); return pool

    for r in rows:
        f = r.get("fields", {}) or {}
        n_active = _get_bool(f, "Active", "Enabled", default=True)
        n_status = str(_field(f, "Status", "status", default="")).strip().lower()
        if not n_active: continue
        if n_status in {"paused", "inactive", "disabled"}: continue
        if not _supports_market(f, market): continue

        rem = f.get("Remaining")
        try:
            rem = int(rem) if rem is not None else None
        except Exception:
            rem = None
        if rem is None:
            sent_today = int(f.get("Sent Today") or 0)
            daily_cap = int(f.get("Daily Reset") or DAILY_LIMIT_FALLBACK)
            rem = max(0, daily_cap - sent_today)
        if rem <= 0: continue

        e164 = _to_e164(f)
        if not e164: continue

        pool.append(NumberState(r["id"], e164, int(rem), base_time))
    return pool

def _pick_number_with_pacing(pool: List[NumberState]) -> Optional[NumberState]:
    if not pool: return None
    pool.sort(key=lambda n: (n.next_time, -n.remaining, n.e164))
    cand = pool[0]
    if cand.remaining <= 0: return None
    return cand

# ─────────────────────────────────────────────────────────────
# UI helper
# ─────────────────────────────────────────────────────────────
def _refresh_ui_icons_for_campaign(drip_tbl, campaign_id: str):
    try:
        for r in drip_tbl.all():  # type: ignore[attr-defined]
            f = r.get("fields", {})
            cids = f.get("Campaign") or []
            if campaign_id in (cids if isinstance(cids, list) else [cids]):
                status = str(f.get("status") or f.get("Status") or "")
                icon = STATUS_ICON.get(status, "")
                if icon and f.get("UI") != icon:
                    _safe_update(drip_tbl, r["id"], {"UI": icon})
    except Exception:
        traceback.print_exc()

# ─────────────────────────────────────────────────────────────
# Dedupe guard
# ─────────────────────────────────────────────────────────────
def _last_n_hours_dt(hours: int) -> datetime:
    return utcnow() - timedelta(hours=hours)

def already_queued(drip_tbl, phone: str, campaign_id: str) -> bool:
    try:
        cutoff_dt = _last_n_hours_dt(DEDUPE_HOURS)
        l10 = last10(phone)
        for r in drip_tbl.all():  # type: ignore[attr-defined]
            f = r.get("fields", {})
            ph = f.get("phone") or f.get("Phone")
            if last10(ph) == l10:
                cids = f.get("Campaign") or []
                cids = cids if isinstance(cids, list) else [cids]
                if campaign_id in cids:
                    status = str(f.get("status") or f.get("Status") or "")
                    when_raw = f.get("next_send_date") or f.get("Next Send Date") or f.get("created_at") or ""
                    when_dt = _parse_time_maybe_ct(when_raw) or utcnow()
                    if status in ("QUEUED", "SENDING", "READY") and when_dt >= cutoff_dt:
                        return True
        return False
    except Exception:
        traceback.print_exc(); return False

# ─────────────────────────────────────────────────────────────
# Phones / status helpers
# ─────────────────────────────────────────────────────────────
def get_phone(f: Dict[str, Any]) -> Optional[str]:
    p1 = f.get("Phone 1") or f.get("Phone 1 (from Linked Owner)")
    p2 = f.get("Phone 2") or f.get("Phone 2 (from Linked Owner)")
    if f.get("Phone 1 Verified") or f.get("Phone 1 Ownership Verified"):
        d = _digits_only(p1)
        if d: return d
    if f.get("Phone 2 Verified") or f.get("Phone 2 Ownership Verified"):
        d = _digits_only(p2)
        if d: return d
    for k in PHONE_FIELDS:
        d = _digits_only(f.get(k))
        if d: return d
    return None

def _is_dnc(f: Dict[str, Any]) -> bool:
    for k in DNC_FIELDS:
        v = f.get(k)
        if v is None: continue
        if isinstance(v, str) and v.strip():
            t = v.strip().lower()
            if t in ("stop", "stopped", "unsubscribed", "do not contact", "do not text", "do not sms"):
                return True
        if v is True or _truthy(v):
            return True
    return False

def _status_tuple(f: Dict[str, Any]) -> Tuple[str, str]:
    raw = _field(f, "status", "Status", default="")
    if isinstance(raw, list): raw = raw[0] if raw else ""
    s = str(raw or "").strip()
    return (s.lower(), s)

def _campaign_is_eligible(f: Dict[str, Any]) -> Tuple[bool, str]:
    status_norm, status_raw = _status_tuple(f)
    go_live = _get_bool(f, "Go Live", "go live", "go_live", "Live", default=False)
    active  = _get_bool(f, "Active", "active", "Enabled", "enabled", default=False)

    if status_norm in BLOCKED_STATUSES:
        return (False, f"status '{status_raw}' is BLOCKED")

    if STRICT_CAMPAIGN_ELIGIBILITY:
        if status_norm not in (ALLOWED_STATUSES - {""}):
            return (False, f"status '{status_raw}' is NOT allowed in strict")
        if not (go_live or active):
            return (False, f"Go Live={go_live} and Active={active} (both false)")
        return (True, "eligible (strict)")
    else:
        if status_norm and status_norm not in ALLOWED_STATUSES:
            return (False, f"status '{status_raw}' not in allowed (permissive)")
        if f.get("Go Live") is False or f.get("Active") is False:
            return (False, "explicit false on Go Live or Active")
        return (True, "eligible (permissive)")

# ─────────────────────────────────────────────────────────────
# Template resolver (anchored to campaigns base)
# ─────────────────────────────────────────────────────────────
def _normalize_link_values(v) -> List[str]:
    out: List[str] = []
    if v is None: return out
    items = v if isinstance(v, list) else [v]
    for x in items:
        if isinstance(x, str):
            s = x.strip()
            if s: out.append(s)
        elif isinstance(x, dict):
            rid = x.get("id")
            if isinstance(rid, str) and rid.strip():
                out.append(rid.strip())
    return out

def _resolve_templates_from_campaign(cf: Dict[str, Any]) -> List[Dict]:
    tids = _normalize_link_values(cf.get("Templates") or cf.get("templates"))
    if DEBUG_CAMPAIGNS:
        cname = cf.get("Name") or cf.get("name") or "Unnamed"
        print(f"[debug] {cname} linked Templates IDs → {tids}")

    t_tbl = get_templates_table()
    resolved: List[Dict] = []
    for tid in tids:
        try:
            row = t_tbl.get(tid) if t_tbl and tid.startswith("rec") else None  # type: ignore[attr-defined]
            if row:
                resolved.append(row)
        except Exception:
            # bad ID vs wrong base → skip
            continue

    # Fallback: inline message on Campaign if available
    if not resolved:
        for fname in ["Message", "Text", "Body", "Script", "Initial Message", "Message Body", "First Touch Message"]:
            body = cf.get(fname)
            if isinstance(body, str) and body.strip():
                resolved.append({"id": f"inline:{fname}", "fields": {"Name": f"(Inline) {fname}", "Message": body}})
                if DEBUG_CAMPAIGNS:
                    print(f"[debug] using inline campaign body from '{fname}'")
                break

    if DEBUG_CAMPAIGNS and not resolved:
        print("[debug] No templates resolved. Ensure Campaigns & Templates are in the SAME base and links are populated.")
    return resolved

# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def _normalize_limit(limit: Optional[int | str]) -> int:
    if limit is None: return 999_999
    try:
        s = str(limit).strip().upper()
        if s in ("", "ALL", "UNLIMITED", "NONE"): return 999_999
        v = int(s); return max(1, v)
    except Exception:
        return 999_999

def run_campaigns(limit: Optional[int | str] = 1, send_after_queue: Optional[bool] = None) -> Dict[str, Any]:
    max_to_process = _normalize_limit(limit)

    if send_after_queue is None:
        send_after_queue = RUNNER_SEND_AFTER_QUEUE
    if _in_quiet_hours(utcnow()):
        send_after_queue = False  # never send during quiet hours

    campaigns = get_campaigns_table()
    templates = get_templates_table()
    prospects = get_prospects_table()
    drip = get_drip_table()
    if not all([campaigns, templates, prospects, drip]):
        return {"ok": False, "processed": 0, "results": [], "errors": ["Missing Airtable tables or env"]}

    try:
        all_campaigns = campaigns.all()  # type: ignore[attr-defined]
    except Exception:
        traceback.print_exc()
        return {"ok": False, "processed": 0, "results": [], "errors": ["Failed to fetch Campaigns"]}

    now_utc = utcnow()
    eligible: List[Dict] = []

    for c in all_campaigns:
        f = c.get("fields", {}) or {}
        name = (f.get("Name") or f.get("name") or c.get("id"))
        ok, why = _campaign_is_eligible(f)
        if not ok:
            if DEBUG_CAMPAIGNS:
                print(f"[skip] {c.get('id')} ({name}) → {why}.")
            continue

        start_dt = _get_time_field(f, "Start Time", "Start", "Start At", "start_time", "Start Date", "Schedule Start")
        end_dt   = _get_time_field(f, "End Time", "End", "End At", "end_time", "End Date", "Schedule End")

        if end_dt and now_utc >= end_dt:
            _safe_update(campaigns, c["id"], {"status": "Completed", "last_run_at": iso_now()})
            if DEBUG_CAMPAIGNS:
                print(f"[campaign] COMPLETE {name}: now>=end")
            continue

        if start_dt and now_utc < start_dt and not PREQUEUE_BEFORE_START:
            if DEBUG_CAMPAIGNS: print(f"[campaign] WAIT {name}: now<start (prequeue off)")
            continue

        if start_dt and now_utc < start_dt and PREQUEUE_BEFORE_START:
            if not ALLOW_QUEUE_OUTSIDE_HOURS and _in_quiet_hours(start_dt):
                if DEBUG_CAMPAIGNS: print(f"[campaign] WAIT {name}: start in quiet; queue-off")
                continue

        if DEBUG_CAMPAIGNS:
            _, status_raw = _status_tuple(f)
            print(f"[campaign] ELIGIBLE {name}: start={start_dt}, end={end_dt}, status={status_raw or '∅'}")
        eligible.append(c)

    processed = 0
    results: List[Dict[str, Any]] = []

    for camp in eligible:
        if processed >= max_to_process:
            break

        cf = camp.get("fields", {}) or {}
        cid = camp["id"]
        name = (cf.get("Name") or cf.get("name") or "Unnamed")
        view = (_field(cf, "View/Segment", "View", default="") or "").strip() or None
        market = _field(cf, "Market", "market", default=None)

        try:
            prospect_rows = prospects.all(view=view) if view else prospects.all()  # type: ignore[attr-defined]
        except Exception:
            traceback.print_exc()
            _safe_update(get_campaigns_table(), cid, {"last_run_at": iso_now()})
            continue

        template_rows = _resolve_templates_from_campaign(cf)
        if not template_rows:
            if DEBUG_CAMPAIGNS:
                print(f"[campaign] SKIP {name}: no Templates linked/resolved")
            _safe_update(get_campaigns_table(), cid, {"last_run_at": iso_now()})
            continue

        start_dt = _get_time_field(cf, "Start Time", "Start", "Start At", "start_time", "Start Date", "Schedule Start")
        prequeue = bool(start_dt and now_utc < start_dt and PREQUEUE_BEFORE_START)
        base_utc = start_dt if prequeue else (max(now_utc, start_dt) if start_dt else now_utc)

        try:
            phase = abs(hash(cid)) % SECONDS_PER_NUMBER_MSG
            base_utc = base_utc + timedelta(seconds=phase)
        except Exception:
            pass

        if _in_quiet_hours(base_utc):
            base_utc = _shift_to_window(base_utc)
        base_utc = _clamp_future(base_utc, min_delta_sec=2)

        number_pool = _load_number_pool(market, base_utc)
        if not number_pool:
            if DEBUG_CAMPAIGNS:
                print(f"[campaign] SKIP {name}: no eligible numbers")
            _safe_update(get_campaigns_table(), cid, {"last_run_at": iso_now()})
            continue

        if prequeue:
            _safe_update(get_campaigns_table(), cid, {"last_run_at": iso_now()})
        else:
            _safe_update(get_campaigns_table(), cid, {"status": "Running", "last_run_at": iso_now()})

        queued = 0
        nums_tbl = get_numbers_table()

        for pr in prospect_rows:
            pf = pr.get("fields", {}) or {}

            if _is_dnc(pf):
                continue

            phone = get_phone(pf)
            if not phone:
                continue
            if already_queued(drip, phone, cid):
                continue

            ns = _pick_number_with_pacing(number_pool)
            if not ns or ns.remaining <= 0:
                break

            # choose a template and build body
            trow = random.choice(template_rows)
            tf = (trow.get("fields", {}) or {})
            raw = tf.get("Message") or tf.get("Text")
            if not raw:
                continue

            ctx = dict(pf)
            ctx.update(_personalization_ctx(pf))
            body = _format_template(str(raw), ctx).strip()
            if not body:
                continue

            scheduled = ns.next_time
            if JITTER_SECONDS:
                scheduled = scheduled + timedelta(seconds=random.randint(0, JITTER_SECONDS))
            if _in_quiet_hours(scheduled):
                scheduled = _shift_to_window(scheduled)
            scheduled = _clamp_future(scheduled, min_delta_sec=2)
            scheduled_local = _local_naive_iso(scheduled)

            payload = {
                "Prospect": [pr["id"]],
                "Campaign": [cid],
                "Template": [trow["id"]] if trow and trow.get("id") else None,
                "Market": market or pf.get("Market"),
                "phone": phone,
                "message_preview": body,
                "from_number": ns.e164,
                "status": "QUEUED",
                "next_send_date": scheduled_local,  # local-naive (QUIET_TZ)
                "Property ID": pf.get("Property ID"),
                "Number Record Id": ns.rec_id,
                "UI": STATUS_ICON.get("QUEUED", "⏳"),
            }
            created = _safe_create(get_drip_table(), {k: v for k, v in payload.items() if v is not None})
            if created:
                queued += 1
                ns.remaining -= 1
                ns.next_time = scheduled + timedelta(seconds=SECONDS_PER_NUMBER_MSG)
                if _in_quiet_hours(ns.next_time):
                    ns.next_time = _shift_to_window(ns.next_time)
                ns.next_time = _clamp_future(ns.next_time, min_delta_sec=2)
                if nums_tbl:
                    _safe_update(nums_tbl, ns.rec_id, {"Last Used": iso_now()})

        batch_result, retry_result = {"total_sent": 0}, {}
        if (not prequeue) and send_after_queue and queued > 0:
            try:
                batch_result = send_batch(campaign_id=cid, limit=MESSAGES_PER_MIN)
            except Exception:
                traceback.print_exc()
            if (batch_result.get("total_sent", 0) or 0) < queued:
                try:
                    retry_result = run_retry(limit=MESSAGES_PER_MIN, view="Failed Sends")
                except Exception:
                    retry_result = {}

        _refresh_ui_icons_for_campaign(get_drip_table(), cid)

        sent_delta = (batch_result.get("total_sent", 0) or 0) + (retry_result.get("retried", 0) or 0)
        new_status = (
            "Scheduled" if prequeue else (
                "Running" if queued and (sent_delta < queued or not send_after_queue)
                else ("Completed" if queued else (_field(cf, "status", "Status", default="Scheduled") or "Scheduled"))
            )
        )

        last_result = {
            "Queued": queued,
            "Sent": batch_result.get("total_sent", 0) or 0,
            "Retries": retry_result.get("retried", 0) or 0,
            "Table": PROSPECTS_TABLE,
            "View": view,
            "Market": market,
            "QuietHoursNow": _in_quiet_hours(now_utc),
            "MPM": MESSAGES_PER_MIN,
            "PerNumberMPM": RATE_PER_NUMBER_PER_MIN,
            "Prequeued": prequeue,
        }

        _safe_update(get_campaigns_table(), cid, {
            "status": new_status,
            "Last Run Result": json.dumps(last_result),
            "last_run_at": iso_now(),
        })

        runs_tbl, kpis_tbl = get_runs_table(), get_kpis_table()
        if runs_tbl:
            _safe_create(
                runs_tbl,
                {
                    "Type": "CAMPAIGN_RUN",
                    "Campaign": name,
                    "Processed": float(sent_delta if not prequeue else queued),
                    "Breakdown": json.dumps({"initial": batch_result, "retries": retry_result}),
                    "Timestamp": iso_now(),
                },
            )
        if kpis_tbl:
            _safe_create(
                kpis_tbl,
                {
                    "Campaign": name,
                    "Metric": "OUTBOUND_SENT" if (not prequeue and send_after_queue) else "MESSAGES_QUEUED",
                    "Value": float(sent_delta if (not prequeue and send_after_queue) else queued),
                    "Date": utcnow().date().isoformat(),
                },
            )

        if DEBUG_CAMPAIGNS:
            print(f"[campaign] {name}: queued={queued}, sent_now={0 if prequeue else sent_delta}, status→{new_status}")

        results.append({
            "campaign": name,
            "queued": queued,
            "sent": 0 if prequeue else (sent_delta if send_after_queue else 0),
            "view": view,
            "market": market,
            "quiet_now": _in_quiet_hours(now_utc),
            "mpm": MESSAGES_PER_MIN,
            "per_number_mpm": RATE_PER_NUMBER_PER_MIN,
        })
        processed += 1

    try:
        update_metrics()
    except Exception:
        traceback.print_exc()

    return {"ok": True, "processed": processed, "results": results, "errors": []}