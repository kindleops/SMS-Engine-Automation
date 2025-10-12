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
# pyairtable compat (v2: Api, v1: Table) — never crash if missing
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
    """
    Return a table-like object exposing .all/.get/.create/.update, or None.
    Works with both pyairtable v2 (Api) and v1 (Table).
    """
    if not (api_key and base_id):
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
# Optional engine hooks (never hard-crash if missing)
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

# Global & per-number rates
MESSAGES_PER_MIN = max(1, int(os.getenv("MESSAGES_PER_MIN", "20")))  # overall batcher hint
RATE_PER_NUMBER_PER_MIN = max(1, int(os.getenv("RATE_PER_NUMBER_PER_MIN", "20")))
PER_NUMBER_GAP_SEC = max(1, int(math.ceil(60.0 / RATE_PER_NUMBER_PER_MIN)))

JITTER_SECONDS = max(0, int(os.getenv("JITTER_SECONDS", "2")))

QUIET_TZ = ZoneInfo(os.getenv("QUIET_TZ", "America/Chicago"))
QUIET_START_HOUR = int(os.getenv("QUIET_START_HOUR", "21"))
QUIET_END_HOUR = int(os.getenv("QUIET_END_HOUR", "9"))

RUNNER_SEND_AFTER_QUEUE = os.getenv("RUNNER_SEND_AFTER_QUEUE", "false").lower() in ("1", "true", "yes")
ALLOW_QUEUE_OUTSIDE_HOURS = os.getenv("ALLOW_QUEUE_OUTSIDE_HOURS", "true").lower() in ("1", "true", "yes")
PREQUEUE_BEFORE_START = os.getenv("PREQUEUE_BEFORE_START", "true").lower() in ("1", "true", "yes")
DEDUPE_HOURS = int(os.getenv("DEDUPE_HOURS", "72"))
DAILY_LIMIT_FALLBACK = int(os.getenv("DAILY_LIMIT", "750"))
DEBUG_CAMPAIGNS = os.getenv("DEBUG_CAMPAIGNS", "false").lower() in ("1", "true", "yes")

PHONE_FIELDS = [
    "phone", "Phone", "Mobile", "Cell", "Phone Number", "Primary Phone",
    "Phone 1", "Phone 2", "Phone 3",
    "Owner Phone", "Owner Phone 1", "Owner Phone 2",
    "Phone 1 (from Linked Owner)", "Phone 2 (from Linked Owner)", "Phone 3 (from Linked Owner)",
]

STATUS_ICON = {
    "QUEUED": "⏳",
    "READY": "⏳",
    "SENDING": "🔄",
    "SENT": "✅",
    "DELIVERED": "✅",
    "FAILED": "❌",
    "CANCELLED": "❌",
}


# ─────────────────────────────────────────────────────────────
# Time / helpers
# ─────────────────────────────────────────────────────────────
def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def iso_now() -> str:
    return utcnow().isoformat()

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

def _local_naive_iso(dt_utc: datetime) -> str:
    local = dt_utc.astimezone(QUIET_TZ).replace(tzinfo=None)
    return local.isoformat(timespec="seconds")

def schedule_time_round_robin(base_utc: datetime, idx: int, numbers_count: int) -> str:
    """
    Per-number pacing: each DID fires every PER_NUMBER_GAP_SEC.
    Messages are interleaved across numbers in round-robin order.
    Example: numbers_count=5, PER_NUMBER_GAP_SEC=3 → each DID sends every 3s,
    overall cadence is ~5 msgs per 3s ≈ 100 msg/min across 5 numbers at 20 each.
    """
    jitter = random.randint(0, JITTER_SECONDS) if JITTER_SECONDS else 0
    # How many messages has the specific DID already "sent" by position?
    per_number_index = idx // max(1, numbers_count)
    t = base_utc + timedelta(seconds=per_number_index * PER_NUMBER_GAP_SEC + jitter)
    if _in_quiet_hours(t):
        t = _shift_to_window(t)
    return _local_naive_iso(t)

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
            dt = _parse_time_maybe_ct(f[n])
            if dt: return dt
    nf = {_norm(k): k for k in f.keys()}
    for n in names:
        k = nf.get(_norm(n))
        if k and f.get(k):
            dt = _parse_time_maybe_ct(f[k])
            if dt: return dt
    return None


# ─────────────────────────────────────────────────────────────
# Airtable table getters (compat)
# ─────────────────────────────────────────────────────────────
@lru_cache(maxsize=None)
def get_campaigns_table():
    t = _make_table(AIRTABLE_KEY, LEADS_CONVOS_BASE, CAMPAIGNS_TABLE)
    return t or _make_table(AIRTABLE_KEY, CAMPAIGN_CONTROL_BASE, CAMPAIGNS_TABLE)

@lru_cache(maxsize=None)
def get_templates_table():
    return _make_table(AIRTABLE_KEY, LEADS_CONVOS_BASE, TEMPLATES_TABLE)

@lru_cache(maxsize=None)
def get_prospects_table():
    return _make_table(AIRTABLE_KEY, LEADS_CONVOS_BASE, PROSPECTS_TABLE)

@lru_cache(maxsize=None)
def get_drip_table():
    return _make_table(AIRTABLE_KEY, LEADS_CONVOS_BASE, DRIP_QUEUE_TABLE)

@lru_cache(maxsize=None)
def get_numbers_table():
    return _make_table(AIRTABLE_KEY, CAMPAIGN_CONTROL_BASE, NUMBERS_TABLE)

@lru_cache(maxsize=None)
def get_runs_table():
    return _make_table(AIRTABLE_KEY, PERFORMANCE_BASE, "Runs/Logs")

@lru_cache(maxsize=None)
def get_kpis_table():
    return _make_table(AIRTABLE_KEY, PERFORMANCE_BASE, "KPIs")


# ─────────────────────────────────────────────────────────────
# Schema helpers + bulletproof create/update
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

_UNKNOWN_RE = re.compile(r'Unknown field name:\s*"([^"]+)"')

def _safe_create(tbl, payload: Dict):
    """Create with schema-map and automatic retry removing unknown fields."""
    if not (tbl and payload):
        return None
    pending = dict(payload)
    for _ in range(6):
        try:
            data = _safe_filter(tbl, pending)
            if not data:
                return None
            return tbl.create(data)  # type: ignore[attr-defined]
        except Exception as e:
            m = _UNKNOWN_RE.search(str(e))
            if m:
                pending.pop(m.group(1), None)
                continue
            traceback.print_exc()
            return None
    return None

def _safe_update(tbl, rid: str, payload: Dict):
    """Update with schema-map and automatic retry removing unknown fields."""
    if not (tbl and rid and payload):
        return None
    pending = dict(payload)
    for _ in range(6):
        try:
            data = _safe_filter(tbl, pending)
            if not data:
                return None
            return tbl.update(rid, data)  # type: ignore[attr-defined]
        except Exception as e:
            m = _UNKNOWN_RE.search(str(e))
            if m:
                pending.pop(m.group(1), None)
                continue
            traceback.print_exc()
            return None
    return None


# ─────────────────────────────────────────────────────────────
# Name & address extraction (natural sounding)
# ─────────────────────────────────────────────────────────────
_TITLE_WORDS = {"mr","mrs","ms","miss","dr","prof","sir","madam","rev","capt","cpt","lt","sgt"}
_ORG_HINTS = {"llc","inc","corp","co","company","trust","estates","holdings","hoa","ltd","pllc","llp","pc"}

def _looks_org(full: str) -> bool:
    s = _norm(full or "")
    return any(hint in s for hint in _ORG_HINTS)

def _clean_token(tok: str) -> str:
    return re.sub(r"[^\w'-]+", "", tok or "").strip()

def _is_initial(tok: str) -> bool:
    t = tok.strip()
    return bool(re.fullmatch(r"[A-Za-z]\.?", t))

def _extract_first_name_natural(full: str) -> Optional[str]:
    """
    Extract a natural, human first name.
    • 'John R Smith' → 'John'
    • 'John R. Smith' → 'John'
    • 'J. Robert Smith' → 'J'
    • 'Smith, John R' → 'John'
    • 'Dr. Jane A. Doe' → 'Jane'
    Returns None for obvious org names.
    """
    if not full:
        return None
    full = " ".join(str(full).split())
    if _looks_org(full):
        return None

    # Handle "Last, First M" forms
    if "," in full:
        parts = [p.strip() for p in full.split(",") if p.strip()]
        if len(parts) >= 2:
            full = parts[1]

    # Take only first party if multiple owners listed
    for sep in ("&", "/", "+"):
        if sep in full:
            full = full.split(sep, 1)[0].strip()

    toks = [_clean_token(t) for t in full.split() if _clean_token(t)]
    if not toks:
        return None

    # Drop titles
    while toks and toks[0].lower().rstrip(".") in _TITLE_WORDS:
        toks.pop(0)
    if not toks:
        return None

    first = toks[0]
    if _is_initial(first):
        return first.replace(".", "").upper()
    return first  # ignore middle initial(s) on purpose

def _compose_address(fields: Dict[str, Any]) -> Optional[str]:
    # Single-field candidates
    for k in ("Address", "Property Address", "Mailing Address", "Property Full Address", "Address (from Property)"):
        v = fields.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()

    # Piece together components when needed
    street = fields.get("Street") or fields.get("Property Street") or fields.get("Mailing Street")
    city   = fields.get("City") or fields.get("Property City") or fields.get("Mailing City")
    state  = fields.get("State") or fields.get("Property State") or fields.get("Mailing State")
    postal = fields.get("Zip") or fields.get("ZIP") or fields.get("Postal") or fields.get("Property Zip")

    parts = [str(x).strip() for x in (street, city, state) if x]
    if postal:
        parts.append(str(postal).strip())
    addr = ", ".join([p for p in parts if p])
    return addr or None

def _personalization_ctx(pf: Dict[str, Any]) -> Dict[str, Any]:
    # Try first name from various fields
    name_fields = [
        "Owner First Name", "First Name", "Owner 1 First Name", "Owner 2 First Name",
        "Owner Name", "Owner 1 Name", "Owner 2 Name", "Full Name", "Name",
    ]
    first = None
    for k in name_fields:
        v = pf.get(k)
        if isinstance(v, str) and v.strip():
            first = _extract_first_name_natural(v)
            if first:
                break
    # As a last resort, try to infer from any name-like field
    if not first:
        for k, v in pf.items():
            if "name" in _norm(k) and isinstance(v, str):
                first = _extract_first_name_natural(v)
                if first:
                    break

    address = _compose_address(pf)

    # Friendly fallbacks so {First} is never awkward
    friendly_first = first or "there"

    # Provide both cases and common synonyms
    ctx = {
        "First": friendly_first,
        "first": friendly_first,
        "Address": address or "",
        "address": address or "",
    }
    return ctx


# ─────────────────────────────────────────────────────────────
# Templating
# ─────────────────────────────────────────────────────────────
def _get(fields: Dict, *names):
    for n in names:
        if n in fields:
            return fields[n]
    nf = {_norm(k): k for k in fields.keys()}
    for n in names:
        k = nf.get(_norm(n))
        if k:
            return fields[k]
    return None

def _format_template(text: str, ctx: Dict[str, Any]) -> str:
    if not text:
        return text
    amap = {_norm(k): ("" if v is None else str(v)) for k, v in (ctx or {}).items()}

    def repl(m):
        raw = (m.group(1) or m.group(2) or "").strip()
        val = amap.get(_norm(raw))
        if val is not None and val != "":
            return val
        # Specific friendly defaults
        if _norm(raw) in ("first",):
            return amap.get("first", "there")
        if _norm(raw) in ("address",):
            return amap.get("address", "")
        # leave unknown token untouched
        return m.group(0)

    return re.sub(r"\{\{([^}]+)\}\}|\{([^}]+)\}", repl, text)

def pick_template(template_ids: Any, templates_table):
    if not (template_ids and templates_table):
        return (None, None)
    tid = random.choice(template_ids) if isinstance(template_ids, list) else str(template_ids)
    try:
        row = templates_table.get(tid)  # type: ignore[attr-defined]
        msg = _get(row.get("fields", {}) if row else {}, "Message", "message", "Text", "text")
        return (msg, tid) if msg else (None, None)
    except Exception:
        traceback.print_exc()
        return (None, None)


# ─────────────────────────────────────────────────────────────
# Numbers (market-aware) + round-robin sequence
# ─────────────────────────────────────────────────────────────
def _parse_dt(s: Any) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        return None

def _supports_market(f: Dict[str, Any], market: Optional[str]) -> bool:
    if not market:
        return True
    if f.get("Market") == market:
        return True
    ms = f.get("Markets")
    return isinstance(ms, list) and market in ms

def _to_e164(f: Dict[str, Any]) -> Optional[str]:
    for key in ("Number", "A Number", "Phone", "E164", "Friendly Name"):
        v = f.get(key)
        if isinstance(v, str) and _digits_only(v):
            d = v if v.startswith("+") else "+" + _digits_only(v)
            return d
    return None

def load_active_numbers(market: Optional[str]) -> List[Tuple[str, str]]:
    """
    Return list of (e164, record_id) for active numbers supporting market, sorted
    to balance load: prioritize larger remaining and least recently used.
    """
    nums = get_numbers_table()
    if not nums:
        return []
    try:
        rows = nums.all()  # type: ignore[attr-defined]
    except Exception:
        traceback.print_exc()
        return []

    elig: List[Tuple[int, datetime, Dict]] = []
    for r in rows:
        f = r.get("fields", {}) or {}
        if f.get("Active") is False:
            continue
        if str(f.get("Status") or "").strip().lower() == "paused":
            continue
        if not _supports_market(f, market):
            continue

        rem = f.get("Remaining")
        try:
            rem = int(rem) if rem is not None else None
        except Exception:
            rem = None
        if rem is None:
            sent_today = int(f.get("Sent Today") or 0)
            daily_cap = int(f.get("Daily Reset") or DAILY_LIMIT_FALLBACK)
            rem = max(0, daily_cap - sent_today)

        if rem <= 0:
            continue

        last_used = _parse_dt(f.get("Last Used")) or datetime(1970, 1, 1, tzinfo=timezone.utc)
        elig.append((rem, last_used, r))

    if not elig:
        return []

    # Sort by remaining desc, last_used asc → spread usage
    elig.sort(key=lambda t: (-t[0], t[1]))

    seq: List[Tuple[str, str]] = []
    for _, __, r in elig:
        e164 = _to_e164(r.get("fields", {}))
        if e164:
            seq.append((e164, r["id"]))
    return seq

def touch_number_usage(rec_id: str):
    nums = get_numbers_table()
    if nums and rec_id:
        _safe_update(nums, rec_id, {"Sent Today": None})  # noop if field missing
        _safe_update(nums, rec_id, {"Last Used": iso_now()})


# ─────────────────────────────────────────────────────────────
# UI helpers
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
# Dedupe guard (per campaign, last 10 digits)
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
        traceback.print_exc()
        return False


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def run_campaigns(limit: int | str = 1, send_after_queue: Optional[bool] = None) -> Dict[str, Any]:
    """
    Queues messages for eligible campaigns (quiet hours respected, prequeue supported),
    round-robins across active numbers at RATE_PER_NUMBER_PER_MIN, de-dupes per campaign,
    and optionally triggers immediate send.
    """
    if isinstance(limit, str) and limit.upper() == "ALL":
        limit = 999_999
    limit = int(limit)

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
        name = _get(f, "Name", "name") or c.get("id")

        # Go Live gate
        go_live = f.get("Go Live")
        if go_live is False:
            if DEBUG_CAMPAIGNS:
                print(f"[campaign] SKIP {name}: Go Live is False")
            continue

        status_val = str(_get(f, "status", "Status") or "").strip().lower()
        if status_val and status_val not in ("scheduled", "running"):
            if DEBUG_CAMPAIGNS:
                print(f"[campaign] SKIP {name}: status={status_val!r}")
            continue

        start_dt = _get_time_field(f, "Start Time", "Start", "Start At", "start_time", "Start Date", "Schedule Start")
        end_dt = _get_time_field(f, "End Time", "End", "End At", "end_time", "End Date", "Schedule End")

        if end_dt and now_utc >= end_dt:
            _safe_update(campaigns, c["id"], {"status": "Completed", "last_run_at": iso_now()})
            if DEBUG_CAMPAIGNS:
                print(f"[campaign] COMPLETE {name}: now>=end")
            continue

        if start_dt and now_utc < start_dt and not PREQUEUE_BEFORE_START:
            if DEBUG_CAMPAIGNS:
                print(f"[campaign] WAIT {name}: now<start (prequeue off)")
            continue

        if start_dt and now_utc < start_dt and PREQUEUE_BEFORE_START:
            if not ALLOW_QUEUE_OUTSIDE_HOURS and _in_quiet_hours(start_dt):
                if DEBUG_CAMPAIGNS:
                    print(f"[campaign] WAIT {name}: start in quiet; queue-off")
                continue

        if DEBUG_CAMPAIGNS:
            print(f"[campaign] ELIGIBLE {name}: start={start_dt}, end={end_dt}, status={status_val or '∅'}")
        eligible.append(c)

    processed = 0
    results: List[Dict[str, Any]] = []

    for camp in eligible:
        if processed >= limit:
            break

        cf = camp.get("fields", {}) or {}
        cid = camp["id"]
        name = _get(cf, "Name", "name") or "Unnamed"
        view = (cf.get("View/Segment") or cf.get("View") or "").strip() or None
        market = _get(cf, "Market", "market")

        # Load all eligible numbers for this market upfront for round-robin
        number_seq = load_active_numbers(market)
        if not number_seq:
            if DEBUG_CAMPAIGNS:
                print(f"[campaign] SKIP {name}: no eligible numbers")
            # Still mark last_run so we don't spin forever
            _safe_update(campaigns, cid, {"last_run_at": iso_now()})
            processed += 1
            continue

        try:
            prospect_rows = prospects.all(view=view) if view else prospects.all()  # type: ignore[attr-defined]
        except Exception:
            traceback.print_exc()
            processed += 1
            continue

        template_ids = _get(cf, "Templates", "templates") or []
        if not template_ids:
            if DEBUG_CAMPAIGNS:
                print(f"[campaign] SKIP {name}: no Templates linked")
            _safe_update(campaigns, cid, {"last_run_at": iso_now()})
            processed += 1
            continue

        start_dt = _get_time_field(cf, "Start Time", "Start", "Start At", "start_time", "Start Date", "Schedule Start")
        prequeue = bool(start_dt and now_utc < start_dt and PREQUEUE_BEFORE_START)
        base_utc = start_dt if prequeue else (max(now_utc, start_dt) if start_dt else now_utc)
        if _in_quiet_hours(base_utc):
            base_utc = _shift_to_window(base_utc)

        if prequeue:
            _safe_update(campaigns, cid, {"last_run_at": iso_now()})
        else:
            _safe_update(campaigns, cid, {"status": "Running", "last_run_at": iso_now()})

        queued = 0
        ncount = len(number_seq)

        for idx, pr in enumerate(prospect_rows):
            pf = pr.get("fields", {}) or {}
            phone = get_phone(pf)
            if not phone:
                continue
            if already_queued(drip, phone, cid):
                continue

            # Pick a number in round-robin across the prepared sequence
            did, did_rec = number_seq[idx % ncount]
            # Optionally update "Last Used" for the DID to keep rotation healthy
            touch_number_usage(did_rec)

            raw, tid = pick_template(template_ids, templates)
            if not raw:
                continue

            ctx = _personalization_ctx(pf)
            body = _format_template(raw, ctx)

            scheduled_local = schedule_time_round_robin(base_utc, idx, ncount)

            payload = {
                "Prospect": [pr["id"]],
                "Campaign": [cid],
                "Template": [tid] if tid else None,
                "Market": market or pf.get("Market"),
                "phone": phone,
                "message_preview": body,
                "from_number": did,                    # <-- safe; schema-mapper will adapt if base uses "From Number"
                "status": "QUEUED",
                "next_send_date": scheduled_local,     # CT-naive
                "Property ID": pf.get("Property ID"),
                "Number Record Id": did_rec,
                "UI": STATUS_ICON.get("QUEUED", "⏳"),
            }
            created = _safe_create(get_drip_table(), {k: v for k, v in payload.items() if v is not None})
            if created:
                queued += 1

        batch_result, retry_result = {"total_sent": 0}, {}
        if (not prequeue) and RUNNER_SEND_AFTER_QUEUE and queued > 0:
            try:
                # send now at global cap hint; worker/batcher enforces true pacing
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
            "Scheduled"
            if prequeue
            else (
                "Running"
                if queued and (sent_delta < queued or not RUNNER_SEND_AFTER_QUEUE)
                else ("Completed" if queued else (_get(cf, "status", "Status") or "Scheduled"))
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
            "PerNumberRate": RATE_PER_NUMBER_PER_MIN,
            "NumbersUsed": ncount,
            "MPM_hint": MESSAGES_PER_MIN,
            "Prequeued": prequeue,
        }

        _safe_update(
            get_campaigns_table(),
            cid,
            {
                "status": new_status,
                "messages_sent": int(cf.get("messages_sent") or 0) + sent_delta,
                "total_sent": int(cf.get("total_sent") or 0) + sent_delta,
                "Last Run Result": json.dumps(last_result),
                "last_run_at": iso_now(),
            },
        )

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
                    "Metric": "OUTBOUND_SENT" if (not prequeue and RUNNER_SEND_AFTER_QUEUE) else "MESSAGES_QUEUED",
                    "Value": float(sent_delta if (not prequeue and RUNNER_SEND_AFTER_QUEUE) else queued),
                    "Date": utcnow().date().isoformat(),
                },
            )

        if DEBUG_CAMPAIGNS:
            print(f"[campaign] {name}: queued={queued}, sent_now={0 if prequeue else sent_delta}, status→{new_status}")

        results.append(
            {
                "campaign": name,
                "queued": queued,
                "sent": 0 if prequeue else (sent_delta if RUNNER_SEND_AFTER_QUEUE else 0),
                "view": view,
                "market": market,
                "quiet_now": _in_quiet_hours(now_utc),
                "per_number_rate": RATE_PER_NUMBER_PER_MIN,
                "numbers_used": ncount,
            }
        )
        processed += 1

    try:
        update_metrics()
    except Exception:
        traceback.print_exc()

    return {"ok": True, "processed": processed, "results": results, "errors": []}
