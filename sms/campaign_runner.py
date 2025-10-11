# sms/campaign_runner.py
from __future__ import annotations

import os, re, json, random, math, traceback
from datetime import datetime, timezone, timedelta, date
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from pyairtable import Api

load_dotenv()

# ----- Optional project pieces -----
try:
    from sms.outbound_batcher import send_batch, format_template
except Exception:
    def send_batch(*args, **kwargs): return {"total_sent": 0}
    def format_template(t: str, f: Dict[str, Any]) -> str: return t
try:
    from sms.retry_runner import run_retry
except Exception:
    def run_retry(*args, **kwargs): return {"retried": 0}
try:
    from sms.metrics_tracker import update_metrics
except Exception:
    def update_metrics(*args, **kwargs): pass

# ============== ENV / CONFIG ==============
AIRTABLE_KEY            = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE       = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
CAMPAIGN_CONTROL_BASE   = os.getenv("CAMPAIGN_CONTROL_BASE") or os.getenv("AIRTABLE_CAMPAIGN_CONTROL_BASE_ID")
PERFORMANCE_BASE        = os.getenv("PERFORMANCE_BASE") or os.getenv("AIRTABLE_PERFORMANCE_BASE_ID")

PROSPECTS_TABLE         = os.getenv("PROSPECTS_TABLE", "Prospects")
CAMPAIGNS_TABLE         = os.getenv("CAMPAIGNS_TABLE", "Campaigns")
TEMPLATES_TABLE         = os.getenv("TEMPLATES_TABLE", "Templates")
DRIP_QUEUE_TABLE        = os.getenv("DRIP_QUEUE_TABLE", "Drip Queue")
NUMBERS_TABLE           = os.getenv("NUMBERS_TABLE", "Numbers")

RUNNER_SEND_AFTER_QUEUE = os.getenv("RUNNER_SEND_AFTER_QUEUE", "false").lower() in ("1", "true", "yes")
DEDUPE_HOURS            = int(os.getenv("DEDUPE_HOURS", "72"))
DAILY_LIMIT_FALLBACK    = int(os.getenv("DAILY_LIMIT", "750"))

# pacing (20/min default)
MESSAGES_PER_MIN        = max(1, int(os.getenv("MESSAGES_PER_MIN", "20")))
SECONDS_PER_MSG         = max(1, int(math.ceil(60.0 / MESSAGES_PER_MIN)))
JITTER_SECONDS          = max(0, int(os.getenv("JITTER_SECONDS", "2")))

# quiet hours (America/Chicago)
QUIET_TZ                = ZoneInfo(os.getenv("QUIET_TZ", "America/Chicago"))
QUIET_START_HOUR        = int(os.getenv("QUIET_START_HOUR", "21"))  # 9 pm
QUIET_END_HOUR          = int(os.getenv("QUIET_END_HOUR", "9"))     # 9 am

PHONE_FIELDS = [
    "phone","Phone","Mobile","Cell","Phone Number","Primary Phone",
    "Phone 1","Phone 2","Phone 3",
    "Owner Phone","Owner Phone 1","Owner Phone 2",
    "Phone 1 (from Linked Owner)","Phone 2 (from Linked Owner)","Phone 3 (from Linked Owner)",
]

STATUS_ICON = {
    "QUEUED": "⏳", "READY": "⏳", "SENDING": "🔄",
    "SENT": "✅", "DELIVERED": "✅", "FAILED": "❌", "CANCELLED": "❌",
}

# ============== helpers ==============
def utcnow() -> datetime: return datetime.now(timezone.utc)
def iso_now() -> str: return utcnow().isoformat()
def _norm(s: Any) -> Any: return re.sub(r"[^a-z0-9]+", "", s.strip().lower()) if isinstance(s, str) else s

def _digits_only(s: Any) -> Optional[str]:
    if not isinstance(s, str): return None
    ds = "".join(re.findall(r"\d+", s))
    return ds if len(ds) >= 10 else None

def last10(s: Any) -> Optional[str]:
    d = _digits_only(s); return d[-10:] if d else None

def get_phone(f: Dict[str, Any]) -> Optional[str]:
    # prefer verified phone 1/2 if present
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

# --- quiet hours / schedule in local CT (store naive local ISO) ---
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
    """Return 'YYYY-MM-DDTHH:MM:SS' in America/Chicago (no Z/no offset)."""
    local = dt_utc.astimezone(QUIET_TZ).replace(tzinfo=None)
    return local.isoformat(timespec="seconds")

def schedule_time(base_utc: datetime, idx: int) -> str:
    jitter = random.randint(0, JITTER_SECONDS) if JITTER_SECONDS else 0
    t = base_utc + timedelta(seconds=idx * SECONDS_PER_MSG + jitter)
    if _in_quiet_hours(t):
        t = _shift_to_window(t)
    return _local_naive_iso(t)  # store CT naive so Airtable UI doesn't shift it

# ============== robust time parsing ==============
def _parse_time_maybe_ct(value: Any) -> Optional[datetime]:
    """
    Accepts:
      - ISO with tz → return UTC
      - naive datetime → treat as America/Chicago, return UTC
      - date-only 'YYYY-MM-DD' → set to QUIET_END_HOUR (e.g., 09:00) in CT, return UTC
    """
    if not value:
        return None
    txt = str(value).strip()
    try:
        if "T" in txt:
            dt = datetime.fromisoformat(txt.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=QUIET_TZ)
            return dt.astimezone(timezone.utc)
        d = date.fromisoformat(txt)
        local = datetime(d.year, d.month, d.day, max(9, QUIET_END_HOUR), 0, 0, tzinfo=QUIET_TZ)
        return local.astimezone(timezone.utc)
    except Exception:
        return None

# ============== Airtable clients ==============
@lru_cache(maxsize=None)
def _api_main():    return Api(AIRTABLE_KEY) if AIRTABLE_KEY else None

@lru_cache(maxsize=None)
def get_campaigns():
    """
    Campaigns may live in Campaign Control base or Leads/Convos.
    Prefer CAMPAIGN_CONTROL_BASE; fallback to LEADS_CONVOS_BASE.
    """
    if not _api_main():
        return None
    if CAMPAIGN_CONTROL_BASE:
        try:
            return _api_main().table(CAMPAIGN_CONTROL_BASE, CAMPAIGNS_TABLE)
        except Exception:
            traceback.print_exc()
    if LEADS_CONVOS_BASE:
        try:
            return _api_main().table(LEADS_CONVOS_BASE, CAMPAIGNS_TABLE)
        except Exception:
            traceback.print_exc()
    return None

@lru_cache(maxsize=None)
def get_templates():
    if not (_api_main() and LEADS_CONVOS_BASE):
        return None
    return _api_main().table(LEADS_CONVOS_BASE, TEMPLATES_TABLE)

@lru_cache(maxsize=None)
def get_prospects():
    if not (_api_main() and LEADS_CONVOS_BASE):
        return None
    return _api_main().table(LEADS_CONVOS_BASE, PROSPECTS_TABLE)

@lru_cache(maxsize=None)
def get_drip():
    if not (_api_main() and LEADS_CONVOS_BASE):
        return None
    return _api_main().table(LEADS_CONVOS_BASE, DRIP_QUEUE_TABLE)

@lru_cache(maxsize=None)
def get_numbers():
    if not (_api_main() and CAMPAIGN_CONTROL_BASE):
        return None
    return _api_main().table(CAMPAIGN_CONTROL_BASE, NUMBERS_TABLE)

@lru_cache(maxsize=None)
def get_runs():
    if not (_api_main() and PERFORMANCE_BASE):
        return None
    return _api_main().table(PERFORMANCE_BASE, "Runs/Logs")

@lru_cache(maxsize=None)
def get_kpis():
    if not (_api_main() and PERFORMANCE_BASE):
        return None
    return _api_main().table(PERFORMANCE_BASE, "KPIs")

# ============== field-safe writes ==============
def _auto_field_map(tbl, sample_id: Optional[str]=None) -> Dict[str,str]:
    try:
        if sample_id:
            probe = tbl.get(sample_id)
        else:
            rows = tbl.all(max_records=1)
            probe = rows[0] if rows else {"fields": {}}
        keys = list(probe.get("fields", {}).keys())
    except Exception:
        keys = []
    return {_norm(k): k for k in keys}

def _safe_filter(tbl, payload: Dict, sample_id: Optional[str]=None) -> Dict:
    amap = _auto_field_map(tbl, sample_id)
    if not amap: return dict(payload)
    out = {}
    for k, v in payload.items():
        mk = amap.get(_norm(k))
        if mk: out[mk] = v
    return out

def _safe_create(tbl, payload: Dict, sample_id: Optional[str]=None):
    try:
        data = _safe_filter(tbl, payload, sample_id)
        return tbl.create(data) if data else None
    except Exception:
        traceback.print_exc(); return None

def _safe_update(tbl, rid: str, payload: Dict, sample_id: Optional[str]=None):
    try:
        data = _safe_filter(tbl, payload, sample_id)
        return tbl.update(rid, data) if (rid and data) else None
    except Exception:
        traceback.print_exc(); return None

# ============== templates ==============
def _get(fields: Dict, *names):
    for n in names:
        if n in fields: return fields[n]
    nf = {_norm(k): k for k in fields.keys()}
    for n in names:
        k = nf.get(_norm(n))
        if k: return fields[k]
    return None

def pick_template(template_ids: Any, templates_table):
    if not (template_ids and templates_table): return (None, None)
    tid = random.choice(template_ids) if isinstance(template_ids, list) else str(template_ids)
    try:
        row = templates_table.get(tid)
        msg = _get(row.get("fields", {}), "Message", "message", "Body", "SMS Body", "Copy") if row else None
        return (msg, tid) if msg else (None, None)
    except Exception:
        traceback.print_exc(); return (None, None)

# ============== numbers picker (Numbers.'Number') ==============
def _parse_dt(s: Any) -> Optional[datetime]:
    if not s: return None
    try: return datetime.fromisoformat(str(s).replace("Z","+00:00"))
    except Exception: return None

def _supports_market(f: Dict[str, Any], market: Optional[str]) -> bool:
    if not market: return True
    if f.get("Market") == market: return True
    ms = f.get("Markets")
    return isinstance(ms, list) and market in ms

def _to_e164(f: Dict[str, Any]) -> Optional[str]:
    for key in ("Number", "A Number", "Phone", "E164", "Friendly Name"):
        v = f.get(key)
        if isinstance(v, str) and _digits_only(v):
            return v if v.strip().startswith("+") else "+" + _digits_only(v)
    return None

def pick_from_number(market: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    nums = get_numbers()
    if not nums: return (None, None)
    try:
        rows = nums.all()
    except Exception:
        traceback.print_exc(); return (None, None)

    elig: List[Tuple[Tuple[int, datetime], Dict]] = []
    for r in rows:
        f = r.get("fields", {})
        if f.get("Active") is False: continue
        if str(f.get("Status") or "").strip().lower() == "paused": continue
        if not _supports_market(f, market): continue

        rem = f.get("Remaining")
        try: rem = int(rem) if rem is not None else None
        except Exception: rem = None
        if rem is None:
            sent_today = int(f.get("Sent Today") or 0)
            daily_cap  = int(f.get("Daily Reset") or DAILY_LIMIT_FALLBACK)
            rem = max(0, daily_cap - sent_today)
        if rem <= 0: continue

        last_used = _parse_dt(f.get("Last Used")) or datetime(1970,1,1,tzinfo=timezone.utc)
        elig.append(((-rem, last_used), r))

    if not elig: return (None, None)

    elig.sort(key=lambda x: x[0])
    chosen = elig[0][1]
    cf = chosen.get("fields", {})
    did = _to_e164(cf)
    if not did: return (None, None)

    _safe_update(get_numbers(), chosen["id"], {
        "Sent Today": int(cf.get("Sent Today") or 0) + 1,
        "Last Used": iso_now(),
    })
    return did, chosen["id"]

# ============== UI helpers ==============
def _refresh_ui_icons_for_campaign(drip_tbl, campaign_id: str):
    try:
        for r in drip_tbl.all():
            f = r.get("fields", {})
            cids = f.get("Campaign") or []
            if campaign_id in (cids if isinstance(cids, list) else [cids]):
                status = str(f.get("status") or f.get("Status") or "")
                icon = STATUS_ICON.get(status, "")
                if icon and f.get("UI") != icon:
                    _safe_update(drip_tbl, r["id"], {"UI": icon})
    except Exception:
        traceback.print_exc()

# ============== dedupe guard ==============
def _last_n_hours_dt(hours: int) -> datetime:
    return utcnow() - timedelta(hours=hours)

def _parse_when_any(f: Dict[str, Any]) -> Optional[datetime]:
    for k in ("next_send_date", "Next Send Date", "scheduled_at", "created_at", "Created At"):
        v = f.get(k)
        # accept CT-naive strings too
        if v:
            # If it looks like a datetime without offset, treat as CT and return UTC
            try:
                txt = str(v)
                if "T" in txt:
                    dt = datetime.fromisoformat(txt.replace("Z","+00:00"))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=QUIET_TZ).astimezone(timezone.utc)
                    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            except Exception:
                pass
    return None

def already_queued(drip_tbl, phone: str, campaign_id: str) -> bool:
    try:
        cutoff_dt = _last_n_hours_dt(DEDUPE_HOURS)
        l10 = last10(phone)
        for r in drip_tbl.all():
            f = r.get("fields", {})
            ph = f.get("phone") or f.get("Phone")
            if last10(ph) == l10:
                cids = f.get("Campaign") or []
                cids = cids if isinstance(cids, list) else [cids]
                if campaign_id in cids:
                    status = str(f.get("status") or f.get("Status") or "")
                    when_dt = _parse_when_any(f) or datetime(1970,1,1,tzinfo=timezone.utc)
                    if status in ("QUEUED","SENDING","READY") and when_dt >= cutoff_dt:
                        return True
        return False
    except Exception:
        traceback.print_exc(); return False

# ============== main runner ==============
def run_campaigns(limit: int | str = 1, send_after_queue: Optional[bool] = None) -> Dict[str, Any]:
    """
    Queue messages for eligible campaigns:
      • Go Live = TRUE and Status ∈ {Scheduled, Running}   (case-insensitive)
      • Now within [Start, End] window (Central-aware; date-only allowed)
      • Each row gets a market-matched DID from Numbers.Number → Drip Queue.from_number
      • next_send_date is CT-naive string at exact start time (respecting quiet hours)
      • Optional immediate send after queue (outside quiet hours)
    """
    if isinstance(limit, str) and limit.upper() == "ALL":
        limit = 999_999
    limit = int(limit)

    if send_after_queue is None:
        send_after_queue = RUNNER_SEND_AFTER_QUEUE
    if _in_quiet_hours(utcnow()):  # never send immediately during quiet hours
        send_after_queue = False

    campaigns = get_campaigns(); templates = get_templates()
    prospects = get_prospects(); drip = get_drip()
    if not all([campaigns, templates, prospects, drip]):
        return {"ok": False, "processed": 0, "results": [], "errors": ["Missing Airtable tables or env"]}

    now_utc = utcnow()
    now_iso = iso_now()

    try:
        all_campaigns = campaigns.all()
    except Exception:
        traceback.print_exc()
        return {"ok": False, "processed": 0, "results": [], "errors": ["Failed to fetch Campaigns"]}

    eligible: List[Dict] = []
    for c in all_campaigns:
        f = c.get("fields", {}) or {}
        # Go Live toggle (default false if present)
        if f.get("Go Live") is False:
            continue

        status_val = str(_get(f, "status", "Status") or "").strip().lower()
        if status_val and status_val not in ("scheduled", "running"):
            continue

        # robust start/end parsing
        start_dt = _parse_time_maybe_ct(_get(f, "Start Time", "start_time", "Start At", "Start"))
        end_dt   = _parse_time_maybe_ct(_get(f, "End Time", "end_time", "End At", "End"))

        if start_dt and now_utc < start_dt:
            continue
        if end_dt and now_utc >= end_dt:
            _safe_update(campaigns, c["id"], {"status": "Completed", "last_run_at": now_iso})
            continue

        eligible.append(c)

    processed = 0
    results: List[Dict[str, Any]] = []

    for camp in eligible:
        if processed >= limit: break

        cf = camp.get("fields", {}) or {}
        cid = camp["id"]
        name = _get(cf, "Name", "name") or "Unnamed"

        # audience: allow multiple field names
        view = (
            (cf.get("View/Segment") or "") or
            (cf.get("Audience View") or "") or
            (cf.get("View") or "")
        ).strip() or None

        try:
            prospect_rows = prospects.all(view=view) if view else prospects.all()
        except Exception:
            traceback.print_exc(); continue

        template_ids = _get(cf, "Templates", "templates") or []
        if not template_ids:
            # no templates → nothing to queue
            _safe_update(campaigns, cid, {"last_run_at": now_iso})
            continue

        market = _get(cf, "Market", "market")

        # Begin run: flip status to Running (non-destructive if was already Running)
        _safe_update(campaigns, cid, {"status": "Running", "last_run_at": now_iso})

        # base time = exact start (if any) or now, then shifted out of quiet hours if needed
        start_dt = _parse_time_maybe_ct(_get(cf, "Start Time", "start_time", "Start At", "Start"))
        base_utc = max(now_utc, start_dt) if start_dt else now_utc
        if _in_quiet_hours(base_utc):
            base_utc = _shift_to_window(base_utc)

        queued = 0

        for idx, pr in enumerate(prospect_rows):
            pf = pr.get("fields", {}) or {}
            phone = get_phone(pf)
            if not phone: 
                continue
            if already_queued(drip, phone, cid): 
                continue

            # choose a DID *per row* to distribute load and ensure from_number is set
            from_number, number_rec_id = pick_from_number(market)
            if not from_number:
                # no available DIDs right now → leave campaign Running, try again next tick
                continue

            raw, tid = pick_template(template_ids, templates)
            if not raw:
                continue

            body = format_template(raw, pf)
            scheduled_local = schedule_time(base_utc, idx)  # CT-naive

            payload = {
                "Prospect": [pr["id"]],
                "Campaign": [cid],
                "Template": [tid] if tid else None,
                "Market": market or pf.get("Market"),
                "phone": phone,
                "message_preview": body,
                "from_number": from_number,
                "From Number": from_number,
                "status": "QUEUED",
                "next_send_date": scheduled_local,   # CT-naive string
                "Property ID": pf.get("Property ID"),
                "Number Record Id": number_rec_id,
                "UI": "⏳",
            }
            created = _safe_create(drip, {k: v for k, v in payload.items() if v is not None})
            if created:
                queued += 1

        # optional immediate send (outside quiet hours), small batch to “kick” the pipeline
        batch_result, retry_result = {"total_sent": 0}, {}
        if send_after_queue and queued > 0 and not _in_quiet_hours(utcnow()):
            try:
                batch_result = send_batch(campaign_id=cid, limit=MESSAGES_PER_MIN)
            except Exception:
                traceback.print_exc()
            if (batch_result.get("total_sent", 0) or 0) < queued:
                # opportunistic retry burst
                for _ in range(3):
                    try:
                        retry_result = run_retry(limit=MESSAGES_PER_MIN, view=None)
                    except Exception:
                        retry_result = {}
                    if (retry_result or {}).get("retried", 0) == 0:
                        break

        _refresh_ui_icons_for_campaign(drip, cid)

        sent_delta = (batch_result.get("total_sent", 0) or 0) + (retry_result.get("retried", 0) or 0)
        last_result = {
            "Queued": queued,
            "Sent": batch_result.get("total_sent", 0) or 0,
            "Retries": retry_result.get("retried", 0) or 0,
            "Table": PROSPECTS_TABLE,
            "View": view,
            "Market": market,
            "QuietHoursNow": _in_quiet_hours(now_utc),
            "MPM": MESSAGES_PER_MIN,
        }
        _safe_update(campaigns, cid, {
            "status": "Running" if queued and (not send_after_queue or sent_delta < queued)
                      else ("Completed" if queued else (_get(cf,"status","Status") or "Running")),
            "messages_sent": int(cf.get("messages_sent") or 0) + sent_delta,
            "total_sent": int(cf.get("total_sent") or 0) + sent_delta,
            "Last Run Result": json.dumps(last_result),
            "last_run_at": iso_now(),
        })

        if get_runs():
            _safe_create(get_runs(), {
                "Type": "CAMPAIGN_RUN",
                "Campaign": name,
                "Processed": float(sent_delta),
                "Breakdown": json.dumps({"initial": batch_result, "retries": retry_result}),
                "Timestamp": iso_now(),
            })
        if get_kpis():
            _safe_create(get_kpis(), {
                "Campaign": name,
                "Metric": "OUTBOUND_SENT" if send_after_queue else "MESSAGES_QUEUED",
                "Value": float(sent_delta if send_after_queue else queued),
                "Date": utcnow().date().isoformat(),
            })

        results.append({
            "campaign": name, "queued": queued,
            "sent": sent_delta if send_after_queue else 0,
            "view": view, "market": market,
            "quiet_now": _in_quiet_hours(now_utc), "mpm": MESSAGES_PER_MIN,
        })
        processed += 1

    try:
        update_metrics()
    except Exception:
        traceback.print_exc()

    return {"ok": True, "processed": processed, "results": results, "errors": []}