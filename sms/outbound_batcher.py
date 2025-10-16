# sms/outbound_batcher.py
from __future__ import annotations

import hashlib
import os
import re
import time
import traceback
from datetime import date, datetime, timedelta, timezone
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

# -------------------------------
# pyairtable compatibility layer
# -------------------------------
_PyTable = None
_PyApi = None
try:
    from pyairtable import Table as _PyTable  # v1 style
except Exception:
    _PyTable = None
try:
    from pyairtable import Api as _PyApi  # v2 style
except Exception:
    _PyApi = None


def _make_table(api_key: Optional[str], base_id: Optional[str], table_name: str):
    """
    Works with both pyairtable styles. Returns a Table-like object
    exposing .all(...), .get(...), .create(...), .update(...), or None.
    NEVER throws if not configured.
    """
    if not (api_key and base_id):
        return None
    try:
        if _PyTable:
            return _PyTable(api_key, base_id, table_name)
        if _PyApi:
            return _PyApi(api_key).table(base_id, table_name)
    except Exception:
        traceback.print_exc()
    return None


# =========================
# ENV / CONFIG
# =========================
LEADS_BASE_ENV = "LEADS_CONVOS_BASE"  # Drip Queue base
PERF_BASE_ENV = "PERFORMANCE_BASE"  # KPIs / Runs
CONTROL_BASE_ENV = "CAMPAIGN_CONTROL_BASE"  # Numbers base

DRIP_TABLE_NAME = os.getenv("DRIP_QUEUE_TABLE", "Drip Queue")
NUMBERS_TABLE_NAME = os.getenv("NUMBERS_TABLE", "Numbers")
CAMPAIGNS_TABLE_NAME = os.getenv("CAMPAIGNS_TABLE", "Campaigns")

# ---- Back-compat constants expected by tests / older code
DRIP_QUEUE_TABLE = DRIP_TABLE_NAME
NUMBERS_TABLE = NUMBERS_TABLE_NAME
CAMPAIGNS_TABLE = CAMPAIGNS_TABLE_NAME

# Rate limits (enforced with Redis across all workers)
RATE_PER_NUMBER_PER_MIN = int(os.getenv("RATE_PER_NUMBER_PER_MIN", "20"))
GLOBAL_RATE_PER_MIN = int(os.getenv("GLOBAL_RATE_PER_MIN", "5000"))
SLEEP_BETWEEN_SENDS_SEC = float(os.getenv("SLEEP_BETWEEN_SENDS_SEC", "0.03"))
RATE_LIMIT_REQUEUE_SECONDS = float(os.getenv("RATE_LIMIT_REQUEUE_SECONDS", "5"))
NO_NUMBER_REQUEUE_SECONDS = float(os.getenv("NO_NUMBER_REQUEUE_SECONDS", "60"))

# Quiet hours (America/Chicago): block actual sending 9pm–9am CT
QUIET_HOURS_ENFORCED = os.getenv("QUIET_HOURS_ENFORCED", "true").lower() in ("1", "true", "yes")
QUIET_START_HOUR_LOCAL = int(os.getenv("QUIET_START_HOUR_LOCAL", "21"))
QUIET_END_HOUR_LOCAL = int(os.getenv("QUIET_END_HOUR_LOCAL", "9"))

# Backfill missing from_number with a market DID from Numbers table
AUTO_BACKFILL_FROM_NUMBER = os.getenv("AUTO_BACKFILL_FROM_NUMBER", "true").lower() in ("1", "true", "yes")

# Redis / Upstash limiter
REDIS_URL = os.getenv("REDIS_URL") or os.getenv("UPSTASH_REDIS_URL")
REDIS_TLS = os.getenv("REDIS_TLS", "true").lower() in ("1", "true", "yes")
UPSTASH_REDIS_REST_URL = os.getenv("UPSTASH_REDIS_REST_URL")
UPSTASH_REDIS_REST_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN")

try:
    import redis
except Exception:
    redis = None
try:
    import requests
except Exception:
    requests = None

# Optional sender
try:
    from sms.message_processor import MessageProcessor
except Exception:
    MessageProcessor = None


# =========================
# Time helpers
# =========================
def utcnow() -> datetime:
    return datetime.now(timezone.utc)


try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None


def _ct_tz():
    return ZoneInfo("America/Chicago") if ZoneInfo else timezone.utc


def central_now() -> datetime:
    return datetime.now(_ct_tz())


def is_quiet_hours_local() -> bool:
    if not QUIET_HOURS_ENFORCED:
        return False
    h = central_now().hour
    return (h >= QUIET_START_HOUR_LOCAL) or (h < QUIET_END_HOUR_LOCAL)


def _parse_iso_maybe_ct(s: Any) -> Optional[datetime]:
    """
    Accepts:
      - ISO with tz   → return as UTC
      - naive datetime string → interpret as Central and convert to UTC
      - date-only 'YYYY-MM-DD' → interpret as that date at 09:00 Central (start of send window)
    """
    if not s:
        return None
    text = str(s).strip()
    try:
        if "T" in text:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=_ct_tz()).astimezone(timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return dt
        # date-only
        d = date.fromisoformat(text)
        local_dt = datetime(d.year, d.month, d.day, max(9, QUIET_END_HOUR_LOCAL), 0, 0, tzinfo=_ct_tz())
        return local_dt.astimezone(timezone.utc)
    except Exception:
        return None


# =========================
# Airtable helpers
# =========================
def _first_env(*names: str) -> Optional[str]:
    for n in names:
        v = os.getenv(n)
        if v:
            return v
    return None


def _api_key_for(base_env: str) -> Optional[str]:
    if base_env == PERF_BASE_ENV:
        return _first_env("AIRTABLE_REPORTING_KEY", "PERFORMANCE_KEY", "AIRTABLE_API_KEY")
    # Leads/Convos
    if base_env == LEADS_BASE_ENV:
        return _first_env("AIRTABLE_ACQUISITIONS_KEY", "LEADS_CONVOS_KEY", "AIRTABLE_API_KEY")
    # Campaign Control
    if base_env == CONTROL_BASE_ENV:
        return _first_env("AIRTABLE_COMPLIANCE_KEY", "CAMPAIGN_CONTROL_KEY", "AIRTABLE_API_KEY")
    # default fallback
    return os.getenv("AIRTABLE_API_KEY")


def _base_value_for(base_env: str) -> Optional[str]:
    if base_env == LEADS_BASE_ENV:
        return _first_env("LEADS_CONVOS_BASE", "AIRTABLE_LEADS_CONVOS_BASE_ID")
    if base_env == PERF_BASE_ENV:
        return _first_env("PERFORMANCE_BASE", "AIRTABLE_PERFORMANCE_BASE_ID")
    if base_env == CONTROL_BASE_ENV:
        return _first_env("CAMPAIGN_CONTROL_BASE", "AIRTABLE_CAMPAIGN_CONTROL_BASE_ID")
    return os.getenv(base_env)


def get_table(base_env: str, table_name: str):
    key = _api_key_for(base_env)
    base = _base_value_for(base_env)
    tbl = _make_table(key, base, table_name)
    if not tbl:
        print(f"⚠️ Missing or failed Airtable client for {base_env}/{table_name}")
    return tbl


_fieldmap_cache: Dict[int, Dict[str, str]] = {}


def _norm(s):
    return re.sub(r"[^a-z0-9]+", "", s.strip().lower()) if isinstance(s, str) else s


def _auto_field_map(table: Any) -> Dict[str, str]:
    tid = id(table)
    cached = _fieldmap_cache.get(tid)
    if cached is not None:
        return cached
    try:
        one = table.all(max_records=1)
        keys = list(one[0].get("fields", {}).keys()) if one else []
    except Exception:
        keys = []
    amap = {_norm(k): k for k in keys}
    _fieldmap_cache[tid] = amap
    return amap


def _remap_existing_only(table: Any, payload: dict) -> dict:
    amap = _auto_field_map(table)
    if not amap:
        return dict(payload)
    out = {}
    for k, v in payload.items():
        ak = amap.get(_norm(k))
        if ak:
            out[ak] = v
    return out


def _parse_iso(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        return None


# =========================
# Numbers picking (CONTROL base)
# =========================
def _remaining_calc(f: dict) -> int:
    if isinstance(f.get("Remaining"), (int, float)):
        return int(f["Remaining"])
    sent = int(f.get("Sent Today") or 0)
    daily_cap = int(f.get("Daily Reset") or os.getenv("DAILY_LIMIT", "750"))
    return max(0, daily_cap - sent)


def _market_match(f: dict, market: Optional[str]) -> bool:
    if not market:
        return True
    if f.get("Market") == market:
        return True
    ms = f.get("Markets") or []
    return isinstance(ms, list) and (market in ms)


def _number_is_paused(f: dict) -> bool:
    status = str(f.get("Status") or "").strip().lower()
    return status in {"paused", "hold", "disabled"}


def _bump_number_counters(numbers_tbl: Any, rec_id: str, f: dict):
    try:
        patch = {"Last Used": utcnow().isoformat()}
        patch["Sent Today"] = int(f.get("Sent Today") or 0) + 1
        if f.get("Remaining") is not None:
            try:
                rem = int(f.get("Remaining") or 0)
            except Exception:
                rem = 0
            patch["Remaining"] = max(0, rem - 1)
        numbers_tbl.update(rec_id, _remap_existing_only(numbers_tbl, patch))
    except Exception:
        traceback.print_exc()


@dataclass
class NumberCandidate:
    record_id: str
    did: str
    fields: dict
    remaining: int
    last_used: datetime

    def sort_key(self) -> Tuple[int, datetime, str]:
        """Return a tuple matching the legacy prioritisation order."""
        return (-self.remaining, self.last_used, self.record_id)


class NumberPicker:
    """Helper that fetches and reuses Numbers rows within a batch run."""

    def __init__(self):
        self.table = get_table(CONTROL_BASE_ENV, NUMBERS_TABLE_NAME)
        self._loaded = False
        self._candidates: List[NumberCandidate] = []

    def _load(self) -> None:
        if self._loaded:
            return
        self._loaded = True
        if not self.table:
            return
        try:
            rows = self.table.all()
        except Exception:
            traceback.print_exc()
            return

        for r in rows:
            f = r.get("fields", {})
            if not f or not f.get("Active", True):
                continue
            if _number_is_paused(f):
                continue
            did = f.get("Number") or f.get("Friendly Name")
            if not did:
                continue
            remaining = _remaining_calc(f)
            if remaining <= 0:
                continue
            last_used = _parse_iso(f.get("Last Used")) or datetime(1970, 1, 1, tzinfo=timezone.utc)
            self._candidates.append(NumberCandidate(r["id"], str(did), dict(f), remaining, last_used))
        self._candidates.sort(key=lambda c: c.sort_key())

    def _refresh_candidate(self, cand: NumberCandidate) -> None:
        if not self.table:
            return
        try:
            latest = self.table.get(cand.record_id)
        except Exception:
            traceback.print_exc()
            return
        if not isinstance(latest, dict):
            return
        fields = dict(latest.get("fields") or {})
        if not fields:
            cand.fields = {}
            cand.remaining = 0
            return
        cand.fields = fields
        cand.did = str(fields.get("Number") or fields.get("Friendly Name") or cand.did)
        cand.remaining = _remaining_calc(fields)
        cand.last_used = _parse_iso(fields.get("Last Used")) or cand.last_used

    def pick(self, market: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
        self._load()
        if not self._candidates or not self.table:
            return None, None

        while True:
            best_index: Optional[int] = None
            best_key: Optional[Tuple[int, datetime, str]] = None
            for idx, cand in enumerate(self._candidates):
                if cand.remaining <= 0:
                    continue
                if not _market_match(cand.fields, market):
                    continue
                key = cand.sort_key()
                if best_key is None or key < best_key:
                    best_key = key
                    best_index = idx

            if best_index is None:
                return None, None

            cand = self._candidates[best_index]
            self._refresh_candidate(cand)
            if cand.remaining <= 0:
                self._candidates.sort(key=lambda c: c.sort_key())
                continue
            break

        _bump_number_counters(self.table, cand.record_id, cand.fields)

        cand.last_used = utcnow()
        cand.remaining = max(0, cand.remaining - 1)
        cand.fields["Sent Today"] = int(cand.fields.get("Sent Today") or 0) + 1
        if cand.fields.get("Remaining") is not None:
            try:
                cand.fields["Remaining"] = max(0, int(cand.fields.get("Remaining") or 0) - 1)
            except Exception:
                cand.fields["Remaining"] = cand.remaining

        self._candidates.sort(key=lambda c: c.sort_key())
        return cand.did, cand.record_id


def pick_number_for_market(market: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    picker = NumberPicker()
    return picker.pick(market)


# =========================
# Phone / UI helpers
# =========================
def _digits_only(s: Any) -> Optional[str]:
    if not isinstance(s, str):
        return None
    ds = "".join(re.findall(r"\d+", s))
    return ds if len(ds) >= 10 else None


STATUS_ICON = {
    "QUEUED": "⏳",
    "READY": "⏳",
    "SENDING": "⏳",
    "SENT": "✅",
    "DELIVERED": "✅",
    "FAILED": "❌",
    "CANCELLED": "❌",
}


def _set_ui(drip_tbl: Any, rec_id: str, status: str):
    try:
        drip_tbl.update(rec_id, _remap_existing_only(drip_tbl, {"UI": STATUS_ICON.get(status, "")}))
    except Exception:
        traceback.print_exc()


# =========================
# Limiters
# =========================
class RedisLimiter:
    LUA = """
    local did_key   = KEYS[1]
    local glob_key  = KEYS[2]
    local per_limit = tonumber(ARGV[1])
    local gl_limit  = tonumber(ARGV[2])
    local ttl_ms    = tonumber(ARGV[3])

    local did_ct = tonumber(redis.call('GET', did_key) or '0')
    local gl_ct  = tonumber(redis.call('GET', glob_key) or '0')

    if did_ct >= per_limit or gl_ct >= gl_limit then
        return 0
    end

    did_ct = redis.call('INCR', did_key)
    if did_ct == 1 then redis.call('PEXPIRE', did_key, ttl_ms) end

    gl_ct = redis.call('INCR', glob_key)
    if gl_ct == 1 then redis.call('PEXPIRE', glob_key, ttl_ms) end

    return 1
    """

    def __init__(self, url: Optional[str], per_limit: int, global_limit: int):
        self.per = per_limit
        self.glob = global_limit
        self.enabled = bool(url and redis)
        if not self.enabled:
            self.r = None
            return
        self.r = redis.from_url(url, ssl=REDIS_TLS, decode_responses=True)
        self.script = self.r.register_script(self.LUA)

    @staticmethod
    def _min_bucket() -> str:
        return datetime.utcnow().strftime("%Y%m%d%H%M")

    @staticmethod
    def _did_key(did: str) -> str:
        did_hash = hashlib.md5(did.encode()).hexdigest()
        return f"rl:did:{RedisLimiter._min_bucket()}:{did_hash}"

    @staticmethod
    def _glob_key() -> str:
        return f"rl:glob:{RedisLimiter._min_bucket()}"

    def try_consume(self, did: str) -> bool:
        if not self.enabled:
            return True
        try:
            keys = [self._did_key(did), self._glob_key()]
            return bool(self.script(keys=keys, args=[self.per, self.glob, 65000]))
        except Exception:
            traceback.print_exc()
            return True


class UpstashRestLimiter:
    def __init__(self, base_url: Optional[str], token: Optional[str], per_limit: int, global_limit: int):
        self.base = (base_url or "").rstrip("/")
        self.tok = token
        self.per = per_limit
        self.glob = global_limit
        self.enabled = bool(self.base and self.tok and requests)

    @staticmethod
    def _min_bucket() -> str:
        return datetime.utcnow().strftime("%Y%m%d%H%M")

    @staticmethod
    def _did_key(did: str) -> str:
        did_hash = hashlib.md5(did.encode()).hexdigest()
        return f"rl:did:{UpstashRestLimiter._min_bucket()}:{did_hash}"

    @staticmethod
    def _glob_key() -> str:
        return f"rl:glob:{UpstashRestLimiter._min_bucket()}"

    def _pipeline(self, commands: List[List[str]]) -> Optional[List[Any]]:
        try:
            resp = requests.post(
                f"{self.base}/pipeline",
                json=commands,
                headers={"Authorization": f"Bearer {self.tok}"},
                timeout=3,
            )
            if resp.ok:
                return resp.json()
        except Exception:
            traceback.print_exc()
        return None

    def try_consume(self, did: str) -> bool:
        if not self.enabled:
            return True
        did_key = self._did_key(did)
        glob_key = self._glob_key()
        res = self._pipeline([["GET", did_key], ["GET", glob_key]]) or []
        try:
            did_ct = int(res[0][1]) if (len(res) > 0 and res[0][1] is not None) else 0
            glob_ct = int(res[1][1]) if (len(res) > 1 and res[1][1] is not None) else 0
        except Exception:
            did_ct, glob_ct = 0, 0
        if did_ct >= self.per or glob_ct >= self.glob:
            return False
        self._pipeline(
            [
                ["INCR", did_key],
                ["EXPIRE", did_key, "60"],
                ["INCR", glob_key],
                ["EXPIRE", glob_key, "60"],
            ]
        )
        return True


class LocalLimiter:
    def __init__(self, per_limit: int, global_limit: int):
        self.per = per_limit
        self.glob = global_limit
        self.per_counts: Dict[str, Tuple[int, int]] = {}
        self.glob_count: Tuple[int, int] = (0, 0)

    def _bucket(self) -> int:
        return int(utcnow().timestamp() // 60)

    def try_consume(self, did: str) -> bool:
        minute = self._bucket()
        g_min, g_ct = self.glob_count
        if g_min != minute:
            g_ct = 0
        if g_ct >= self.glob:
            return False
        d_min, d_ct = self.per_counts.get(did, (minute, 0))
        if d_min != minute:
            d_ct = 0
        if d_ct >= self.per:
            return False
        self.glob_count = (minute, g_ct + 1)
        self.per_counts[did] = (minute, d_ct + 1)
        return True


def build_limiter() -> object:
    if REDIS_URL and redis:
        return RedisLimiter(REDIS_URL, RATE_PER_NUMBER_PER_MIN, GLOBAL_RATE_PER_MIN)
    if UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN and requests:
        return UpstashRestLimiter(UPSTASH_REDIS_REST_URL, UPSTASH_REDIS_REST_TOKEN, RATE_PER_NUMBER_PER_MIN, GLOBAL_RATE_PER_MIN)
    return LocalLimiter(RATE_PER_NUMBER_PER_MIN, GLOBAL_RATE_PER_MIN)


# =========================
# Main: SEND from Drip Queue
# =========================
def send_batch(campaign_id: str | None = None, limit: int = 500):
    """
    Drains Drip Queue rows that are due (QUEUED/READY/SENDING && next_send_date <= now),
    fills missing `from_number` via Numbers, enforces minute limiter,
    updates UI, logs KPIs. Safe across pyairtable v1/v2 and missing deps.
    """
    drip = get_table(LEADS_BASE_ENV, DRIP_TABLE_NAME)
    if not drip:
        return {"ok": False, "error": "Missing Drip Queue table", "total_sent": 0}

    if is_quiet_hours_local():
        return {"ok": False, "quiet_hours": True, "note": "Quiet hours (Central) — sending paused.", "total_sent": 0}

    try:
        rows = drip.all()
    except Exception:
        traceback.print_exc()
        return {"ok": False, "error": "Failed to read Drip Queue", "total_sent": 0}

    now = utcnow()
    due: List[dict] = []
    for r in rows:
        f = r.get("fields", {})
        if campaign_id:
            cids = f.get("Campaign") or []
            if not isinstance(cids, list):
                cids = [cids]
            if campaign_id not in cids:
                continue
        status = str(f.get("status") or f.get("Status") or "")
        if status not in ("QUEUED", "READY", "SENDING"):
            continue
        when = _parse_iso_maybe_ct(f.get("next_send_date") or f.get("Next Send Date") or f.get("scheduled_at")) or now
        if when <= now:
            due.append(r)

    if not due:
        return {"ok": True, "total_sent": 0, "note": "No due messages"}

    due.sort(
        key=lambda r: _parse_iso_maybe_ct(
            r.get("fields", {}).get("next_send_date")
            or r.get("fields", {}).get("Next Send Date")
            or r.get("fields", {}).get("scheduled_at")
        )
        or now
    )
    due = due[:limit]

    limiter = build_limiter()
    number_picker = NumberPicker()
    total_sent = 0
    total_failed = 0
    errors: List[str] = []

    for r in due:
        rid = r["id"]
        f = r.get("fields", {})
        raw_phone = f.get("phone") or f.get("Phone")
        phone = _digits_only(raw_phone) or raw_phone
        if not phone:
            total_failed += 1
            err_msg = "missing_phone"
            errors.append(err_msg)
            try:
                drip.update(
                    rid,
                    _remap_existing_only(
                        drip,
                        {
                            "status": "FAILED",
                            "last_error": err_msg,
                        },
                    ),
                )
                _set_ui(drip, rid, "FAILED")
            except Exception:
                traceback.print_exc()
            continue

        did = f.get("from_number") or f.get("From Number")
        market = f.get("Market")

        # Backfill DID
        if not did and AUTO_BACKFILL_FROM_NUMBER:
            did, _num_id = number_picker.pick(market)
            if did:
                try:
                    drip.update(rid, _remap_existing_only(drip, {"from_number": did, "From Number": did}))
                except Exception:
                    traceback.print_exc()

        if not did:
            errors.append(f"No available number for {phone} (market={market})")
            # push out a bit so we don't spin on it every loop
            try:
                new_time = (utcnow() + timedelta(seconds=NO_NUMBER_REQUEUE_SECONDS)).isoformat()
                drip.update(rid, _remap_existing_only(drip, {"next_send_date": new_time}))
            except Exception:
                traceback.print_exc()
            continue

        # Rate limit
        if not limiter.try_consume(did):
            try:
                new_time = (utcnow() + timedelta(seconds=RATE_LIMIT_REQUEUE_SECONDS)).isoformat()
                drip.update(rid, _remap_existing_only(drip, {"next_send_date": new_time}))
            except Exception:
                traceback.print_exc()
            continue

        # Mark SENDING + UI
        try:
            drip.update(rid, _remap_existing_only(drip, {"status": "SENDING"}))
            _set_ui(drip, rid, "SENDING")
        except Exception:
            traceback.print_exc()

        # Compose + send
        body = f.get("message_preview") or f.get("Message Preview") or f.get("message") or ""
        property_id = f.get("Property ID")

        ok = False
        err_msg = None
        try:
            result = None
            if MessageProcessor:
                try:
                    # prefer signature that supports from_number
                    result = MessageProcessor.send(
                        phone=phone,
                        body=body,
                        from_number=did,
                        property_id=property_id,
                        direction="OUT",
                    )
                except TypeError:
                    # fallback to older signature
                    result = MessageProcessor.send(
                        phone=phone,
                        body=body,
                        property_id=property_id,
                        direction="OUT",
                    )
                ok = (result or {}).get("status") == "sent"
                if not ok:
                    err_msg = (result or {}).get("error", "send_failed")
            else:
                ok = True
        except Exception as e:
            ok = False
            err_msg = str(e)

        # Update status + UI
        if ok:
            total_sent += 1
            try:
                drip.update(
                    rid,
                    _remap_existing_only(
                        drip,
                        {
                            "status": "SENT",
                            "sent_at": utcnow().isoformat(),
                        },
                    ),
                )
                _set_ui(drip, rid, "SENT")
            except Exception:
                traceback.print_exc()
        else:
            total_failed += 1
            if err_msg:
                errors.append(err_msg)
            try:
                drip.update(
                    rid,
                    _remap_existing_only(
                        drip,
                        {
                            "status": "FAILED",
                            "last_error": (err_msg or "send_failed")[:500],
                        },
                    ),
                )
                _set_ui(drip, rid, "FAILED")
            except Exception:
                traceback.print_exc()

        if SLEEP_BETWEEN_SENDS_SEC > 0:
            time.sleep(SLEEP_BETWEEN_SENDS_SEC)

    # KPIs (best effort)
    runs = get_table(PERF_BASE_ENV, "Runs/Logs")
    kpis = get_table(PERF_BASE_ENV, "KPIs")
    now_iso = utcnow().isoformat()
    if runs:
        try:
            runs.create(
                _remap_existing_only(
                    runs,
                    {
                        "Type": "OUTBOUND_SEND",
                        "Processed": float(total_sent),
                        "Breakdown": f"sent={total_sent}, failed={total_failed}",
                        "Timestamp": now_iso,
                    },
                )
            )
        except Exception:
            traceback.print_exc()
    if kpis and (total_sent or total_failed):
        try:
            if total_sent:
                kpis.create(
                    _remap_existing_only(
                        kpis,
                        {
                            "Campaign": "ALL",
                            "Metric": "OUTBOUND_SENT",
                            "Value": float(total_sent),
                            "Date": utcnow().date().isoformat(),
                        },
                    )
                )
            if total_failed:
                kpis.create(
                    _remap_existing_only(
                        kpis,
                        {
                            "Campaign": "ALL",
                            "Metric": "OUTBOUND_FAILED",
                            "Value": float(total_failed),
                            "Date": utcnow().date().isoformat(),
                        },
                    )
                )
        except Exception:
            traceback.print_exc()

    return {
        "ok": True,
        "total_sent": total_sent,
        "total_failed": total_failed,
        "skipped_rate_limit": max(0, len(due) - (total_sent + total_failed)),
        "quiet_hours": False,
        "errors": errors,
    }


# =========================
# Back-compat shim for tests
# =========================
def reset_daily_quotas():
    """
    Legacy hook referenced by tests; real implementation lives elsewhere.
    Kept as a harmless stub so tests can monkeypatch it.
    """
    return {"ok": True, "note": "noop (stubbed in tests)"}
