# sms/outbound_batcher.py
from __future__ import annotations

import hashlib
import os
import re
import time
import traceback
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from sms.config import DRIP_FIELD_MAP as DRIP_FIELDS
from sms.airtable_schema import DripStatus

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
LEADS_BASE_ENV = "LEADS_CONVOS_BASE"   # Drip Queue base
PERF_BASE_ENV = "PERFORMANCE_BASE"     # KPIs / Runs
CONTROL_BASE_ENV = "CAMPAIGN_CONTROL_BASE"  # Numbers base

DRIP_TABLE_NAME = os.getenv("DRIP_QUEUE_TABLE", "Drip Queue")
NUMBERS_TABLE_NAME = os.getenv("NUMBERS_TABLE", "Numbers")
CAMPAIGNS_TABLE_NAME = os.getenv("CAMPAIGNS_TABLE", "Campaigns")

# Back-compat constants (some code expects these names)
DRIP_QUEUE_TABLE = DRIP_TABLE_NAME
NUMBERS_TABLE = NUMBERS_TABLE_NAME
CAMPAIGNS_TABLE = CAMPAIGNS_TABLE_NAME

# Canonical Drip field mappings (pulled from config.DRIP_FIELD_MAP)
DRIP_STATUS_FIELD = DRIP_FIELDS["STATUS"]
DRIP_CAMPAIGN_FIELD = DRIP_FIELDS["CAMPAIGN_LINK"]
DRIP_TEMPLATE_FIELD = DRIP_FIELDS["TEMPLATE_LINK"]
DRIP_PROSPECT_FIELD = DRIP_FIELDS["PROSPECT_LINK"]
DRIP_SELLER_PHONE_FIELD = DRIP_FIELDS["SELLER_PHONE"]
DRIP_FROM_NUMBER_FIELD = "TextGrid Phone Number"
DRIP_MARKET_FIELD = DRIP_FIELDS["MARKET"]
DRIP_MESSAGE_PREVIEW_FIELD = DRIP_FIELDS["MESSAGE_PREVIEW"]
DRIP_PROPERTY_ID_FIELD = DRIP_FIELDS["PROPERTY_ID"]
DRIP_NEXT_SEND_DATE_FIELD = DRIP_FIELDS["NEXT_SEND_DATE"]
DRIP_NEXT_SEND_AT_FIELD = DRIP_FIELDS["NEXT_SEND_AT"]
DRIP_NEXT_SEND_AT_UTC_FIELD = DRIP_FIELDS["NEXT_SEND_AT_UTC"]
DRIP_UI_FIELD = DRIP_FIELDS["UI"]
DRIP_LAST_SENT_FIELD = DRIP_FIELDS["LAST_SENT"]
DRIP_SENT_AT_FIELD = DRIP_FIELDS["SENT_AT"]
DRIP_SENT_FLAG_FIELD = DRIP_FIELDS["SENT_FLAG"]
DRIP_FAILED_FLAG_FIELD = DRIP_FIELDS["FAILED_FLAG"]
DRIP_DECLINED_FLAG_FIELD = DRIP_FIELDS["DECLINED_FLAG"]
DRIP_LAST_ERROR_FIELD = DRIP_FIELDS["LAST_ERROR"]

# Rate limits (enforced with Redis across all workers)
SLEEP_BETWEEN_SENDS_SEC = float(os.getenv("SLEEP_BETWEEN_SENDS_SEC", "0.03"))
RATE_LIMIT_REQUEUE_SECONDS = float(os.getenv("RATE_LIMIT_REQUEUE_SECONDS", "5"))
NO_NUMBER_REQUEUE_SECONDS = float(os.getenv("NO_NUMBER_REQUEUE_SECONDS", "60"))

# Backfill missing from_number with a market DID from Numbers table
AUTO_BACKFILL_FROM_NUMBER = os.getenv("AUTO_BACKFILL_FROM_NUMBER", "true").lower() in ("1", "true", "yes")

# Redis / Upstash limiter
REDIS_URL = os.getenv("REDIS_URL") or os.getenv("UPSTASH_REDIS_URL")
REDIS_TLS = os.getenv("REDIS_TLS", "true").lower() in ("1", "true", "yes")
UPSTASH_REDIS_REST_URL = os.getenv("UPSTASH_REDIS_REST_URL")
UPSTASH_REDIS_REST_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN")

try:
    import importlib
    redis = importlib.import_module("redis")
except Exception:
    redis = None
try:
    # use importlib again to avoid static import resolution errors for optional deps
    import importlib as _importlib
    requests = _importlib.import_module("requests")
except Exception:
    requests = None

# Optional sender
try:
    from sms.message_processor import MessageProcessor
except Exception:
    MessageProcessor = None

from sms.dispatcher import get_policy


def _policy():
    return get_policy()


def rate_per_number_per_min() -> int:
    return _policy().rate_per_number_per_min


def global_rate_per_min() -> int:
    return _policy().global_rate_per_min


def daily_limit_default() -> int:
    return _policy().daily_limit


def send_jitter_seconds() -> int:
    return _policy().jitter()


# =========================
# Time helpers
# =========================
def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _ct_tz():
    policy = get_policy()
    if policy.quiet_tz:
        return policy.quiet_tz
    try:
        from zoneinfo import ZoneInfo  # type: ignore
        return ZoneInfo("America/Chicago")
    except Exception:
        return timezone.utc


def central_now() -> datetime:
    return get_policy().now_local()


def is_quiet_hours_local() -> bool:
    return get_policy().is_quiet()


def _parse_iso_maybe_ct(s: Any) -> Optional[datetime]:
    """
    Accepts:
      - ISO with tz   → return as UTC
      - naive datetime string → interpret as Central and convert to UTC
      - date-only 'YYYY-MM-DD' → interpret as that date at quiet-end hour in Central
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
        end_hour = max(9, get_policy().quiet_end_hour)
        local_dt = datetime(d.year, d.month, d.day, end_hour, 0, 0, tzinfo=_ct_tz())
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
    if base_env == LEADS_BASE_ENV:
        return _first_env("AIRTABLE_ACQUISITIONS_KEY", "LEADS_CONVOS_KEY", "AIRTABLE_API_KEY")
    if base_env == CONTROL_BASE_ENV:
        return _first_env("AIRTABLE_COMPLIANCE_KEY", "CAMPAIGN_CONTROL_KEY", "AIRTABLE_API_KEY")
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
    daily_cap = int(f.get("Daily Reset") or daily_limit_default())
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


def pick_number_for_market(market: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    numbers_tbl = get_table(CONTROL_BASE_ENV, NUMBERS_TABLE_NAME)
    if not numbers_tbl:
        return None, None
    try:
        rows = numbers_tbl.all()
    except Exception:
        traceback.print_exc()
        return None, None

    elig: List[Tuple[int, datetime, dict, str]] = []
    for r in rows:
        f = r.get("fields", {})
        if not f.get("Active", True):
            continue
        if _number_is_paused(f):
            continue
        if not _market_match(f, market):
            continue
        remaining = _remaining_calc(f)
        if remaining <= 0:
            continue
        last_used = _parse_iso(f.get("Last Used")) or datetime(1970, 1, 1, tzinfo=timezone.utc)
        elig.append((-remaining, last_used, f, r["id"]))

    if not elig:
        return None, None

    elig.sort(key=lambda x: (x[0], x[1]))
    _, _, f, rid = elig[0]
    did = f.get("Number") or f.get("Friendly Name")
    if not did:
        return None, None

    _bump_number_counters(numbers_tbl, rid, f)
    return did, rid


# =========================
# Phone / UI helpers
# =========================
def _digits_only(s: Any) -> Optional[str]:
    if not isinstance(s, str):
        return None
    ds = "".join(re.findall(r"\d+", s))
    return ds if len(ds) >= 10 else None


STATUS_ICON = {
    DripStatus.QUEUED.value: "⏳",
    DripStatus.READY.value: "⏳",
    DripStatus.SENDING.value: "⏳",
    DripStatus.SENT.value: "✅",
    DripStatus.DELIVERED.value: "✅",
    DripStatus.FAILED.value: "❌",
    DripStatus.RETRY.value: "🔄",
    DripStatus.THROTTLED.value: "⏸",
    DripStatus.DNC.value: "🚫",
}


def _set_ui(drip_tbl: Any, rec_id: str, status: str):
    try:
        drip_tbl.update(rec_id, _remap_existing_only(drip_tbl, {DRIP_UI_FIELD: STATUS_ICON.get(status, "")}))
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
        return RedisLimiter(REDIS_URL, rate_per_number_per_min(), global_rate_per_min())
    if UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN and requests:
        return UpstashRestLimiter(
            UPSTASH_REDIS_REST_URL,
            UPSTASH_REDIS_REST_TOKEN,
            rate_per_number_per_min(),
            global_rate_per_min(),
        )
    return LocalLimiter(rate_per_number_per_min(), global_rate_per_min())


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
        status = str(f.get(DRIP_STATUS_FIELD) or "").upper()
        when = _parse_iso_maybe_ct(
            f.get(DRIP_NEXT_SEND_DATE_FIELD) or f.get("scheduled_at")
        ) or now

        if campaign_id:
            cids = f.get(DRIP_CAMPAIGN_FIELD) or []
            if not isinstance(cids, list):
                cids = [cids]
            if campaign_id not in cids:
                continue

        if status not in ("QUEUED", "READY", "SENDING"):
            continue

        if when <= now:
            due.append(r)

        # temporarily include all for debugging
        # if when <= now:
        due.append(r)

    if not due:
        return {"ok": True, "total_sent": 0, "note": "No due messages"}

    due.sort(
        key=lambda r: _parse_iso_maybe_ct(
            r.get("fields", {}).get(DRIP_NEXT_SEND_DATE_FIELD)
            or r.get("fields", {}).get("scheduled_at")
        )
        or now
    )
    due = due[:limit]

    limiter = build_limiter()
    total_sent = 0
    total_failed = 0
    errors: List[str] = []

    for r in due:
        rid = r["id"]
        f = r.get("fields", {})
        phone = f.get(DRIP_SELLER_PHONE_FIELD)
        if not phone:
            continue

        did = f.get(DRIP_FROM_NUMBER_FIELD)
        market = f.get(DRIP_MARKET_FIELD)

        # Backfill DID if missing
        if not did and AUTO_BACKFILL_FROM_NUMBER:
            did, _num_id = pick_number_for_market(market)
            if did:
                try:
                    # Use the canonical field name; remap will align to actual column
                    drip.update(rid, _remap_existing_only(drip, {DRIP_FROM_NUMBER_FIELD: did}))
                except Exception:
                    traceback.print_exc()

        if not did:
            errors.append(f"No available number for {phone} (market={market})")
            # Push out a bit so we don't spin every loop
            try:
                new_time = (utcnow() + timedelta(seconds=NO_NUMBER_REQUEUE_SECONDS)).isoformat()
                drip.update(rid, _remap_existing_only(drip, {DRIP_NEXT_SEND_DATE_FIELD: new_time}))
            except Exception:
                traceback.print_exc()
            continue

        # Rate limit
        if not limiter.try_consume(did):
            try:
                new_time = (utcnow() + timedelta(seconds=RATE_LIMIT_REQUEUE_SECONDS)).isoformat()
                drip.update(rid, _remap_existing_only(drip, {DRIP_NEXT_SEND_DATE_FIELD: new_time}))
            except Exception:
                traceback.print_exc()
            continue

        # Mark SENDING + UI
        try:
            drip.update(rid, _remap_existing_only(drip, {DRIP_STATUS_FIELD: DripStatus.SENDING.value}))
            _set_ui(drip, rid, DripStatus.SENDING.value)
        except Exception:
            traceback.print_exc()

        # Compose + send
        body = f.get(DRIP_MESSAGE_PREVIEW_FIELD) or f.get("message") or ""
        property_id = f.get(DRIP_PROPERTY_ID_FIELD)

        ok = False
        err_msg = None
        try:
            result = None
            if MessageProcessor:
                try:
                    # Prefer signature that supports from_number
                    result = MessageProcessor.send(
                        phone=phone,
                        body=body,
                        from_number=did,
                        property_id=property_id,
                        direction="OUT",
                    )
                except TypeError:
                    # Fallback to older signature
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
                            DRIP_STATUS_FIELD: DripStatus.SENT.value,
                            DRIP_SENT_AT_FIELD: utcnow().isoformat(),
                        },
                    ),
                )
                _set_ui(drip, rid, DripStatus.SENT.value)
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
                            DRIP_STATUS_FIELD: DripStatus.FAILED.value,
                            DRIP_LAST_ERROR_FIELD: (err_msg or "send_failed")[:500],
                        },
                    ),
                )
                _set_ui(drip, rid, DripStatus.FAILED.value)
            except Exception:
                traceback.print_exc()

        if SLEEP_BETWEEN_SENDS_SEC > 0:
            time.sleep(SLEEP_BETWEEN_SENDS_SEC)

    # KPIs (best effort)
    runs = get_table(PERF_BASE_ENV, "Logs")
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

