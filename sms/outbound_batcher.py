# sms/outbound_batcher.py
from __future__ import annotations

import os, re, time, traceback, hashlib
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, Tuple, List

from pyairtable import Table

# =========================
# ENV / CONFIG
# =========================
LEADS_BASE_ENV          = "LEADS_CONVOS_BASE"          # Drip Queue base
PERF_BASE_ENV           = "PERFORMANCE_BASE"           # KPIs / Runs
CONTROL_BASE_ENV        = "CAMPAIGN_CONTROL_BASE"      # Numbers base

DRIP_TABLE_NAME         = os.getenv("DRIP_QUEUE_TABLE", "Drip Queue")
NUMBERS_TABLE_NAME      = os.getenv("NUMBERS_TABLE", "Numbers")
CAMPAIGNS_TABLE_NAME    = os.getenv("CAMPAIGNS_TABLE", "Campaigns")

# Rate limits (enforced with Redis across all workers)
RATE_PER_NUMBER_PER_MIN = int(os.getenv("RATE_PER_NUMBER_PER_MIN", "20"))   # per DID / minute
GLOBAL_RATE_PER_MIN     = int(os.getenv("GLOBAL_RATE_PER_MIN", "5000"))     # optional global cap
SLEEP_BETWEEN_SENDS_SEC = float(os.getenv("SLEEP_BETWEEN_SENDS_SEC", "0.03"))

# When a row is rate-limited, nudge its next_send_date forward slightly so we don’t spin on it
RATE_LIMIT_REQUEUE_SECONDS = float(os.getenv("RATE_LIMIT_REQUEUE_SECONDS", "5"))

# Quiet hours (America/Chicago): block actual sending 9pm–9am CT
QUIET_HOURS_ENFORCED    = os.getenv("QUIET_HOURS_ENFORCED", "true").lower() in ("1","true","yes")
QUIET_START_HOUR_LOCAL  = int(os.getenv("QUIET_START_HOUR_LOCAL", "21"))  # 21:00
QUIET_END_HOUR_LOCAL    = int(os.getenv("QUIET_END_HOUR_LOCAL", "9"))     # 09:00

# Backfill missing from_number with a market DID from Numbers table
AUTO_BACKFILL_FROM_NUMBER = os.getenv("AUTO_BACKFILL_FROM_NUMBER", "true").lower() in ("1","true","yes")

# Redis (for cross-process limiter)
REDIS_URL  = os.getenv("REDIS_URL") or os.getenv("UPSTASH_REDIS_URL")  # e.g. rediss://default:token@host:6379
REDIS_TLS  = os.getenv("REDIS_TLS", "true").lower() in ("1","true","yes")

# Upstash REST fallback (works without TCP, not fully atomic but good enough if single worker)
UPSTASH_REDIS_REST_URL   = os.getenv("UPSTASH_REDIS_REST_URL")  # e.g. https://xxxx.upstash.io
UPSTASH_REDIS_REST_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN")

try:
    import redis  # TCP client (best)
except Exception:
    redis = None

try:
    import requests  # for Upstash REST fallback
except Exception:
    requests = None

# Optional sender (your low-level dispatcher)
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

def central_now() -> datetime:
    if ZoneInfo:
        return datetime.now(ZoneInfo("America/Chicago"))
    return datetime.now(timezone.utc)  # fallback

def is_quiet_hours_local() -> bool:
    if not QUIET_HOURS_ENFORCED:
        return False
    h = central_now().hour
    return (h >= QUIET_START_HOUR_LOCAL) or (h < QUIET_END_HOUR_LOCAL)


# =========================
# Airtable helpers
# =========================
def _api_key_for(base_env: str) -> Optional[str]:
    if base_env == PERF_BASE_ENV:
        return os.getenv("AIRTABLE_REPORTING_KEY") or os.getenv("AIRTABLE_API_KEY")
    return os.getenv("AIRTABLE_API_KEY")

def get_table(base_env: str, table_name: str) -> Table | None:
    key  = _api_key_for(base_env)
    base = os.getenv(base_env)
    if not (key and base):
        print(f"⚠️ Missing Airtable config for {base_env}/{table_name}")
        return None
    try:
        return Table(key, base, table_name)
    except Exception:
        traceback.print_exc()
        return None

def _norm(s): return re.sub(r"[^a-z0-9]+","",s.strip().lower()) if isinstance(s,str) else s

def _auto_field_map(table: Table):
    try:
        one = table.all(max_records=1)
        keys = list(one[0].get("fields", {}).keys()) if one else []
    except Exception:
        keys = []
    return {_norm(k): k for k in keys}

def _remap_existing_only(table: Table, payload: dict) -> dict:
    amap = _auto_field_map(table)
    if not amap:
        # optimistic send if we couldn't probe fields
        return dict(payload)
    out = {}
    for k,v in payload.items():
        ak = amap.get(_norm(k))
        if ak: out[ak] = v
    return out

def _parse_iso(s):
    if not s: return None
    try: return datetime.fromisoformat(str(s).replace("Z","+00:00"))
    except Exception: return None


# =========================
# Numbers picking (CONTROL base)
# =========================
def _remaining_calc(f: dict) -> int:
    if isinstance(f.get("Remaining"), (int,float)):
        return int(f["Remaining"])
    sent = int(f.get("Sent Today") or 0)
    daily_cap = int(f.get("Daily Reset") or os.getenv("DAILY_LIMIT", "750"))
    return max(0, daily_cap - sent)

def _market_match(f: dict, market: Optional[str]) -> bool:
    if not market: return True
    if f.get("Market") == market: return True
    ms = f.get("Markets") or []
    return isinstance(ms, list) and (market in ms)

def _bump_number_counters(numbers_tbl: Table, rec_id: str, f: dict):
    try:
        patch = {"Last Used": utcnow().isoformat()}
        patch["Sent Today"] = int(f.get("Sent Today") or 0) + 1
        if f.get("Remaining") is not None:
            patch["Remaining"] = max(0, int(f.get("Remaining") or 0) - 1)
        numbers_tbl.update(rec_id, _remap_existing_only(numbers_tbl, patch))
    except Exception:
        traceback.print_exc()

def pick_number_for_market(market: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (from_number_string, numbers_record_id).
    Uses CONTROL BASE / Numbers table; DID stored in field 'Number' (preferred) or 'Friendly Name'.
    """
    numbers_tbl = get_table(CONTROL_BASE_ENV, NUMBERS_TABLE_NAME)
    if not numbers_tbl: return None, None
    try:
        rows = numbers_tbl.all()
    except Exception:
        traceback.print_exc()
        return None, None

    elig: List[Tuple[int, datetime, dict, str]] = []
    for r in rows:
        f = r.get("fields", {})
        if not f.get("Active", True): continue
        if not _market_match(f, market): continue
        remaining = _remaining_calc(f)
        if remaining <= 0: continue
        last_used = _parse_iso(f.get("Last Used")) or datetime(1970,1,1,tzinfo=timezone.utc)
        # sort key: highest remaining, least-recently used
        elig.append((-remaining, last_used, f, r["id"]))

    if not elig:
        return None, None

    elig.sort(key=lambda x: (x[0], x[1]))
    _, _, f, rid = elig[0]
    did = f.get("Number") or f.get("Friendly Name")
    if not did:
        return None, None

    # optimistic bump (soft) to reduce contention between parallel workers
    _bump_number_counters(numbers_tbl, rid, f)
    return did, rid


# =========================
# Phone / UI helpers
# =========================
def _digits_only(s: Any) -> Optional[str]:
    if not isinstance(s, str): return None
    ds = "".join(re.findall(r"\d+", s))
    return ds if len(ds) >= 10 else None

STATUS_ICON = {"QUEUED":"⏳","READY":"⏳","SENDING":"⏳","SENT":"✅","DELIVERED":"✅","FAILED":"❌","CANCELLED":"❌"}
def _set_ui(drip_tbl: Table, rec_id: str, status: str):
    try:
        drip_tbl.update(rec_id, _remap_existing_only(drip_tbl, {"UI": STATUS_ICON.get(status,"")}))
    except Exception:
        traceback.print_exc()


# =========================
# Redis-backed minute limiter (cross-process)
# =========================
class RedisLimiter:
    """
    Atomically checks + increments both per-DID and global minute counters.
    Uses one Lua script so it's safe with multiple workers.
    """
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

    def __init__(self, url: Optional[str], per_limit:int, global_limit:int):
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
    """
    Best-effort limiter using Upstash REST (no Lua). Not fully atomic across both keys,
    but good enough if you typically run one worker. Requires requests.
    """
    def __init__(self, base_url: Optional[str], token: Optional[str], per_limit:int, global_limit:int):
        self.base = (base_url or "").rstrip("/")
        self.tok  = token
        self.per  = per_limit
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
        did_key  = self._did_key(did)
        glob_key = self._glob_key()
        # INCR + EXPIRE each; order reduces overage risk a bit
        cmds = [
            ["GET", did_key],
            ["GET", glob_key],
        ]
        res = self._pipeline(cmds) or []
        try:
            did_ct  = int(res[0][1]) if (len(res) > 0 and res[0][1] is not None) else 0
            glob_ct = int(res[1][1]) if (len(res) > 1 and res[1][1] is not None) else 0
        except Exception:
            did_ct, glob_ct = 0, 0

        if did_ct >= self.per or glob_ct >= self.glob:
            return False

        self._pipeline([
            ["INCR", did_key], ["EXPIRE", did_key, "60"],
            ["INCR", glob_key], ["EXPIRE", glob_key, "60"],
        ])
        return True


# fallback in-process limiter if redis is not available
class LocalLimiter:
    def __init__(self, per_limit:int, global_limit:int):
        self.per = per_limit
        self.glob = global_limit
        self.per_counts: Dict[str, Tuple[int,int]] = {}
        self.glob_count: Tuple[int,int] = (0,0)
    def _bucket(self) -> int: return int(utcnow().timestamp() // 60)
    def try_consume(self, did:str) -> bool:
        minute = self._bucket()
        g_min, g_ct = self.glob_count
        if g_min != minute: g_ct = 0
        if g_ct >= self.glob: return False
        d_min, d_ct = self.per_counts.get(did, (minute, 0))
        if d_min != minute: d_ct = 0
        if d_ct >= self.per: return False
        # consume
        self.glob_count = (minute, g_ct + 1)
        self.per_counts[did] = (minute, d_ct + 1)
        return True

def build_limiter() -> object:
    # Best → Redis TCP (Lua) ; else Upstash REST ; else Local
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
    Drains Drip Queue rows that are due (status in QUEUED/READY and next_send_date <= now),
    fills missing `from_number` using Numbers (CONTROL base, field 'Number'),
    enforces quiet hours and a cross-process Redis minute limiter,
    updates UI (⏳ while sending; ✅/❌ on completion).
    """
    drip = get_table(LEADS_BASE_ENV, DRIP_TABLE_NAME)
    if not drip:
        return {"ok": False, "error": "Missing Drip Queue table", "total_sent": 0}

    # Block sends during quiet hours (queueing happens in campaign runner)
    if is_quiet_hours_local():
        return {"ok": False, "quiet_hours": True, "note": "Quiet hours (Central) — sending paused.", "total_sent": 0}

    # Load candidates and filter in Python (no fragile formulas)
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
            if not isinstance(cids, list): cids = [cids]
            if campaign_id not in cids:
                continue
        status = str(f.get("status") or f.get("Status") or "")
        if status not in ("QUEUED","READY"):
            continue
        when = _parse_iso(f.get("next_send_date") or f.get("Next Send Date") or now.isoformat())
        if not when or when > now:
            continue
        due.append(r)

    if not due:
        return {"ok": True, "total_sent": 0, "note": "No due messages"}

    # Oldest first, then truncate by limit
    due.sort(key=lambda r: _parse_iso(r.get("fields", {}).get("next_send_date") or r.get("fields", {}).get("Next Send Date") or now.isoformat()))
    due = due[:limit]

    limiter = build_limiter()
    total_sent = 0
    total_failed = 0
    errors: List[str] = []

    for r in due:
        rid = r["id"]
        f = r.get("fields", {})
        phone = f.get("phone") or f.get("Phone")
        if not phone:
            continue

        did = f.get("from_number") or f.get("From Number")
        market = f.get("Market")

        # Backfill from_number if missing
        if not did and AUTO_BACKFILL_FROM_NUMBER:
            did, _num_id = pick_number_for_market(market)
            if did:
                try:
                    drip.update(rid, _remap_existing_only(drip, {"from_number": did}))
                except Exception:
                    traceback.print_exc()

        if not did:
            errors.append(f"No available number for {phone} (market={market})")
            continue

        # Rate limit (cross-process safe)
        if not limiter.try_consume(did):
            # push a little so we don't spin on this same row every loop
            try:
                new_time = (utcnow() + timedelta(seconds=RATE_LIMIT_REQUEUE_SECONDS)).isoformat()
                drip.update(rid, _remap_existing_only(drip, {"next_send_date": new_time}))
            except Exception:
                traceback.print_exc()
            continue

        # Mark SENDING + ⏳
        try:
            drip.update(rid, _remap_existing_only(drip, {"status":"SENDING"}))
            _set_ui(drip, rid, "SENDING")
        except Exception:
            traceback.print_exc()

        # Compose send payload
        body = f.get("message_preview") or f.get("Message Preview") or f.get("message") or ""
        property_id = f.get("Property ID")

        # Send
        ok = False
        err_msg = None
        try:
            if MessageProcessor:
                result = MessageProcessor.send(
                    phone=phone,
                    body=body,
                    from_number=did,
                    property_id=property_id,
                    direction="OUT",
                )
                ok = (result or {}).get("status") == "sent"
                if not ok:
                    err_msg = (result or {}).get("error", "send_failed")
            else:
                ok = True  # plumbing test
        except Exception as e:
            ok = False
            err_msg = str(e)

        # Update status + UI
        if ok:
            total_sent += 1
            try:
                drip.update(rid, _remap_existing_only(drip, {
                    "status":"SENT",
                    "sent_at": utcnow().isoformat(),
                }))
                _set_ui(drip, rid, "SENT")
            except Exception:
                traceback.print_exc()
        else:
            total_failed += 1
            if err_msg: errors.append(err_msg)
            try:
                drip.update(rid, _remap_existing_only(drip, {
                    "status":"FAILED",
                    "last_error": (err_msg or "send_failed")[:500],
                }))
                _set_ui(drip, rid, "FAILED")
            except Exception:
                traceback.print_exc()

        if SLEEP_BETWEEN_SENDS_SEC > 0:
            time.sleep(SLEEP_BETWEEN_SENDS_SEC)

    # KPI logging (best effort)
    runs = get_table(PERF_BASE_ENV, "Runs/Logs")
    kpis = get_table(PERF_BASE_ENV, "KPIs")
    now_iso = utcnow().isoformat()
    if runs:
        try:
            runs.create(_remap_existing_only(runs, {
                "Type": "OUTBOUND_SEND",
                "Processed": float(total_sent),
                "Breakdown": f"sent={total_sent}, failed={total_failed}",
                "Timestamp": now_iso,
            }))
        except Exception: traceback.print_exc()
    if kpis and (total_sent or total_failed):
        try:
            if total_sent:
                kpis.create(_remap_existing_only(kpis, {
                    "Campaign": "ALL",
                    "Metric": "OUTBOUND_SENT",
                    "Value": float(total_sent),
                    "Date": utcnow().date().isoformat(),
                }))
            if total_failed:
                kpis.create(_remap_existing_only(kpis, {
                    "Campaign": "ALL",
                    "Metric": "OUTBOUND_FAILED",
                    "Value": float(total_failed),
                    "Date": utcnow().date().isoformat(),
                }))
        except Exception: traceback.print_exc()

    return {
        "ok": True,
        "total_sent": total_sent,
        "total_failed": total_failed,
        "skipped_rate_limit": max(0, len(due) - (total_sent + total_failed)),
        "quiet_hours": False,
        "errors": errors,
    }