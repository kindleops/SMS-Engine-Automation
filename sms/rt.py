# sms/rt.py
from __future__ import annotations

import os, hashlib, traceback
from datetime import datetime, timezone, timedelta
from typing import Optional

# ---------------- optional deps ----------------
try:
    import redis as _redis  # TCP client (best)
except Exception:
    _redis = None

try:
    import requests  # Upstash REST fallback
except Exception:
    requests = None

# ---------------- env / config ----------------
REDIS_URL = os.getenv("REDIS_URL") or os.getenv("UPSTASH_REDIS_URL")  # e.g. rediss://default:token@host:6379
REDIS_TLS = os.getenv("REDIS_TLS", "true").lower() in ("1", "true", "yes")

UPSTASH_REDIS_REST_URL = os.getenv("UPSTASH_REDIS_REST_URL")  # e.g. https://xxxxx.upstash.io
UPSTASH_REDIS_REST_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN")

QUIET_TZ = os.getenv("QUIET_TZ", "America/Chicago")
# Accept either QUIET_HOURS_LOCAL or legacy QUIET_HOURS_CST, format "21-9" (start-end, 24h)
QUIET_SPEC = os.getenv("QUIET_HOURS_LOCAL") or os.getenv("QUIET_HOURS_CST", "21-9")

GLOBAL_RATE_PER_MIN = int(os.getenv("GLOBAL_RATE_PER_MIN", "5000"))  # optional global cap across all DIDs

KEY_PREFIX = os.getenv("RATE_LIMIT_KEY_PREFIX", "sms")  # allow scoping per env/project


# ---------------- time helpers ----------------
def _tz_now():
    try:
        from zoneinfo import ZoneInfo

        return datetime.now(ZoneInfo(QUIET_TZ))
    except Exception:
        return datetime.now(timezone.utc)


def _parse_quiet(spec: str) -> tuple[int, int]:
    """
    Return (start_hour, end_hour) as ints in 0..23.
    Spec is "21-9" meaning quiet from 21:00 → 09:00 next day.
    """
    try:
        a, b = spec.split("-")
        start = int(a.strip())
        end = int(b.strip())
        start = max(0, min(23, start))
        end = max(0, min(23, end))
        return start, end
    except Exception:
        return 21, 9  # safe default


def in_cst_quiet_hours(now_ts: Optional[float] = None) -> bool:
    """
    Backward-compatible: checks quiet hours in the configured timezone (default: America/Chicago).
    If `now_ts` is provided, it is treated as a POSIX timestamp.
    """
    start, end = _parse_quiet(QUIET_SPEC)
    now = _tz_now() if now_ts is None else datetime.fromtimestamp(now_ts, _tz_now().tzinfo)
    h = now.hour
    if start == end:
        # degenerate → treat as no quiet hours
        return False
    if start < end:
        # e.g., 9 → 17 (same day window)
        return start <= h < end
    # overnight window, e.g., 21 → 9
    return (h >= start) or (h < end)


def seconds_until_quiet_end() -> int:
    """How many seconds until quiet window ends (0 if not currently quiet)."""
    if not in_cst_quiet_hours():
        return 0
    start, end = _parse_quiet(QUIET_SPEC)
    now = _tz_now()
    today = now.replace(minute=0, second=0, microsecond=0)
    if start < end:
        # same-day window
        end_dt = today.replace(hour=end)
        if now >= end_dt:
            return 0
        return int((end_dt - now).total_seconds())
    # overnight window
    end_dt = today.replace(hour=end)
    if now.hour >= start:
        # ends tomorrow at `end`
        end_dt = end_dt.replace(day=end_dt.day) + timedelta(days=1)
    return max(1, int((end_dt - now).total_seconds()))


# ---------------- redis helpers / keys ----------------
def _minute_bucket() -> str:
    # UTC minute bucket for consistency across workers
    return datetime.utcnow().strftime("%Y%m%d%H%M")


def _hash_did(did: str) -> str:
    return hashlib.md5((did or "").encode()).hexdigest()


def _key(*parts: str) -> str:
    return ":".join([KEY_PREFIX, *[p for p in parts if p]])


# cached TCP client
_RTCP = None


def _redis_tcp():
    global _RTCP
    if _RTCP is not None:
        return _RTCP
    if not (REDIS_URL and _redis):
        _RTCP = None
        return None
    try:
        _RTCP = _redis.from_url(REDIS_URL, ssl=REDIS_TLS, decode_responses=True, socket_timeout=3)
    except Exception:
        traceback.print_exc()
        _RTCP = None
    return _RTCP


# ---------------- limiters ----------------
class _LuaLimiter:
    """
    Strong atomic limiter using Redis TCP + Lua:
      - per-DID/minute
      - global/minute
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

    def __init__(self, per_limit: int, global_limit: int):
        self.per = max(1, per_limit)
        self.glob = max(1, global_limit)
        self.r = _redis_tcp()
        self.script = self.r.register_script(self.LUA) if self.r else None

    def take(self, did: str) -> bool:
        if not self.r or not self.script:
            return True  # fail-open
        try:
            bucket = _minute_bucket()
            did_key = _key("rl", "did", bucket, _hash_did(did or ""))
            glob_key = _key("rl", "glob", bucket)
            ok = self.script(keys=[did_key, glob_key], args=[self.per, self.glob, 65000])
            return bool(ok)
        except Exception:
            traceback.print_exc()
            return True  # fail-open to avoid wedging sends


class _UpstashRestLimiter:
    """
    Best-effort limiter using Upstash REST (no Lua / not fully atomic across both keys).
    Good enough for single worker or light parallelism.
    """

    def __init__(self, per_limit: int, global_limit: int):
        self.base = (UPSTASH_REDIS_REST_URL or "").rstrip("/")
        self.tok = UPSTASH_REDIS_REST_TOKEN
        self.per = max(1, per_limit)
        self.glob = max(1, global_limit)
        self.enabled = bool(self.base and self.tok and requests)

    def _get(self, k: str) -> int:
        try:
            resp = requests.post(f"{self.base}/get/{k}", headers={"Authorization": f"Bearer {self.tok}"}, timeout=2)
            if resp.ok:
                j = resp.json()
                v = j.get("result")
                return int(v) if v is not None else 0
        except Exception:
            traceback.print_exc()
        return 0

    def _incr_exp(self, k: str, ttl: int = 60) -> None:
        try:
            # pipeline via REST
            cmds = [["INCR", k], ["EXPIRE", k, str(ttl)]]
            requests.post(f"{self.base}/pipeline", json=cmds, headers={"Authorization": f"Bearer {self.tok}"}, timeout=2)
        except Exception:
            traceback.print_exc()

    def take(self, did: str) -> bool:
        if not self.enabled:
            return True
        bucket = _minute_bucket()
        did_key = _key("rl", "did", bucket, _hash_did(did or ""))
        glob_key = _key("rl", "glob", bucket)
        did_ct = self._get(did_key)
        glob_ct = self._get(glob_key)
        if did_ct >= self.per or glob_ct >= self.glob:
            return False
        self._incr_exp(did_key)
        self._incr_exp(glob_key)
        return True


class _LocalLimiter:
    """Process-local minute bucket limiter (dev fallback)."""

    def __init__(self, per_limit: int, global_limit: int):
        self.per = max(1, per_limit)
        self.glob = max(1, global_limit)
        self._bucket = None
        self._per = {}
        self._glob = 0

    def _roll(self):
        m = _minute_bucket()
        if m != self._bucket:
            self._bucket = m
            self._per.clear()
            self._glob = 0

    def take(self, did: str) -> bool:
        self._roll()
        if self._glob >= self.glob:
            return False
        c = self._per.get(did or "", 0)
        if c >= self.per:
            return False
        self._glob += 1
        self._per[did or ""] = c + 1
        return True


# choose the best available limiter
def _build_limiter(per_min: int, global_min: int):
    if _redis_tcp():
        return _LuaLimiter(per_min, global_min)
    if UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN and requests:
        return _UpstashRestLimiter(per_min, global_min)
    return _LocalLimiter(per_min, global_min)


# a cached limiter instance; will be rebuilt on demand if per/global change
_LIMITER_CACHE = {}


def _limiter(per_min: int, global_min: int):
    key = (per_min, global_min)
    if key not in _LIMITER_CACHE:
        _LIMITER_CACHE[key] = _build_limiter(per_min, global_min)
    return _LIMITER_CACHE[key]


# ---------------- public API (backward-compatible) ----------------
def take_token(did: str, max_per_min: int) -> bool:
    """
    Backward-compatible entry point.
    Uses a cross-process minute limiter (Redis TCP → Upstash REST → Local).
    Also enforces an optional global cap via GLOBAL_RATE_PER_MIN (env).
    """
    per = int(max_per_min or 1)
    glob = int(GLOBAL_RATE_PER_MIN or 999999)
    return _limiter(per, glob).take(did or "")


# Optional: richer API if you want explicit global caps per call
def take_token2(did: str, per_min: int, global_per_min: Optional[int] = None) -> bool:
    glob = int(global_per_min) if global_per_min is not None else int(GLOBAL_RATE_PER_MIN or 999999)
    return _limiter(int(per_min or 1), glob).take(did or "")


# --------------- simple diagnostics ----------------
def minute_bucket_key_examples(did: str) -> dict:
    """Returns the exact keys used this minute (useful when debugging in Redis)."""
    bucket = _minute_bucket()
    return {
        "did_key": _key("rl", "did", bucket, _hash_did(did or "")),
        "glob_key": _key("rl", "glob", bucket),
        "bucket": bucket,
    }
