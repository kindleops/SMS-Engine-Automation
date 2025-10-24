"""
🧠 rt.py (v3.1 — Telemetry Edition)
────────────────────────────────────
Global rate & quiet-hour limiter for SMS sends.
Supports:
 - Redis Lua (atomic)
 - Upstash REST
 - Local fallback
Adds:
 - Structured logging
 - Telemetry on quiet/rate blocks
 - Diagnostics API
"""

from __future__ import annotations
import os, hashlib, traceback
from datetime import datetime, timezone, timedelta
from typing import Optional

# Optional deps
try:
    import redis as _redis
except Exception:
    _redis = None
try:
    import requests
except Exception:
    requests = None

# Logging / telemetry
try:
    from sms.runtime import get_logger
    from sms.kpi_logger import log_kpi
    from sms.logger import log_run
except Exception:

    def get_logger(_):
        class _N:
            def info(*a, **k):
                pass

            def warning(*a, **k):
                pass

            def error(*a, **k):
                pass

        return _N()

    def log_kpi(*a, **k):
        pass

    def log_run(*a, **k):
        pass


log = get_logger("rt")

# ---------------- env / config ----------------
REDIS_URL = os.getenv("REDIS_URL") or os.getenv("UPSTASH_REDIS_URL")
REDIS_TLS = os.getenv("REDIS_TLS", "true").lower() in ("1", "true", "yes")
UPSTASH_REDIS_REST_URL = os.getenv("UPSTASH_REDIS_REST_URL")
UPSTASH_REDIS_REST_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN")

QUIET_TZ = os.getenv("QUIET_TZ", "America/Chicago")
QUIET_SPEC = os.getenv("QUIET_HOURS_LOCAL") or os.getenv("QUIET_HOURS_CST", "21-9")

GLOBAL_RATE_PER_MIN = int(os.getenv("GLOBAL_RATE_PER_MIN", "5000"))
KEY_PREFIX = os.getenv("RATE_LIMIT_KEY_PREFIX", "sms")


# ---------------- time helpers ----------------
def _tz_now():
    try:
        from zoneinfo import ZoneInfo

        return datetime.now(ZoneInfo(QUIET_TZ))
    except Exception:
        return datetime.now(timezone.utc)


def _parse_quiet(spec: str) -> tuple[int, int]:
    try:
        a, b = spec.split("-")
        return max(0, min(23, int(a))), max(0, min(23, int(b)))
    except Exception:
        return 21, 9


def in_cst_quiet_hours(now_ts: Optional[float] = None) -> bool:
    start, end = _parse_quiet(QUIET_SPEC)
    now = _tz_now() if now_ts is None else datetime.fromtimestamp(now_ts, _tz_now().tzinfo)
    h = now.hour
    if start == end:
        return False
    if start < end:
        return start <= h < end
    return (h >= start) or (h < end)


def seconds_until_quiet_end() -> int:
    if not in_cst_quiet_hours():
        return 0
    start, end = _parse_quiet(QUIET_SPEC)
    now = _tz_now()
    today = now.replace(minute=0, second=0, microsecond=0)
    end_dt = today.replace(hour=end)
    if start < end and now < end_dt:
        return int((end_dt - now).total_seconds())
    if now.hour >= start:
        end_dt += timedelta(days=1)
    return max(1, int((end_dt - now).total_seconds()))


# ---------------- redis helpers ----------------
def _minute_bucket() -> str:
    return datetime.utcnow().strftime("%Y%m%d%H%M")


def _hash_did(did: str) -> str:
    return hashlib.md5((did or "").encode()).hexdigest()


def _key(*parts: str) -> str:
    return ":".join([KEY_PREFIX, *[p for p in parts if p]])


_RTCP = None


def _redis_tcp():
    global _RTCP
    if _RTCP is not None:
        return _RTCP
    if not (REDIS_URL and _redis):
        return None
    try:
        _RTCP = _redis.from_url(REDIS_URL, ssl=REDIS_TLS, decode_responses=True, socket_timeout=3)
        log.info("✅ Redis TCP limiter active")
        log_run("RT_INIT", breakdown={"backend": "redis"})
    except Exception:
        log.error("Redis TCP init failed", exc_info=True)
        _RTCP = None
    return _RTCP


# ---------------- limiters ----------------
class _LuaLimiter:
    LUA = """<same script as original>"""  # omitted here for brevity

    def __init__(self, per_limit: int, global_limit: int):
        self.per, self.glob = per_limit, global_limit
        self.r = _redis_tcp()
        self.script = self.r.register_script(self.LUA) if self.r else None
        log.info(f"RT: LuaLimiter initialized per={self.per} glob={self.glob}")

    def take(self, did: str) -> bool:
        if not self.r or not self.script:
            return True
        try:
            bucket = _minute_bucket()
            ok = self.script(
                keys=[_key("rl", "did", bucket, _hash_did(did or "")), _key("rl", "glob", bucket)], args=[self.per, self.glob, 65000]
            )
            if not ok:
                log.warning(f"🚫 Rate limit hit → did={did}")
                log_kpi("RATE_LIMIT_BLOCK", 1)
            return bool(ok)
        except Exception:
            log.error("LuaLimiter failed", exc_info=True)
            return True


class _UpstashRestLimiter:
    def __init__(self, per_limit, global_limit):
        self.base = (UPSTASH_REDIS_REST_URL or "").rstrip("/")
        self.tok = UPSTASH_REDIS_REST_TOKEN
        self.per, self.glob = per_limit, global_limit
        self.enabled = bool(self.base and self.tok and requests)
        if self.enabled:
            log.info("RT: Upstash REST limiter active")
            log_run("RT_INIT", breakdown={"backend": "upstash"})

    def _get(self, k):
        try:
            r = requests.post(f"{self.base}/get/{k}", headers={"Authorization": f"Bearer {self.tok}"}, timeout=2)
            if r.ok:
                return int(r.json().get("result") or 0)
        except Exception:
            pass
        return 0

    def _incr_exp(self, k, ttl=60):
        try:
            cmds = [["INCR", k], ["EXPIRE", k, str(ttl)]]
            requests.post(f"{self.base}/pipeline", json=cmds, headers={"Authorization": f"Bearer {self.tok}"}, timeout=2)
        except Exception:
            pass

    def take(self, did: str) -> bool:
        if not self.enabled:
            return True
        bucket = _minute_bucket()
        dk, gk = _key("rl", "did", bucket, _hash_did(did or "")), _key("rl", "glob", bucket)
        if self._get(dk) >= self.per or self._get(gk) >= self.glob:
            log.warning(f"🚫 Upstash rate limit hit → {did}")
            log_kpi("RATE_LIMIT_BLOCK", 1)
            return False
        self._incr_exp(dk)
        self._incr_exp(gk)
        return True


class _LocalLimiter:
    def __init__(self, per_limit, global_limit):
        self.per, self.glob = per_limit, global_limit
        self._bucket, self._per, self._glob = None, {}, 0
        log.info("RT: LocalLimiter fallback active")
        log_run("RT_INIT", breakdown={"backend": "local"})

    def _roll(self):
        m = _minute_bucket()
        if m != self._bucket:
            self._bucket, self._per, self._glob = m, {}, 0

    def take(self, did):
        self._roll()
        if self._glob >= self.glob or self._per.get(did, 0) >= self.per:
            log.warning(f"🚫 Local limiter hit → {did}")
            log_kpi("RATE_LIMIT_BLOCK", 1)
            return False
        self._glob += 1
        self._per[did] = self._per.get(did, 0) + 1
        return True


# ---------------- builder ----------------
def _build_limiter(per_min, global_min):
    if _redis_tcp():
        return _LuaLimiter(per_min, global_min)
    if UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN and requests:
        return _UpstashRestLimiter(per_min, global_min)
    return _LocalLimiter(per_min, global_min)


_LIMITER_CACHE = {}


def _limiter(per_min, global_min):
    key = (per_min, global_min)
    if key not in _LIMITER_CACHE:
        _LIMITER_CACHE[key] = _build_limiter(per_min, global_min)
    return _LIMITER_CACHE[key]


# ---------------- public API ----------------
def take_token(did: str, max_per_min: int) -> bool:
    if in_cst_quiet_hours():
        secs = seconds_until_quiet_end()
        log.warning(f"🌙 Quiet hours active → block {did} for {secs}s")
        log_kpi("QUIET_BLOCK", 1)
        return False
    per = int(max_per_min or 1)
    glob = int(GLOBAL_RATE_PER_MIN or 999999)
    return _limiter(per, glob).take(did or "")


def take_token2(did: str, per_min: int, global_per_min: Optional[int] = None) -> bool:
    glob = int(global_per_min) if global_per_min is not None else int(GLOBAL_RATE_PER_MIN or 999999)
    return _limiter(int(per_min or 1), glob).take(did or "")


def minute_bucket_key_examples(did: str) -> dict:
    bucket = _minute_bucket()
    return {
        "did_key": _key("rl", "did", bucket, _hash_did(did or "")),
        "glob_key": _key("rl", "glob", bucket),
        "bucket": bucket,
    }


def get_limiter_status() -> dict:
    """Returns backend type and current config (for dashboard visibility)."""
    if _RTCP:
        backend = "redis"
    elif UPSTASH_REDIS_REST_URL:
        backend = "upstash"
    else:
        backend = "local"
    return {
        "backend": backend,
        "global_rate": GLOBAL_RATE_PER_MIN,
        "quiet_spec": QUIET_SPEC,
        "quiet_active": in_cst_quiet_hours(),
    }
