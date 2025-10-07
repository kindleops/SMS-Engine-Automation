import os, time, math
import redis

r = redis.from_url(os.getenv("REDIS_URL")) if os.getenv("REDIS_URL") else None

def _key(*parts): return ":".join(["sms"]+[p for p in parts if p])

def in_cst_quiet_hours(now_ts=None):
    from datetime import datetime, timezone
    import zoneinfo
    tz = zoneinfo.ZoneInfo("America/Chicago")
    quiet = os.getenv("QUIET_HOURS_CST","21-09")
    end, start = [int(x) for x in quiet.split("-")]  # 21, 09
    now = datetime.now(tz) if not now_ts else datetime.fromtimestamp(now_ts, tz)
    h = now.hour
    # quiet if 21:00–23:59 or 00:00–08:59
    return (h >= end) or (h < start)

def take_token(did: str, max_per_min: int) -> bool:
    """Simple token bucket: 20 tokens per 60s per DID."""
    if not r:  # no Redis -> allow (dev mode)
        return True
    bucket = _key("lim", did)
    with r.pipeline() as p:
        p.incr(bucket, 1)
        p.expire(bucket, 60)
        count, _ = p.execute()
    return int(count) <= max_per_min