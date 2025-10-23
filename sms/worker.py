"""
🚀 Advanced SMS Worker (Bulletproof Edition)
--------------------------------------------
Handles:
  • Campaign scheduling & hydration
  • Outbound batch sending
  • Retry handling
  • Autoresponder
  • Metrics aggregation
  • Health heartbeat + distributed locks

Safe for multi-instance operation using Redis NX locks.
"""

from __future__ import annotations
import os, time, json, uuid, signal, traceback, random
from datetime import datetime, timezone
from contextlib import contextmanager
from typing import Any, Dict, Optional

try:
    import redis as _redis
except ImportError:
    _redis = None

# ────────────────────────────────────────────────
# ENV CONFIG HELPERS
# ────────────────────────────────────────────────
def _env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip().lower() in ("1", "true", "yes", "on")

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

# ────────────────────────────────────────────────
# ENV VARIABLES
# ────────────────────────────────────────────────
INTERVAL_SEC = _env_int("WORKER_INTERVAL_SEC", 30)
IDLE_INTERVAL_SEC = _env_int("WORKER_IDLE_INTERVAL_SEC", INTERVAL_SEC)
JITTER_SEC = _env_int("WORKER_JITTER_SEC", 5)

ENABLE_CAMPAIGNS = _env_bool("ENABLE_CAMPAIGNS", True)
ENABLE_SEND = _env_bool("ENABLE_SEND", True)
ENABLE_RETRY = _env_bool("ENABLE_RETRY", True)
ENABLE_AUTORESPONDER = _env_bool("ENABLE_AUTORESPONDER", True)
ENABLE_METRICS = _env_bool("ENABLE_METRICS", True)

CAMPAIGN_LIMIT = os.getenv("CAMPAIGN_LIMIT", "ALL")
CAMPAIGN_SEND_AFTER = _env_bool("RUNNER_SEND_AFTER_QUEUE", False)

SEND_BATCH_LIMIT = _env_int("SEND_BATCH_LIMIT", 500)
RETRY_LIMIT = _env_int("RETRY_LIMIT", 100)
AUTORESPONDER_LIMIT = _env_int("AUTORESPONDER_LIMIT", 50)
AUTORESPONDER_VIEW = os.getenv("AUTORESPONDER_VIEW", "Unprocessed Inbounds")

RUN_ONCE = _env_bool("WORKER_RUN_ONCE", False)
MAX_CYCLES = _env_int("WORKER_MAX_CYCLES", 0)

HEALTHCHECK_URL = os.getenv("HEALTHCHECK_URL")
KEY_PREFIX = os.getenv("RATE_LIMIT_KEY_PREFIX", "sms")
WORKER_NAME = os.getenv("WORKER_NAME", os.getenv("RENDER_SERVICE_NAME", "rei-sms-worker"))
INSTANCE_ID = os.getenv("WORKER_INSTANCE_ID", str(uuid.uuid4())[:8])

REDIS_URL = os.getenv("REDIS_URL") or os.getenv("UPSTASH_REDIS_URL")
REDIS_TLS = _env_bool("REDIS_TLS", True)

# ────────────────────────────────────────────────
# IMPORT DEFERRED RUNNERS (lazy to avoid crash loops)
# ────────────────────────────────────────────────
def _run_campaigns(limit: Any, send_after_queue: bool):
    try:
        from sms.campaign_runner import run_campaigns
        return run_campaigns(limit=limit, send_after_queue=send_after_queue)
    except Exception as e:
        traceback.print_exc()
        return {"ok": False, "error": str(e)}

def _send_batch(limit: int):
    try:
        from sms.outbound_batcher import send_batch
        return send_batch(limit=limit)
    except Exception as e:
        traceback.print_exc()
        return {"ok": False, "error": str(e)}

def _run_retry(limit: int):
    try:
        from sms.retry_runner import run_retry
        return run_retry(limit=limit)
    except Exception as e:
        traceback.print_exc()
        return {"ok": False, "error": str(e)}

def _run_autoresponder(limit: int, view: str):
    try:
        from sms.autoresponder import run_autoresponder
        return run_autoresponder(limit=limit, view=view)
    except Exception as e:
        traceback.print_exc()
        return {"ok": False, "error": str(e)}

def _update_metrics():
    try:
        from sms.metrics_tracker import update_metrics
        return update_metrics()
    except Exception as e:
        traceback.print_exc()
        return {"ok": False, "error": str(e)}

# ────────────────────────────────────────────────
# REDIS DIST LOCK + HEARTBEAT
# ────────────────────────────────────────────────
class Dist:
    def __init__(self):
        self.r = None
        if REDIS_URL and _redis:
            try:
                self.r = _redis.from_url(REDIS_URL, ssl=REDIS_TLS, decode_responses=True, socket_timeout=3)
            except Exception:
                traceback.print_exc()
                self.r = None

    def hb(self, key: str, ttl: int = 120):
        """Heartbeat TTL to detect dead workers."""
        if not self.r:
            return
        try:
            self.r.setex(key, ttl, datetime.now(timezone.utc).isoformat())
        except Exception:
            traceback.print_exc()

    @contextmanager
    def lock(self, name: str, ttl: int):
        """Redis NX lock (safe for multi-worker operation)."""
        if not self.r:
            yield True
            return
        key = f"{KEY_PREFIX}:lock:{name}"
        token = str(uuid.uuid4())
        acquired = False
        try:
            acquired = bool(self.r.set(key, token, nx=True, ex=ttl))
        except Exception:
            traceback.print_exc()
            acquired = True
        try:
            yield acquired
        finally:
            if acquired and self.r:
                try:
                    lua = """
                    if redis.call('GET', KEYS[1]) == ARGV[1] then
                        return redis.call('DEL', KEYS[1])
                    else
                        return 0
                    end
                    """
                    self.r.eval(lua, 1, key, token)
                except Exception:
                    traceback.print_exc()

DIST = Dist()

# ────────────────────────────────────────────────
# SIGNAL HANDLERS
# ────────────────────────────────────────────────
_shutdown = False

def _signal_handler(signum, frame):
    global _shutdown
    _shutdown = True
    print(f"👋 {WORKER_NAME}[{INSTANCE_ID}] got signal {signum}, shutting down...")

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

# ────────────────────────────────────────────────
# UTILITIES
# ────────────────────────────────────────────────
def _sleep_with_jitter(base: int):
    """Sleep with jitter to avoid multiple workers aligning."""
    j = random.randint(0, max(0, JITTER_SEC))
    time.sleep(min(3600, base + j))

def _ping_health():
    if not HEALTHCHECK_URL:
        return
    try:
        import requests
        requests.post(HEALTHCHECK_URL, json={
            "service": WORKER_NAME,
            "instance": INSTANCE_ID,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }, timeout=3)
    except Exception:
        pass

def _log(event: str, **extra):
    data = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "service": WORKER_NAME,
        "instance": INSTANCE_ID,
        "event": event,
        **extra,
    }
    print(json.dumps(data, ensure_ascii=False))

def _compact(res: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Trim result payloads for cleaner cycle logs."""
    if not isinstance(res, dict):
        return {}
    keep = ("ok", "processed", "total_sent", "retried", "errors")
    filtered = {k: v for k, v in res.items() if k in keep}
    return filtered

# ────────────────────────────────────────────────
# MAIN WORKER LOOP
# ────────────────────────────────────────────────
def main():
    print(f"🚀 Starting {WORKER_NAME} [{INSTANCE_ID}] | interval={INTERVAL_SEC}s idle={IDLE_INTERVAL_SEC}s jitter={JITTER_SEC}s")
    cycles = 0

    while True:
        if _shutdown:
            break
        if MAX_CYCLES and cycles >= MAX_CYCLES:
            break

        cycles += 1
        DIST.hb(f"{KEY_PREFIX}:hb:{WORKER_NAME}:{INSTANCE_ID}", ttl=max(60, INTERVAL_SEC * 3))
        did_work = False
        results: Dict[str, Any] = {}

        # --- Campaign Scheduler ---
        if ENABLE_CAMPAIGNS:
            with DIST.lock("campaigns", ttl=max(30, INTERVAL_SEC * 2)) as ok:
                if ok:
                    res = _run_campaigns(CAMPAIGN_LIMIT, CAMPAIGN_SEND_AFTER)
                    results["campaigns"] = res
                    did_work |= bool(res and (res.get("processed") or res.get("queued")))
                else:
                    _log("skip_lock", step="campaigns")

        # --- Outbound Sending ---
        if ENABLE_SEND:
            with DIST.lock("send_batch", ttl=max(60, INTERVAL_SEC * 2)) as ok:
                if ok:
                    res = _send_batch(SEND_BATCH_LIMIT)
                    results["send_batch"] = res
                    did_work |= bool(res and res.get("total_sent"))
                else:
                    _log("skip_lock", step="send_batch")

        # --- Retry Failed Sends ---
        if ENABLE_RETRY:
            with DIST.lock("retry", ttl=max(30, INTERVAL_SEC * 2)) as ok:
                if ok:
                    res = _run_retry(RETRY_LIMIT)
                    results["retry"] = res
                    did_work |= bool(res and res.get("retried"))
                else:
                    _log("skip_lock", step="retry")

        # --- Autoresponder (Inbound) ---
        if ENABLE_AUTORESPONDER:
            with DIST.lock("autoresponder", ttl=max(30, INTERVAL_SEC * 2)) as ok:
                if ok:
                    res = _run_autoresponder(AUTORESPONDER_LIMIT, AUTORESPONDER_VIEW)
                    results["autoresponder"] = res
                    did_work |= bool(res and res.get("processed"))
                else:
                    _log("skip_lock", step="autoresponder")

        # --- Metrics Rollup ---
        if ENABLE_METRICS:
            with DIST.lock("metrics", ttl=max(30, INTERVAL_SEC * 2)) as ok:
                if ok:
                    results["metrics"] = _update_metrics()
                else:
                    _log("skip_lock", step="metrics")

        # --- Summary & Telemetry ---
        _log("cycle", cycle=cycles, summary={k: _compact(v) for k, v in results.items()})
        _ping_health()

        if RUN_ONCE:
            break

        _sleep_with_jitter(IDLE_INTERVAL_SEC if not did_work else INTERVAL_SEC)

    _log("shutdown", cycles=cycles)
    print(f"🏁 {WORKER_NAME}[{INSTANCE_ID}] stopped cleanly after {cycles} cycles.")
    

if __name__ == "__main__":
    main()
