# worker.py
from __future__ import annotations

import os
import time
import json
import uuid
import signal
import traceback
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Optional

# Optional Redis for distributed locks + heartbeat
try:
    import redis as _redis
except Exception:
    _redis = None

# ----------------
# Env / toggles
# ----------------
INTERVAL_SEC          = int(os.getenv("WORKER_INTERVAL_SEC", "30"))
IDLE_INTERVAL_SEC     = int(os.getenv("WORKER_IDLE_INTERVAL_SEC", str(INTERVAL_SEC)))
JITTER_SEC            = int(os.getenv("WORKER_JITTER_SEC", "0"))

ENABLE_CAMPAIGNS      = os.getenv("ENABLE_CAMPAIGNS", "1").lower() in ("1", "true", "yes")
ENABLE_SEND           = os.getenv("ENABLE_SEND", "1").lower() in ("1", "true", "yes")
ENABLE_RETRY          = os.getenv("ENABLE_RETRY", "1").lower() in ("1", "true", "yes")
ENABLE_AUTORESPONDER  = os.getenv("ENABLE_AUTORESPONDER", "1").lower() in ("1", "true", "yes")
ENABLE_METRICS        = os.getenv("ENABLE_METRICS", "1").lower() in ("1", "true", "yes")

CAMPAIGN_LIMIT        = os.getenv("CAMPAIGN_LIMIT", "ALL")  # "ALL" or number
CAMPAIGN_SEND_AFTER   = os.getenv("RUNNER_SEND_AFTER_QUEUE", "0").lower() in ("1", "true", "yes")

SEND_BATCH_LIMIT      = int(os.getenv("SEND_BATCH_LIMIT", "500"))
RETRY_LIMIT           = int(os.getenv("RETRY_LIMIT", "100"))
AUTORESPONDER_LIMIT   = int(os.getenv("AUTORESPONDER_LIMIT", "50"))
AUTORESPONDER_VIEW    = os.getenv("AUTORESPONDER_VIEW", "Unprocessed Inbounds")

RUN_ONCE              = os.getenv("WORKER_RUN_ONCE", "0").lower() in ("1", "true", "yes")
MAX_CYCLES            = int(os.getenv("WORKER_MAX_CYCLES", "0"))  # 0 = infinite

# Health/heartbeat
HEALTHCHECK_URL       = os.getenv("HEALTHCHECK_URL")  # optional ping
KEY_PREFIX            = os.getenv("RATE_LIMIT_KEY_PREFIX", "sms")
WORKER_NAME           = os.getenv("WORKER_NAME", os.getenv("RENDER_SERVICE_NAME", "worker"))
INSTANCE_ID           = os.getenv("WORKER_INSTANCE_ID", str(uuid.uuid4())[:8])

# Redis / Upstash
REDIS_URL             = os.getenv("REDIS_URL") or os.getenv("UPSTASH_REDIS_URL")
REDIS_TLS             = os.getenv("REDIS_TLS", "true").lower() in ("1","true","yes")

# ----------------
# Lazy imports of project runners (never crash the loop on import)
# ----------------
def _run_campaigns(limit: Any, send_after_queue: bool) -> Dict[str, Any]:
    try:
        from sms.campaign_runner import run_campaigns
        return run_campaigns(limit=limit, send_after_queue=send_after_queue)
    except Exception:
        traceback.print_exc()
        return {"ok": False, "error": "campaigns_failed"}

def _send_batch(limit: int) -> Dict[str, Any]:
    try:
        from sms.outbound_batcher import send_batch
        return send_batch(limit=limit)
    except Exception:
        traceback.print_exc()
        return {"ok": False, "error": "send_batch_failed"}

def _run_retry(limit: int) -> Dict[str, Any]:
    try:
        from sms.retry_runner import run_retry
        return run_retry(limit=limit)
    except Exception:
        traceback.print_exc()
        return {"ok": False, "error": "retry_failed"}

def _run_autoresponder(limit: int, view: str) -> Dict[str, Any]:
    try:
        from sms.autoresponder import run_autoresponder
        return run_autoresponder(limit=limit, view=view)
    except Exception:
        traceback.print_exc()
        return {"ok": False, "error": "autoresponder_failed"}

def _update_metrics() -> Dict[str, Any]:
    try:
        from sms.metrics_tracker import update_metrics
        return update_metrics()
    except Exception:
        traceback.print_exc()
        return {"ok": False, "error": "metrics_failed"}

# ----------------
# Redis Lock + Heartbeat
# ----------------
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
        if not self.r:
            return
        try:
            self.r.setex(key, ttl, datetime.now(timezone.utc).isoformat())
        except Exception:
            traceback.print_exc()

    @contextmanager
    def lock(self, name: str, ttl: int):
        """
        Redis NX lock; only one instance runs a task at a time.
        If no Redis, just yield (single-instance best effort).
        """
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
            acquired = True  # fail-open

        try:
            yield acquired
        finally:
            if acquired and self.r:
                try:
                    # release only if owner
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

# ----------------
# Utilities
# ----------------
_shutdown = False
def _signal_handler(signum, frame):
    global _shutdown
    _shutdown = True
    print(f"👋 {WORKER_NAME}[{INSTANCE_ID}] received signal {signum}, shutting down gracefully...")

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

def _jitter_sleep(base: int):
    import random
    j = random.randint(0, max(0, JITTER_SEC))
    time.sleep(base + j)

def _ping_health():
    if not HEALTHCHECK_URL:
        return
    try:
        import requests
        requests.post(HEALTHCHECK_URL, json={"service": WORKER_NAME, "instance": INSTANCE_ID, "ts": datetime.now(timezone.utc).isoformat()}, timeout=3)
    except Exception:
        pass

def _log(event: str, **kw):
    payload = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "service": WORKER_NAME,
        "instance": INSTANCE_ID,
        "event": event,
        **kw,
    }
    print(json.dumps(payload, ensure_ascii=False))

# ----------------
# Main loop
# ----------------
def main():
    cycles = 0
    while True:
        if _shutdown:
            break
        if MAX_CYCLES and cycles >= MAX_CYCLES:
            break

        cycles += 1
        DIST.hb(f"{KEY_PREFIX}:hb:{WORKER_NAME}:{INSTANCE_ID}", ttl=max(60, INTERVAL_SEC * 3))

        # Track whether any work happened (to choose idle vs active sleep)
        did_work = False
        results: Dict[str, Dict[str, Any]] = {}

        # 1) Queue campaigns
        if ENABLE_CAMPAIGNS:
            with DIST.lock("campaigns", ttl=max(15, INTERVAL_SEC * 2)) as ok:
                if ok:
                    res = _run_campaigns(limit=CAMPAIGN_LIMIT, send_after_queue=CAMPAIGN_SEND_AFTER)
                    results["campaigns"] = res
                    did_work = did_work or bool((res or {}).get("processed") or (res or {}).get("results"))
                else:
                    _log("skip_lock", step="campaigns")

        # 2) Send outbound (rate-limit & quiet-hours handled inside)
        if ENABLE_SEND:
            with DIST.lock("send_batch", ttl=max(15, INTERVAL_SEC * 2)) as ok:
                if ok:
                    res = _send_batch(limit=SEND_BATCH_LIMIT)
                    results["send_batch"] = res
                    did_work = did_work or bool((res or {}).get("total_sent"))
                else:
                    _log("skip_lock", step="send_batch")

        # 3) Retry loop
        if ENABLE_RETRY:
            with DIST.lock("retry", ttl=max(15, INTERVAL_SEC * 2)) as ok:
                if ok:
                    res = _run_retry(limit=RETRY_LIMIT)
                    results["retry"] = res
                    did_work = did_work or bool((res or {}).get("retried"))
                else:
                    _log("skip_lock", step="retry")

        # 4) Autoresponder
        if ENABLE_AUTORESPONDER:
            with DIST.lock("autoresponder", ttl=max(15, INTERVAL_SEC * 2)) as ok:
                if ok:
                    res = _run_autoresponder(limit=AUTORESPONDER_LIMIT, view=AUTORESPONDER_VIEW)
                    results["autoresponder"] = res
                    did_work = did_work or bool((res or {}).get("processed"))
                else:
                    _log("skip_lock", step="autoresponder")

        # 5) Metrics rollup
        if ENABLE_METRICS:
            with DIST.lock("metrics", ttl=max(15, INTERVAL_SEC * 2)) as ok:
                if ok:
                    res = _update_metrics()
                    results["metrics"] = res
                else:
                    _log("skip_lock", step="metrics")

        # Emit a compact cycle summary
        _log("cycle",
             cycle=cycles,
             results={k: {ik: iv for ik, iv in v.items() if ik in ("ok","processed","results","total_sent","retried","processed","errors")} for k, v in results.items()})

        _ping_health()

        # Run once / exit if requested
        if RUN_ONCE:
            break

        # Sleep (idle vs active)
        _jitter_sleep(IDLE_INTERVAL_SEC if not did_work else INTERVAL_SEC)

    _log("shutdown", cycle=cycles)

if __name__ == "__main__":
    main()