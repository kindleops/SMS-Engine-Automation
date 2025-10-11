# sms/worker.py
from __future__ import annotations

"""
Continuous SMS worker:
- Queues campaigns when they're eligible (start time hit), pushes prospects to Drip
- Sends due Drip items (rate/quiet handled inside outbound_batcher)
- Retries failed sends
- Runs autoresponder
- Rolls up metrics
All steps are protected by optional Redis NX locks so you can run >1 worker safely.
"""

import os
import time
import json
import uuid
import signal
import traceback
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Optional

# Optional Redis for distributed locks + heartbeat (fail-open if missing)
try:
    import redis as _redis  # type: ignore
except Exception:  # pragma: no cover
    _redis = None  # type: ignore

# ----------------
# Env / toggles
# ----------------
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

INTERVAL_SEC          = _env_int("WORKER_INTERVAL_SEC", 30)        # active sleep between cycles
IDLE_INTERVAL_SEC     = _env_int("WORKER_IDLE_INTERVAL_SEC", INTERVAL_SEC)  # when no work happened
JITTER_SEC            = _env_int("WORKER_JITTER_SEC", 0)           # add 0..JITTER_SEC seconds

ENABLE_CAMPAIGNS      = _env_bool("ENABLE_CAMPAIGNS", True)
ENABLE_SEND           = _env_bool("ENABLE_SEND", True)
ENABLE_RETRY          = _env_bool("ENABLE_RETRY", True)
ENABLE_AUTORESPONDER  = _env_bool("ENABLE_AUTORESPONDER", True)
ENABLE_METRICS        = _env_bool("ENABLE_METRICS", True)

CAMPAIGN_LIMIT        = os.getenv("CAMPAIGN_LIMIT", "ALL")  # "ALL" or integer string
CAMPAIGN_SEND_AFTER   = _env_bool("RUNNER_SEND_AFTER_QUEUE", False)

SEND_BATCH_LIMIT      = _env_int("SEND_BATCH_LIMIT", 500)
RETRY_LIMIT           = _env_int("RETRY_LIMIT", 100)
AUTORESPONDER_LIMIT   = _env_int("AUTORESPONDER_LIMIT", 50)
AUTORESPONDER_VIEW    = os.getenv("AUTORESPONDER_VIEW", "Unprocessed Inbounds")

RUN_ONCE              = _env_bool("WORKER_RUN_ONCE", False)
MAX_CYCLES            = _env_int("WORKER_MAX_CYCLES", 0)  # 0 = infinite

# Health/heartbeat
HEALTHCHECK_URL       = os.getenv("HEALTHCHECK_URL")  # optional ping endpoint
KEY_PREFIX            = os.getenv("RATE_LIMIT_KEY_PREFIX", "sms")
WORKER_NAME           = os.getenv("WORKER_NAME", os.getenv("RENDER_SERVICE_NAME", "rei-sms-worker"))
INSTANCE_ID           = os.getenv("WORKER_INSTANCE_ID", str(uuid.uuid4())[:8])

# Redis / Upstash
REDIS_URL             = os.getenv("REDIS_URL") or os.getenv("UPSTASH_REDIS_URL")
REDIS_TLS             = _env_bool("REDIS_TLS", True)

# ----------------
# Lazy imports of project runners (never crash the loop on import)
# ----------------
def _run_campaigns(limit: Any, send_after_queue: bool) -> Dict[str, Any]:
    try:
        from sms.campaign_runner import run_campaigns  # type: ignore
        return run_campaigns(limit=limit, send_after_queue=send_after_queue)
    except Exception:
        traceback.print_exc()
        return {"ok": False, "error": "campaigns_failed"}

def _send_batch(limit: int) -> Dict[str, Any]:
    try:
        from sms.outbound_batcher import send_batch  # type: ignore
        return send_batch(limit=limit)
    except Exception:
        traceback.print_exc()
        return {"ok": False, "error": "send_batch_failed"}

def _run_retry(limit: int) -> Dict[str, Any]:
    try:
        from sms.retry_runner import run_retry  # type: ignore
        return run_retry(limit=limit)
    except Exception:
        traceback.print_exc()
        return {"ok": False, "error": "retry_failed"}

def _run_autoresponder(limit: int, view: str) -> Dict[str, Any]:
    try:
        from sms.autoresponder import run_autoresponder  # type: ignore
        return run_autoresponder(limit=limit, view=view)
    except Exception:
        traceback.print_exc()
        return {"ok": False, "error": "autoresponder_failed"}

def _update_metrics() -> Dict[str, Any]:
    try:
        from sms.metrics_tracker import update_metrics  # type: ignore
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
                self.r = _redis.from_url(
                    REDIS_URL, ssl=REDIS_TLS, decode_responses=True, socket_timeout=3
                )
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
        If no Redis, yield True (single-instance best effort).
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
            # fail-open so work doesn't stop just because Redis is flaky
            acquired = True

        try:
            yield acquired
        finally:
            if acquired and self.r:
                try:
                    # release only if owner (prevents stealing)
                    lua = """
                    if redis.call('GET', KEYS[1]) == ARGV[1] then
                        return redis.call('DEL', KEYS[1])
                    else
                        return 0
                    end
                    """
                    self.r.eval(lua, 1, key, token)
                except Exception:
                    # Don't crash on unlock
                    traceback.print_exc()

DIST = Dist()

# ----------------
# Utilities
# ----------------
_shutdown = False
def _signal_handler(signum, frame):
    global _shutdown
    _shutdown = True
    print(f"👋 {WORKER_NAME}[{INSTANCE_ID}] got signal {signum}, shutting down...")

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

def _jitter_sleep(base: int):
    import random
    b = max(0, int(base))
    j = random.randint(0, max(0, int(JITTER_SEC)))
    # Guard total to avoid negative or crazy sleeps
    time.sleep(min(3600, b + j))

def _ping_health():
    if not HEALTHCHECK_URL:
        return
    try:
        import requests  # present in requirements
        requests.post(
            HEALTHCHECK_URL,
            json={
                "service": WORKER_NAME,
                "instance": INSTANCE_ID,
                "ts": datetime.now(timezone.utc).isoformat(),
            },
            timeout=3,
        )
    except Exception:
        # fire-and-forget
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

def _compact(res: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Trim noisy keys for cycle summaries.
    """
    if not isinstance(res, dict):
        return {}
    keep = ("ok", "processed", "results", "total_sent", "retried", "errors")
    out = {k: v for k, v in res.items() if k in keep}
    # Avoid dumping huge 'results' blobs
    if "results" in out and isinstance(out["results"], list) and len(out["results"]) > 5:
        out["results"] = out["results"][:5] + [{"truncated": len(out["results"]) - 5}]
    return out

# ----------------
# Main loop
# ----------------
def main():
    print(f"🚀 Starting {WORKER_NAME} [{INSTANCE_ID}]  interval={INTERVAL_SEC}s  idle={IDLE_INTERVAL_SEC}s")
    cycles = 0
    while True:
        if _shutdown:
            break
        if MAX_CYCLES and cycles >= MAX_CYCLES:
            break

        cycles += 1
        # Heartbeat
        DIST.hb(f"{KEY_PREFIX}:hb:{WORKER_NAME}:{INSTANCE_ID}", ttl=max(60, INTERVAL_SEC * 3))

        did_work = False
        results: Dict[str, Dict[str, Any]] = {}

        # 1) Queue campaigns (flip to Running and push to Drip as needed)
        if ENABLE_CAMPAIGNS:
            with DIST.lock("campaigns", ttl=max(30, INTERVAL_SEC * 2)) as ok:
                if ok:
                    res = _run_campaigns(limit=CAMPAIGN_LIMIT, send_after_queue=CAMPAIGN_SEND_AFTER)
                    results["campaigns"] = res
                    # If there are any eligible campaigns processed or any result rows, we consider it work
                    did_work = did_work or bool(res and (res.get("processed") or res.get("results")))
                else:
                    _log("skip_lock", step="campaigns")

        # 2) Send due messages (rate/quiet enforced inside sender)
        if ENABLE_SEND:
            with DIST.lock("send_batch", ttl=max(60, INTERVAL_SEC * 2)) as ok:
                if ok:
                    res = _send_batch(limit=SEND_BATCH_LIMIT)
                    results["send_batch"] = res
                    did_work = did_work or bool(res and res.get("total_sent"))
                else:
                    _log("skip_lock", step="send_batch")

        # 3) Retry failed sends opportunistically
        if ENABLE_RETRY:
            with DIST.lock("retry", ttl=max(30, INTERVAL_SEC * 2)) as ok:
                if ok:
                    res = _run_retry(limit=RETRY_LIMIT)
                    results["retry"] = res
                    did_work = did_work or bool(res and res.get("retried"))
                else:
                    _log("skip_lock", step="retry")

        # 4) Autoresponder (inbounds)
        if ENABLE_AUTORESPONDER:
            with DIST.lock("autoresponder", ttl=max(30, INTERVAL_SEC * 2)) as ok:
                if ok:
                    res = _run_autoresponder(limit=AUTORESPONDER_LIMIT, view=AUTORESPONDER_VIEW)
                    results["autoresponder"] = res
                    did_work = did_work or bool(res and res.get("processed"))
                else:
                    _log("skip_lock", step="autoresponder")

        # 5) Metrics rollup (best effort; not counted as "work" for sleep pacing)
        if ENABLE_METRICS:
            with DIST.lock("metrics", ttl=max(30, INTERVAL_SEC * 2)) as ok:
                if ok:
                    results["metrics"] = _update_metrics()
                else:
                    _log("skip_lock", step="metrics")

        # Emit compact cycle summary
        _log(
            "cycle",
            cycle=cycles,
            summary={k: _compact(v) for k, v in results.items()},
        )

        _ping_health()

        if RUN_ONCE:
            break

        # Sleep (idle vs active)
        _jitter_sleep(IDLE_INTERVAL_SEC if not did_work else INTERVAL_SEC)

    _log("shutdown", cycle=cycles)
    print(f"🏁 {WORKER_NAME} [{INSTANCE_ID}] stopped cleanly.")

if __name__ == "__main__":
    main()