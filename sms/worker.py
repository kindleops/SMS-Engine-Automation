"""
🚀 Advanced SMS Worker (Bulletproof Edition) — ++HARDENED++
- Per-runner timeouts (prevents hangs)
- Adaptive backoff on failure streaks
- Optional system metrics (psutil if available)
- Startup warmup jitter
- Final metrics flush on shutdown
- Same ENV toggles / Redis locks / telemetry
"""

from __future__ import annotations
import os, time, json, uuid, signal, traceback, random
from datetime import datetime, timezone
from contextlib import contextmanager
from typing import Any, Dict, Optional, Callable
import concurrent.futures

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


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


# ────────────────────────────────────────────────
# ENV VARIABLES
# ────────────────────────────────────────────────
INTERVAL_SEC = _env_int("WORKER_INTERVAL_SEC", 30)
IDLE_INTERVAL_SEC = _env_int("WORKER_IDLE_INTERVAL_SEC", INTERVAL_SEC)
JITTER_SEC = _env_int("WORKER_JITTER_SEC", 5)

# Warmup (helps on cold deploys). If 0 → disabled.
WARMUP_MIN_SEC = _env_int("WORKER_WARMUP_MIN_SEC", 5)
WARMUP_MAX_SEC = _env_int("WORKER_WARMUP_MAX_SEC", 15)

# Per-task timeout (seconds)
RUNNER_TIMEOUT_SEC = _env_int("WORKER_RUNNER_TIMEOUT_SEC", 120)

# Adaptive backoff (on failure streak)
BACKOFF_MAX_EXP = _env_int("WORKER_BACKOFF_MAX_EXP", 3)  # caps 2^exp multiplier
BACKOFF_BASE = _env_float("WORKER_BACKOFF_BASE", 1.0)  # multiplier base on INTERVAL

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
# RUNNER SAFETY WRAPPER (timeouts)
# ────────────────────────────────────────────────
def _run_safely(fn: Callable[[], Dict[str, Any]], timeout_sec: int = RUNNER_TIMEOUT_SEC) -> Dict[str, Any]:
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(fn)
            return fut.result(timeout=timeout_sec) or {}
    except concurrent.futures.TimeoutError:
        msg = f"Timeout after {timeout_sec}s"
        print(f"⏱️ {fn.__name__} {msg}")
        return {"ok": False, "error": msg}
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
def _sleep_with_jitter(base: float):
    """Sleep with jitter to avoid multiple workers aligning."""
    j = random.randint(0, max(0, JITTER_SEC))
    time.sleep(min(3600, base + j))


def _ping_health():
    if not HEALTHCHECK_URL:
        return
    try:
        import requests

        requests.post(
            HEALTHCHECK_URL,
            json={
                "service": WORKER_NAME,
                "instance": INSTANCE_ID,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
            timeout=3,
        )
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
    keep = ("ok", "processed", "total_sent", "retried", "errors", "queued")
    filtered = {k: v for k, v in res.items() if k in keep}
    return filtered


def _sys_metrics() -> Optional[Dict[str, Any]]:
    """Lightweight system metrics (optional)."""
    try:
        import psutil  # optional

        return {
            "cpu": psutil.cpu_percent(interval=0.1),
            "mem": round(psutil.virtual_memory().percent, 1),
        }
    except Exception:
        return None


# ────────────────────────────────────────────────
# MAIN WORKER LOOP
# ────────────────────────────────────────────────
def main():
    print(f"🚀 Starting {WORKER_NAME} [{INSTANCE_ID}] | interval={INTERVAL_SEC}s idle={IDLE_INTERVAL_SEC}s jitter={JITTER_SEC}s")

    # Warmup (only if enabled)
    if (WARMUP_MAX_SEC > 0) and not RUN_ONCE:
        warm = random.randint(max(0, WARMUP_MIN_SEC), max(WARMUP_MIN_SEC, WARMUP_MAX_SEC))
        print(f"⏳ Warming up worker for {warm}s ...")
        time.sleep(warm)

    cycles = 0
    fail_streak = 0  # consecutive cycle-level failures

    try:
        while True:
            if _shutdown:
                break
            if MAX_CYCLES and cycles >= MAX_CYCLES:
                break

            cycles += 1
            DIST.hb(f"{KEY_PREFIX}:hb:{WORKER_NAME}:{INSTANCE_ID}", ttl=max(60, INTERVAL_SEC * 3))
            did_work = False
            cycle_ok = True
            results: Dict[str, Any] = {}

            lock_ttl_short = max(30, INTERVAL_SEC * 2)
            lock_ttl_long = max(60, INTERVAL_SEC * 2)

            # --- Campaign Scheduler ---
            if ENABLE_CAMPAIGNS:
                with DIST.lock("campaigns", ttl=lock_ttl_short) as ok:
                    if ok:
                        res = _run_safely(lambda: _run_campaigns(CAMPAIGN_LIMIT, CAMPAIGN_SEND_AFTER))
                        results["campaigns"] = res
                        did_work |= bool(res and (res.get("processed") or res.get("queued")))
                        cycle_ok &= bool(res.get("ok", True))
                    else:
                        _log("skip_lock", step="campaigns")

            # --- Outbound Sending ---
            if ENABLE_SEND:
                with DIST.lock("send_batch", ttl=lock_ttl_long) as ok:
                    if ok:
                        res = _run_safely(lambda: _send_batch(SEND_BATCH_LIMIT))
                        results["send_batch"] = res
                        did_work |= bool(res and res.get("total_sent"))
                        cycle_ok &= bool(res.get("ok", True))
                    else:
                        _log("skip_lock", step="send_batch")

            # --- Retry Failed Sends ---
            if ENABLE_RETRY:
                with DIST.lock("retry", ttl=lock_ttl_short) as ok:
                    if ok:
                        res = _run_safely(lambda: _run_retry(RETRY_LIMIT))
                        results["retry"] = res
                        did_work |= bool(res and res.get("retried"))
                        cycle_ok &= bool(res.get("ok", True))
                    else:
                        _log("skip_lock", step="retry")

            # --- Autoresponder (Inbound) ---
            if ENABLE_AUTORESPONDER:
                with DIST.lock("autoresponder", ttl=lock_ttl_short) as ok:
                    if ok:
                        res = _run_safely(lambda: _run_autoresponder(AUTORESPONDER_LIMIT, AUTORESPONDER_VIEW))
                        results["autoresponder"] = res
                        did_work |= bool(res and res.get("processed"))
                        cycle_ok &= bool(res.get("ok", True))
                    else:
                        _log("skip_lock", step="autoresponder")

            # --- Metrics Rollup ---
            if ENABLE_METRICS:
                with DIST.lock("metrics", ttl=lock_ttl_short) as ok:
                    if ok:
                        res = _run_safely(_update_metrics)
                        results["metrics"] = res
                        cycle_ok &= bool(res.get("ok", True))
                    else:
                        _log("skip_lock", step="metrics")

            # --- Summary & Telemetry ---
            _log("cycle", cycle=cycles, sys=_sys_metrics(), summary={k: _compact(v) for k, v in results.items()})
            _ping_health()

            # Update failure streak & choose sleep
            fail_streak = 0 if cycle_ok else min(fail_streak + 1, BACKOFF_MAX_EXP)
            if RUN_ONCE:
                break

            if did_work:
                _sleep_with_jitter(INTERVAL_SEC)
            else:
                # Adaptive backoff when cycles keep failing (protects Airtable)
                backoff_mult = BACKOFF_BASE * (2**fail_streak)
                sleep_sec = max(1.0, IDLE_INTERVAL_SEC * backoff_mult)
                _sleep_with_jitter(sleep_sec)

    finally:
        # Final metrics flush on shutdown (best-effort)
        if ENABLE_METRICS:
            try:
                res = _run_safely(_update_metrics, timeout_sec=max(30, RUNNER_TIMEOUT_SEC // 2))
                _log("metrics_flush", result=_compact(res))
            except Exception:
                traceback.print_exc()

    _log("shutdown", cycles=cycles)
    print(f"🏁 {WORKER_NAME}[{INSTANCE_ID}] stopped cleanly after {cycles} cycles.")


if __name__ == "__main__":
    main()
