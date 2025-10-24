"""
⚙️  Engine Runner — Hardened Edition
-----------------------------------
Adds:
  • Per-step timeout (prevents stuck Airtable/API calls)
  • Signal-safe retry/backoff loop
  • Compact structured logs
  • Health pings at start and finish
  • Graceful shutdown + Redis-safe cleanup
"""

from __future__ import annotations

import os
import sys
import json
import time
import uuid
import signal
import random
import traceback
import argparse
import concurrent.futures
from typing import Any, Dict, Optional, Tuple
from datetime import datetime, timezone

# ---------- Optional Redis (for distributed lock; safe fallback) ----------
try:
    import redis as _redis
except Exception:
    _redis = None

# ---------- Try to import the dispatcher (safe fallback) ----------
try:
    from sms.dispatcher import run_engine
except Exception as _imp_err:
    run_engine = None
    _IMP_ERR_TXT = f"Dispatcher import failed: {_imp_err}"

# =========================
# Env / Defaults
# =========================
ENV = {
    "ENABLE_PROSPECTS": os.getenv("ENABLE_PROSPECTS", "1").lower() in ("1", "true", "yes"),
    "ENABLE_LEADS": os.getenv("ENABLE_LEADS", "1").lower() in ("1", "true", "yes"),
    "ENABLE_INBOUNDS": os.getenv("ENABLE_INBOUNDS", "1").lower() in ("1", "true", "yes"),
    "PROSPECTS_LIMIT": os.getenv("PROSPECTS_LIMIT", "50"),  # "ALL" or number
    "LEADS_RETRY_LIMIT": int(os.getenv("LEADS_RETRY_LIMIT", "100")),
    "INBOUNDS_LIMIT": int(os.getenv("INBOUNDS_LIMIT", "25")),
    # Retries/backoff for each step
    "RETRIES": int(os.getenv("ENGINE_RETRIES", "2")),
    "BASE_BACKOFF_SEC": int(os.getenv("ENGINE_BASE_BACKOFF", "2")),
    # Timeout per step
    "STEP_TIMEOUT_SEC": int(os.getenv("ENGINE_STEP_TIMEOUT_SEC", "180")),
    # Distributed lock
    "REDIS_URL": os.getenv("REDIS_URL") or os.getenv("UPSTASH_REDIS_URL"),
    "REDIS_TLS": os.getenv("REDIS_TLS", "true").lower() in ("1", "true", "yes"),
    "LOCK_TTL_SEC": int(os.getenv("ENGINE_LOCK_TTL_SEC", "300")),
    "LOCK_KEY": os.getenv("ENGINE_LOCK_KEY", "sms:engine_runner:lock"),
    # Observability
    "HEALTHCHECK_URL": os.getenv("HEALTHCHECK_URL"),
    "SERVICE_NAME": os.getenv("ENGINE_SERVICE_NAME", "engine_runner"),
    "INSTANCE_ID": os.getenv("ENGINE_INSTANCE_ID", str(uuid.uuid4())[:8]),
}

_SHUTDOWN = False


# ---------- Signal Handling ----------
def _sig_handler(signum, frame):
    global _SHUTDOWN
    _SHUTDOWN = True
    jlog("signal", sig=signum, note="shutdown_requested")


signal.signal(signal.SIGINT, _sig_handler)
signal.signal(signal.SIGTERM, _sig_handler)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def jlog(event: str, **kw):
    print(
        json.dumps(
            {"ts": _now_iso(), "event": event, "service": ENV["SERVICE_NAME"], "instance": ENV["INSTANCE_ID"], **kw},
            ensure_ascii=False,
        )
    )


def _health_ping(stage: str, ok: bool, extra: Optional[Dict[str, Any]] = None):
    url = ENV["HEALTHCHECK_URL"]
    if not url:
        return
    try:
        import requests

        payload = {
            "ts": _now_iso(),
            "service": ENV["SERVICE_NAME"],
            "instance": ENV["INSTANCE_ID"],
            "stage": stage,
            "ok": bool(ok),
        }
        if extra:
            payload.update(extra)
        requests.post(url, json=payload, timeout=3)
    except Exception:
        pass


# ---------- Redis lock helper ----------
class DistLock:
    def __init__(self, url: Optional[str], tls: bool, key: str, ttl: int):
        self.key = key
        self.ttl = ttl
        self.token = str(uuid.uuid4())
        self.r = None
        if url and _redis:
            try:
                self.r = _redis.from_url(url, ssl=tls, decode_responses=True, socket_timeout=3)
            except Exception:
                traceback.print_exc()
                self.r = None

    def acquire(self) -> bool:
        if not self.r:
            return True
        try:
            return bool(self.r.set(self.key, self.token, nx=True, ex=self.ttl))
        except Exception:
            traceback.print_exc()
            return True

    def release(self):
        if not self.r:
            return
        try:
            lua = """
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('DEL', KEYS[1])
            else
                return 0
            end
            """
            self.r.eval(lua, 1, self.key, self.token)
        except Exception:
            traceback.print_exc()


# ---------- Timeout-safe wrapper ----------
def _run_with_timeout(fn, timeout_sec: int, *args, **kwargs):
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(fn, *args, **kwargs)
            return fut.result(timeout=timeout_sec)
    except concurrent.futures.TimeoutError:
        raise RuntimeError(f"timeout_after_{timeout_sec}s")


# ---------- Step Runner with retries ----------
def _run_step(name: str, fn, *args, retries: int, base_backoff: int, **kwargs) -> Tuple[bool, Dict[str, Any]]:
    attempts, last_err = 0, None
    timeout_sec = ENV["STEP_TIMEOUT_SEC"]

    while attempts <= retries and not _SHUTDOWN:
        try:
            jlog("step_start", step=name, attempt=attempts + 1, timeout_sec=timeout_sec)
            rv = _run_with_timeout(fn, timeout_sec, *args, **kwargs)
            jlog("step_ok", step=name, attempt=attempts + 1, result_summary=_compact_result(rv))
            return True, rv if isinstance(rv, dict) else {"result": rv}
        except Exception as e:
            last_err = str(e)
            traceback.print_exc()
            if attempts == retries:
                break
            delay = base_backoff * (2**attempts) + random.randint(0, 2)
            jlog("step_retry", step=name, attempt=attempts + 1, delay_sec=delay, error=last_err)
            end = time.time() + delay
            while time.time() < end and not _SHUTDOWN:
                time.sleep(0.25)
            attempts += 1
    jlog("step_fail", step=name, error=last_err or "unknown")
    return False, {"ok": False, "error": last_err or "unknown"}


def _compact_result(result: Any) -> Dict[str, Any]:
    if not isinstance(result, dict):
        return {"ok": bool(result)}
    keys = ("ok", "processed", "results", "total_sent", "retried", "errors", "quiet_hours", "queued")
    return {k: result.get(k) for k in keys if k in result}


# ---------- CLI ----------
def _parse_args():
    p = argparse.ArgumentParser(description="Run the SMS engine steps once (cron-friendly).")
    p.add_argument("--prospects", action="store_true", help="Run prospects step (outbound queue).")
    p.add_argument("--leads", action="store_true", help="Run leads step (retries/followups).")
    p.add_argument("--inbounds", action="store_true", help="Run inbounds step (autoresponder).")
    p.add_argument("--all", action="store_true", help="Run all steps (default).")
    p.add_argument("--prospects-limit", type=str, default=ENV["PROSPECTS_LIMIT"])
    p.add_argument("--leads-retry-limit", type=int, default=ENV["LEADS_RETRY_LIMIT"])
    p.add_argument("--inbounds-limit", type=int, default=ENV["INBOUNDS_LIMIT"])
    p.add_argument("--retries", type=int, default=ENV["RETRIES"])
    p.add_argument("--backoff", type=int, default=ENV["BASE_BACKOFF_SEC"])
    p.add_argument("--no-lock", action="store_true", help="Skip distributed lock.")
    return p.parse_args()


# ---------- MAIN ----------
def main():
    if run_engine is None:
        jlog("fatal_import", error=_IMP_ERR_TXT)
        print(_IMP_ERR_TXT, file=sys.stderr)
        sys.exit(2)

    args = _parse_args()

    run_all = args.all or (not args.prospects and not args.leads and not args.inbounds)
    do_prospects = (args.prospects or run_all) and ENV["ENABLE_PROSPECTS"]
    do_leads = (args.leads or run_all) and ENV["ENABLE_LEADS"]
    do_inbounds = (args.inbounds or run_all) and ENV["ENABLE_INBOUNDS"]

    lock = DistLock(
        url=None if args.no_lock else ENV["REDIS_URL"],
        tls=ENV["REDIS_TLS"],
        key=ENV["LOCK_KEY"],
        ttl=ENV["LOCK_TTL_SEC"],
    )
    acquired = lock.acquire()
    if not acquired:
        jlog("skip_run_locked", lock_key=ENV["LOCK_KEY"])
        _health_ping("locked", ok=False, extra={"lock_key": ENV["LOCK_KEY"]})
        sys.exit(0)

    exit_code = 0
    jlog("runner_start", steps={"prospects": do_prospects, "leads": do_leads, "inbounds": do_inbounds})
    _health_ping("runner_start", ok=True)

    try:
        # --- Prospects ---
        if do_prospects and not _SHUTDOWN:
            limit = args.prospects_limit.strip() if isinstance(args.prospects_limit, str) else str(args.prospects_limit)
            ok, res = _run_step(
                "prospects",
                run_engine,
                "prospects",
                limit=("ALL" if limit.upper() == "ALL" else int(limit)),
                retries=args.retries,
                base_backoff=args.backoff,
            )
            _health_ping("prospects", ok=ok, extra=_compact_result(res))
            if not ok:
                exit_code = 1

        # --- Leads ---
        if do_leads and not _SHUTDOWN:
            ok, res = _run_step(
                "leads",
                run_engine,
                "leads",
                retry_limit=int(args.leads_retry_limit),
                retries=args.retries,
                base_backoff=args.backoff,
            )
            _health_ping("leads", ok=ok, extra=_compact_result(res))
            if not ok:
                exit_code = 1

        # --- Inbounds ---
        if do_inbounds and not _SHUTDOWN:
            ok, res = _run_step(
                "inbounds",
                run_engine,
                "inbounds",
                limit=int(args.inbounds_limit),
                retries=args.retries,
                base_backoff=args.backoff,
            )
            _health_ping("inbounds", ok=ok, extra=_compact_result(res))
            if not ok:
                exit_code = 1

        jlog("runner_finish", code=exit_code)
        _health_ping("runner_finish", ok=(exit_code == 0))

    finally:
        try:
            lock.release()
        except Exception:
            traceback.print_exc()

    sys.exit(exit_code)


if __name__ == "__main__":
    main()