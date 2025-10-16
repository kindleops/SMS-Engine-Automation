# engine_runner.py
from __future__ import annotations

import os
import sys
import json
import time
import uuid
import signal
import random
import traceback
from typing import Any, Callable, Dict, Optional, Tuple
from datetime import datetime, timezone
import argparse

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

_TRUE_VALUES = {"1", "true", "yes", "y", "on"}


def _env_bool(name: str, default: str = "0") -> bool:
    return os.getenv(name, default).strip().lower() in _TRUE_VALUES


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _prospects_limit_type(value: str) -> str | int:
    cleaned = value.strip()
    if not cleaned:
        raise argparse.ArgumentTypeError("Prospects limit cannot be empty.")
    if cleaned.upper() == "ALL":
        return "ALL"
    try:
        parsed = int(cleaned)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Prospects limit must be an integer or 'ALL'.") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("Prospects limit must be non-negative or 'ALL'.")
    return parsed


def _env_prospects_limit(default: int | str) -> int | str:
    raw = os.getenv("PROSPECTS_LIMIT")
    if raw is None:
        return default
    try:
        return _prospects_limit_type(raw)
    except argparse.ArgumentTypeError:
        return default


def _non_negative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Value must be an integer.") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("Value must be greater than or equal to 0.")
    return parsed


def _positive_int(value: str) -> int:
    parsed = _non_negative_int(value)
    if parsed == 0:
        raise argparse.ArgumentTypeError("Value must be greater than 0.")
    return parsed


# =========================
# Env / Defaults
# =========================
_DEFAULT_PROSPECTS_LIMIT: int | str = 50

ENV = {
    "ENABLE_PROSPECTS": _env_bool("ENABLE_PROSPECTS", "1"),
    "ENABLE_LEADS": _env_bool("ENABLE_LEADS", "1"),
    "ENABLE_INBOUNDS": _env_bool("ENABLE_INBOUNDS", "1"),
    "PROSPECTS_LIMIT": _env_prospects_limit(_DEFAULT_PROSPECTS_LIMIT),
    "LEADS_RETRY_LIMIT": max(0, _env_int("LEADS_RETRY_LIMIT", 100)),
    "INBOUNDS_LIMIT": max(0, _env_int("INBOUNDS_LIMIT", 25)),
    # Retries/backoff for each step
    "RETRIES": max(0, _env_int("ENGINE_RETRIES", 2)),  # per step
    "BASE_BACKOFF_SEC": max(1, _env_int("ENGINE_BASE_BACKOFF", 2)),  # exponential: 2,4,8...
    # Distributed lock (optional)
    "REDIS_URL": os.getenv("REDIS_URL") or os.getenv("UPSTASH_REDIS_URL"),
    "REDIS_TLS": _env_bool("REDIS_TLS", "true"),
    "LOCK_TTL_SEC": max(1, _env_int("ENGINE_LOCK_TTL_SEC", 300)),
    "LOCK_KEY": os.getenv("ENGINE_LOCK_KEY", "sms:engine_runner:lock"),
    # Observability
    "HEALTHCHECK_URL": os.getenv("HEALTHCHECK_URL"),  # optional POST ping
    "SERVICE_NAME": os.getenv("ENGINE_SERVICE_NAME", "engine_runner"),
    "INSTANCE_ID": os.getenv("ENGINE_INSTANCE_ID", str(uuid.uuid4())[:8]),
}

_SHUTDOWN = False


def _sig_handler(signum, frame):
    global _SHUTDOWN
    _SHUTDOWN = True


signal.signal(signal.SIGINT, _sig_handler)
signal.signal(signal.SIGTERM, _sig_handler)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def jlog(event: str, **kw):
    """Emit a structured JSON log line."""

    payload = {
        "ts": _now_iso(),
        "event": event,
        "service": ENV["SERVICE_NAME"],
        "instance": ENV["INSTANCE_ID"],
        **kw,
    }
    print(json.dumps(payload, ensure_ascii=False))


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
        pass  # best effort


# ---------- Redis lock helper ----------
class DistLock:
    def __init__(self, url: Optional[str], tls: bool, key: str, ttl: int):
        self.key = key
        self.ttl = ttl
        self.r = None
        self.token = str(uuid.uuid4())
        if url and _redis:
            try:
                self.r = _redis.from_url(url, ssl=tls, decode_responses=True, socket_timeout=3)
            except Exception:
                traceback.print_exc()
                self.r = None

    def acquire(self) -> bool:
        if not self.r:
            return True  # no redis -> single-run best effort
        try:
            return bool(self.r.set(self.key, self.token, nx=True, ex=self.ttl))
        except Exception:
            traceback.print_exc()
            return True  # fail-open

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


# ---------- Safe runner with retries ----------
def _run_step(
    name: str,
    fn: Callable[..., Any],
    *args,
    retries: int,
    base_backoff: int,
    **kwargs,
) -> Tuple[bool, Dict[str, Any]]:
    last_err: Optional[str] = None
    for attempt in range(retries + 1):
        if _SHUTDOWN:
            break
        attempt_no = attempt + 1
        try:
            jlog("step_start", step=name, attempt=attempt_no)
            result = fn(*args, **kwargs)
            normalized = _normalize_result(result)
            jlog("step_ok", step=name, attempt=attempt_no, result_summary=_compact_result(normalized))
            return True, normalized
        except Exception as exc:  # noqa: BLE001 - we want to log every error
            last_err = str(exc)
            traceback.print_exc()
            if attempt == retries:
                break
            delay = base_backoff * (2**attempt)
            delay += random.randint(0, 2)  # jitter to avoid thundering herd
            jlog("step_retry", step=name, attempt=attempt_no, delay_sec=delay, error=last_err)
            time.sleep(delay)
    jlog("step_fail", step=name, error=last_err or "unknown")
    return False, {"ok": False, "error": last_err or "unknown"}


def _normalize_result(result: Any) -> Dict[str, Any]:
    if isinstance(result, dict):
        normalized = dict(result)
        normalized["ok"] = bool(normalized.get("ok", True))
        return normalized

    return {"result": result, "ok": bool(result)}


def _compact_result(result: Any) -> Dict[str, Any]:
    """Trim big dicts to the fields you care about in logs."""
    if not isinstance(result, dict):
        return {"ok": bool(result)}
    keys = (
        "ok",
        "processed",
        "results",
        "total_sent",
        "retried",
        "errors",
        "quiet_hours",
        "result",
        "error",
    )
    return {k: result.get(k) for k in keys if k in result}


def _normalize_prospects_limit(limit: int | str) -> int | str:
    if isinstance(limit, str):
        return "ALL"
    return limit


# ---------- CLI ----------
def _parse_args():
    p = argparse.ArgumentParser(description="Run the SMS engine steps once (cron-friendly).")
    p.add_argument("--prospects", action="store_true", help="Run prospects step (outbound queue).")
    p.add_argument("--leads", action="store_true", help="Run leads step (retries/followups).")
    p.add_argument("--inbounds", action="store_true", help="Run inbounds step (autoresponder).")
    p.add_argument("--all", action="store_true", help="Run all steps (default).")

    p.add_argument("--prospects-limit", type=_prospects_limit_type, default=ENV["PROSPECTS_LIMIT"])
    p.add_argument("--leads-retry-limit", type=_non_negative_int, default=ENV["LEADS_RETRY_LIMIT"])
    p.add_argument("--inbounds-limit", type=_non_negative_int, default=ENV["INBOUNDS_LIMIT"])

    p.add_argument("--retries", type=_non_negative_int, default=ENV["RETRIES"])
    p.add_argument("--backoff", type=_positive_int, default=ENV["BASE_BACKOFF_SEC"])
    p.add_argument("--no-lock", action="store_true", help="Skip distributed lock.")
    return p.parse_args()


def main():
    if run_engine is None:
        jlog("fatal_import", error=_IMP_ERR_TXT)
        print(_IMP_ERR_TXT, file=sys.stderr)
        sys.exit(2)

    args = _parse_args()

    # Determine which steps to run (CLI overrides env toggles)
    run_all = args.all or (not args.prospects and not args.leads and not args.inbounds)
    do_prospects = (args.prospects or run_all) and ENV["ENABLE_PROSPECTS"]
    do_leads = (args.leads or run_all) and ENV["ENABLE_LEADS"]
    do_inbounds = (args.inbounds or run_all) and ENV["ENABLE_INBOUNDS"]

    # Distributed lock (so only one cron instance executes at a time)
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

    try:
        step_plan = [
            ("prospects", do_prospects, {"limit": _normalize_prospects_limit(args.prospects_limit)}),
            ("leads", do_leads, {"retry_limit": args.leads_retry_limit}),
            ("inbounds", do_inbounds, {"limit": args.inbounds_limit}),
        ]

        for step_name, should_run, call_kwargs in step_plan:
            if not should_run or _SHUTDOWN:
                continue

            ok, res = _run_step(
                step_name,
                run_engine,
                step_name,
                retries=args.retries,
                base_backoff=args.backoff,
                **call_kwargs,
            )
            _health_ping(step_name, ok=ok, extra=_compact_result(res))
            if not ok:
                exit_code = 1

        jlog("runner_finish", code=exit_code)

    finally:
        try:
            lock.release()
        except Exception:
            traceback.print_exc()

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
