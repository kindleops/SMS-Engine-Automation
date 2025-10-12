# sms/main.py
from __future__ import annotations

import os
from datetime import datetime, timezone, date
from typing import Optional, Dict, Any

from fastapi import FastAPI, Header, HTTPException, Query
from dotenv import load_dotenv

# ── Safe Airtable helpers (no hard Table import) ─────────────────────────────
try:
    from sms.airtable_client import get_runs, get_kpis, get_leads, get_convos, get_templates, safe_create, remap_existing_only
except Exception:  # extreme fallback
    get_runs = get_kpis = get_leads = get_convos = get_templates = lambda: None

    def safe_create(*_a, **_k):
        return None  # type: ignore

    def remap_existing_only(*_a, **_k):
        return {}  # type: ignore


# ── Core engine modules (each guarded) ──────────────────────────────────────
try:
    from sms.outbound_batcher import send_batch
except Exception:

    def send_batch(*_a, **_k):
        return {"ok": False, "error": "send_batch unavailable"}


try:
    from sms.autoresponder import run_autoresponder
except Exception:

    def run_autoresponder(*_a, **_k):
        return {"ok": False, "error": "autoresponder unavailable"}


# Prefer the file you shipped as reset_daily_quotas.py
try:
    from sms.quota_reset import reset_daily_quotas  # fallback to old name if reset_daily_quotas.py is missing
except Exception:

    def reset_daily_quotas(*_a, **_k):
        return {"ok": False, "error": "quota reset unavailable"}


try:
    from sms.metrics_tracker import update_metrics, _notify  # type: ignore
except Exception:

    def update_metrics(*_a, **_k):
        return {"ok": False, "error": "metrics tracker unavailable"}

    def _notify(msg: str):  # best-effort stub
        print(f"[notify] {msg}")


try:
    from sms.inbound_webhook import router as inbound_router
except Exception:
    inbound_router = None

try:
    from sms.campaign_runner import run_campaigns
except Exception:

    def run_campaigns(*_a, **_k):
        return {"ok": False, "error": "campaign runner unavailable"}


try:
    from sms.kpi_aggregator import aggregate_kpis
except Exception:

    def aggregate_kpis(*_a, **_k):
        return {"ok": False, "error": "kpi aggregator unavailable"}


try:
    from sms.retry_runner import run_retry
except Exception:

    def run_retry(*_a, **_k):
        return {"ok": False, "error": "retry runner unavailable"}


try:
    from sms.followup_flow import run_followups
except Exception:

    def run_followups(*_a, **_k):
        return {"ok": True, "skipped": "followups unavailable"}


try:
    from sms.dispatcher import run_engine
except Exception:

    def run_engine(*_a, **_k):
        return {"ok": False, "error": "dispatcher unavailable"}


try:
    from sms.health_strict import strict_health
except Exception:

    def strict_health(mode: str):
        return {"ok": True, "mode": mode, "note": "strict health shim"}


# Optional Numbers admin tools (name-compatible wrappers)
try:
    from sms.admin_numbers import (
        backfill_numbers_for_existing_queue,
    )

    # Back-compat names used by older code:
    def backfill_drip_from_numbers(dry_run: bool = True):
        # our impl doesn't do dry_run; return actual changes + flag for transparency
        res = backfill_numbers_for_existing_queue()
        res["dry_run_ignored"] = dry_run
        return res

    def recalc_numbers_sent_today(for_date: str):
        # Not implemented in admin_numbers; report gracefully
        return {"ok": True, "note": "recalc not implemented in this build", "date": for_date}

    def reset_numbers_daily_counters():
        # Not implemented in admin_numbers; recommend using reset_daily_quotas endpoint
        return {"ok": True, "note": "use /reset-quotas which resets Numbers daily counters"}
except Exception:

    def backfill_drip_from_numbers(*_a, **_k):
        return {"ok": False, "error": "admin_numbers missing"}

    def recalc_numbers_sent_today(*_a, **_k):
        return {"ok": False, "error": "admin_numbers missing"}

    def reset_numbers_daily_counters(*_a, **_k):
        return {"ok": False, "error": "admin_numbers missing"}


# ── Load .env (relative to repo root) ───────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, "..", ".env")
load_dotenv(dotenv_path=ENV_PATH, override=True)

# ── FastAPI app ─────────────────────────────────────────────────────────────
app = FastAPI(title="REI SMS Engine", version="1.2")
if inbound_router:
    app.include_router(inbound_router)

# ── ENV CONFIG ──────────────────────────────────────────────────────────────
CRON_TOKEN = os.getenv("CRON_TOKEN")
TEST_MODE = os.getenv("TEST_MODE", "false").lower() in ("1", "true", "yes")
STRICT_MODE = os.getenv("STRICT_MODE", "false").lower() in ("1", "true", "yes")

# Quiet hours (Central Time) – default 21:00–09:00 local (no sends)
QUIET_HOURS_ENFORCED = os.getenv("QUIET_HOURS_ENFORCED", "true").lower() in ("1", "true", "yes")
QUIET_START = int(os.getenv("QUIET_START_HOUR_LOCAL", "21"))
QUIET_END = int(os.getenv("QUIET_END_HOUR_LOCAL", "9"))
ALLOW_QUEUE_OUTSIDE_HOURS = os.getenv("ALLOW_QUEUE_OUTSIDE_HOURS", "true").lower() in ("1", "true", "yes")
AUTORESPONDER_ALWAYS_ON = os.getenv("AUTORESPONDER_ALWAYS_ON", "true").lower() in ("1", "true", "yes")

# Bases (for startup hints only)
PERF_BASE = os.getenv("PERFORMANCE_BASE") or os.getenv("AIRTABLE_PERFORMANCE_BASE_ID")
LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")


# ── Helpers ─────────────────────────────────────────────────────────────────
def log_error(context: str, err: Exception | str):
    msg = f"❌ {context}: {err}"
    print(msg)
    try:
        _notify(msg)
    except Exception:
        pass


def check_token(x_cron_token: Optional[str]):
    if CRON_TOKEN and x_cron_token != CRON_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")


def iso_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def get_perf_tables():
    return get_runs(), get_kpis()


def log_run(runs_tbl, step: str, result: Dict[str, Any]):
    if not runs_tbl:
        return None
    try:
        payload = {
            "Type": step,
            "Processed": float(result.get("processed") or result.get("total_sent") or result.get("sent") or 0),
            "Breakdown": str(result),
            "Timestamp": iso_timestamp(),
        }
        return safe_create(runs_tbl, payload)
    except Exception as e:
        log_error(f"Log Run {step}", e)
        return None


def log_kpi(kpis_tbl, metric: str, value: int | float):
    if not kpis_tbl:
        return None
    try:
        payload = {
            "Campaign": "ALL",
            "Metric": metric,
            "Value": float(value),
            "Date": datetime.now(timezone.utc).date().isoformat(),
        }
        return safe_create(kpis_tbl, payload)
    except Exception as e:
        log_error(f"Log KPI {metric}", e)
        return None


# Quiet hours helpers (Central)
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # py>=3.9 should have this


def central_now():
    if ZoneInfo:
        return datetime.now(ZoneInfo("America/Chicago"))
    return datetime.now(timezone.utc)


def is_quiet_hours_local() -> bool:
    """True if now is within quiet hours in America/Chicago."""
    if not QUIET_HOURS_ENFORCED:
        return False
    h = central_now().hour
    # quiet if 21:00–23:59 or 00:00–08:59 by default
    return (h >= QUIET_START) or (h < QUIET_END)


# ── Startup Checks ──────────────────────────────────────────────────────────
@app.on_event("startup")
def startup_checks():
    try:
        print("✅ Environment loaded:")
        print(f"   LEADS_CONVOS_BASE: {LEADS_CONVOS_BASE}")
        print(f"   PERFORMANCE_BASE: {PERF_BASE}")
        print(f"   STRICT_MODE: {STRICT_MODE}, TEST_MODE: {TEST_MODE}")
        print(f"   QUIET_HOURS_ENFORCED: {QUIET_HOURS_ENFORCED} (Local {QUIET_START:02d}:00–{QUIET_END:02d}:00)")

        missing = [
            k
            for k in ["AIRTABLE_API_KEY", "LEADS_CONVOS_BASE", "PERFORMANCE_BASE"]
            if not os.getenv(k) and not os.getenv(k.replace("BASE", "_BASE_ID"))
        ]
        if missing:
            msg = f"🚨 Missing env vars → {', '.join(missing)}"
            log_error("Startup checks", msg)
            if STRICT_MODE:
                raise RuntimeError(msg)

        # Light smoke test: attempt to build handles (won't crash if missing)
        _ = get_templates()
        _ = get_leads()
        print("✅ Startup checks passed")
    except Exception as e:
        log_error("Startup exception", e)
        if STRICT_MODE:
            raise


# ── Health ─────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {
        "ok": True,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "quiet_hours": is_quiet_hours_local(),
        "local_time_central": central_now().isoformat(),
        "version": "1.2",
    }


@app.get("/health/strict")
def health_strict_endpoint(mode: str = Query("prospects", description="prospects | leads | inbounds")):
    return strict_health(mode=mode)


# ── Outbound / Campaigns ───────────────────────────────────────────────────
@app.post("/send")
async def send_endpoint(
    x_cron_token: str | None = Header(None),
    campaign_id: str | None = Query(None),
    limit: int = Query(500),
):
    check_token(x_cron_token)
    if TEST_MODE:
        return {"ok": True, "status": "mock_send", "campaign": campaign_id}
    if is_quiet_hours_local():
        return {"ok": False, "error": "Quiet hours in effect (Central). Sending blocked.", "quiet_hours": True}
    return send_batch(campaign_id=campaign_id, limit=limit)


@app.post("/run-campaigns")
async def run_campaigns_endpoint(
    limit: str = Query("ALL"),
    x_cron_token: str | None = Header(None),
    send_after_queue: Optional[bool] = Query(None, description="Override default. If True, attempts to send immediately."),
):
    """
    Runs campaign queueing. During quiet hours:
      - If ALLOW_QUEUE_OUTSIDE_HOURS=True, queue only (no send).
      - Otherwise, skip entirely.
    """
    check_token(x_cron_token)
    if TEST_MODE:
        return {"ok": True, "status": "mock_campaign_runner"}

    if is_quiet_hours_local():
        if not ALLOW_QUEUE_OUTSIDE_HOURS:
            return {"ok": False, "error": "Quiet hours in effect (Central). Queueing disabled.", "quiet_hours": True}
        res = run_campaigns(limit=None if str(limit).upper() == "ALL" else int(limit), send_after_queue=False)
        res["note"] = "Queued only (quiet hours)."
        res["quiet_hours"] = True
        return res

    return run_campaigns(limit=None if str(limit).upper() == "ALL" else int(limit), send_after_queue=send_after_queue)


# ── Autoresponder / Followups / Retry / KPIs ───────────────────────────────
@app.post("/autoresponder/{mode}")
async def autoresponder_endpoint(
    mode: str,
    limit: int = 50,
    view: str = "Unprocessed Inbounds",
    x_cron_token: str | None = Header(None),
):
    """
    Modes: autoresponder | ai-closer | manual-qa
    Autoresponder is allowed 24/7 by default (AUTORESPONDER_ALWAYS_ON).
    """
    check_token(x_cron_token)
    os.environ["PROCESSED_BY_LABEL"] = mode.replace("-", " ").title()
    if TEST_MODE:
        return {"ok": True, "status": "mock_autoresponder", "mode": mode}
    if not AUTORESPONDER_ALWAYS_ON and is_quiet_hours_local():
        return {"ok": False, "error": "Quiet hours in effect (Central). Autoresponder disabled by config.", "quiet_hours": True}
    return run_autoresponder(limit=limit, view=view)


@app.post("/reset-quotas")
async def reset_quotas_endpoint(x_cron_token: str | None = Header(None)):
    check_token(x_cron_token)
    return reset_daily_quotas()


@app.post("/metrics")
async def metrics_endpoint(x_cron_token: str | None = Header(None)):
    check_token(x_cron_token)
    return update_metrics()


@app.post("/retry")
async def retry_endpoint(
    limit: int = 100,
    view: str | None = None,
    x_cron_token: str | None = Header(None),
):
    check_token(x_cron_token)
    if TEST_MODE:
        return {"ok": True, "status": "mock_retry"}
    if is_quiet_hours_local():
        return {"ok": False, "error": "Quiet hours in effect (Central). Retries blocked.", "quiet_hours": True}
    return run_retry(limit=limit, view=view)


@app.post("/aggregate-kpis")
async def aggregate_kpis_endpoint(x_cron_token: str | None = Header(None)):
    check_token(x_cron_token)
    return aggregate_kpis()


# One-shot “all steps” cron – smart about quiet hours
@app.post("/cron/all")
async def cron_all_endpoint(
    limit: int = 500,
    x_cron_token: str | None = Header(None),
):
    """
    Order of ops:
      - STRICT health checks (prospects, leads, inbounds)
      - If quiet hours:
          * Autoresponder (if allowed), Metrics, Aggregate KPIs, Campaign queue-only
          * Skip send/retry
      - If normal hours:
          * Send, Autoresponder, Followups, Metrics, Retry, Aggregate KPIs, Campaigns
    """
    check_token(x_cron_token)
    results: Dict[str, Any] = {}
    totals = {"processed": 0, "errors": 0}
    runs_tbl, kpis_tbl = get_perf_tables()

    # health gates
    for mode in ["prospects", "leads", "inbounds"]:
        health_result = strict_health(mode)
        results[f"{mode}_health"] = health_result
        if not health_result.get("ok"):
            log_run(runs_tbl, f"{mode.upper()}_HEALTH_FAIL", health_result)
            return {"ok": False, "error": f"Health check failed for {mode}", "results": results}

    # Quiet hours
    if is_quiet_hours_local():
        if AUTORESPONDER_ALWAYS_ON:
            try:
                r = run_autoresponder(limit=50, view="Unprocessed Inbounds")
                results["autoresponder"] = r
                log_run(runs_tbl, "AUTORESPONDER", r)
                totals["processed"] += r.get("processed", 0)
            except Exception as e:
                log_error("AUTORESPONDER", e)
                totals["errors"] += 1

        results["followups"] = {"ok": True, "skipped": "quiet_hours"}

        for step_name, func in (("METRICS", update_metrics), ("AGGREGATE_KPIS", aggregate_kpis)):
            try:
                r = func()
                results[step_name.lower()] = r
                log_run(runs_tbl, step_name, r)
            except Exception as e:
                log_error(step_name, e)
                totals["errors"] += 1

        if ALLOW_QUEUE_OUTSIDE_HOURS:
            try:
                r = run_campaigns(limit=None, send_after_queue=False)
                r["note"] = "Queued only (quiet hours)."
                r["quiet_hours"] = True
                results["campaign_runner"] = r
                log_run(runs_tbl, "CAMPAIGN_RUNNER", r)
            except Exception as e:
                log_error("CAMPAIGN_RUNNER", e)
                totals["errors"] += 1
        else:
            results["campaign_runner"] = {"ok": True, "skipped": "quiet_hours"}

        results["outbound"] = {"ok": True, "skipped": "quiet_hours"}
        results["retry"] = {"ok": True, "skipped": "quiet_hours"}

        log_kpi(kpis_tbl, "TOTAL_PROCESSED", totals["processed"])
        log_kpi(kpis_tbl, "TOTAL_ERRORS", totals["errors"])
        results["timestamp"] = iso_timestamp()
        results["quiet_hours"] = True
        return {"ok": True, "results": results, "totals": totals}

    # Normal hours
    steps = [
        ("OUTBOUND", lambda: send_batch(limit=limit)),
        ("AUTORESPONDER", lambda: run_autoresponder(limit=50, view="Unprocessed Inbounds")),
        ("FOLLOWUPS", run_followups),
        ("METRICS", update_metrics),
        ("RETRY", lambda: run_retry(limit=100)),
        ("AGGREGATE_KPIS", aggregate_kpis),
        ("CAMPAIGN_RUNNER", lambda: run_campaigns(limit=None)),
    ]
    for step, func in steps:
        try:
            r = {"ok": True, "status": f"mock_{step.lower()}"} if TEST_MODE else func()
            results[step.lower()] = r
            log_run(runs_tbl, step, r)
            totals["processed"] += r.get("processed", 0)
        except Exception as e:
            log_error(step, e)
            totals["errors"] += 1

    log_kpi(kpis_tbl, "TOTAL_PROCESSED", totals["processed"])
    log_kpi(kpis_tbl, "TOTAL_ERRORS", totals["errors"])
    return {"ok": True, "results": results, "totals": totals, "timestamp": iso_timestamp()}


# ── Numbers Admin (from_number + quotas) ────────────────────────────────────
@app.post("/admin/numbers/backfill")
def numbers_backfill_endpoint(
    dry_run: bool = Query(True),
    x_cron_token: str | None = Header(None),
):
    """
    Fills Drip Queue.from_number for QUEUED items using Numbers table (market-aware).
    """
    check_token(x_cron_token)
    try:
        return backfill_drip_from_numbers(dry_run=dry_run)
    except Exception as e:
        log_error("numbers_backfill", e)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/admin/numbers/recalc")
def numbers_recalc_endpoint(
    for_date: Optional[str] = Query(None, description="YYYY-MM-DD (defaults to today in Central)"),
    x_cron_token: str | None = Header(None),
):
    """
    Recalculates Numbers.'Sent Today' by scanning Drip Queue for the given date.
    (Shim returns a helpful note if not implemented in this build.)
    """
    check_token(x_cron_token)
    try:
        target = for_date or date.fromisoformat(central_now().date().isoformat()).isoformat()
        return recalc_numbers_sent_today(target)
    except Exception as e:
        log_error("numbers_recalc", e)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/admin/numbers/reset")
def numbers_reset_endpoint(
    x_cron_token: str | None = Header(None),
):
    """
    Resets daily counters on Numbers (shim will suggest /reset-quotas if needed).
    """
    check_token(x_cron_token)
    try:
        return reset_numbers_daily_counters()
    except Exception as e:
        log_error("numbers_reset", e)
        raise HTTPException(status_code=500, detail=str(e))


# ── Engine (manual) ─────────────────────────────────────────────────────────
@app.get("/engine")
def trigger_engine(mode: str, limit: int = 50, retry_limit: int = 100):
    valid_modes = {"prospects", "leads", "inbounds"}
    if mode not in valid_modes:
        raise HTTPException(status_code=400, detail=f"Invalid mode '{mode}'")
    health = strict_health(mode)
    if not health.get("ok"):
        raise HTTPException(status_code=500, detail=f"Health check failed for {mode}")
    return {"ok": True, "mode": mode, "result": run_engine(mode, limit=limit, retry_limit=retry_limit)}
