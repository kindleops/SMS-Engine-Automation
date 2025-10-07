# sms/main.py
from __future__ import annotations

import os
from datetime import datetime, timezone, date
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Query
from pyairtable import Table
from dotenv import load_dotenv

# Core engine modules
from sms.outbound_batcher import send_batch
from sms.autoresponder import run_autoresponder
from sms.quota_reset import reset_daily_quotas
from sms.metrics_tracker import update_metrics, _notify
from sms.inbound_webhook import router as inbound_router
from sms.campaign_runner import run_campaigns
from sms.kpi_aggregator import aggregate_kpis
from sms.retry_runner import run_retry
from sms.followup_flow import run_followups
from sms.dispatcher import run_engine
from sms.health_strict import strict_health

# Optional Numbers admin tools (used by several endpoints below)
try:
    from sms.admin_numbers import (
        backfill_drip_from_numbers,
        recalc_numbers_sent_today,
        reset_numbers_daily_counters,
    )
except Exception:
    # Safe fallbacks if the helpers aren’t present in this build
    def backfill_drip_from_numbers(*args, **kwargs): return {"ok": False, "error": "admin_numbers missing"}
    def recalc_numbers_sent_today(*args, **kwargs): return {"ok": False, "error": "admin_numbers missing"}
    def reset_numbers_daily_counters(*args, **kwargs): return {"ok": False, "error": "admin_numbers missing"}

# --- Load .env ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, "..", ".env")
load_dotenv(dotenv_path=ENV_PATH, override=True)

# --- FastAPI app ---
app = FastAPI(title="REI SMS Engine", version="1.2")
app.include_router(inbound_router)

# --- ENV CONFIG ---
CRON_TOKEN = os.getenv("CRON_TOKEN")
TEST_MODE = os.getenv("TEST_MODE", "false").lower() in ("1", "true", "yes")
STRICT_MODE = os.getenv("STRICT_MODE", "false").lower() in ("1", "true", "yes")

# Quiet hours (Central Time) – default 09:00–21:00
QUIET_HOURS_ENFORCED = os.getenv("QUIET_HOURS_ENFORCED", "true").lower() in ("1", "true", "yes")
QUIET_START = int(os.getenv("QUIET_START_HOUR_LOCAL", "21"))  # 21:00 local (CST/CDT)
QUIET_END = int(os.getenv("QUIET_END_HOUR_LOCAL", "9"))       # 09:00 local (CST/CDT)
ALLOW_QUEUE_OUTSIDE_HOURS = os.getenv("ALLOW_QUEUE_OUTSIDE_HOURS", "true").lower() in ("1", "true", "yes")
AUTORESPONDER_ALWAYS_ON = os.getenv("AUTORESPONDER_ALWAYS_ON", "true").lower() in ("1", "true", "yes")

# Bases & keys
PERF_BASE = os.getenv("PERFORMANCE_BASE")
PERF_KEY = os.getenv("AIRTABLE_REPORTING_KEY") or os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE")
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")

# Tables
TEMPLATES_TABLE = os.getenv("TEMPLATES_TABLE", "Templates")
LEADS_TABLE = os.getenv("LEADS_TABLE", "Leads")

# Airtable clients (smoke-test in startup)
templates = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, TEMPLATES_TABLE) if AIRTABLE_API_KEY else None
leads = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, LEADS_TABLE) if AIRTABLE_API_KEY else None

# -------------------------
# Helpers
# -------------------------
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
    if not (PERF_KEY and PERF_BASE):
        return None, None
    try:
        runs = Table(PERF_KEY, PERF_BASE, "Runs/Logs")
        kpis = Table(PERF_KEY, PERF_BASE, "KPIs")
        return runs, kpis
    except Exception as e:
        log_error("Init Performance tables", e)
        return None, None

def log_run(runs: Optional[Table], step: str, result: dict):
    if not runs: return None
    try:
        return runs.create({
            "Type": step,
            "Processed": result.get("processed") or result.get("total_sent") or result.get("sent") or 0,
            "Breakdown": str(result),
            "Timestamp": iso_timestamp(),
        })
    except Exception as e:
        log_error(f"Log Run {step}", e)
        return None

def log_kpi(kpis: Optional[Table], metric: str, value: int | float):
    if not kpis: return None
    try:
        return kpis.create({
            "Campaign": "ALL",
            "Metric": metric,
            "Value": value,
            "Date": datetime.now(timezone.utc).date().isoformat(),
        })
    except Exception as e:
        log_error(f"Log KPI {metric}", e)
        return None

# --- Quiet hours (Central) helpers: 09:00–21:00 local ---
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # Should exist on py3.9+

def central_now():
    if ZoneInfo:
        return datetime.now(ZoneInfo("America/Chicago"))
    # Fallback: UTC printed, still enforces via hours below if you choose to map
    return datetime.now(timezone.utc)

def is_quiet_hours_local() -> bool:
    """True if we are within quiet hours in America/Chicago."""
    if not QUIET_HOURS_ENFORCED:
        return False
    now_local = central_now()
    hour = now_local.hour
    # Quiet between QUIET_START -> 24 and 0 -> QUIET_END (wrap-around)
    if QUIET_START <= 23 and QUIET_END >= 0 and QUIET_START != QUIET_END:
        if QUIET_START < 24:
            if hour >= QUIET_START:  # from start to midnight
                return True
        if hour < QUIET_END:        # from midnight to end
            return True
    return False

# -------------------------
# Startup Checks
# -------------------------
@app.on_event("startup")
def startup_checks():
    try:
        print("✅ Environment loaded:")
        print(f"   LEADS_CONVOS_BASE: {LEADS_CONVOS_BASE}")
        print(f"   PERFORMANCE_BASE: {PERF_BASE}")
        print(f"   STRICT_MODE: {STRICT_MODE}, TEST_MODE: {TEST_MODE}")
        print(f"   QUIET_HOURS_ENFORCED: {QUIET_HOURS_ENFORCED} (Local 21:00–09:00 by default)")

        missing = [k for k in ["AIRTABLE_API_KEY", "LEADS_CONVOS_BASE", "PERFORMANCE_BASE"] if not os.getenv(k)]
        if missing:
            msg = f"🚨 Missing env vars → {', '.join(missing)}"
            log_error("Startup checks", msg)
            if STRICT_MODE:
                raise RuntimeError(msg)

        if not templates or not leads:
            msg = "🚨 Airtable tables not initialized"
            log_error("Startup checks", msg)
            if STRICT_MODE:
                raise RuntimeError(msg)

        print("✅ Startup checks passed")
    except Exception as e:
        log_error("Startup exception", e)
        if STRICT_MODE:
            raise

# -------------------------
# Health
# -------------------------
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

# -------------------------
# Outbound / Campaigns
# -------------------------
@app.post("/send")
async def send_endpoint(
    x_cron_token: str | None = Header(None),
    campaign_id: str = Query(None),
    limit: int = Query(500),
):
    check_token(x_cron_token)
    if TEST_MODE:
        return {"ok": True, "status": "mock_send", "campaign": campaign_id}
    if is_quiet_hours_local():
        # Block active sending during quiet hours
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
        # Force queue-only during quiet hours
        result = run_campaigns(limit=None if str(limit).upper() == "ALL" else int(limit), send_after_queue=False)
        result["note"] = "Queued only (quiet hours)."
        result["quiet_hours"] = True
        return result

    # Normal hours
    return run_campaigns(limit=None if str(limit).upper() == "ALL" else int(limit), send_after_queue=send_after_queue)

# -------------------------
# Autoresponder / Followups / Retry / KPIs
# -------------------------
@app.post("/autoresponder/{mode}")
async def autoresponder_endpoint(
    mode: str,
    limit: int = 50,
    view: str = "Unprocessed Inbounds",
    x_cron_token: str | None = Header(None),
):
    """
    Modes: autoresponder | ai-closer | manual-qa
    Autoresponder is allowed 24/7 by default (AUTORESPONDER_ALWAYS_ON),
    since responding to inbound is generally permissible even in quiet hours.
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
    # We block sending-like operations during quiet hours
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
          * Run AUTORESPONDER (if allowed), FOLLOWUPS (optional), METRICS, AGGREGATE_KPIS
          * Queue-only CAMPAIGN_RUNNER (if allowed)
          * Skip SEND/RETRY
      - If normal hours:
          * Run SEND, AUTORESPONDER, FOLLOWUPS, METRICS, RETRY, AGGREGATE_KPIS, CAMPAIGN_RUNNER
    """
    check_token(x_cron_token)
    results, totals = {}, {"processed": 0, "errors": 0}
    runs, kpis = get_perf_tables()

    # health gates
    for mode in ["prospects", "leads", "inbounds"]:
        health_result = strict_health(mode)
        results[f"{mode}_health"] = health_result
        if not health_result.get("ok"):
            log_run(runs, f"{mode.upper()}_HEALTH_FAIL", health_result)
            return {"ok": False, "error": f"Health check failed for {mode}", "results": results}

    # Quiet hours logic
    if is_quiet_hours_local():
        # 1) autoresponder (if allowed)
        if AUTORESPONDER_ALWAYS_ON:
            try:
                r = run_autoresponder(limit=50, view="Unprocessed Inbounds")
                results["autoresponder"] = r
                log_run(runs, "AUTORESPONDER", r)
                totals["processed"] += r.get("processed", 0)
            except Exception as e:
                log_error("AUTORESPONDER", e); totals["errors"] += 1

        # 2) followups (optional – these may be sends; skip to be safe)
        results["followups"] = {"ok": True, "skipped": "quiet_hours"}

        # 3) metrics + kpis
        for step_name, func in (("METRICS", update_metrics), ("AGGREGATE_KPIS", aggregate_kpis)):
            try:
                r = func()
                results[step_name.lower()] = r
                log_run(runs, step_name, r)
            except Exception as e:
                log_error(step_name, e); totals["errors"] += 1

        # 4) campaigns: queue-only if allowed
        if ALLOW_QUEUE_OUTSIDE_HOURS:
            try:
                r = run_campaigns(limit=None, send_after_queue=False)
                r["note"] = "Queued only (quiet hours)."
                r["quiet_hours"] = True
                results["campaign_runner"] = r
                log_run(runs, "CAMPAIGN_RUNNER", r)
            except Exception as e:
                log_error("CAMPAIGN_RUNNER", e); totals["errors"] += 1
        else:
            results["campaign_runner"] = {"ok": True, "skipped": "quiet_hours"}

        # 5) send / retry blocked
        results["outbound"] = {"ok": True, "skipped": "quiet_hours"}
        results["retry"] = {"ok": True, "skipped": "quiet_hours"}

        log_kpi(kpis, "TOTAL_PROCESSED", totals["processed"])
        log_kpi(kpis, "TOTAL_ERRORS", totals["errors"])
        results["timestamp"] = iso_timestamp()
        results["quiet_hours"] = True
        return {"ok": True, "results": results, "totals": totals}

    # Normal hours full run
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
            log_run(runs, step, r)
            totals["processed"] += r.get("processed", 0)
        except Exception as e:
            log_error(step, e); totals["errors"] += 1

    log_kpi(kpis, "TOTAL_PROCESSED", totals["processed"])
    log_kpi(kpis, "TOTAL_ERRORS", totals["errors"])
    return {"ok": True, "results": results, "totals": totals, "timestamp": iso_timestamp()}

# -------------------------
# Numbers Admin (helpful for from_number + quotas)
# -------------------------
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
    Resets daily counters on Numbers (run nightly).
    """
    check_token(x_cron_token)
    try:
        return reset_numbers_daily_counters()
    except Exception as e:
        log_error("numbers_reset", e)
        raise HTTPException(status_code=500, detail=str(e))

# -------------------------
# Engine (manual)
# -------------------------
@app.get("/engine")
def trigger_engine(mode: str, limit: int = 50, retry_limit: int = 100):
    valid_modes = {"prospects", "leads", "inbounds"}
    if mode not in valid_modes:
        raise HTTPException(status_code=400, detail=f"Invalid mode '{mode}'")
    strict_health(mode)
    return {"ok": True, "mode": mode, "result": run_engine(mode, limit=limit, retry_limit=retry_limit)}
