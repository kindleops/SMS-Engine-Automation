import os
import traceback
from datetime import datetime, timezone
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Query
from pyairtable import Table

# --- Load .env ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, "..", ".env")
load_dotenv(dotenv_path=ENV_PATH, override=True)

# --- SMS Engine Modules ---
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
from sms.health_strict import strict_health   # ✅ moved out to its own module

# --- FastAPI app ---
app = FastAPI(title="REI SMS Engine", version="1.0")
app.include_router(inbound_router)

# --- ENV CONFIG ---
CRON_TOKEN = os.getenv("CRON_TOKEN")

PERF_BASE = os.getenv("PERFORMANCE_BASE")
PERF_KEY = os.getenv("AIRTABLE_REPORTING_KEY") or os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE")
AIRTABLE_API_KEY  = os.getenv("AIRTABLE_API_KEY")

# Airtable defaults
TEMPLATES_TABLE = os.getenv("TEMPLATES_TABLE", "Templates")
LEADS_TABLE     = os.getenv("LEADS_TABLE", "Leads")

# Airtable clients
templates = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, TEMPLATES_TABLE) if AIRTABLE_API_KEY else None
leads     = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, LEADS_TABLE) if AIRTABLE_API_KEY else None


# -------------------------
# Helpers
# -------------------------
def get_perf_tables():
    if not (PERF_KEY and PERF_BASE):
        return None, None
    try:
        runs = Table(PERF_KEY, PERF_BASE, "Runs/Logs")
        kpis = Table(PERF_KEY, PERF_BASE, "KPIs")
        return runs, kpis
    except Exception:
        print("⚠️ Failed to init Performance tables")
        traceback.print_exc()
        return None, None


def check_token(x_cron_token: str | None):
    if CRON_TOKEN and x_cron_token != CRON_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")


def iso_timestamp():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def log_run(runs, step: str, result: dict):
    try:
        return runs.create({
            "Type": step,
            "Processed": result.get("processed") or result.get("total_sent") or result.get("sent") or 0,
            "Breakdown": str(result),
            "Timestamp": iso_timestamp()
        })
    except Exception:
        traceback.print_exc()
        return None


def log_kpi(kpis, metric: str, value: int):
    try:
        return kpis.create({
            "Campaign": "ALL",
            "Metric": metric,
            "Value": value,
            "Date": datetime.now(timezone.utc).date().isoformat()
        })
    except Exception:
        traceback.print_exc()
        return None


# -------------------------
# Startup Checks (merged)
# -------------------------
STRICT_MODE = os.getenv("STRICT_MODE", "false").lower() in ("1", "true", "yes")

@app.on_event("startup")
def startup_checks():
    try:
        print("✅ Environment loaded:")
        print(f"   LEADS_CONVOS_BASE: {LEADS_CONVOS_BASE}")
        print(f"   PERFORMANCE_BASE: {PERF_BASE}")
        print(f"   CONVERSATIONS_TABLE: {os.getenv('CONVERSATIONS_TABLE', 'Conversations')}")
        print(f"   STRICT_MODE: {STRICT_MODE}")

        # Sanity check env vars
        missing = []
        for key in ["AIRTABLE_API_KEY", "LEADS_CONVOS_BASE", "PERFORMANCE_BASE"]:
            if not os.getenv(key):
                missing.append(key)

        if missing:
            msg = f"🚨 Missing env vars → {', '.join(missing)}"
            print(msg)
            _notify(msg)
            if STRICT_MODE:
                raise RuntimeError(msg)

        # Airtable smoke test
        if not templates or not leads:
            msg = "🚨 Airtable tables not initialized"
            print(msg)
            _notify(msg)
            if STRICT_MODE:
                raise RuntimeError(msg)

        print("✅ Startup checks passed")

    except Exception as e:
        err = f"❌ Startup exception: {e}"
        print(err)
        try:
            _notify(err)
        except:
            pass
        if STRICT_MODE:
            raise


# -------------------------
# Routes
# -------------------------

@app.get("/health")
def health():
    return {"ok": True, "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/health/strict")
def health_strict_endpoint(mode: str = Query("prospects", description="prospects | leads | inbounds")):
    """Strict Airtable connectivity check."""
    return strict_health(mode=mode)


@app.post("/send")
async def send_endpoint(
    x_cron_token: str | None = Header(None),
    campaign_id: str = Query(None),
    limit: int = Query(500)
):
    check_token(x_cron_token)
    return send_batch(campaign_id=campaign_id, limit=limit)


@app.post("/autoresponder")
async def autoresponder_endpoint(
    limit: int = 50,
    view: str = "Unprocessed Inbounds",
    x_cron_token: str | None = Header(None),
):
    check_token(x_cron_token)
    os.environ["PROCESSED_BY_LABEL"] = "Autoresponder"
    return run_autoresponder(limit=limit, view=view)


@app.post("/ai-closer")
async def ai_closer_endpoint(
    limit: int = 50,
    view: str = "Unprocessed Inbounds",
    x_cron_token: str | None = Header(None)
):
    check_token(x_cron_token)
    os.environ["PROCESSED_BY_LABEL"] = "AI Closer"
    return run_autoresponder(limit=limit, view=view)


@app.post("/manual-qa")
async def manual_qa_endpoint(
    limit: int = 50,
    view: str = "Unprocessed Inbounds",
    x_cron_token: str | None = Header(None)
):
    check_token(x_cron_token)
    os.environ["PROCESSED_BY_LABEL"] = "Manual QA"
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
async def retry_endpoint(limit: int = 100, view: str | None = None, x_cron_token: str | None = Header(None)):
    check_token(x_cron_token)
    return run_retry(limit=limit, view=view)


@app.post("/aggregate-kpis")
async def aggregate_kpis_endpoint(x_cron_token: str | None = Header(None)):
    check_token(x_cron_token)
    return aggregate_kpis()


@app.post("/run-campaigns")
async def run_campaigns_endpoint(
    limit: str = Query("ALL"),
    x_cron_token: str | None = Header(None)
):
    check_token(x_cron_token)
    lim = 9999 if str(limit).upper() == "ALL" else int(limit)
    return run_campaigns(limit=lim)


@app.post("/cron/all")
async def cron_all_endpoint(
    limit: int = 500,
    x_cron_token: str | None = Header(None)
):
    check_token(x_cron_token)
    results = {}
    runs, kpis = get_perf_tables()
    totals = {"processed": 0, "errors": 0}

    # Strict preflight for all modes
    for mode in ["prospects", "leads", "inbounds"]:
        health_result = strict_health(mode)
        results[f"{mode}_health"] = health_result
        if not health_result.get("ok"):
            if runs:
                log_run(runs, f"{mode.upper()}_HEALTH_FAIL", health_result)
            return {
                "ok": False,
                "error": f"Health check failed for {mode}",
                "results": results,
                "totals": totals,
                "timestamp": iso_timestamp()
            }

    steps = [
        ("OUTBOUND", lambda: send_batch(limit=limit)),
        ("AUTORESPONDER", lambda: run_autoresponder(limit=50, view="Unprocessed Inbounds")),
        ("FOLLOWUPS", run_followups),
        ("METRICS", update_metrics),
        ("RETRY", lambda: run_retry(limit=100)),
        ("AGGREGATE_KPIS", aggregate_kpis),
        ("CAMPAIGN_RUNNER", lambda: run_campaigns(limit=9999)),
    ]

    for step, func in steps:
        try:
            result = func()
            results[step.lower()] = result
            if runs:
                log_run(runs, step, result)
            processed = result.get("processed") or result.get("total_sent") or result.get("sent") or 0
            totals["processed"] += processed
        except Exception as e:
            err = str(e)
            results[f"{step.lower()}_error"] = err
            totals["errors"] += 1
            if runs:
                log_run(runs, step, {"error": err})

    if kpis:
        log_kpi(kpis, "TOTAL_PROCESSED", totals["processed"])
        log_kpi(kpis, "TOTAL_ERRORS", totals["errors"])

    if runs:
        try:
            runs.create({
                "Type": "DAILY_SUMMARY",
                "Processed": totals["processed"],
                "Breakdown": str(results),
                "Timestamp": iso_timestamp()
            })
        except Exception:
            traceback.print_exc()

    return {"ok": True, "results": results, "totals": totals, "timestamp": iso_timestamp()}


@app.get("/engine")
def trigger_engine(
    mode: str = Query(..., description="prospects | leads | inbounds"),
    limit: int = 50,
    retry_limit: int = 100
):
    valid_modes = {"prospects", "leads", "inbounds"}
    if mode not in valid_modes:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid mode '{mode}'. Must be one of {', '.join(valid_modes)}."
        )

    strict_health(mode)  # ✅ preflight check
    result = run_engine(mode, limit=limit, retry_limit=retry_limit)
    return {"ok": True, "mode": mode, "result": result}