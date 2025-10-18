# sms/main.py
from __future__ import annotations

import os
from datetime import datetime, timezone, date
from typing import Optional, Dict, Any

from fastapi import FastAPI, Header, HTTPException, Query, Request
from dotenv import load_dotenv

from sms.dispatcher import get_policy

# ───────────────────────────── Load .env ─────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, "..", ".env")
load_dotenv(dotenv_path=ENV_PATH, override=True)

# ─────────────────────── Safe Airtable helpers ───────────────────────
try:
    from sms.airtable_client import (
        get_runs, get_kpis, get_leads, get_convos, get_templates,
        safe_create, remap_existing_only,
    )
except Exception:
    get_runs = get_kpis = get_leads = get_convos = get_templates = lambda: None
    def safe_create(*_a, **_k): return None  # type: ignore
    def remap_existing_only(*_a, **_k): return {}  # type: ignore

# ───────────────────── Core engine modules (guarded) ─────────────────
try:
    from sms.outbound_batcher import send_batch
except Exception:
    def send_batch(*_a, **_k): return {"ok": False, "error": "send_batch unavailable"}

def _build_autoresponder():
    try:
        from sms.autoresponder import run as _run
        return lambda limit=50, view=None: _run(limit=limit, view=view)
    except Exception:
        try:
            from sms.autoresponder import run_autoresponder as _run2
            return lambda limit=50, view=None: _run2(limit=limit, view=view)
        except Exception:
            return lambda *a, **k: {"ok": False, "error": "autoresponder unavailable"}

run_autoresponder = _build_autoresponder()

try:
    from sms.quota_reset import reset_daily_quotas
except Exception:
    def reset_daily_quotas(*_a, **_k): return {"ok": False, "error": "quota reset unavailable"}

try:
    from sms.metrics_tracker import update_metrics, _notify  # type: ignore
except Exception:
    def update_metrics(*_a, **_k): return {"ok": False, "error": "metrics tracker unavailable"}
    def _notify(msg: str): print(f"[notify] {msg}")

try:
    from sms.inbound_webhook import router as inbound_router
except Exception:
    inbound_router = None

try:
    from sms.campaign_runner import run_campaigns, get_campaigns_table
except Exception:
    def run_campaigns(*_a, **_k): return {"ok": False, "error": "campaign runner unavailable"}
    def get_campaigns_table(): return None

try:
    from sms.kpi_aggregator import aggregate_kpis
except Exception:
    def aggregate_kpis(*_a, **_k): return {"ok": False, "error": "kpi aggregator unavailable"}

try:
    from sms.retry_runner import run_retry
except Exception:
    def run_retry(*_a, **_k): return {"ok": False, "error": "retry runner unavailable"}

try:
    from sms.followup_flow import run_followups
except Exception:
    def run_followups(*_a, **_k): return {"ok": True, "skipped": "followups unavailable"}

try:
    from sms.dispatcher import run_engine
except Exception:
    def run_engine(*_a, **_k): return {"ok": False, "error": "dispatcher unavailable"}

try:
    from sms.health_strict import strict_health
except Exception:
    def strict_health(mode: str): return {"ok": True, "mode": mode, "note": "strict health shim"}

# Optional Drip admin (UTC timestamp normalizer)
try:
    from sms.drip_admin import normalize_next_send_dates
except Exception:
    def normalize_next_send_dates(*_a, **_k): return {"ok": False, "error": "drip_admin unavailable"}

# Optional numbers admin wrappers (name-compat)
try:
    from sms.admin_numbers import backfill_numbers_for_existing_queue
    def backfill_drip_from_numbers(dry_run: bool = True):
        res = backfill_numbers_for_existing_queue()
        res["dry_run_ignored"] = dry_run
        return res
    def recalc_numbers_sent_today(for_date: str):
        return {"ok": True, "note": "recalc not implemented in this build", "date": for_date}
    def reset_numbers_daily_counters():
        return {"ok": True, "note": "use /reset-quotas which resets Numbers daily counters"}
except Exception:
    def backfill_drip_from_numbers(*_a, **_k): return {"ok": False, "error": "admin_numbers missing"}
    def recalc_numbers_sent_today(*_a, **_k): return {"ok": False, "error": "admin_numbers missing"}
    def reset_numbers_daily_counters(*_a, **_k): return {"ok": False, "error": "admin_numbers missing"}

# ─────────────────────────── FastAPI app ────────────────────────────
app = FastAPI(title="REI SMS Engine", version="1.4.0")
if inbound_router:
    app.include_router(inbound_router)

# ─────────────────────── ENV / runtime toggles ──────────────────────
CRON_TOKEN = os.getenv("CRON_TOKEN")
TEST_MODE = os.getenv("TEST_MODE", "false").lower() in ("1", "true", "yes")
STRICT_MODE = os.getenv("STRICT_MODE", "false").lower() in ("1", "true", "yes")

# Quiet hours (Central Time)
_POLICY = get_policy()
QUIET_HOURS_ENFORCED = os.getenv("QUIET_HOURS_ENFORCED", "true" if _POLICY.quiet_enforced else "false").lower() in ("1", "true", "yes")
QUIET_START = int(os.getenv("QUIET_START_HOUR_LOCAL", str(_POLICY.quiet_start_hour)))   # 9p
QUIET_END   = int(os.getenv("QUIET_END_HOUR_LOCAL",   str(_POLICY.quiet_end_hour)))    # 9a
ALLOW_QUEUE_OUTSIDE_HOURS = os.getenv("ALLOW_QUEUE_OUTSIDE_HOURS", "true").lower() in ("1", "true", "yes")
AUTORESPONDER_ALWAYS_ON   = os.getenv("AUTORESPONDER_ALWAYS_ON",   "true").lower() in ("1", "true", "yes")

# Base hints (for logs only)
PERF_BASE = os.getenv("PERFORMANCE_BASE") or os.getenv("AIRTABLE_PERFORMANCE_BASE_ID")
LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")

# ─────────────────────────── Helpers ────────────────────────────────
def log_error(context: str, err: Exception | str):
    msg = f"❌ {context}: {err}"
    print(msg)
    try: _notify(msg)
    except Exception: pass

def iso_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

def get_perf_tables():
    return get_runs(), get_kpis()

def log_run(runs_tbl, step: str, result: Dict[str, Any]):
    if not runs_tbl: return None
    try:
        payload = {
            "Type": step,
            "Processed": float(result.get("processed") or result.get("total_sent") or result.get("sent") or 0),
            "Breakdown": str(result),
            "Timestamp": iso_timestamp(),
        }
        return safe_create(runs_tbl, payload)
    except Exception as e:
        log_error(f"Log Run {step}", e); return None

def log_kpi(kpis_tbl, metric: str, value: int | float):
    if not kpis_tbl: return None
    try:
        payload = {
            "Campaign": "ALL",
            "Metric": metric,
            "Value": float(value),
            "Date": datetime.now(timezone.utc).date().isoformat(),
        }
        return safe_create(kpis_tbl, payload)
    except Exception as e:
        log_error(f"Log KPI {metric}", e); return None

# Token intake: query ?token=, headers x-webhook-token / x-cron-token, or Authorization: Bearer
def _extract_token(request: Request, qp_token: Optional[str], h_webhook: Optional[str], h_cron: Optional[str]) -> str:
    if qp_token: return qp_token
    if h_webhook: return h_webhook
    if h_cron: return h_cron
    auth = request.headers.get("authorization") or ""
    if auth.lower().startswith("bearer "): return auth.split(" ", 1)[1]
    return ""

def _require_token(request: Request, qp_token: Optional[str], h_webhook: Optional[str], h_cron: Optional[str]):
    if not CRON_TOKEN:  # unsecured mode (local dev)
        return
    token = _extract_token(request, qp_token, h_webhook, h_cron)
    if token != CRON_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

# Quiet-hours helpers
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # py>=3.9 should have this

def central_now():
    if _POLICY.quiet_tz:
        return datetime.now(_POLICY.quiet_tz)
    if ZoneInfo:
        return datetime.now(ZoneInfo("America/Chicago"))
    return datetime.now(timezone.utc)

def is_quiet_hours_local() -> bool:
    if not QUIET_HOURS_ENFORCED: return False
    h = central_now().hour
    return (h >= QUIET_START) or (h < QUIET_END)

# ───────────────────────────── Utilities ─────────────────────────────
def _parse_limit_param(raw: Optional[str]) -> Optional[int]:
    if raw is None: return None
    try:
        s = str(raw).strip().upper()
        if s in ("", "ALL", "NONE", "UNLIMITED"): return None
        v = int(s); return max(v, 1)
    except Exception:
        print(f"[warn] Invalid limit param: {raw!r} → treating as None")
        return None

def _runner_limit_arg(safe_limit: Optional[int]) -> int | str:
    return safe_limit if (safe_limit and safe_limit > 0) else "ALL"

# ─────────────────────────── Startup ────────────────────────────────
@app.on_event("startup")
def startup_checks():
    try:
        print("✅ Environment loaded:")
        print(f"   LEADS_CONVOS_BASE: {LEADS_CONVOS_BASE}")
        print(f"   PERFORMANCE_BASE:  {PERF_BASE}")
        print(f"   STRICT_MODE={STRICT_MODE} TEST_MODE={TEST_MODE}")
        print(f"   QUIET_HOURS_ENFORCED={QUIET_HOURS_ENFORCED} ({QUIET_START:02d}:00–{QUIET_END:02d}:00 CT)")
        missing = [
            k for k in ["AIRTABLE_API_KEY", "LEADS_CONVOS_BASE", "PERFORMANCE_BASE"]
            if not os.getenv(k) and not os.getenv(k.replace("BASE", "_BASE_ID"))
        ]
        if missing:
            msg = f"🚨 Missing env vars → {', '.join(missing)}"
            log_error("Startup checks", msg)
            if STRICT_MODE: raise RuntimeError(msg)
        _ = get_templates(); _ = get_leads()  # smoke
        print("✅ Startup checks passed")
    except Exception as e:
        log_error("Startup exception", e)
        if STRICT_MODE: raise

# ───────────────────────────── Health ───────────────────────────────
@app.get("/ping")
def ping():
    return {"ok": True, "pong": True, "time": iso_timestamp()}

@app.post("/echo-token")
def echo_token(
    x_cron_token: Optional[str] = Header(None),
    x_webhook_token: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
    request: Request = None,
):
    return {
        "ok": True,
        "x_cron_token": x_cron_token,
        "x_webhook_token": x_webhook_token,
        "q_token": token,
        "auth_header": request.headers.get("authorization") if request else None,
    }

@app.get("/health")
def health():
    return {
        "ok": True,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "quiet_hours": is_quiet_hours_local(),
        "local_time_central": central_now().isoformat(),
        "version": "1.4.0",
    }

@app.get("/healthz")
def healthz():
    """Health check endpoint for Render.com and monitoring systems."""
    return {
        "ok": True,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "quiet_hours": is_quiet_hours_local(),
        "local_time_central": central_now().isoformat(),
        "version": "1.4.0",
    }

@app.get("/health/strict")
def health_strict_endpoint(mode: str = Query("prospects", description="prospects | leads | inbounds")):
    return strict_health(mode=mode)

# ─────────────────────── Outbound / Send now ────────────────────────
@app.post("/send")
async def send_endpoint(
    request: Request,
    x_cron_token: Optional[str] = Header(None),
    x_webhook_token: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
    campaign_id: Optional[str] = Query(None),
    limit: int = Query(500),
):
    _require_token(request, token, x_webhook_token, x_cron_token)
    if TEST_MODE:
        return {"ok": True, "status": "mock_send", "campaign": campaign_id}
    if is_quiet_hours_local():
        return {"ok": False, "error": "Quiet hours (Central). Sending blocked.", "quiet_hours": True}
    return send_batch(campaign_id=campaign_id, limit=limit)

# ─────────────── Campaigns (hardened; no limit crash) ───────────────
@app.post("/run-campaigns")
async def run_campaigns_endpoint(
    request: Request,
    limit: Optional[str] = Query("ALL"),
    x_cron_token: Optional[str] = Header(None),
    x_webhook_token: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
    send_after_queue: Optional[bool] = Query(None, description="If true, attempt immediate sends when not in quiet hours."),
):
    """
    Queue campaigns (and optionally send immediately).
    Quiet hours behavior:
      - If ALLOW_QUEUE_OUTSIDE_HOURS=True → queue only (send_after_queue forced False)
      - Else → skip entirely
    """
    _require_token(request, token, x_webhook_token, x_cron_token)
    safe_limit = _parse_limit_param(limit)
    runner_limit = _runner_limit_arg(safe_limit)

    if TEST_MODE:
        return {"ok": True, "status": "mock_campaign_runner", "limit": runner_limit}

    if is_quiet_hours_local():
        if not ALLOW_QUEUE_OUTSIDE_HOURS:
            return {"ok": False, "error": "Quiet hours (Central). Queueing disabled.", "quiet_hours": True}
        try:
            res = run_campaigns(limit=runner_limit, send_after_queue=False)
            res.update({"note": "Queued only (quiet hours).", "quiet_hours": True})
            return res
        except Exception as e:
            log_error("run_campaigns (quiet hours)", e)
            raise HTTPException(status_code=500, detail=str(e))

    try:
        return run_campaigns(limit=runner_limit, send_after_queue=send_after_queue)
    except Exception as e:
        log_error("run_campaigns (normal hours)", e)
        raise HTTPException(status_code=500, detail=str(e))

# ─────────────── Manual Campaign Controls ───────────────
def _safe_update(tbl, rid: str, patch: Dict[str, Any]):
    if not (tbl and rid and patch): return None
    try: return tbl.update(rid, patch)
    except Exception as e: log_error("Airtable update", e); return None

@app.post("/campaign/{campaign_id}/start")
def campaign_start(
    campaign_id: str,
    request: Request,
    x_cron_token: Optional[str] = Header(None),
    x_webhook_token: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
):
    _require_token(request, token, x_webhook_token, x_cron_token)
    tbl = get_campaigns_table()
    if not tbl: raise HTTPException(500, "Campaigns table unavailable")
    _safe_update(tbl, campaign_id, {"status": "Scheduled", "Active": True, "Go Live": True, "last_run_at": iso_timestamp()})
    return {"ok": True, "campaign": campaign_id, "status": "Scheduled"}

@app.post("/campaign/{campaign_id}/stop")
def campaign_stop(
    campaign_id: str,
    request: Request,
    x_cron_token: Optional[str] = Header(None),
    x_webhook_token: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
):
    _require_token(request, token, x_webhook_token, x_cron_token)
    tbl = get_campaigns_table()
    if not tbl: raise HTTPException(500, "Campaigns table unavailable")
    _safe_update(tbl, campaign_id, {"status": "Paused", "Active": False, "Go Live": False, "last_run_at": iso_timestamp()})
    return {"ok": True, "campaign": campaign_id, "status": "Paused"}

@app.post("/campaign/{campaign_id}/kick")
def campaign_kick(
    campaign_id: str,
    request: Request,
    limit: Optional[str] = Query("ALL"),
    x_cron_token: Optional[str] = Header(None),
    x_webhook_token: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
):
    """
    One-click: mark campaign active and immediately run the campaign runner.
    Honors quiet hours (will queue-only or block accordingly).
    """
    _require_token(request, token, x_webhook_token, x_cron_token)
    tbl = get_campaigns_table()
    if not tbl: raise HTTPException(500, "Campaigns table unavailable")
    _safe_update(tbl, campaign_id, {"status": "Scheduled", "Active": True, "Go Live": True})
    # Narrow run to this campaign by letting campaign_runner filter naturally.
    safe_limit = _parse_limit_param(limit)
    runner_limit = _runner_limit_arg(safe_limit)
    if is_quiet_hours_local():
        if not ALLOW_QUEUE_OUTSIDE_HOURS:
            return {"ok": False, "error": "Quiet hours (Central). Queueing disabled.", "quiet_hours": True}
        res = run_campaigns(limit=runner_limit, send_after_queue=False)
        res.update({"note": "Queued only (quiet hours).", "quiet_hours": True})
        return res
    return run_campaigns(limit=runner_limit, send_after_queue=True)

# ─────────────── Autoresponder / Followups / Retry / KPIs ───────────
@app.post("/autoresponder/{mode}")
async def autoresponder_endpoint(
    request: Request,
    mode: str,
    limit: int = 50,
    view: str = "Unprocessed Inbounds",
    x_cron_token: Optional[str] = Header(None),
    x_webhook_token: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
):
    _require_token(request, token, x_webhook_token, x_cron_token)
    os.environ["PROCESSED_BY_LABEL"] = mode.replace("-", " ").title()
    if TEST_MODE:
        return {"ok": True, "status": "mock_autoresponder", "mode": mode}
    if not AUTORESPONDER_ALWAYS_ON and is_quiet_hours_local():
        return {"ok": False, "error": "Quiet hours (Central). Autoresponder disabled by config.", "quiet_hours": True}
    return run_autoresponder(limit=limit, view=view)

@app.post("/reset-quotas")
async def reset_quotas_endpoint(
    request: Request,
    x_cron_token: Optional[str] = Header(None),
    x_webhook_token: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
):
    _require_token(request, token, x_webhook_token, x_cron_token)
    return reset_daily_quotas()

@app.post("/metrics")
async def metrics_endpoint(
    request: Request,
    x_cron_token: Optional[str] = Header(None),
    x_webhook_token: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
):
    _require_token(request, token, x_webhook_token, x_cron_token)
    return update_metrics()

@app.post("/retry")
async def retry_endpoint(
    request: Request,
    limit: int = 100,
    view: Optional[str] = None,
    x_cron_token: Optional[str] = Header(None),
    x_webhook_token: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
):
    _require_token(request, token, x_webhook_token, x_cron_token)
    if TEST_MODE:
        return {"ok": True, "status": "mock_retry"}
    if is_quiet_hours_local():
        return {"ok": False, "error": "Quiet hours (Central). Retries blocked.", "quiet_hours": True}
    return run_retry(limit=limit, view=view)

@app.post("/aggregate-kpis")
async def aggregate_kpis_endpoint(
    request: Request,
    x_cron_token: Optional[str] = Header(None),
    x_webhook_token: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
):
    _require_token(request, token, x_webhook_token, x_cron_token)
    return aggregate_kpis()

# ─────────────── One-shot “all steps” orchestrator ────────────────
@app.post("/cron/all")
async def cron_all_endpoint(
    request: Request,
    limit: int = 500,
    x_cron_token: Optional[str] = Header(None),
    x_webhook_token: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
):
    """
    Order of ops:
      - STRICT health: prospects, leads, inbounds
      - Quiet hours:
          * Autoresponder (if allowed), Metrics, Aggregate KPIs, Campaign queue-only
          * Skip send/retry
      - Normal hours:
          * Send, Autoresponder, Followups, Metrics, Retry, Aggregate KPIs, Campaigns
    """
    _require_token(request, token, x_webhook_token, x_cron_token)
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

    # Quiet hours flow
    if is_quiet_hours_local():
        if AUTORESPONDER_ALWAYS_ON:
            try:
                r = run_autoresponder(limit=50, view="Unprocessed Inbounds")
                results["autoresponder"] = r
                log_run(runs_tbl, "AUTORESPONDER", r)
                totals["processed"] += r.get("processed", 0)
            except Exception as e:
                log_error("AUTORESPONDER", e); totals["errors"] += 1

        results["followups"] = {"ok": True, "skipped": "quiet_hours"}

        for step_name, func in (("METRICS", update_metrics), ("AGGREGATE_KPIS", aggregate_kpis)):
            try:
                r = func()
                results[step_name.lower()] = r
                log_run(runs_tbl, step_name, r)
            except Exception as e:
                log_error(step_name, e); totals["errors"] += 1

        if ALLOW_QUEUE_OUTSIDE_HOURS:
            try:
                r = run_campaigns(limit="ALL", send_after_queue=False)
                r.update({"note": "Queued only (quiet hours).", "quiet_hours": True})
                results["campaign_runner"] = r
                log_run(runs_tbl, "CAMPAIGN_RUNNER", r)
            except Exception as e:
                log_error("CAMPAIGN_RUNNER", e); totals["errors"] += 1
        else:
            results["campaign_runner"] = {"ok": True, "skipped": "quiet_hours"}

        results["outbound"] = {"ok": True, "skipped": "quiet_hours"}
        results["retry"]    = {"ok": True, "skipped": "quiet_hours"}

        log_kpi(kpis_tbl, "TOTAL_PROCESSED", totals["processed"])
        log_kpi(kpis_tbl, "TOTAL_ERRORS", totals["errors"])
        results["timestamp"] = iso_timestamp()
        results["quiet_hours"] = True
        return {"ok": True, "results": results, "totals": totals}

    # Normal hours flow
    steps = [
        ("OUTBOUND",        lambda: send_batch(limit=limit)),
        ("AUTORESPONDER",   lambda: run_autoresponder(limit=50, view="Unprocessed Inbounds")),
        ("FOLLOWUPS",       run_followups),
        ("METRICS",         update_metrics),
        ("RETRY",           lambda: run_retry(limit=100)),
        ("AGGREGATE_KPIS",  aggregate_kpis),
        ("CAMPAIGN_RUNNER", lambda: run_campaigns(limit="ALL")),
    ]
    for step, func in steps:
        try:
            r = {"ok": True, "status": f"mock_{step.lower()}"} if TEST_MODE else func()
            results[step.lower()] = r
            log_run(runs_tbl, step, r)
            totals["processed"] += r.get("processed", 0)
        except Exception as e:
            log_error(step, e); totals["errors"] += 1

    log_kpi(kpis_tbl, "TOTAL_PROCESSED", totals["processed"])
    log_kpi(kpis_tbl, "TOTAL_ERRORS", totals["errors"])
    return {"ok": True, "results": results, "totals": totals, "timestamp": iso_timestamp()}

# ─────────────── Drip Admin (UTC normalize queued sends) ────────────
@app.post("/admin/drip/normalize")
def drip_normalize(
    request: Request,
    dry_run: bool = Query(True),
    force_now: bool = Query(False),
    limit: int = Query(1000),
    x_cron_token: Optional[str] = Header(None),
    x_webhook_token: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
):
    _require_token(request, token, x_webhook_token, x_cron_token)
    return normalize_next_send_dates(dry_run=dry_run, force_now=force_now, limit=limit)

@app.post("/admin/drip/force-now")
def drip_force_now(
    request: Request,
    limit: int = Query(1000),
    x_cron_token: Optional[str] = Header(None),
    x_webhook_token: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
):
    _require_token(request, token, x_webhook_token, x_cron_token)
    return normalize_next_send_dates(dry_run=False, force_now=True, limit=limit)

# ─────────────── Numbers Admin (from_number + quotas) ───────────────
@app.post("/admin/numbers/backfill")
def numbers_backfill_endpoint(
    request: Request,
    dry_run: bool = Query(True),
    x_cron_token: Optional[str] = Header(None),
    x_webhook_token: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
):
    _require_token(request, token, x_webhook_token, x_cron_token)
    try:
        return backfill_drip_from_numbers(dry_run=dry_run)
    except Exception as e:
        log_error("numbers_backfill", e)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/admin/numbers/recalc")
def numbers_recalc_endpoint(
    request: Request,
    for_date: Optional[str] = Query(None, description="YYYY-MM-DD (defaults to today in Central)"),
    x_cron_token: Optional[str] = Header(None),
    x_webhook_token: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
):
    _require_token(request, token, x_webhook_token, x_cron_token)
    try:
        target = for_date or date.fromisoformat(central_now().date().isoformat()).isoformat()
        return recalc_numbers_sent_today(target)
    except Exception as e:
        log_error("numbers_recalc", e)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/admin/numbers/reset")
def numbers_reset_endpoint(
    request: Request,
    x_cron_token: Optional[str] = Header(None),
    x_webhook_token: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
):
    _require_token(request, token, x_webhook_token, x_cron_token)
    try:
        return reset_numbers_daily_counters()
    except Exception as e:
        log_error("numbers_reset", e)
        raise HTTPException(status_code=500, detail=str(e))

# ───────────────────── Engine (manual debugging) ────────────────────
@app.get("/engine")
def trigger_engine(
    request: Request,
    mode: str,
    limit: int = 50,
    retry_limit: int = 100,
    x_cron_token: Optional[str] = Header(None),
    x_webhook_token: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
):
    _require_token(request, token, x_webhook_token, x_cron_token)
    valid_modes = {"prospects", "leads", "inbounds"}
    if mode not in valid_modes:
        raise HTTPException(status_code=400, detail=f"Invalid mode '{mode}'")
    health = strict_health(mode)
    if not health.get("ok"):
        raise HTTPException(status_code=500, detail=f"Health check failed for {mode}")
    return {"ok": True, "mode": mode, "result": run_engine(mode, limit=limit, retry_limit=retry_limit)}