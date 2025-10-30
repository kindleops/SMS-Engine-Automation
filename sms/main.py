from __future__ import annotations

"""
REI SMS Engine — Enterprise Async Main (v3.0, no-scheduler)
- Runs on Render & Docker
- Full async endpoints with immediate Airtable Runs/KPIs logging
- Strict quiet-hours, CRON token auth, TEST_MODE gating
- Defensive imports & graceful fallbacks
- Fine-grained telemetry + per-step error isolation
- No 'Go Live' field usage
- Scheduler removed
"""

import asyncio
import os
import json
import traceback
from datetime import datetime, timezone, date
from typing import Optional, Dict, Any, Callable, Tuple
from sms.inbound_webhook import router as inbound_router
from sms.delivery_webhook import router as delivery_router, router_root as delivery_router_root

from fastapi import FastAPI, Header, HTTPException, Query, Request
from pydantic import BaseModel
from dotenv import load_dotenv

# ───────────────────────────── Load .env early ─────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, "..", ".env")
load_dotenv(dotenv_path=ENV_PATH, override=True)

# ─────────────────────────── Project policy (quiet hours) ───────────────────
from sms.dispatcher import get_policy

_POLICY = get_policy()


# ─────────────────────────── Lazy & guarded imports ─────────────────────────
def _guarded_import(path: str, attr: Optional[str] = None, fallback: Any = None):
    try:
        module = __import__(path, fromlist=[attr] if attr else [])
        return getattr(module, attr) if attr else module
    except Exception:
        return fallback


# Airtable safe helpers (all optional):
try:
    from sms.airtable_client import (
        get_runs,
        get_kpis,
        get_leads,
        get_convos,
        get_templates,
        safe_create,
        remap_existing_only,
    )
except Exception:
    get_runs = get_kpis = get_leads = get_convos = get_templates = lambda: None  # type: ignore

    def safe_create(*_a, **_k):
        return None  # type: ignore

    def remap_existing_only(*_a, **_k):
        return {}  # type: ignore


# Outbound batcher
_send_batch = _guarded_import("sms.outbound_batcher", "send_batch", fallback=None)

# Autoresponder (two possible names in tree)
def _build_autoresponder():
    run = _guarded_import("sms.autoresponder", "run", fallback=None)
    if run:
        return lambda limit=50, view=None: run(limit=limit, view=view)
    run2 = _guarded_import("sms.autoresponder", "run_autoresponder", fallback=None)
    if run2:
        return lambda limit=50, view=None: run2(limit=limit, view=view)
    return None

_run_autoresponder = _build_autoresponder()

# Quotas, metrics, followups, retry, dispatcher, health
_reset_daily_quotas = _guarded_import("sms.quota_reset", "reset_daily_quotas", fallback=None)
_update_metrics = _guarded_import("sms.metrics_tracker", "update_metrics", fallback=None)
_notify = _guarded_import("sms.metrics_tracker", "_notify", fallback=lambda m: print(f"[notify] {m}"))
_run_campaigns = _guarded_import("sms.campaign_runner", "run_campaigns", fallback=None)
_get_campaigns_tbl = _guarded_import("sms.campaign_runner", "get_campaigns_table", fallback=None)
_aggregate_kpis = _guarded_import("sms.kpi_aggregator", "aggregate_kpis", fallback=None)
_run_retry = _guarded_import("sms.retry_runner", "run_retry", fallback=None)
_run_followups = _guarded_import("sms.followup_flow", "run_followups", fallback=lambda: {"ok": True, "skipped": "followups unavailable"})
_run_engine = _guarded_import("sms.dispatcher", "run_engine", fallback=lambda *a, **k: {"ok": False, "error": "dispatcher unavailable"})
_strict_health = _guarded_import(
    "sms.health_strict", "strict_health", fallback=lambda mode: {"ok": True, "mode": mode, "note": "strict health shim"}
)

# Reliable fallback for Campaigns table if helper not present
if _get_campaigns_tbl is None:
    def _get_campaigns_tbl():
        try:
            from sms.datastore import CONNECTOR
            h = CONNECTOR.campaigns()
            return getattr(h, "table", h)
        except Exception:
            return None

# Optional inbound router (mounted if available)
_inbound_router = _guarded_import("sms.inbound_webhook", "router", fallback=None)

# Optional Drip admin
_normalize_next_send_dates = _guarded_import(
    "sms.drip_admin", "normalize_next_send_dates", fallback=lambda *a, **k: {"ok": False, "error": "drip_admin unavailable"}
)

# Optional numbers admin
_backfill_numbers_for_existing_queue = _guarded_import("sms.admin_numbers", "backfill_numbers_for_existing_queue", fallback=None)


def _backfill_drip_from_numbers(dry_run: bool = True) -> Dict[str, Any]:
    if _backfill_numbers_for_existing_queue:
        res = _backfill_numbers_for_existing_queue(dry_run=dry_run)  # pass through
        res["dry_run"] = dry_run
        return res
    return {"ok": False, "error": "admin_numbers missing"}


def _recalc_numbers_sent_today(for_date: str) -> Dict[str, Any]:
    if _backfill_numbers_for_existing_queue:
        return {"ok": True, "note": "recalc not implemented in this build", "date": for_date}
    return {"ok": False, "error": "admin_numbers missing"}


def _reset_numbers_daily_counters() -> Dict[str, Any]:
    if _backfill_numbers_for_existing_queue:
        return {"ok": True, "note": "use /reset-quotas which resets Numbers daily counters"}
    return {"ok": False, "error": "admin_numbers missing"}


# ─────────────────────────── FastAPI app ────────────────────────────
# Force redeploy after revert
app = FastAPI(title="REI SMS Engine", version="3.0.0")
if _inbound_router:
    app.include_router(inbound_router)       # → /inbound
    app.include_router(delivery_router)      # → /delivery/...
    app.include_router(delivery_router_root)

# ─────────────────────── ENV / runtime toggles ──────────────────────
CRON_TOKEN = os.getenv("CRON_TOKEN")
TEST_MODE = os.getenv("TEST_MODE", "false").lower() in ("1", "true", "yes")
STRICT_MODE = os.getenv("STRICT_MODE", "false").lower() in ("1", "true", "yes")

QUIET_HOURS_ENFORCED = os.getenv("QUIET_HOURS_ENFORCED", "true" if _POLICY.quiet_enforced else "false").lower() in ("1", "true", "yes")
QUIET_START = int(os.getenv("QUIET_START_HOUR_LOCAL", str(_POLICY.quiet_start_hour)))
QUIET_END = int(os.getenv("QUIET_END_HOUR_LOCAL", str(_POLICY.quiet_end_hour)))
ALLOW_QUEUE_OUTSIDE_HOURS = os.getenv("ALLOW_QUEUE_OUTSIDE_HOURS", "true").lower() in ("1", "true", "yes")
AUTORESPONDER_ALWAYS_ON = os.getenv("AUTORESPONDER_ALWAYS_ON", "true").lower() in ("1", "true", "yes")

PERF_BASE = os.getenv("PERFORMANCE_BASE") or os.getenv("AIRTABLE_PERFORMANCE_BASE_ID")
LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")

# ─────────────────────────── Time helpers ───────────────────────────
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None


def _iso_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def central_now() -> datetime:
    if _POLICY.quiet_tz:
        return datetime.now(_POLICY.quiet_tz)
    if ZoneInfo:
        return datetime.now(ZoneInfo("America/Chicago"))
    return datetime.now(timezone.utc)


def is_quiet_hours_local() -> bool:
    if not QUIET_HOURS_ENFORCED:
        return False
    h = central_now().hour
    return (h >= QUIET_START) or (h < QUIET_END)


# ─────────────────────────── Auth helpers ───────────────────────────
def _extract_token(request: Request, qp_token: Optional[str], h_webhook: Optional[str], h_cron: Optional[str]) -> str:
    if qp_token:
        return qp_token
    if h_webhook:
        return h_webhook
    if h_cron:
        return h_cron
    auth = request.headers.get("authorization") or ""
    if auth.lower().startswith("bearer "):
        return auth.split(" ", 1)[1]
    return ""


def _require_token(request: Request, qp_token: Optional[str], h_webhook: Optional[str], h_cron: Optional[str]):
    if not CRON_TOKEN:
        return
    token = _extract_token(request, qp_token, h_webhook, h_cron)
    if token != CRON_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")


# ─────────────────────────── Logging + telemetry ────────────────────
def _log_error(context: str, err: Exception | str):
    msg = f"❌ {context}: {err}"
    print(msg)
    try:
        _notify(msg)
    except Exception:
        pass


def _get_perf_tables() -> Tuple[Any, Any]:
    return get_runs(), get_kpis()


async def _log_run_async(runs_tbl, step: str, result: Dict[str, Any]):
    if not runs_tbl:
        return
    try:
        payload = {
            "Type": step,
            "Processed": float(result.get("processed") or result.get("total_sent") or result.get("sent") or 0),
            "Breakdown": json.dumps(result, ensure_ascii=False),
            "Timestamp": _iso_ts(),
        }
        await asyncio.to_thread(safe_create, runs_tbl, payload)
    except Exception as e:
        _log_error(f"Log Run {step}", e)


async def _log_kpi_async(kpis_tbl, metric: str, value: int | float):
    if not kpis_tbl:
        return
    try:
        payload = {
            "Campaign": "ALL",
            "Metric": metric,
            "Value": float(value),
            "Date": datetime.now(timezone.utc).date().isoformat(),
        }
        await asyncio.to_thread(safe_create, kpis_tbl, payload)
    except Exception as e:
        _log_error(f"Log KPI {metric}", e)


# ─────────────────────────── Utility parsing ─────────────────────────
def _parse_limit_param(raw: Optional[str]) -> Optional[int]:
    if raw is None:
        return None
    try:
        s = str(raw).strip().upper()
        if s in ("", "ALL", "NONE", "UNLIMITED"):
            return None
        v = int(s)
        return max(v, 1)
    except Exception:
        print(f"[warn] Invalid limit param: {raw!r} → treating as None")
        return None


class RunCampaignsRequest(BaseModel):
    limit: Optional[int] = None
    send_after_queue: Optional[bool] = None


def _runner_limit_arg(safe_limit: Optional[int]) -> int | str:
    return safe_limit if (safe_limit and safe_limit > 0) else "ALL"


# ─────────────────────────── Startup checks ─────────────────────────
@app.on_event("startup")
async def startup_checks():
    try:
        print("✅ Environment loaded:")
        print(f"   LEADS_CONVOS_BASE: {LEADS_CONVOS_BASE}")
        print(f"   PERFORMANCE_BASE:  {PERF_BASE}")
        print(f"   STRICT_MODE={STRICT_MODE} TEST_MODE={TEST_MODE}")
        print(f"   QUIET_HOURS_ENFORCED={QUIET_HOURS_ENFORCED} ({QUIET_START:02d}:00–{QUIET_END:02d}:00 CT)")

        missing: list[str] = []
        if not os.getenv("AIRTABLE_API_KEY"):
            missing.append("AIRTABLE_API_KEY")
        if not (os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")):
            missing.append("LEADS_CONVOS_BASE|AIRTABLE_LEADS_CONVOS_BASE_ID")
        if not (os.getenv("PERFORMANCE_BASE") or os.getenv("AIRTABLE_PERFORMANCE_BASE_ID")):
            missing.append("PERFORMANCE_BASE|AIRTABLE_PERFORMANCE_BASE_ID")

        if missing:
            msg = f"🚨 Missing env vars → {', '.join(missing)}"
            _log_error("Startup checks", msg)
            if STRICT_MODE:
                raise RuntimeError(msg)

        # Smoke checks (non-fatal)
        _ = get_templates()
        _ = get_leads()
        print("✅ Startup checks passed")
    except Exception as e:
        _log_error("Startup exception", e)
        if STRICT_MODE:
            raise


# ─────────────────────────── Health ────────────────────────────────
@app.get("/ping")
async def ping():
    return {"ok": True, "pong": True, "time": _iso_ts()}


@app.post("/echo-token")
async def echo_token(
    request: Request,
    x_cron_token: Optional[str] = Header(None),
    x_webhook_token: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
):
    return {
        "ok": True,
        "x_cron_token": x_cron_token,
        "x_webhook_token": x_webhook_token,
        "q_token": token,
        "auth_header": request.headers.get("authorization"),
    }


@app.get("/health")
async def health():
    return {
        "ok": True,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "quiet_hours": is_quiet_hours_local(),
        "local_time_central": central_now().isoformat(),
        "version": "3.0.0",
    }


@app.get("/healthz")
async def healthz():
    return {
        "ok": True,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "quiet_hours": is_quiet_hours_local(),
        "local_time_central": central_now().isoformat(),
        "version": "3.0.0",
    }


@app.get("/health/strict")
async def health_strict_endpoint(mode: str = Query("prospects", description="prospects | leads | inbounds")):
    try:
        return _strict_health(mode=mode)
    except Exception as e:
        _log_error("strict_health", e)
        return {"ok": False, "error": str(e), "mode": mode}


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
    # =====================================================
    # EMERGENCY NUCLEAR STOP - OVERRIDE ALL OTHER LOGIC
    # =====================================================
    return {"ok": False, "error": "EMERGENCY_STOP_ALL_SENDING_DISABLED", "emergency": True}
    
    if TEST_MODE:
        return {"ok": True, "status": "mock_send", "campaign": campaign_id}
    if is_quiet_hours_local():
        return {"ok": False, "error": "Quiet hours (Central). Sending blocked.", "quiet_hours": True}
    if not _send_batch:
        return {"ok": False, "error": "send_batch unavailable"}
    return await asyncio.to_thread(_send_batch, campaign_id, limit)


# ─────────────── Campaigns (queue + optional immediate send) ───────────────
@app.post("/run-campaigns")
async def run_campaigns_endpoint(
    request: Request,
    limit: Optional[str] = Query("ALL"),
    x_cron_token: Optional[str] = Header(None),
    x_webhook_token: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
    send_after_queue: Optional[bool] = Query(None, description="If true, attempt immediate sends when not in quiet hours."),
    payload: Optional[RunCampaignsRequest] = None,
):
    """
    Queue campaigns (and optionally send immediately).
    Quiet hours behavior:
      - If ALLOW_QUEUE_OUTSIDE_HOURS=True → queue only (send_after_queue forced False)
      - Else → skip entirely
    """
    _require_token(request, token, x_webhook_token, x_cron_token)
    # =====================================================
    # EMERGENCY NUCLEAR STOP - OVERRIDE ALL OTHER LOGIC
    # =====================================================
    return {"ok": False, "error": "EMERGENCY_STOP_ALL_CAMPAIGNS_DISABLED", "emergency": True}
    
    if not _run_campaigns:
        return {"ok": False, "error": "campaign runner unavailable"}

    if payload:
        if payload.limit is not None:
            limit = str(payload.limit)
        if payload.send_after_queue is not None:
            send_after_queue = payload.send_after_queue

    safe_limit = _parse_limit_param(limit)
    runner_limit = _runner_limit_arg(safe_limit)
    send_flag = bool(send_after_queue) if send_after_queue is not None else False

    if TEST_MODE:
        return {
            "ok": True,
            "status": "mock_campaign_runner",
            "limit": runner_limit,
            "send_after_queue": send_flag,
        }

    try:
        if is_quiet_hours_local():
            if not ALLOW_QUEUE_OUTSIDE_HOURS:
                return {"ok": False, "error": "Quiet hours (Central). Queueing disabled.", "quiet_hours": True}
            res = await asyncio.to_thread(_run_campaigns, runner_limit, False)
            res.update({"note": "Queued only (quiet hours).", "quiet_hours": True})
            return res
        return await asyncio.to_thread(_run_campaigns, runner_limit, send_flag)
    except Exception as e:
        _log_error("run_campaigns", e)
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────── Manual Campaign Controls (no 'Go Live') ────────────
def _safe_update(tbl, rid: str, patch: Dict[str, Any]):
    if not (tbl and rid and patch):
        return None
    try:
        return tbl.update(rid, patch)
    except Exception as e:
        _log_error("Airtable update", e)
        return None


@app.post("/campaign/{campaign_id}/start")
async def campaign_start(
    campaign_id: str,
    request: Request,
    x_cron_token: Optional[str] = Header(None),
    x_webhook_token: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
):
    _require_token(request, token, x_webhook_token, x_cron_token)
    tbl = _get_campaigns_tbl()
    if not tbl:
        raise HTTPException(500, "Campaigns table unavailable")
    await asyncio.to_thread(
        _safe_update, tbl, campaign_id, {"Status": "Scheduled", "Active": True, "Last Run At": _iso_ts()}
    )
    return {"ok": True, "campaign": campaign_id, "status": "Scheduled"}


@app.post("/campaign/{campaign_id}/stop")
async def campaign_stop(
    campaign_id: str,
    request: Request,
    x_cron_token: Optional[str] = Header(None),
    x_webhook_token: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
):
    _require_token(request, token, x_webhook_token, x_cron_token)
    tbl = _get_campaigns_tbl()
    if not tbl:
        raise HTTPException(500, "Campaigns table unavailable")
    await asyncio.to_thread(
        _safe_update, tbl, campaign_id, {"Status": "Paused", "Active": False, "Last Run At": _iso_ts()}
    )
    return {"ok": True, "campaign": campaign_id, "status": "Paused"}


@app.post("/campaign/{campaign_id}/kick")
async def campaign_kick(
    campaign_id: str,
    request: Request,
    limit: Optional[str] = Query("ALL"),
    x_cron_token: Optional[str] = Header(None),
    x_webhook_token: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
):
    """
    One-click: mark campaign scheduled and immediately run the campaign runner.
    Honors quiet hours (will queue-only or block accordingly).
    """
    _require_token(request, token, x_webhook_token, x_cron_token)
    # =====================================================
    # EMERGENCY NUCLEAR STOP - OVERRIDE ALL OTHER LOGIC
    # =====================================================
    return {"ok": False, "error": "EMERGENCY_STOP_ALL_CAMPAIGN_KICKS_DISABLED", "emergency": True}
    
    tbl = _get_campaigns_tbl()
    if not tbl:
        raise HTTPException(500, "Campaigns table unavailable")
    await asyncio.to_thread(_safe_update, tbl, campaign_id, {"Status": "Scheduled", "Active": True})
    safe_limit = _parse_limit_param(limit)
    runner_limit = _runner_limit_arg(safe_limit)
    if not _run_campaigns:
        return {"ok": False, "error": "campaign runner unavailable"}
    if is_quiet_hours_local():
        if not ALLOW_QUEUE_OUTSIDE_HOURS:
            return {"ok": False, "error": "Quiet hours (Central). Queueing disabled.", "quiet_hours": True}
        res = await asyncio.to_thread(_run_campaigns, runner_limit, False)
        res.update({"note": "Queued only (quiet hours).", "quiet_hours": True})
        return res
    return await asyncio.to_thread(_run_campaigns, runner_limit, True)


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
    if not _run_autoresponder:
        return {"ok": False, "error": "autoresponder unavailable"}
    if not AUTORESPONDER_ALWAYS_ON and is_quiet_hours_local():
        return {"ok": False, "error": "Quiet hours (Central). Autoresponder disabled by config.", "quiet_hours": True}
    return await asyncio.to_thread(_run_autoresponder, limit, view)


@app.post("/reset-quotas")
async def reset_quotas_endpoint(
    request: Request,
    x_cron_token: Optional[str] = Header(None),
    x_webhook_token: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
):
    _require_token(request, token, x_webhook_token, x_cron_token)
    if not _reset_daily_quotas:
        return {"ok": False, "error": "quota reset unavailable"}
    return await asyncio.to_thread(_reset_daily_quotas)


@app.post("/metrics")
async def metrics_endpoint(
    request: Request,
    x_cron_token: Optional[str] = Header(None),
    x_webhook_token: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
):
    _require_token(request, token, x_webhook_token, x_cron_token)
    if not _update_metrics:
        return {"ok": False, "error": "metrics tracker unavailable"}
    return await asyncio.to_thread(_update_metrics)


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
    # =====================================================
    # EMERGENCY NUCLEAR STOP - OVERRIDE ALL OTHER LOGIC
    # =====================================================
    return {"ok": False, "error": "EMERGENCY_STOP_ALL_RETRIES_DISABLED", "emergency": True}
    
    if TEST_MODE:
        return {"ok": True, "status": "mock_retry"}
    if is_quiet_hours_local():
        return {"ok": False, "error": "Quiet hours (Central). Retries blocked.", "quiet_hours": True}
    if not _run_retry:
        return {"ok": False, "error": "retry runner unavailable"}
    return await asyncio.to_thread(_run_retry, limit)


@app.post("/aggregate-kpis")
async def aggregate_kpis_endpoint(
    request: Request,
    x_cron_token: Optional[str] = Header(None),
    x_webhook_token: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
):
    _require_token(request, token, x_webhook_token, x_cron_token)
    if not _aggregate_kpis:
        return {"ok": False, "error": "kpi aggregator unavailable"}
    return await asyncio.to_thread(_aggregate_kpis)


# ─────────────── /cron/all (concurrent orchestration + immediate logs) ─────
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
    Immediate Airtable Run logs per step; KPIs at end.
    """
    _require_token(request, token, x_webhook_token, x_cron_token)
    results: Dict[str, Any] = {}
    totals = {"processed": 0, "errors": 0}
    runs_tbl, kpis_tbl = _get_perf_tables()

    # Strict health gates
    for mode in ["prospects", "leads", "inbounds"]:
        try:
            health_result = _strict_health(mode)
        except Exception as e:
            health_result = {"ok": False, "error": str(e)}
        results[f"{mode}_health"] = health_result
        await _log_run_async(runs_tbl, f"{mode.upper()}_HEALTH", health_result)
        if not health_result.get("ok"):
            await _log_run_async(runs_tbl, f"{mode.upper()}_HEALTH_FAIL", health_result)
            await _log_kpi_async(kpis_tbl, "TOTAL_ERRORS", 1)
            return {"ok": False, "error": f"Health check failed for {mode}", "results": results}

    # Quiet hours flow
    if is_quiet_hours_local():
        # autoresponder if allowed
        if AUTORESPONDER_ALWAYS_ON and _run_autoresponder:
            try:
                r = await asyncio.to_thread(_run_autoresponder, 50, "Unprocessed Inbounds")
            except Exception as e:
                r = {"ok": False, "error": str(e)}
            results["autoresponder"] = r
            await _log_run_async(runs_tbl, "AUTORESPONDER", r)
            totals["processed"] += int(r.get("processed", 0)) if isinstance(r.get("processed", 0), int) else 0
        else:
            results["autoresponder"] = {"ok": True, "skipped": "quiet_hours"}
            await _log_run_async(runs_tbl, "AUTORESPONDER", results["autoresponder"])

        # followups skipped
        results["followups"] = {"ok": True, "skipped": "quiet_hours"}
        await _log_run_async(runs_tbl, "FOLLOWUPS", results["followups"])

        # metrics + aggregate KPIs (best effort)
        if _update_metrics:
            try:
                r = await asyncio.to_thread(_update_metrics)
            except Exception as e:
                r = {"ok": False, "error": str(e)}
        else:
            r = {"ok": False, "error": "metrics tracker unavailable"}
        results["metrics"] = r
        await _log_run_async(runs_tbl, "METRICS", r)

        if _aggregate_kpis:
            try:
                r = await asyncio.to_thread(_aggregate_kpis)
            except Exception as e:
                r = {"ok": False, "error": str(e)}
        else:
            r = {"ok": False, "error": "kpi aggregator unavailable"}
        results["aggregate_kpis"] = r
        await _log_run_async(runs_tbl, "AGGREGATE_KPIS", r)

        # campaigns queue-only
        if _run_campaigns and ALLOW_QUEUE_OUTSIDE_HOURS:
            try:
                r = await asyncio.to_thread(_run_campaigns, "ALL", False)
                r.update({"note": "Queued only (quiet hours).", "quiet_hours": True})
            except Exception as e:
                r = {"ok": False, "error": str(e)}
        else:
            r = {"ok": True, "skipped": "quiet_hours"}
        results["campaign_runner"] = r
        await _log_run_async(runs_tbl, "CAMPAIGN_RUNNER", r)

        # outbound + retry skipped
        results["outbound"] = {"ok": True, "skipped": "quiet_hours"}
        results["retry"] = {"ok": True, "skipped": "quiet_hours"}
        await _log_run_async(runs_tbl, "OUTBOUND", results["outbound"])
        await _log_run_async(runs_tbl, "RETRY", results["retry"])

        # final KPIs
        await _log_kpi_async(kpis_tbl, "TOTAL_PROCESSED", totals["processed"])
        await _log_kpi_async(kpis_tbl, "TOTAL_ERRORS", totals["errors"])
        return {"ok": True, "results": results, "totals": totals, "quiet_hours": True, "timestamp": _iso_ts()}

    # Normal hours flow — run steps, log as they complete, some in parallel
    step_results: Dict[str, Dict[str, Any]] = {}

    async def _run_step(name: str, func: Callable[[], Any]):
        try:
            res = {"ok": True, "status": f"mock_{name.lower()}"} if TEST_MODE else await asyncio.to_thread(func)
        except Exception as e:
            res = {"ok": False, "error": str(e)}
        step_results[name.lower()] = res
        await _log_run_async(runs_tbl, name, res)
        p = res.get("processed", 0) or res.get("total_sent", 0) or res.get("sent", 0)
        if isinstance(p, (int, float)):
            totals["processed"] += int(p)
        return res

    tasks = []

    if _send_batch:
        tasks.append(_run_step("OUTBOUND", lambda: _send_batch(limit=limit)))
    else:
        step_results["outbound"] = {"ok": False, "error": "send_batch unavailable"}
        await _log_run_async(runs_tbl, "OUTBOUND", step_results["outbound"])

    if _run_autoresponder:
        tasks.append(_run_step("AUTORESPONDER", lambda: _run_autoresponder(50, "Unprocessed Inbounds")))
    else:
        step_results["autoresponder"] = {"ok": False, "error": "autoresponder unavailable"}
        await _log_run_async(runs_tbl, "AUTORESPONDER", step_results["autoresponder"])

    tasks.append(_run_step("FOLLOWUPS", lambda: _run_followups()))

    if _update_metrics:
        tasks.append(_run_step("METRICS", lambda: _update_metrics()))
    else:
        step_results["metrics"] = {"ok": False, "error": "metrics tracker unavailable"}
        await _log_run_async(runs_tbl, "METRICS", step_results["metrics"])

    if _run_retry:
        tasks.append(_run_step("RETRY", lambda: _run_retry(limit=100)))
    else:
        step_results["retry"] = {"ok": False, "error": "retry runner unavailable"}
        await _log_run_async(runs_tbl, "RETRY", step_results["retry"])

    if _aggregate_kpis:
        tasks.append(_run_step("AGGREGATE_KPIS", lambda: _aggregate_kpis()))
    else:
        step_results["aggregate_kpis"] = {"ok": False, "error": "kpi aggregator unavailable"}
        await _log_run_async(runs_tbl, "AGGREGATE_KPIS", step_results["aggregate_kpis"])

    if _run_campaigns:
        tasks.append(_run_step("CAMPAIGN_RUNNER", lambda: _run_campaigns("ALL")))
    else:
        step_results["campaign_runner"] = {"ok": False, "error": "campaign runner unavailable"}
        await _log_run_async(runs_tbl, "CAMPAIGN_RUNNER", step_results["campaign_runner"])

    if tasks:
        await asyncio.gather(*tasks)

    results.update(step_results)

    await _log_kpi_async(kpis_tbl, "TOTAL_PROCESSED", totals["processed"])
    await _log_kpi_async(kpis_tbl, "TOTAL_ERRORS", totals["errors"])
    return {"ok": True, "results": results, "totals": totals, "timestamp": _iso_ts()}


# ─────────────── Drip Admin (UTC normalize queued sends) ────────────
@app.post("/admin/drip/normalize")
async def drip_normalize(
    request: Request,
    dry_run: bool = Query(True),
    force_now: bool = Query(False),
    limit: int = Query(1000),
    x_cron_token: Optional[str] = Header(None),
    x_webhook_token: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
):
    _require_token(request, token, x_webhook_token, x_cron_token)
    return await asyncio.to_thread(_normalize_next_send_dates, dry_run, force_now, limit)


@app.post("/admin/drip/force-now")
async def drip_force_now(
    request: Request,
    limit: int = Query(1000),
    x_cron_token: Optional[str] = Header(None),
    x_webhook_token: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
):
    _require_token(request, token, x_webhook_token, x_cron_token)
    return await asyncio.to_thread(_normalize_next_send_dates, False, True, limit)


# ─────────────── Numbers Admin (from_number + quotas) ───────────────
@app.post("/admin/numbers/backfill")
async def numbers_backfill_endpoint(
    request: Request,
    dry_run: bool = Query(True),
    x_cron_token: Optional[str] = Header(None),
    x_webhook_token: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
):
    _require_token(request, token, x_webhook_token, x_cron_token)
    try:
        return await asyncio.to_thread(_backfill_drip_from_numbers, dry_run)
    except Exception as e:
        _log_error("numbers_backfill", e)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/admin/numbers/recalc")
async def numbers_recalc_endpoint(
    request: Request,
    for_date: Optional[str] = Query(None, description="YYYY-MM-DD (defaults to today in Central)"),
    x_cron_token: Optional[str] = Header(None),
    x_webhook_token: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
):
    _require_token(request, token, x_webhook_token, x_cron_token)
    try:
        target = for_date or date.fromisoformat(central_now().date().isoformat()).isoformat()
        return await asyncio.to_thread(_recalc_numbers_sent_today, target)
    except Exception as e:
        _log_error("numbers_recalc", e)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/admin/numbers/reset")
async def numbers_reset_endpoint(
    request: Request,
    x_cron_token: Optional[str] = Header(None),
    x_webhook_token: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
):
    _require_token(request, token, x_webhook_token, x_cron_token)
    try:
        return await asyncio.to_thread(_reset_numbers_daily_counters)
    except Exception as e:
        _log_error("numbers_reset", e)
        raise HTTPException(status_code=500, detail=str(e))


# ───────────────────── Engine (manual debugging) ────────────────────
@app.get("/engine")
async def trigger_engine(
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
    health = _strict_health(mode)
    if not health.get("ok"):
        raise HTTPException(status_code=500, detail=f"Health check failed for {mode}")
    res = await asyncio.to_thread(_run_engine, mode, limit=limit, retry_limit=retry_limit)
    return {"ok": True, "mode": mode, "result": res}