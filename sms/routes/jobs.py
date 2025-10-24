# sms/routes/jobs.py
"""
🧠 Scheduled Job Router
-----------------------
Secure endpoints to manually or CRON-trigger background jobs
(e.g. autolinker, intent detection, lead promotion, AI enrichment).
"""

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request
import os, subprocess, sys, shlex, logging

log = logging.getLogger("jobs")
CRON_TOKEN = os.getenv("CRON_TOKEN")
JOB_TIMEOUT = int(os.getenv("JOB_TIMEOUT_SEC", "600"))  # default 10 min

router = APIRouter(prefix="/jobs", tags=["jobs"])


# -------------------------------------------------------------------
# Auth Guard
# -------------------------------------------------------------------
def _extract_token(request: Request, qp_token: str | None, h_cron: str | None) -> str:
    if qp_token:
        return qp_token
    if h_cron:
        return h_cron
    auth = request.headers.get("authorization") or ""
    if auth.lower().startswith("bearer "):
        return auth.split(" ", 1)[1]
    return ""


def require_cron(
    request: Request,
    token: str | None = Query(default=None),
    x_cron_token: str | None = Header(default=None),
) -> None:
    """Require CRON_TOKEN in header, query, or bearer token."""
    if not CRON_TOKEN:
        return
    provided = _extract_token(request, token, x_cron_token)
    if provided != CRON_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------
def _run_python_module(mod: str, args: list[str] | None = None) -> dict:
    """Run a module in a subprocess and capture stdout/stderr safely."""
    cmd = [sys.executable, "-m", mod] + (args or [])
    cmd_str = " ".join(shlex.quote(c) for c in cmd)
    log.info(f"🚀 Starting job: {cmd_str}")
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, timeout=JOB_TIMEOUT)
        text = out.decode("utf-8", "ignore")[:5000]  # truncate noisy output
        log.info(f"✅ Job {mod} completed successfully.")
        return {"ok": True, "module": mod, "output": text}
    except subprocess.TimeoutExpired:
        log.error(f"⏰ Job {mod} timed out after {JOB_TIMEOUT}s")
        return {"ok": False, "module": mod, "error": f"Timed out after {JOB_TIMEOUT}s"}
    except subprocess.CalledProcessError as e:
        text = e.output.decode("utf-8", "ignore")[:5000]
        log.error(f"❌ Job {mod} failed: {e}")
        return {"ok": False, "module": mod, "error": text}


# -------------------------------------------------------------------
# Job Registry
# -------------------------------------------------------------------
JOB_MAP = {
    "autolinker": "sms.workers.autolinker_worker",
    "intent": "sms.workers.intent_worker",
    "lead-promoter": "sms.workers.lead_promoter",
}


def _launch(job_key: str, *extra_args: str):
    mod = JOB_MAP.get(job_key)
    if not mod:
        raise HTTPException(status_code=404, detail=f"Unknown job: {job_key}")
    return _run_python_module(mod, list(extra_args))


# -------------------------------------------------------------------
# Routes
# -------------------------------------------------------------------
@router.post("/{job_name}")
def run_job(job_name: str, _: Request = Depends(require_cron)):
    """
    Run a registered background job by name.
    Example: POST /jobs/autolinker?token=XYZ
    """
    return _launch(job_name)
