# sms/routes/jobs.py
from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request
import os, subprocess, sys

CRON_TOKEN = os.getenv("CRON_TOKEN")

router = APIRouter(prefix="/jobs", tags=["jobs"])

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
    """
    Guard that enforces CRON_TOKEN when it is configured. Matches main app behaviour.
    """
    if not CRON_TOKEN:
        return
    provided = _extract_token(request, token, x_cron_token)
    if provided != CRON_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

def _py(cmd: list[str]) -> dict:
    try:
        out = subprocess.check_output([sys.executable] + cmd, stderr=subprocess.STDOUT, timeout=300)
        return {"ok": True, "output": out.decode("utf-8", "ignore")}
    except subprocess.CalledProcessError as e:
        return {"ok": False, "output": e.output.decode("utf-8", "ignore")}

@router.post("/autolinker")
def run_autolinker(_: Request = Depends(require_cron)):
    return _py(["-m","sms.workers.autolinker_worker","--once"])  # if you add an --once branch; otherwise omit

@router.post("/intent-batch")
def run_intent(_: Request = Depends(require_cron)):
    return _py(["-m","sms.workers.intent_worker"])

@router.post("/lead-promoter")
def run_promoter(_: Request = Depends(require_cron)):
    return _py(["-m","sms.workers.lead_promoter"])

@router.post("/ai-enrichment")
def run_ai(_: Request = Depends(require_cron)):
    return _py(["-m","sms.workers.ai_enrichment"])
