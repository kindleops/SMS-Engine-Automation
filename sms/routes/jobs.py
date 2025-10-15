# sms/routes/jobs.py
from fastapi import APIRouter, Depends, HTTPException, Request
import os, subprocess, sys
from sms.auth import require_cron  # your existing bearer/x-cron-token guard

router = APIRouter(prefix="/jobs", tags=["jobs"])

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