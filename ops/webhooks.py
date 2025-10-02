# ops/webhooks.py
from __future__ import annotations
import os
from fastapi import FastAPI, Request, HTTPException
from ops.airtable_sync import AirtableSync

app = FastAPI(title="Ops Webhooks")

SYNC = AirtableSync()  # uses env

# ---------- GitHub webhook ----------
@app.post("/webhooks/github")
async def github_webhook(request: Request):
    try:
        payload = await request.json()
        event = request.headers.get("X-GitHub-Event", "unknown")

        # Push events to Issues table if relevant
        if event == "issues":
            action = payload.get("action")
            issue = payload.get("issue", {})
            title = issue.get("title")
            url = issue.get("html_url")
            severity = "INFO" if action in ("opened", "edited") else "LOW"
            SYNC.log_issue(source="GitHub", title=f"[{action}] {title}", severity=severity, url=url, meta=payload)
        elif event == "push":
            repo = payload.get("repository", {}).get("full_name", "unknown")
            sha = payload.get("after", "")[:7]
            branch = payload.get("ref", "").split("/")[-1]
            SYNC.log_deploy(service=repo, env=os.getenv("ENVIRONMENT", "prod"), git_sha=sha, outcome="PUSH", meta={"branch": branch})
        else:
            SYNC.log_issue(source="GitHub", title=f"Unhandled event: {event}", severity="INFO")

        return {"ok": True}
    except Exception as e:
        SYNC.log_error(service="webhooks", message=f"GitHub webhook error: {e}")
        raise HTTPException(status_code=500, detail="github webhook failed")

# ---------- Render deploy hook ----------
@app.post("/webhooks/render")
async def render_webhook(request: Request):
    try:
        payload = await request.json()
        service = payload.get("service", "render")
        status = payload.get("status", "unknown")
        commit = payload.get("commit", "")[:7]
        env = payload.get("environment", os.getenv("ENVIRONMENT", "prod"))

        SYNC.log_deploy(service=service, env=env, git_sha=commit or "unknown", outcome=status, meta=payload)
        return {"ok": True}
    except Exception as e:
        SYNC.log_error(service="webhooks", message=f"Render webhook error: {e}")
        raise HTTPException(status_code=500, detail="render webhook failed")