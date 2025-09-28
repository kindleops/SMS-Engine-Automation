# sms/health_strict.py
import os
from datetime import datetime, timezone
from fastapi import HTTPException
from pyairtable import Table
from sms.metrics_tracker import _notify

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE")


def iso_timestamp():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def strict_health(mode: str = "prospects"):
    """
    Actively verifies Airtable connectivity per engine mode.
    Returns {ok: True} only if all required tables respond.
    """

    if not AIRTABLE_API_KEY or not LEADS_CONVOS_BASE:
        raise HTTPException(
            status_code=500,
            detail={"ok": False, "errors": ["Missing Airtable API key or base ID"]},
        )

    required = {"Templates": Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, "Templates")}

    if mode == "prospects":
        required["Campaigns"] = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, "Campaigns")
        required["Prospects"] = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, "Prospects")
        required["Drip Queue"] = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, "Drip Queue")
    elif mode == "leads":
        required["Leads"] = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, "Leads")
        required["Conversations"] = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, "Conversations")
    elif mode == "inbounds":
        required["Conversations"] = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, "Conversations")
        required["Leads"] = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, "Leads")
        required["Prospects"] = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, "Prospects")
    else:
        # ✅ test_invalid_mode_raises expects 400 here
        raise HTTPException(
            status_code=400,
            detail={"ok": False, "errors": [f"Invalid mode: {mode}"]},
        )

    errors = []
    for name, tbl in required.items():
        try:
            tbl.all(max_records=1)  # smoke test
        except Exception as e:
            errors.append(f"{name} check failed: {e}")

    if errors:
        msg = f"❌ Strict health failed for mode={mode}: " + "; ".join(errors)
        print(msg)
        try:
            _notify(msg)
        except Exception as notify_err:  # ✅ no bare except
            print(f"Notify failed: {notify_err}")
        raise HTTPException(
            status_code=500,
            detail={"ok": False, "mode": mode, "errors": errors},
        )

    return {
        "ok": True,
        "mode": mode,
        "checked": list(required.keys()),
        "timestamp": iso_timestamp(),
    }