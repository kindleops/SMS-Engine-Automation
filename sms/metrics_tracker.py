# sms/metrics_tracker.py

from __future__ import annotations
import os
import json
import traceback
import requests
from datetime import datetime, timezone, timedelta
from functools import lru_cache

from sms.textgrid_sender import send_message

try:
    from pyairtable import Table
except ImportError:
    Table = None


# --- Alerts Config ---
ALERT_PHONE: str | None = os.getenv("ALERT_PHONE")
ALERT_EMAIL_WEBHOOK: str | None = os.getenv("ALERT_EMAIL_WEBHOOK")
OPT_OUT_THRESHOLD: float = float(os.getenv("OPT_OUT_ALERT_THRESHOLD", "2.5"))  # %
DELIVERY_THRESHOLD: float = float(os.getenv("DELIVERY_ALERT_THRESHOLD", "90"))  # %
COOLDOWN_HOURS: int = int(os.getenv("OPT_OUT_ALERT_COOLDOWN_HOURS", "24"))


# -----------------------
# Airtable Factories
# -----------------------
@lru_cache(maxsize=None)
def get_runs() -> Table | None:
    api_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("PERFORMANCE_BASE")
    return Table(api_key, base_id, "Runs/Logs") if api_key and base_id and Table else None


@lru_cache(maxsize=None)
def get_kpis() -> Table | None:
    api_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("PERFORMANCE_BASE")
    return Table(api_key, base_id, "KPIs") if api_key and base_id and Table else None


@lru_cache(maxsize=None)
def get_campaigns() -> Table | None:
    api_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("CAMPAIGN_CONTROL_BASE")
    table = os.getenv("CAMPAIGNS_TABLE", "Campaigns")
    return Table(api_key, base_id, table) if api_key and base_id and Table else None


@lru_cache(maxsize=None)
def get_convos() -> Table | None:
    api_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("LEADS_CONVOS_BASE")
    table = os.getenv("CONVERSATIONS_TABLE", "Conversations")
    return Table(api_key, base_id, table) if api_key and base_id and Table else None


@lru_cache(maxsize=None)
def get_templates() -> Table | None:
    api_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("LEADS_CONVOS_BASE")
    table = os.getenv("TEMPLATES_TABLE", "Templates")
    return Table(api_key, base_id, table) if api_key and base_id and Table else None


# -----------------------
# Helpers
# -----------------------
def _parse_dt(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        return None


def _should_alert(last_alert_at, rate: float, threshold: float) -> bool:
    """Check if alert should trigger (threshold + cooldown)."""
    if rate < threshold:
        return False
    if isinstance(last_alert_at, list):
        last_alert_at = last_alert_at[0]
    dt = _parse_dt(last_alert_at)
    if not dt:
        return True
    return datetime.now(timezone.utc) - dt >= timedelta(hours=COOLDOWN_HOURS)


def _notify(msg: str) -> None:
    """Send alert via SMS and/or webhook."""
    print(f"🚨 ALERT: {msg}")
    if ALERT_PHONE:
        try:
            send_message(ALERT_PHONE, msg)
        except Exception as e:
            print(f"❌ SMS alert failed: {e}")
    if ALERT_EMAIL_WEBHOOK:
        try:
            requests.post(ALERT_EMAIL_WEBHOOK, json={"text": msg}, timeout=10)
        except Exception as e:
            print(f"❌ Webhook alert failed: {e}")


# -----------------------
# Metrics Update
# -----------------------
def update_metrics() -> dict:
    """
    Pull campaign + conversation data from Airtable,
    calculate metrics, push KPIs + logs, and trigger alerts.
    """
    campaigns = get_campaigns()
    convos = get_convos()
    runs = get_runs()
    kpis = get_kpis()

    if not campaigns or not convos:
        return {"ok": False, "error": "Missing Airtable setup"}

    today = datetime.now(timezone.utc).date().isoformat()
    summary: list[dict] = []
    global_stats = {"sent": 0, "delivered": 0, "failed": 0, "responses": 0, "optouts": 0}
    run_id: str | None = None

    for camp in campaigns.all():
        try:
            cf = camp.get("fields", {})
            camp_name = cf.get("Name") or "Unknown"
            last_alert_at = cf.get("last_alert_at")

            # outbound sent
            sent = convos.all(formula=f"AND({{direction}}='OUT',{{Campaign}}='{camp_name}')")
            total_sent = len(sent)

            delivered = [r for r in sent if r["fields"].get("status") == "DELIVERED"]
            failed = [r for r in sent if r["fields"].get("status") == "FAILED"]

            inbound = convos.all(formula=f"AND({{direction}}='IN',{{Campaign}}='{camp_name}')")
            responses = len(inbound)

            optouts = [r for r in inbound if "stop" in str(r["fields"].get("message", "")).lower()]
            total_optouts = len(optouts)

            delivery_rate = round((len(delivered) / total_sent * 100), 2) if total_sent else 0.0
            response_rate = round((responses / total_sent * 100), 2) if total_sent else 0.0
            optout_rate = round((total_optouts / total_sent * 100), 2) if total_sent else 0.0

            # update Airtable
            campaigns.update(camp["id"], {
                "total_sent": total_sent,
                "delivered": len(delivered),
                "failed": len(failed),
                "responses": responses,
                "optouts": total_optouts,
                "delivery_rate": delivery_rate,
                "response_rate": response_rate,
                "optout_rate": optout_rate,
                "last_metrics_update": datetime.now(timezone.utc).isoformat(),
            })

            # alerts
            if _should_alert(last_alert_at, optout_rate, OPT_OUT_THRESHOLD):
                _notify(f"⚠️ High opt-out rate for {camp_name}: {optout_rate}%")
            if _should_alert(last_alert_at, 100 - delivery_rate, 100 - DELIVERY_THRESHOLD):
                _notify(f"⚠️ Low delivery rate for {camp_name}: {delivery_rate}%")

            # KPIs
            if kpis:
                for metric, value in [
                    ("TOTAL_SENT", total_sent),
                    ("DELIVERED", len(delivered)),
                    ("FAILED", len(failed)),
                    ("RESPONSES", responses),
                    ("OPTOUTS", total_optouts),
                    ("DELIVERY_RATE", delivery_rate),
                    ("RESPONSE_RATE", response_rate),
                    ("OPTOUT_RATE", optout_rate),
                ]:
                    kpis.create({
                        "Campaign": camp_name,
                        "Metric": metric,
                        "Value": value,
                        "Date": today,
                    })

            # append summary
            summary.append({
                "campaign": camp_name,
                "sent": total_sent,
                "delivered": len(delivered),
                "failed": len(failed),
                "responses": responses,
                "optouts": total_optouts,
                "delivery_rate": delivery_rate,
                "response_rate": response_rate,
                "optout_rate": optout_rate,
            })

            # accumulate global stats
            global_stats["sent"] += total_sent
            global_stats["delivered"] += len(delivered)
            global_stats["failed"] += len(failed)
            global_stats["responses"] += responses
            global_stats["optouts"] += total_optouts

        except Exception:
            print(f"❌ Metrics update failed for {camp.get('id')}")
            traceback.print_exc()

    # global KPIs
    if kpis:
        for metric, value in [
            ("TOTAL_SENT", global_stats["sent"]),
            ("DELIVERED", global_stats["delivered"]),
            ("FAILED", global_stats["failed"]),
            ("RESPONSES", global_stats["responses"]),
            ("OPTOUTS", global_stats["optouts"]),
        ]:
            try:
                kpis.create({
                    "Campaign": "ALL",
                    "Metric": metric,
                    "Value": value,
                    "Date": today,
                })
            except Exception:
                traceback.print_exc()

    # runs log
    if runs:
        try:
            run_record = runs.create({
                "Type": "METRICS_UPDATE",
                "Processed": global_stats["sent"],
                "Breakdown": json.dumps(summary, indent=2),
                "Timestamp": datetime.now(timezone.utc).isoformat(),
            })
            run_id = run_record["id"]
        except Exception:
            traceback.print_exc()

    return {"summary": summary, "global": global_stats, "run_id": run_id}