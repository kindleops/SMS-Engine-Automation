# sms/metrics_tracker.py
import os, json, traceback, requests
from datetime import datetime, timezone, timedelta
from pyairtable import Table
from sms.textgrid_sender import send_message

# --- Airtable Config ---
API_KEY          = os.getenv("AIRTABLE_API_KEY")
PERF_BASE        = os.getenv("PERFORMANCE_BASE")
LEADS_CONVOS_BASE= os.getenv("LEADS_CONVOS_BASE")
CONTROL_BASE     = os.getenv("CAMPAIGN_CONTROL_BASE")

CAMPAIGNS_TABLE      = os.getenv("CAMPAIGNS_TABLE", "Campaigns")
CONVERSATIONS_TABLE  = os.getenv("CONVERSATIONS_TABLE", "Conversations")
TEMPLATES_TABLE      = os.getenv("TEMPLATES_TABLE", "Templates")

# --- Alerts Config ---
ALERT_PHONE         = os.getenv("ALERT_PHONE")
ALERT_EMAIL_WEBHOOK = os.getenv("ALERT_EMAIL_WEBHOOK")
OPT_OUT_THRESHOLD   = float(os.getenv("OPT_OUT_ALERT_THRESHOLD", "2.5"))      # %
DELIVERY_THRESHOLD  = float(os.getenv("DELIVERY_ALERT_THRESHOLD", "90"))      # %
COOLDOWN_HOURS      = int(os.getenv("OPT_OUT_ALERT_COOLDOWN_HOURS", "24"))

# --- Init Tables ---
def _table(base, table):
    return Table(API_KEY, base, table) if base and API_KEY else None

runs      = _table(PERF_BASE, "Runs/Logs")
kpis      = _table(PERF_BASE, "KPIs")
campaigns = _table(CONTROL_BASE, CAMPAIGNS_TABLE)
convos    = _table(LEADS_CONVOS_BASE, CONVERSATIONS_TABLE)
templates = _table(LEADS_CONVOS_BASE, TEMPLATES_TABLE)


# -----------------------
# Helpers
# -----------------------
def _parse_dt(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        return None

def _should_alert(last_alert_at, rate, threshold):
    """Check if we should trigger alert based on threshold + cooldown."""
    if rate < threshold:
        return False
    if isinstance(last_alert_at, list):
        last_alert_at = last_alert_at[0]
    dt = _parse_dt(last_alert_at)
    if not dt:
        return True
    return datetime.now(timezone.utc) - dt >= timedelta(hours=COOLDOWN_HOURS)

def _notify(msg):
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
def update_metrics():
    if not campaigns or not convos:
        return {"ok": False, "error": "Missing Airtable setup"}

    today = datetime.now(timezone.utc).date().isoformat()
    summary = []
    global_stats = {"sent":0,"delivered":0,"failed":0,"responses":0,"optouts":0}
    run_id = None

    for camp in campaigns.all():
        cf = camp.get("fields", {})
        camp_name = cf.get("Name") or "Unknown"
        last_alert_at = cf.get("last_alert_at")

        # Outbound → Sent
        sent = convos.all(formula=f"AND({{direction}}='OUT',{{Campaign}}='{camp_name}')")
        total_sent = len(sent)

        # Delivery metrics
        delivered = [r for r in sent if r["fields"].get("status") == "DELIVERED"]
        failed    = [r for r in sent if r["fields"].get("status") == "FAILED"]

        # Inbound → Responses
        inbound = convos.all(formula=f"AND({{direction}}='IN',{{Campaign}}='{camp_name}')")
        responses = len(inbound)

        # Opt-outs
        optouts = [r for r in inbound if "stop" in str(r["fields"].get("message","")).lower()]
        total_optouts = len(optouts)

        # Rates
        delivery_rate = round((len(delivered) / total_sent * 100), 2) if total_sent else 0.0
        response_rate = round((responses / total_sent * 100), 2) if total_sent else 0.0
        optout_rate   = round((total_optouts / total_sent * 100), 2) if total_sent else 0.0

        # Update Campaign row
        try:
            campaigns.update(camp["id"], {
                "total_sent": total_sent,
                "delivered": len(delivered),
                "failed": len(failed),
                "responses": responses,
                "optouts": total_optouts,
                "delivery_rate": delivery_rate,
                "response_rate": response_rate,
                "optout_rate": optout_rate,
                "last_metrics_update": datetime.now(timezone.utc).isoformat()
            })
        except Exception:
            traceback.print_exc()

        # Alerts
        if _should_alert(last_alert_at, optout_rate, OPT_OUT_THRESHOLD):
            _notify(f"⚠️ High opt-out rate for {camp_name}: {optout_rate}%")
        if _should_alert(last_alert_at, 100 - delivery_rate, 100 - DELIVERY_THRESHOLD):
            _notify(f"⚠️ Low delivery rate for {camp_name}: {delivery_rate}%")

        # KPIs per campaign
        if kpis:
            for metric, value in [
                ("TOTAL_SENT", total_sent),
                ("DELIVERED", len(delivered)),
                ("FAILED", len(failed)),
                ("RESPONSES", responses),
                ("OPTOUTS", total_optouts),
                ("DELIVERY_RATE", delivery_rate),
                ("RESPONSE_RATE", response_rate),
                ("OPTOUT_RATE", optout_rate)
            ]:
                try:
                    kpis.create({
                        "Campaign": camp_name,
                        "Metric": metric,
                        "Value": value,
                        "Date": today
                    })
                except Exception:
                    traceback.print_exc()

        # Append to summary
        summary.append({
            "campaign": camp_name,
            "sent": total_sent,
            "delivered": len(delivered),
            "failed": len(failed),
            "responses": responses,
            "optouts": total_optouts,
            "delivery_rate": delivery_rate,
            "response_rate": response_rate,
            "optout_rate": optout_rate
        })

        # Global stats accumulation
        global_stats["sent"]      += total_sent
        global_stats["delivered"] += len(delivered)
        global_stats["failed"]    += len(failed)
        global_stats["responses"] += responses
        global_stats["optouts"]   += total_optouts

    # Global KPIs
    if kpis:
        for metric, value in [
            ("TOTAL_SENT", global_stats["sent"]),
            ("DELIVERED", global_stats["delivered"]),
            ("FAILED", global_stats["failed"]),
            ("RESPONSES", global_stats["responses"]),
            ("OPTOUTS", global_stats["optouts"])
        ]:
            try:
                kpis.create({
                    "Campaign": "ALL",
                    "Metric": metric,
                    "Value": value,
                    "Date": today
                })
            except Exception:
                traceback.print_exc()

    # Runs Log
    if runs:
        try:
            run_record = runs.create({
                "Type": "METRICS_UPDATE",
                "Processed": global_stats["sent"],
                "Breakdown": json.dumps(summary, indent=2),
                "Timestamp": datetime.now(timezone.utc).isoformat()
            })
            run_id = run_record["id"]
        except Exception:
            traceback.print_exc()

    return {"summary": summary, "global": global_stats, "run_id": run_id}