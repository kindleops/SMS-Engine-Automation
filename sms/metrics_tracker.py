import os
from datetime import datetime, timezone, timedelta
from pyairtable import Table
from sms.textgrid_sender import send_message
import requests
import traceback

# --- Airtable Config ---
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
PERFORMANCE_BASE = os.getenv("PERFORMANCE_BASE") or os.getenv("AIRTABLE_PERFORMANCE_BASE_ID")
LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
CAMPAIGN_CONTROL_BASE = os.getenv("CAMPAIGN_CONTROL_BASE") or os.getenv("AIRTABLE_CAMPAIGN_CONTROL_BASE_ID")

# Tables
CAMPAIGNS_TABLE = os.getenv("CAMPAIGNS_TABLE", "Campaigns")
CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")

def _init_table(api_key, base, table_name):
    if not api_key or not base:
        print(f"⚠️ Missing Airtable env for {table_name}")
        return None
    try:
        return Table(api_key, base, table_name)
    except Exception:
        print(f"❌ Failed to init table {table_name}")
        traceback.print_exc()
        return None

runs      = _init_table(AIRTABLE_API_KEY, PERFORMANCE_BASE, "Runs/Logs")
kpis      = _init_table(AIRTABLE_API_KEY, PERFORMANCE_BASE, "KPIs")
campaigns = _init_table(AIRTABLE_API_KEY, CAMPAIGN_CONTROL_BASE, CAMPAIGNS_TABLE)
convos    = _init_table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, CONVERSATIONS_TABLE)

# --- Alerts Config ---
ALERT_PHONE = os.getenv("ALERT_PHONE")             # SMS alert
ALERT_EMAIL_WEBHOOK = os.getenv("ALERT_EMAIL_WEBHOOK")  # Slack/Teams webhook
THRESHOLD = float(os.getenv("OPT_OUT_ALERT_THRESHOLD", "2.5"))
COOLDOWN_HOURS = int(os.getenv("OPT_OUT_ALERT_COOLDOWN_HOURS", "24"))

# --- Helpers ---
def _parse_dt(s: str | None):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

def _should_alert(last_alert_at_iso: str | None, rate: float) -> bool:
    """Decide if an alert should be fired based on threshold + cooldown."""
    if rate < THRESHOLD:
        return False
    if not last_alert_at_iso:
        return True
    last_alert_at = _parse_dt(last_alert_at_iso)
    if not last_alert_at:
        return True
    return datetime.now(timezone.utc) - last_alert_at >= timedelta(hours=COOLDOWN_HOURS)

def _notify(campaign: str, rate: float, sent: int, optouts: int):
    msg = (
        f"⚠️ High opt-out rate for '{campaign}': {rate:.2f}% "
        f"(opt-outs {optouts}/{sent}). Threshold {THRESHOLD:.2f}%."
    )
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

# --- Main Update Function ---
def update_metrics():
    if not campaigns or not convos:
        return {"ok": False, "error": "Missing Airtable setup"}

    all_campaigns = campaigns.all()
    summary = []

    today = datetime.now(timezone.utc).date().isoformat()
    global_sent = 0
    global_optouts = 0
    run_id = None

    for camp in all_campaigns:
        cf = camp.get("fields", {})
        view_name = cf.get("view_name") or cf.get("name") or "Unknown"
        last_alert_at = cf.get("last_alert_at")

        # Count OUTBOUND sent
        sent_records = convos.all(
            formula=f"AND({{Direction}} = 'OUT', {{Campaign}} = '{view_name.replace("'", "''")}')"
        )
        total_sent = len(sent_records)

        # Count opt-outs (NO + WRONG)
        neg_records = convos.all(
            formula=f"AND({{Campaign}} = '{view_name.replace("'", "''")}', "
                    f"OR(LEFT({{Status}}, 12) = 'PROCESSED-NO', LEFT({{Status}}, 15) = 'PROCESSED-WRONG'))"
        )
        total_opt_outs = len(neg_records)

        rate = round((total_opt_outs / total_sent * 100), 2) if total_sent else 0.0

        # Accumulate totals
        global_sent += total_sent
        global_optouts += total_opt_outs

        # Update campaign row
        try:
            campaigns.update(camp["id"], {
                "total_sent": total_sent,
                "total_opt_outs": total_opt_outs,
                "opt_out_rate": rate,
            })
        except Exception:
            print(f"⚠️ Failed to update Campaign row for {view_name}")
            traceback.print_exc()

        # Alerts
        if _should_alert(last_alert_at, rate):
            _notify(view_name, rate, total_sent, total_opt_outs)
            try:
                campaigns.update(camp["id"], {"last_alert_at": datetime.now(timezone.utc).isoformat()})
            except Exception:
                pass

        # KPIs
        if kpis:
            for metric, value in [
                ("TOTAL_SENT", total_sent),
                ("TOTAL_OPTOUTS", total_opt_outs),
                ("OPTOUT_RATE", rate),
            ]:
                try:
                    kpis.create({"Campaign": view_name, "Metric": metric, "Value": value, "Date": today})
                except Exception as e:
                    print(f"❌ Failed to write KPI {metric} for {view_name}: {e}")
                    traceback.print_exc()

        summary.append({"campaign": view_name, "sent": total_sent, "optouts": total_opt_outs, "rate": rate})

    # Global totals
    global_rate = round((global_optouts / global_sent * 100), 2) if global_sent else 0.0
    if kpis:
        for metric, value in [
            ("TOTAL_SENT", global_sent),
            ("TOTAL_OPTOUTS", global_optouts),
            ("OPTOUT_RATE", global_rate),
        ]:
            try:
                kpis.create({"Campaign": "ALL", "Metric": metric, "Value": value, "Date": today})
            except Exception as e:
                print(f"❌ Failed to write global KPI {metric}: {e}")
                traceback.print_exc()

    # Log run
    if runs:
        try:
            run_record = runs.create({
                "Type": "METRICS_UPDATE",
                "Processed": global_sent,
                "Breakdown": str(summary),
                "Timestamp": datetime.now(timezone.utc).isoformat()
            })
            run_id = run_record["id"]
            print(f"📝 Logged run: {run_id}")
        except Exception:
            print(f"❌ Failed to log run")
            traceback.print_exc()

    print("📊 Metrics updated:", summary, "🌍 Global Totals:",
          {"sent": global_sent, "optouts": global_optouts, "rate": global_rate})

    return {
        "summary": summary,
        "global": {"sent": global_sent, "optouts": global_optouts, "rate": global_rate},
        "run_id": run_id,
    }