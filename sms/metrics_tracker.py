# sms/metrics_tracker.py
import os
from datetime import datetime, timezone, timedelta
from pyairtable import Table
from sms.textgrid_sender import send_message
import requests

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
PERFORMANCE_BASE = os.getenv("AIRTABLE_PERFORMANCE_BASE_ID")
runs = Table(AIRTABLE_API_KEY, PERFORMANCE_BASE, "Runs/Logs")
kpis = Table(AIRTABLE_API_KEY, PERFORMANCE_BASE, "KPIs")

ALERT_PHONE = os.getenv("ALERT_PHONE")  # +1XXXXXXXXXX
ALERT_EMAIL_WEBHOOK = os.getenv("ALERT_EMAIL_WEBHOOK")  # optional
THRESHOLD = float(os.getenv("OPT_OUT_ALERT_THRESHOLD", "2.5"))
COOLDOWN_HOURS = int(os.getenv("OPT_OUT_ALERT_COOLDOWN_HOURS", "24"))

LEADS_CONVOS_BASE = os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
CAMPAIGN_CONTROL_BASE = os.getenv("AIRTABLE_CAMPAIGN_CONTROL_BASE_ID")
CAMPAIGNS_TABLE = os.getenv("CAMPAIGNS_TABLE", "Campaigns")
CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")

campaigns = Table(AIRTABLE_API_KEY, CAMPAIGN_CONTROL_BASE, CAMPAIGNS_TABLE)
convos = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, CONVERSATIONS_TABLE)

def _parse_dt(s: str | None):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

def _should_alert(last_alert_at_iso: str | None, rate: float) -> bool:
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
        f"⚠️ Opt-out rate high for '{campaign}': {rate:.2f}% "
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

def update_metrics():
    all_campaigns = campaigns.all()
    summary = []

    for camp in all_campaigns:
        cf = camp.get("fields", {})
        view_name = cf.get("view_name") or cf.get("name") or "Unknown"
        last_alert_at = cf.get("last_alert_at")

        # Count OUTBOUND sent for this campaign
        sent_records = convos.all(formula="AND({direction} = 'OUT', {campaign} = '{}')".format(view_name.replace("'", "\\'")))
        total_sent = len(sent_records)

        # Count opt-outs (we treat PROCESSED-NO and PROCESSED-WRONG as negatives)
        # Prefer same campaign match; fallback to status only if you don't record campaign on Conversations.
        neg_records = convos.all(formula="AND({campaign} = '{}', OR(LEFT({status}, 12) = 'PROCESSED-NO', LEFT({status}, 15) = 'PROCESSED-WRONG'))".format(view_name.replace("'", "\\'")))
        total_opt_outs = len(neg_records)

        rate = round((total_opt_outs / total_sent * 100), 2) if total_sent else 0.0

        # Update campaign metrics back to Airtable (make sure fields exist)
        campaigns.update(camp["id"], {
            "total_sent": total_sent,
            "total_opt_outs": total_opt_outs,
            "opt_out_rate": rate,
        })

        # Alerting with cooldown
        if _should_alert(last_alert_at, rate):
            _notify(view_name, rate, total_sent, total_opt_outs)
            campaigns.update(camp["id"], {"last_alert_at": datetime.now(timezone.utc).isoformat()})

        summary.append({
            "campaign": view_name,
            "sent": total_sent,
            "opt_outs": total_opt_outs,
            "rate": rate
        })

    print("📊 Metrics updated:", summary)
    return {"summary": summary}