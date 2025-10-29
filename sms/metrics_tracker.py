"""
📊 Metrics Tracker v3.1
──────────────────────────────
Refactored for unified telemetry + structured logging.

Responsibilities:
 - Compute per-campaign + global SMS metrics
 - Write KPI + Run entries to Performance base
 - Auto-handle Airtable computed/unknown fields
 - Trigger SMS/webhook alerts for abnormal rates
"""

from __future__ import annotations
import os, json, re, traceback
from datetime import datetime, timezone, timedelta
from functools import lru_cache
from typing import List, Optional, Dict, Any

from dotenv import load_dotenv

load_dotenv()

from sms.config import CONV_FIELDS, CONVERSATIONS_FIELDS
from sms.runtime import get_logger

logger = get_logger("metrics_tracker")

def _sync_to_campaign_control_base(campaign_name: str, metrics_data: Dict[str, Any]):
    """Sync campaign metrics to Campaign Control Base"""
    try:
        # Check if sync is enabled
        if not os.getenv("CAMPAIGN_CONTROL_SYNC_ENABLED", "true").lower() in ("1", "true", "yes"):
            return
            
        from sms.datastore import CONNECTOR
        control_handle = CONNECTOR.campaign_control_campaigns()
        
        if control_handle.in_memory:
            logger.debug("Campaign Control Base not configured - using in-memory fallback")
            return
            
        control_campaigns = control_handle.table
        
        # Search for existing campaign in control base
        formula = f"{{Campaign Name}}='{campaign_name.replace("'", "\\'")}'"
        
        try:
            existing = control_campaigns.all(formula=formula, max_records=1)
        except Exception as api_error:
            # Handle specific access/permission errors gracefully
            error_msg = str(api_error).lower()
            if any(x in error_msg for x in ["403", "forbidden", "permission", "not found"]):
                logger.debug(f"Campaign Control Base access denied - skipping metrics sync for '{campaign_name}'")
                return
            else:
                # Re-raise other unexpected errors
                raise
        
        # Map metrics to control base fields
        control_metrics = {
            "Total Sent": metrics_data.get("total_sent", 0),
            "Total Delivered": metrics_data.get("total_delivered", 0),
            "Total Failed": metrics_data.get("total_failed", 0),
            "Total Replies": metrics_data.get("total_replies", 0),
            "Total Opt Outs": metrics_data.get("total_opt_outs", 0),
            "Delivery Rate": metrics_data.get("delivery_rate", 0),
            "Opt Out Rate": metrics_data.get("opt_out_rate", 0),
            "Last Metrics Update": datetime.now(timezone.utc).isoformat()
        }
        
        if existing:
            # Update existing campaign
            control_campaigns.update(existing[0]["id"], control_metrics)
            logger.debug(f"📊 Synced metrics to Campaign Control Base for '{campaign_name}'")
        else:
            logger.debug(f"⚠️ Campaign '{campaign_name}' not found in Campaign Control Base")
            
    except Exception as e:
        logger.warning(f"⚠️ Campaign Control Base metrics sync failed for '{campaign_name}': {e}")

# ─────────────────────────── Optional imports ───────────────────────────
try:
    from pyairtable import Api as _ATApi
except Exception:
    _ATApi = None

try:
    from pyairtable import Table as _ATTable
except Exception:
    _ATTable = None

try:
    from sms.textgrid_sender import send_message
except Exception:
    send_message = None

try:
    from sms.kpi_logger import log_kpi
except Exception:

    def log_kpi(*_a, **_k):
        pass


try:
    from sms.logger import log_run
except Exception:

    def log_run(*_a, **_k):
        pass


# ─────────────────────────── Alerts / thresholds ───────────────────────────
ALERT_PHONE = os.getenv("ALERT_PHONE")
ALERT_FROM_NUMBER = os.getenv("ALERT_FROM_NUMBER") or os.getenv("TEXTGRID_ALERT_FROM")
ALERT_WEBHOOK = os.getenv("ALERT_EMAIL_WEBHOOK")
OPT_OUT_THRESHOLD = float(os.getenv("OPT_OUT_ALERT_THRESHOLD", "2.5"))  # %
DELIVERY_THRESHOLD = float(os.getenv("DELIVERY_ALERT_THRESHOLD", "90"))  # %
COOLDOWN_HOURS = int(os.getenv("OPT_OUT_ALERT_COOLDOWN_HOURS", "24"))

# ─────────────────────────── Airtable setup ───────────────────────────
MAIN_KEY = os.getenv("AIRTABLE_API_KEY")
REPORTING_KEY = os.getenv("AIRTABLE_REPORTING_KEY") or MAIN_KEY
LEADS_BASE = os.getenv("LEADS_CONVOS_BASE")
PERF_BASE = os.getenv("PERFORMANCE_BASE")

CAMPAIGNS_TABLE = os.getenv("CAMPAIGNS_TABLE", "Campaigns")
CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")
KPIS_TABLE = os.getenv("KPIS_TABLE_NAME", "KPIs")
RUNS_TABLE = os.getenv("RUNS_TABLE_NAME", "Logs")

# ─────────────────────────── Field mappings ───────────────────────────
CONV_FROM_FIELD = CONV_FIELDS["FROM"]
CONV_STATUS_FIELD = CONV_FIELDS["STATUS"]
CONV_DIRECTION_FIELD = CONV_FIELDS["DIRECTION"]
CONV_MESSAGE_FIELD = CONV_FIELDS["BODY"]
CONV_CAMPAIGN_FIELD = CONVERSATIONS_FIELDS.get("CAMPAIGN_LINK", "Campaign")

DELIVERED_STATES = {"DELIVERED"}
FAILED_STATES = {"FAILED", "UNDELIVERED", "UNDELIVERABLE"}


# ─────────────────────────── Airtable factories ───────────────────────────
def _make_table(api_key: Optional[str], base_id: Optional[str], table_name: str):
    if not (api_key and base_id):
        return None
    try:
        if _ATApi:
            return _ATApi(api_key).table(base_id, table_name)
        if _ATTable:
            return _ATTable(api_key, base_id, table_name)
    except Exception as e:
        logger.error(f"Airtable init failed for {table_name}: {e}", exc_info=True)
    return None


@lru_cache(maxsize=None)
def _t_campaigns():
    return _make_table(MAIN_KEY, LEADS_BASE, CAMPAIGNS_TABLE)


@lru_cache(maxsize=None)
def _t_convos():
    return _make_table(MAIN_KEY, LEADS_BASE, CONVERSATIONS_TABLE)


# ─────────────────────────── Utilities ───────────────────────────
def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _parse_dt(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        return None


def _norm(s: str):
    return re.sub(r"[^a-z0-9]+", "", s.strip().lower()) if isinstance(s, str) else s


def _field(name: str) -> str:
    return "{" + name + "}"


def _campaign_formula(name: str) -> str:
    safe = (name or "").replace("'", "\\'")
    return f"{_field(CONV_CAMPAIGN_FIELD)}='{safe}'"


def _try_fetch(table, formula: str) -> list:
    try:
        return table.all(formula=formula)
    except Exception as e:
        logger.warning(f"⚠️ Fetch failed for formula {formula[:40]}...: {e}")
        return []


def _status(rec):
    return str(rec.get("fields", {}).get(CONV_STATUS_FIELD, "")).upper()


def _body(rec):
    return str(rec.get("fields", {}).get(CONV_MESSAGE_FIELD, "")).lower()


def _direction(rec):
    return str(rec.get("fields", {}).get(CONV_DIRECTION_FIELD, "")).upper()


def _notify(msg: str):
    logger.warning(f"🚨 ALERT: {msg}")
    if ALERT_PHONE and ALERT_FROM_NUMBER and send_message:
        try:
            send_message(from_number=ALERT_FROM_NUMBER, to=ALERT_PHONE, message=msg)
        except Exception as e:
            logger.warning(f"❌ SMS alert failed: {e}")
    elif ALERT_PHONE and send_message and not ALERT_FROM_NUMBER:
        logger.warning("❌ SMS alert skipped: ALERT_FROM_NUMBER not configured")
    if ALERT_WEBHOOK and ALERT_WEBHOOK.startswith(("http://", "https://")):
        try:
            import requests

            requests.post(ALERT_WEBHOOK, json={"text": msg}, timeout=10)
        except Exception as e:
            logger.warning(f"❌ Webhook alert failed: {e}")


def _should_alert(last_alert_at, rate: float, threshold: float) -> bool:
    if rate < threshold:
        return False
    dt = _parse_dt(last_alert_at[0] if isinstance(last_alert_at, list) else last_alert_at)
    if not dt:
        return True
    return datetime.now(timezone.utc) - dt >= timedelta(hours=COOLDOWN_HOURS)


# ─────────────────────────── Core ───────────────────────────
def update_metrics() -> dict:
    """
    Compute per-campaign & global SMS metrics:
     - Delivery rate
     - Opt-out rate
     - Response count
     - KPI + Logs entries
    """
    campaigns = _t_campaigns()
    convos = _t_convos()
    if not (campaigns and convos):
        logger.error("Missing Airtable setup (campaigns/conversations)")
        return {"ok": False, "error": "Missing Airtable setup"}

    today = datetime.now(timezone.utc).date().isoformat()
    summary, global_stats = [], {"sent": 0, "delivered": 0, "failed": 0, "responses": 0, "optouts": 0}
    logger.info("📊 Starting metrics update...")

    try:
        all_campaigns = campaigns.all()
    except Exception as e:
        logger.error(f"Failed to fetch Campaigns: {e}", exc_info=True)
        return {"ok": False, "error": "Failed to fetch Campaigns"}

    for camp in all_campaigns:
        cf = camp.get("fields", {}) or {}
        camp_id = camp.get("id")
        camp_name = cf.get("Name") or cf.get("name") or "Unknown"

        try:
            fbf = _campaign_formula(camp_name)
            dirf = _field(CONV_DIRECTION_FIELD)
            sent = _try_fetch(convos, f"AND(LOWER({dirf})='out', {fbf})")
            inbound = _try_fetch(convos, f"AND(LOWER({dirf})='in', {fbf})")

            total_sent = len(sent)
            delivered = [r for r in sent if _status(r) in DELIVERED_STATES]
            failed = [r for r in sent if _status(r) in FAILED_STATES]
            responses = len(inbound)
            optouts = [r for r in inbound if "stop" in _body(r)]
            total_optouts = len(optouts)

            delivery_rate = round(len(delivered) / total_sent * 100, 2) if total_sent else 0
            optout_rate = round(total_optouts / total_sent * 100, 2) if total_sent else 0

            # Update campaign counters safely
            patch = {
                "total_sent": total_sent,
                "total_delivered": len(delivered),
                "total_failed": len(failed),
                "total_replies": responses,
                "total_opt_outs": total_optouts,
                "delivery_rate": delivery_rate,
                "opt_out_rate": optout_rate,
                "last_run_at": _now_iso(),
            }
            try:
                campaigns.update(camp_id, patch)
                
                # Sync to Campaign Control Base
                _sync_to_campaign_control_base(camp_name, patch)
                
            except Exception as e:
                logger.warning(f"⚠️ Campaign update failed: {e}")

            # Alerts
            last_alert_at = cf.get("last_alert_at") or cf.get("Last Alert At")
            alerted = False
            if _should_alert(last_alert_at, optout_rate, OPT_OUT_THRESHOLD):
                _notify(f"⚠️ High opt-out rate for {camp_name}: {optout_rate}% (sent={total_sent})")
                alerted = True
            bad_delivery = 100 - delivery_rate
            if _should_alert(last_alert_at, bad_delivery, 100 - DELIVERY_THRESHOLD):
                _notify(f"⚠️ Low delivery rate for {camp_name}: {delivery_rate}% (sent={total_sent})")
                alerted = True
            if alerted:
                try:
                    campaigns.update(camp_id, {"last_alert_at": _now_iso()})
                except Exception as e:
                    logger.warning(f"⚠️ Alert timestamp update failed: {e}")

            # KPI logs
            for metric, value in [
                ("TOTAL_SENT", total_sent),
                ("DELIVERED", len(delivered)),
                ("FAILED", len(failed)),
                ("RESPONSES", responses),
                ("OPTOUTS", total_optouts),
                ("DELIVERY_RATE", delivery_rate),
                ("OPTOUT_RATE", optout_rate),
            ]:
                log_kpi(metric, value, campaign=camp_name)

            summary.append(
                {
                    "campaign": camp_name,
                    "sent": total_sent,
                    "delivered": len(delivered),
                    "failed": len(failed),
                    "responses": responses,
                    "optouts": total_optouts,
                    "delivery_rate": delivery_rate,
                    "optout_rate": optout_rate,
                }
            )

            # Global rollup
            global_stats["sent"] += total_sent
            global_stats["delivered"] += len(delivered)
            global_stats["failed"] += len(failed)
            global_stats["responses"] += responses
            global_stats["optouts"] += total_optouts

        except Exception as e:
            logger.warning(f"❌ Metrics update failed for {camp_name}: {e}", exc_info=True)

    # Global KPI summary
    for metric, value in [
        ("TOTAL_SENT", global_stats["sent"]),
        ("DELIVERED", global_stats["delivered"]),
        ("FAILED", global_stats["failed"]),
        ("RESPONSES", global_stats["responses"]),
        ("OPTOUTS", global_stats["optouts"]),
    ]:
        log_kpi(metric, value, campaign="ALL")

    log_run("METRICS_UPDATE", processed=global_stats["sent"], breakdown=summary)
    logger.info(f"✅ Metrics update complete → {len(summary)} campaigns | sent={global_stats['sent']}")
    return {"ok": True, "summary": summary, "global": global_stats}
