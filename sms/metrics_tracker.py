# sms/metrics_tracker.py
from __future__ import annotations

import os, json, re, traceback
from datetime import datetime, timezone, timedelta
from functools import lru_cache
from typing import List, Optional, Dict, Any

from dotenv import load_dotenv
load_dotenv()

# ───────────────────────────────── pyairtable (v2 Api / v1 Table) ─────────────────────────────────
try:
    from pyairtable import Api as _ATApi
except Exception:
    _ATApi = None

try:
    from pyairtable import Table as _ATTable
except Exception:
    _ATTable = None

# Optional SMS (best effort)
try:
    from sms.textgrid_sender import send_message
except Exception:
    send_message = None

# ───────────────────────────────────────── Alerts / thresholds ───────────────────────────────────
ALERT_PHONE: str | None = os.getenv("ALERT_PHONE")
ALERT_EMAIL_WEBHOOK: str | None = os.getenv("ALERT_EMAIL_WEBHOOK")  # must be http(s) to send
OPT_OUT_THRESHOLD: float = float(os.getenv("OPT_OUT_ALERT_THRESHOLD", "2.5"))   # %
DELIVERY_THRESHOLD: float = float(os.getenv("DELIVERY_ALERT_THRESHOLD", "90"))  # %
COOLDOWN_HOURS: int = int(os.getenv("OPT_OUT_ALERT_COOLDOWN_HOURS", "24"))

# ───────────────────────────────────────── Env: bases/keys ───────────────────────────────────────
MAIN_KEY       = os.getenv("AIRTABLE_API_KEY")
REPORTING_KEY  = os.getenv("AIRTABLE_REPORTING_KEY") or MAIN_KEY
LEADS_BASE     = os.getenv("LEADS_CONVOS_BASE")
PERF_BASE      = os.getenv("PERFORMANCE_BASE")

# ───────────────────────────────────────── Table names ───────────────────────────────────────────
CAMPAIGNS_TABLE     = os.getenv("CAMPAIGNS_TABLE", "Campaigns")
CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")
KPIS_TABLE          = os.getenv("KPIS_TABLE_NAME", "KPIs")
RUNS_TABLE          = os.getenv("RUNS_TABLE_NAME", "Runs/Logs")

# ───────────────────────────────── Field mappings (env overrideable) ─────────────────────────────
CONV_FROM_FIELD         = os.getenv("CONV_FROM_FIELD", "phone")
CONV_TO_FIELD           = os.getenv("CONV_TO_FIELD", "to_number")
CONV_MESSAGE_FIELD      = os.getenv("CONV_MESSAGE_FIELD", "message")
CONV_STATUS_FIELD       = os.getenv("CONV_STATUS_FIELD", "status")
CONV_DIRECTION_FIELD    = os.getenv("CONV_DIRECTION_FIELD", "direction")
CONV_TEXTGRID_ID_FIELD  = os.getenv("CONV_TEXTGRID_ID_FIELD", "TextGrid ID")
CONV_RECEIVED_AT_FIELD  = os.getenv("CONV_RECEIVED_AT_FIELD", "received_at")
CONV_INTENT_FIELD       = os.getenv("CONV_INTENT_FIELD", "intent_detected")
CONV_PROCESSED_BY_FIELD = os.getenv("CONV_PROCESSED_BY_FIELD", "processed_by")
CONV_SENT_AT_FIELD      = os.getenv("CONV_SENT_AT_FIELD", "sent_at")
CONV_CAMPAIGN_FIELD     = os.getenv("CONV_CAMPAIGN_FIELD", "Campaign")  # linked/name field used in formulas

# Normalize statuses to UPPER for matching
DELIVERED_STATES = {"DELIVERED"}          # adjust if you track more granular states
FAILED_STATES    = {"FAILED", "UNDELIVERED", "UNDELIVERABLE"}

# ───────────────────────────────────── Airtable factory ──────────────────────────────────────────
def _make_table(api_key: Optional[str], base_id: Optional[str], table_name: str):
    if not (api_key and base_id):
        return None
    try:
        if _ATApi is not None:
            return _ATApi(api_key).table(base_id, table_name)
        if _ATTable is not None:
            return _ATTable(api_key, base_id, table_name)
    except Exception:
        traceback.print_exc()
    return None

@lru_cache(maxsize=None)
def _t_campaigns():  return _make_table(MAIN_KEY,      LEADS_BASE, CAMPAIGNS_TABLE)
@lru_cache(maxsize=None)
def _t_convos():     return _make_table(MAIN_KEY,      LEADS_BASE, CONVERSATIONS_TABLE)
@lru_cache(maxsize=None)
def _t_kpis():       return _make_table(REPORTING_KEY, PERF_BASE,  KPIS_TABLE)
@lru_cache(maxsize=None)
def _t_runs():       return _make_table(REPORTING_KEY, PERF_BASE,  RUNS_TABLE)

# ───────────────────────────────────────── Helpers ───────────────────────────────────────────────
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _parse_dt(s: str | None) -> datetime | None:
    if not s: return None
    try: return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception: return None

def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", s.strip().lower()) if isinstance(s, str) else s

def _auto_field_map(table, sample_record_id: Optional[str] = None) -> dict[str, str]:
    keys: List[str] = []
    try:
        rec = table.get(sample_record_id) if sample_record_id else None
        if not rec:
            page = table.all(max_records=1)
            rec = page[0] if page else {"fields": {}}
        keys = list(rec.get("fields", {}).keys())
    except Exception:
        pass
    return {_norm(k): k for k in keys}

def _remap_existing_only(table, payload: dict, sample_record_id: Optional[str] = None) -> dict:
    amap = _auto_field_map(table, sample_record_id)
    out: dict = {}
    for k, v in payload.items():
        ak = amap.get(_norm(k))
        if ak: out[ak] = v
    return out

# Regex matchers for 422 error text from Airtable
_UNKNOWN_RE    = re.compile(r'Unknown field name:\s*"([^"]+)"', re.I)
_COMPUTED_RE   = re.compile(r'field\s*"([^"]+)"\s*cannot accept a value because the field is computed', re.I)
_INVALIDVAL_RE = re.compile(r'INVALID_VALUE_FOR_COLUMN.*?Field\s*"([^"]+)"', re.I)

def _safe_update(table, rec_id: str, payload: dict, sample_record_id: Optional[str] = None):
    """Update while automatically stripping unknown/computed/invalid fields that cause 422s."""
    if not (table and rec_id and payload): return None
    pending = dict(_remap_existing_only(table, payload, sample_record_id))
    for _ in range(8):
        try:
            if not pending: return None
            return table.update(rec_id, pending)
        except Exception as e:
            msg = str(e)
            m = _UNKNOWN_RE.search(msg) or _COMPUTED_RE.search(msg) or _INVALIDVAL_RE.search(msg)
            if m:
                pending.pop(m.group(1), None)
                continue
            traceback.print_exc()
            return None
    return None

def _safe_create(table, payload: dict, sample_record_id: Optional[str] = None):
    """Create while stripping unknown/computed/invalid fields."""
    if not (table and payload): return None
    pending = dict(_remap_existing_only(table, payload, sample_record_id))
    for _ in range(8):
        try:
            if not pending: return None
            return table.create(pending)
        except Exception as e:
            msg = str(e)
            m = _UNKNOWN_RE.search(msg) or _COMPUTED_RE.search(msg) or _INVALIDVAL_RE.search(msg)
            if m:
                pending.pop(m.group(1), None)
                continue
            traceback.print_exc()
            return None
    return None

def _safe_len(x) -> int:
    try: return len(x)
    except Exception: return 0

def _notify(msg: str) -> None:
    print(f"🚨 ALERT: {msg}")
    # SMS alert (best effort)
    if ALERT_PHONE and send_message:
        try:
            send_message(ALERT_PHONE, msg)
        except Exception as e:
            print(f"❌ SMS alert failed: {e}")
    # Webhook (Slack/Teams/email-gateway URL ONLY)
    if ALERT_EMAIL_WEBHOOK and str(ALERT_EMAIL_WEBHOOK).startswith(("http://", "https://")):
        try:
            import requests
            requests.post(ALERT_EMAIL_WEBHOOK, json={"text": msg}, timeout=10)
        except Exception as e:
            print(f"❌ Webhook alert failed: {e}")

def _should_alert(last_alert_at, rate: float, threshold: float) -> bool:
    # For opt-out we pass optout_rate, threshold=OPT_OUT_THRESHOLD
    # For delivery we pass (100 - delivery), threshold=(100 - DELIVERY_THRESHOLD)
    if rate < threshold: return False
    if isinstance(last_alert_at, list): last_alert_at = last_alert_at[0]
    dt = _parse_dt(last_alert_at)
    if not dt: return True
    return datetime.now(timezone.utc) - dt >= timedelta(hours=COOLDOWN_HOURS)

def _field(name: str) -> str:
    return "{" + name + "}"

def _campaign_match_formula(campaign_name: str) -> str:
    """Equality on linked field compares primary values of linked records in Airtable."""
    safe = (campaign_name or "").replace("'", r"\'")
    return f"{_field(CONV_CAMPAIGN_FIELD)}='{safe}'"

def _status(rec, field=CONV_STATUS_FIELD) -> str:
    try: return str(rec["fields"].get(field, "")).strip().upper()
    except Exception: return ""

def _body(rec) -> str:
    try: return str(rec["fields"].get(CONV_MESSAGE_FIELD, "")).lower()
    except Exception: return ""

def _direction(rec) -> str:
    try: return str(rec["fields"].get(CONV_DIRECTION_FIELD, "")).strip().upper()
    except Exception: return ""

# ───────────────────────────────────────── Core ─────────────────────────────────────────────────
def update_metrics() -> dict:
    """
    Pull Campaigns + Conversations, compute delivery/opt-out/response metrics,
    write KPIs & a Runs/Logs entry, and send alerts with cooldown.
    - Reads:   Campaigns, Conversations (LEADS_CONVOS_BASE)
    - Writes:  KPIs, Runs/Logs (PERFORMANCE_BASE)
    - Never writes into computed Campaign fields (422-proof)
    """
    campaigns = _t_campaigns()
    convos    = _t_convos()
    runs      = _t_runs()
    kpis      = _t_kpis()

    if not (campaigns and convos):
        return {"ok": False, "error": "Missing Airtable setup (campaigns/conversations tables)"}

    today = datetime.now(timezone.utc).date().isoformat()
    summary: list[dict] = []
    global_stats = {"sent": 0, "delivered": 0, "failed": 0, "responses": 0, "optouts": 0}
    run_id: str | None = None

    try:
        all_campaigns = campaigns.all()
    except Exception:
        traceback.print_exc()
        return {"ok": False, "error": "Failed to fetch Campaigns"}

    for camp in all_campaigns:
        try:
            cf        = camp.get("fields", {}) or {}
            camp_id   = camp.get("id")
            camp_name = cf.get("Name") or cf.get("name") or "Unknown"

            # Build case-insensitive formulas for OUT/IN by using LOWER()
            fbf_campaign = _campaign_match_formula(camp_name)
            dir_field = _field(CONV_DIRECTION_FIELD)
            formula_out = f"AND(LOWER({dir_field})='out', {fbf_campaign})"
            formula_in  = f"AND(LOWER({dir_field})='in',  {fbf_campaign})"

            # Outbound
            try:
                sent = convos.all(formula=formula_out)
            except Exception:
                traceback.print_exc()
                sent = []
            total_sent = _safe_len(sent)
            delivered  = [r for r in sent if _status(r) in DELIVERED_STATES]
            failed     = [r for r in sent if _status(r) in FAILED_STATES]

            # Inbound (responses)
            try:
                inbound = convos.all(formula=formula_in)
            except Exception:
                traceback.print_exc()
                inbound = []
            responses      = _safe_len(inbound)
            optouts        = [r for r in inbound if "stop" in _body(r)]  # simple SMS STOP detector
            total_optouts  = _safe_len(optouts)

            delivery_rate  = round((len(delivered) / total_sent * 100), 2) if total_sent else 0.0
            optout_rate    = round((total_optouts / total_sent * 100), 2) if total_sent else 0.0

            # Update Campaigns (strip unknown/computed on the fly)
            # IMPORTANT: Do NOT assume counters are writable; bases often compute them.
            camp_patch = {
                "total_sent":       total_sent,
                "total_delivered":  len(delivered),
                "total_failed":     len(failed),
                "total_replies":    responses,
                "total_opt_outs":   total_optouts,
                "delivery_rate":    delivery_rate,
                "opt_out_rate":     optout_rate,
                "last_run_at":      _now_iso(),
            }
            _safe_update(campaigns, camp_id, camp_patch, sample_record_id=camp_id)

            # Alerts with cooldown (shared "last_alert_at")
            last_alert_at = cf.get("last_alert_at") or cf.get("Last Alert At")
            alerted = False
            if _should_alert(last_alert_at, optout_rate, OPT_OUT_THRESHOLD):
                _notify(f"⚠️ High opt-out rate for {camp_name}: {optout_rate}% (sent={total_sent})")
                alerted = True
            # Convert delivery threshold into “bad rate” to reuse _should_alert logic
            bad_delivery_rate = 100.0 - delivery_rate
            bad_delivery_threshold = 100.0 - DELIVERY_THRESHOLD
            if _should_alert(last_alert_at, bad_delivery_rate, bad_delivery_threshold):
                _notify(f"⚠️ Low delivery rate for {camp_name}: {delivery_rate}% (sent={total_sent})")
                alerted = True
            if alerted:
                _safe_update(campaigns, camp_id, {"last_alert_at": _now_iso()}, sample_record_id=camp_id)

            # KPIs (best effort)
            if kpis:
                for metric, value in [
                    ("TOTAL_SENT",   total_sent),
                    ("DELIVERED",    len(delivered)),
                    ("FAILED",       len(failed)),
                    ("RESPONSES",    responses),
                    ("OPTOUTS",      total_optouts),
                    ("DELIVERY_RATE",delivery_rate),
                    ("OPTOUT_RATE",  optout_rate),
                ]:
                    _safe_create(
                        kpis,
                        {
                            "Campaign": camp_name,
                            "Metric":   metric,
                            "Value":    float(value),
                            "Date":     today,
                            "Timestamp":_now_iso(),
                        },
                    )

            # Summary row
            summary.append({
                "campaign":       camp_name,
                "sent":           total_sent,
                "delivered":      len(delivered),
                "failed":         len(failed),
                "responses":      responses,
                "optouts":        total_optouts,
                "delivery_rate":  delivery_rate,
                "optout_rate":    optout_rate,
            })

            # Global rollup
            global_stats["sent"]       += total_sent
            global_stats["delivered"]  += len(delivered)
            global_stats["failed"]     += len(failed)
            global_stats["responses"]  += responses
            global_stats["optouts"]    += total_optouts

        except Exception:
            print(f"❌ Metrics update failed for Campaign {camp.get('id')}")
            traceback.print_exc()

    # Global KPIs
    if kpis:
        for metric, value in [
            ("TOTAL_SENT", global_stats["sent"]),
            ("DELIVERED",  global_stats["delivered"]),
            ("FAILED",     global_stats["failed"]),
            ("RESPONSES",  global_stats["responses"]),
            ("OPTOUTS",    global_stats["optouts"]),
        ]:
            _safe_create(
                kpis,
                {
                    "Campaign": "ALL",
                    "Metric":   metric,
                    "Value":    float(value),
                    "Date":     today,
                    "Timestamp":_now_iso(),
                },
            )

    # Runs / Logs
    run_id = None
    if runs:
        try:
            rec = _safe_create(
                runs,
                {
                    "Type":      "METRICS_UPDATE",
                    "Processed": float(global_stats["sent"]),
                    "Breakdown": json.dumps(summary, ensure_ascii=False),
                    "Timestamp": _now_iso(),
                },
            )
            run_id = (rec or {}).get("id")
        except Exception:
            traceback.print_exc()

    return {"ok": True, "summary": summary, "global": global_stats, "run_id": run_id}
