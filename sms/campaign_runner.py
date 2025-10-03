# sms/campaign_runner.py
from __future__ import annotations

import os
import re
import json
import random
import traceback
from datetime import datetime, timezone, timedelta
from functools import lru_cache
from typing import Any, Dict, Tuple, List

from dotenv import load_dotenv
load_dotenv()  # ensure .env is loaded

from pyairtable import Api

from sms.outbound_batcher import send_batch, format_template
from sms.metrics_tracker import update_metrics
from sms.retry_runner import run_retry  # 🔁 retry handler


# ======================
# Airtable config
# ======================
CAMPAIGNS_TABLE = os.getenv("CAMPAIGNS_TABLE", "Campaigns")
TEMPLATES_TABLE = os.getenv("TEMPLATES_TABLE", "Templates")
DRIP_QUEUE_TABLE = os.getenv("DRIP_QUEUE_TABLE", "Drip Queue")

LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE")       # e.g., appMn2MKocaJ9I3rW
PERFORMANCE_BASE  = os.getenv("PERFORMANCE_BASE")        # e.g., appzRWrpFggxlRBgL

MAIN_KEY      = os.getenv("AIRTABLE_API_KEY")
REPORTING_KEY = os.getenv("AIRTABLE_REPORTING_KEY", MAIN_KEY)

# Prospect table aliases (your tables are literally "(P1)".."(P4)")
TABLE_ALIASES = {
    "P1": os.getenv("P1_TABLE", "(P1)"),
    "P2": os.getenv("P2_TABLE", "(P2)"),
    "P3": os.getenv("P3_TABLE", "(P3)"),
    "P4": os.getenv("P4_TABLE", "(P4)"),
}

# Common phone field variants (covers your P1 linked-owner fields)
PHONE_FIELDS = [
    "phone", "Phone", "Mobile", "Cell",
    "Owner Phone", "Owner Phone 1", "Owner Phone 2",
    "Phone 1", "Phone 2", "Phone 3",
    "Primary Phone", "Phone Number",
    "Phone 1 (from Linked Owner)", "Phone 2 (from Linked Owner)", "Phone 3 (from Linked Owner)",
]


# ======================
# Helpers
# ======================
def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _norm(s: Any) -> Any:
    return re.sub(r"[^a-z0-9]+", "", s.strip().lower()) if isinstance(s, str) else s


def _auto_field_map(table, sample_record_id: str | None = None) -> Dict[str, str]:
    """normalized_field_name -> actual Airtable field name for this table."""
    try:
        if sample_record_id:
            rec = table.get(sample_record_id)
        else:
            rows = table.all(max_records=1)
            rec = rows[0] if rows else {"fields": {}}
        keys = list(rec.get("fields", {}).keys())
    except Exception:
        keys = []
    return {_norm(k): k for k in keys}


def _remap_existing_only(table, payload: Dict, sample_record_id: str | None = None) -> Dict:
    """Include only keys that already exist on the table (prevents 422 UNKNOWN_FIELD_NAME)."""
    amap = _auto_field_map(table, sample_record_id)
    out: Dict[str, Any] = {}
    for k, v in payload.items():
        ak = amap.get(_norm(k))
        if ak:
            out[ak] = v
    return out


def _safe_create(table, payload: Dict, sample_record_id: str | None = None) -> None:
    """
    Create with field-remap shielding.
    If the table is empty (no sample), try an optimistic create first.
    On 422, progressively trim to a conservative subset.
    """
    try:
        amap = _auto_field_map(table, sample_record_id)

        # Case A: we have a map -> remap + create
        if amap:
            mapped = {amap.get(_norm(k)): v for k, v in payload.items() if amap.get(_norm(k))}
            if mapped:
                table.create(mapped)
                return
            # Nothing mapped (unexpected) -> try optimistic create
            table.create(payload)
            return

        # Case B: table looks empty -> optimistic create, then fallbacks
        try:
            table.create(payload)
            return
        except Exception:
            # Trim to likely-safe keys
            conservative_keys = [
                "Prospect", "Leads", "Lead", "Contact",
                "Campaign", "Template",
                "phone", "Phone", "to", "To",
                "message_preview", "Message Preview", "Message",
                "status", "Status",
                "from_number", "From Number",
                "next_send_date", "Next Send Date",
                "Property ID", "property_id",
            ]
            trimmed = {k: v for k, v in payload.items() if k in conservative_keys}
            if trimmed:
                try:
                    table.create(trimmed)
                    return
                except Exception:
                    pass
            # Minimal last-ditch
            minimal = {k: v for k, v in payload.items() if k.lower() in ("phone", "status")}
            if minimal:
                table.create(minimal)
                return
            raise
    except Exception:
        traceback.print_exc()


def _get(f: Dict, *names) -> Any:
    """Read a field trying multiple variants; supports normalized match."""
    for n in names:
        if n in f:
            return f[n]
    nf = {_norm(k): k for k in f.keys()}
    for n in names:
        key = nf.get(_norm(n))
        if key:
            return f[key]
    return None


def _digits_only(s: str | None) -> str | None:
    if not isinstance(s, str):
        return None
    ds = "".join(re.findall(r"\d+", s))
    return ds if len(ds) >= 10 else None


def get_phone(f: Dict[str, Any]) -> str | None:
    for k in PHONE_FIELDS:
        v = f.get(k)
        d = _digits_only(v)
        if d:
            return d
    return None


def pick_template(template_ids: List[str] | None, templates_table) -> Tuple[str | None, str | None]:
    """Pick a random template from linked templates in campaign."""
    if not template_ids:
        return None, None
    # linked fields can be a list of ids or a single id/string
    if isinstance(template_ids, list) and template_ids:
        tid = random.choice(template_ids)
    else:
        tid = str(template_ids)
    try:
        tmpl = templates_table.get(tid)
    except Exception:
        return None, None
    if not tmpl:
        return None, None
    msg = _get(tmpl.get("fields", {}), "Message", "message")
    return (msg, tid) if msg else (None, None)


def _resolve_table_name(prefix: str) -> str:
    """Accepts 'P1' or '(P1)' etc. Returns the real table name, defaulting to '(P#)'."""
    p = prefix.strip().upper().replace("(", "").replace(")", "")
    return TABLE_ALIASES.get(p, f"({p})")


def _parse_view_segment(value: str) -> Tuple[str | None, str | None]:
    """
    Parse inputs like:
      'P2 / Ramsey County, MN - Tax Delinquent'
      '(P3)/Some View'
      'Some View'  -> (no prefix returned)
    Returns (prefix, view) where prefix is 'P#' or None.
    """
    if not value:
        return None, None
    m = re.match(r"^\(?\s*(P[1-4])\s*\)?(?:\s*/\s*(.+))?$", value.strip(), flags=re.I)
    if m:
        return m.group(1).upper(), (m.group(2) or "").strip() or None
    return None, value.strip() or None


# ======================
# Airtable clients (cached)
# ======================
@lru_cache(maxsize=None)
def _api_main():
    return Api(MAIN_KEY) if (MAIN_KEY and LEADS_CONVOS_BASE) else None


@lru_cache(maxsize=None)
def _api_reporting():
    return Api(REPORTING_KEY) if (REPORTING_KEY and PERFORMANCE_BASE) else None


@lru_cache(maxsize=None)
def get_campaigns():
    api = _api_main()
    return api.table(LEADS_CONVOS_BASE, CAMPAIGNS_TABLE) if api else None


@lru_cache(maxsize=None)
def get_templates():
    api = _api_main()
    return api.table(LEADS_CONVOS_BASE, TEMPLATES_TABLE) if api else None


def get_prospects(table_name: str):
    api = _api_main()
    return api.table(LEADS_CONVOS_BASE, table_name) if api else None


@lru_cache(maxsize=None)
def get_drip():
    api = _api_main()
    return api.table(LEADS_CONVOS_BASE, DRIP_QUEUE_TABLE) if api else None


@lru_cache(maxsize=None)
def get_runs():
    api = _api_reporting()
    return api.table(PERFORMANCE_BASE, "Runs/Logs") if api else None


@lru_cache(maxsize=None)
def get_kpis():
    api = _api_reporting()
    return api.table(PERFORMANCE_BASE, "KPIs") if api else None


# ======================
# Main runner
# ======================
def run_campaigns(limit: str | int = 1, retry_limit: int = 3) -> Dict[str, Any]:
    """
    Execute scheduled campaigns:
      • Supports prospect tables (P1..P4) whose actual table names are '(P1)'..'(P4)'
      • Rotates templates
      • Throttles outbound to ~20 msgs/minute
      • Retries failed sends
      • Updates only existing fields on Airtable (prevents 422s)
      • Logs to Runs + KPIs and triggers a metrics refresh at the end
    """
    campaigns = get_campaigns()
    templates = get_templates()
    drip      = get_drip()
    runs      = get_runs()
    kpis      = get_kpis()

    if not (campaigns and templates and drip):
        print("⚠️ CampaignRunner: Missing Airtable tables or API env. Check .env / load_dotenv().")
        return {"ok": False, "processed": 0, "results": [], "errors": ["Missing Airtable tables"]}

    now = utcnow()
    now_iso = now.isoformat()

    if isinstance(limit, str) and limit.upper() == "ALL":
        limit = 999_999

    # Fetch all Campaigns and filter to eligible
    try:
        all_campaigns = campaigns.all()
    except Exception:
        traceback.print_exc()
        return {"ok": False, "processed": 0, "results": [], "errors": ["Failed to fetch campaigns"]}

    # Accept both Scheduled and Running so you can resume a partial run
    eligible_campaigns = []
    for c in all_campaigns:
        f = c.get("fields", {})
        status_val = str(_get(f, "status", "Status") or "")
        if status_val in ("Scheduled", "Running"):
            eligible_campaigns.append(c)

    processed = 0
    results: List[Dict[str, Any]] = []

    for camp in eligible_campaigns:
        if processed >= int(limit):
            break

        f: Dict[str, Any] = camp.get("fields", {})
        cid = camp["id"]
        name = _get(f, "Name", "name") or "Unnamed"

        # Skip paused/cancelled if they slipped through
        if _get(f, "status", "Status") in ("Paused", "Cancelled"):
            continue

        # Time window
        start_str = _get(f, "start_time", "Start Time")
        end_str   = _get(f, "end_time", "End Time")
        start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00")) if start_str else None
        end_dt   = datetime.fromisoformat(end_str.replace("Z", "+00:00")) if end_str else None

        if start_dt and now < start_dt:
            continue
        if end_dt and now > end_dt:
            payload = {"status": "Completed", "last_run_at": now_iso}
            mapped = _remap_existing_only(campaigns, payload, sample_record_id=cid)
            if mapped:
                try:
                    campaigns.update(cid, mapped)
                except Exception:
                    traceback.print_exc()
            continue

        # Templates (linked records)
        template_ids = _get(f, "templates", "Templates") or []
        if not template_ids:
            print(f"⚠️ Campaign '{name}' missing templates; skipping")
            continue

        # Prospect source parsing
        # Accept fields: "View/Segment", "view", "View", "Segment"
        view_raw = (_get(f, "View/Segment", "view", "View", "Segment") or "").strip()
        prefix_from_view, view = _parse_view_segment(view_raw)

        # Default table field (accept both 'Prospect Table' and 'prospect_table'); fallback to P1
        table_field = _get(f, "Prospect Table", "prospect_table") or "P1"
        # If the "Prospect Table" is already a P#/ (P#), keep parsing; otherwise treat as literal table
        if re.fullmatch(r"\(?\s*P[1-4]\s*\)?", str(table_field), flags=re.I):
            table_name = _resolve_table_name(str(table_field))
        else:
            table_name = str(table_field).strip()
        # If the view cell included a prefix, it wins
        if prefix_from_view:
            table_name = _resolve_table_name(prefix_from_view)

        prospects_table = get_prospects(table_name)
        if not prospects_table:
            print(f"⚠️ Campaign '{name}' missing prospect table {table_name}")
            continue

        # Fetch prospects; if the token lacks permission for the view, try without the view as a fallback
        try:
            prospect_records = prospects_table.all(view=view) if view else prospects_table.all()
        except Exception as e:
            print(f"⚠️ Prospect fetch failed for {table_name} (view={view!r}). Retrying without view. Error: {e}")
            try:
                prospect_records = prospects_table.all()
                view = None
            except Exception:
                traceback.print_exc()
                continue

        total_prospects = len(prospect_records)
        queued = 0

        # Queue prospects (throttled)
        for idx, prospect in enumerate(prospect_records):
            pf = prospect.get("fields", {})
            phone = get_phone(pf)
            if not phone:
                continue

            template_text, chosen_tid = pick_template(template_ids, templates)
            if not template_text:
                continue

            personalized_text = format_template(template_text, pf)
            next_send = now + timedelta(seconds=idx * 3)  # ~20 msg/min

            try:
                _safe_create(
                    drip,
                    {
                        # Drip queue may use different link field names; _safe_create only keeps valid ones
                        "Prospect": [prospect["id"]],
                        "Leads": [prospect["id"]],
                        "Campaign": [cid],
                        "Template": [chosen_tid],
                        "phone": phone,
                        "message_preview": personalized_text,
                        "status": "QUEUED",
                        "from_number": None,  # plug number pools later
                        "next_send_date": next_send.isoformat(),
                        "Property ID": _get(pf, "Property ID", "property_id"),
                    },
                )
                queued += 1
            except Exception:
                print(f"❌ Failed to queue {phone}")
                traceback.print_exc()

        # Send a batch (scope to this campaign id so we don’t pick a different one)
        batch_result = send_batch(campaign_id=cid, limit=500)

        # Retry loop
        retry_result: Dict[str, Any] = {}
        if batch_result.get("total_sent", 0) < queued:
            for _ in range(int(retry_limit)):
                retry_result = run_retry(limit=100, view="Failed Sends")
                if retry_result.get("retried", 0) == 0:
                    break

        # Compute progress
        current_sent = _get(f, "messages_sent", "Messages Sent") or 0
        sent_so_far = (current_sent or 0) + (batch_result.get("total_sent", 0) or 0) + (retry_result.get("retried", 0) or 0)
        completed = total_prospects > 0 and (sent_so_far >= total_prospects)

        # Update campaign (only fields that actually exist)
        payload = {
            "status": "Completed" if completed else "Running",
            "total_sent": sent_so_far,  # your schema has this field
            "Last Run Result": json.dumps(
                {
                    "Queued": queued,
                    "Sent": batch_result.get("total_sent", 0),
                    "Retries": retry_result.get("retried", 0),
                    "Completed": completed,
                    "Table": table_name,
                    "View": view,
                }
            ),
            "last_run_at": now_iso,
        }
        mapped = _remap_existing_only(campaigns, payload, sample_record_id=cid)
        if mapped:
            try:
                campaigns.update(cid, mapped)
            except Exception:
                traceback.print_exc()

        # Logs/KPIs
        if runs:
            try:
                _safe_create(
                    runs,
                    {
                        "Type": "CAMPAIGN_RUN",
                        "Campaign": name,
                        "Processed": float(sent_so_far),
                        "Breakdown": json.dumps({"initial": batch_result, "retries": retry_result}),
                        "Timestamp": now_iso,
                    },
                )
            except Exception:
                traceback.print_exc()

        if kpis:
            try:
                _safe_create(
                    kpis,
                    {
                        "Campaign": name,
                        "Metric": "OUTBOUND_SENT",
                        "Value": float(sent_so_far),
                        "Date": now.date().isoformat(),
                    },
                )
            except Exception:
                traceback.print_exc()

        results.append(
            {
                "campaign": name,
                "queued": queued,
                "sent": sent_so_far,
                "completed": completed,
                "retries": retry_result.get("retried", 0),
                "table": table_name,
                "view": view,
            }
        )
        processed += 1

    # Cross-campaign metrics
    try:
        update_metrics()
    except Exception:
        traceback.print_exc()

    return {"ok": True, "processed": processed, "results": results, "errors": []}
