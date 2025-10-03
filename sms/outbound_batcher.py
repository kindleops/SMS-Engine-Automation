# sms/outbound_batcher.py
import os, re, traceback
from datetime import datetime, timezone
from pyairtable import Table

LEADS_BASE_ENV = "LEADS_CONVOS_BASE"
PERF_BASE_ENV  = "PERFORMANCE_BASE"
NUMBERS_TABLE  = os.getenv("NUMBERS_TABLE", "Numbers")
_last_reset_date = None

def utcnow(): return datetime.now(timezone.utc)
def _norm(s): return re.sub(r"[^a-z0-9]+","",s.strip().lower()) if isinstance(s,str) else s

def _auto_field_map(table: Table):
    try:
        one = table.all(max_records=1)
        keys = list(one[0].get("fields", {}).keys()) if one else []
    except Exception:
        keys = []
    return {_norm(k): k for k in keys}

def _remap_existing_only(table: Table, payload: dict) -> dict:
    amap = _auto_field_map(table)
    out = {}
    for k,v in payload.items():
        ak = amap.get(_norm(k))
        if ak: out[ak] = v
    return out

def _parse_iso(s):
    if not s: return None
    try: return datetime.fromisoformat(str(s).replace("Z","+00:00"))
    except Exception: return None

def _api_key_for(base_env):
    return (os.getenv("AIRTABLE_REPORTING_KEY") or os.getenv("AIRTABLE_API_KEY")) if base_env==PERF_BASE_ENV else os.getenv("AIRTABLE_API_KEY")

def get_table(base_env: str, table_name: str) -> Table | None:
    key  = _api_key_for(base_env)
    base = os.getenv(base_env)
    if not key or not base:
        print(f"⚠️ Missing Airtable config for {base_env}")
        return None
    try:
        return Table(key, base, table_name)
    except Exception:
        traceback.print_exc()
        return None

def get_perf_tables():
    return get_table(PERF_BASE_ENV,"Runs/Logs"), get_table(PERF_BASE_ENV,"KPIs")

def ensure_today_rows():
    global _last_reset_date
    today = utcnow().date().isoformat()
    if _last_reset_date != today:
        try:
            print(f"⚡ Auto-resetting quotas for {today}")
            from sms.quota_reset import reset_daily_quotas
            reset_daily_quotas()
        except Exception:
            traceback.print_exc()
        _last_reset_date = today

# ---------- template personalization (exported) ----------
def _first_from_name(name: str | None) -> str:
    return (name or "there").split()[0]

def format_template(template: str, lead_fields: dict) -> str:
    full_name = (
        lead_fields.get("First") or lead_fields.get("First Name") or
        lead_fields.get("Owner Name") or lead_fields.get("Phone 1 Name (Primary) (from Linked Owner)") or
        lead_fields.get("Owner First Name") or lead_fields.get("Name") or ""
    )
    first = _first_from_name(full_name)
    address = (
        lead_fields.get("Property Address") or lead_fields.get("Address") or
        lead_fields.get("Mailing Address") or "your property"
    )
    try:
        return template.format(First=first, Address=address)
    except Exception:
        return template

# ---------- sending number ----------
def _remaining(f: dict) -> float:
    for k in ("Remaining","remaining","Remaining Today","remaining_today"):
        if isinstance(f.get(k), (int,float)): return float(f[k])
    return -1.0

def pick_number():
    numbers = get_table(LEADS_BASE_ENV, NUMBERS_TABLE)
    if not numbers: return None, None
    try:
        rows = numbers.all(max_records=200)   # no formula here
        rows = [r for r in rows if _remaining(r.get("fields",{})) > 0]
    except Exception:
        traceback.print_exc(); return None, None
    if not rows: return None, None
    row = rows[0]
    return row["id"], (row["fields"].get("Number") or row["fields"].get("number"))

# ---------- phones on leads ----------
PHONE_FIELDS = [
    "phone","Phone","Mobile","Cell",
    "Owner Phone","Owner Phone 1","Owner Phone 2",
    "Phone 1 (from Linked Owner)","Phone 2 (from Linked Owner)","Phone 3 (from Linked Owner)",
]
def get_phone(f: dict):
    for k in PHONE_FIELDS:
        v = f.get(k)
        if v: return v
    return None

# ---------- main ----------
def send_batch(campaign_id: str | None = None, limit: int = 500):
    ensure_today_rows()

    campaigns = get_table(LEADS_BASE_ENV,"Campaigns")
    templates = get_table(LEADS_BASE_ENV,"Templates")
    drip      = get_table(LEADS_BASE_ENV,"Drip Queue")
    if not (campaigns and templates and drip):
        return {"ok": False, "type":"Prospect", "error":"Missing Airtable tables", "queued":0, "sent":0, "completed":False, "retries":0, "errors":["Missing Airtable tables"]}

    # 1) pick eligible campaign WITHOUT Airtable formulas
    if campaign_id:
        try:
            campaign = campaigns.get(campaign_id)
        except Exception:
            traceback.print_exc()
            return {"ok": False, "type":"Prospect", "error":"Campaign not found", "queued":0, "sent":0, "completed":False, "retries":0, "errors":["Campaign not found"]}
    else:
        try:
            rows = campaigns.all()
        except Exception:
            traceback.print_exc()
            return {"ok": False, "type":"Prospect", "error":"Failed to read campaigns", "queued":0, "sent":0, "completed":False, "retries":0, "errors":["Failed to read campaigns"]}

        now_dt = utcnow()
        eligible = []
        for r in rows:
            f = r.get("fields", {})
            status = str(f.get("status") or f.get("Status") or "")
            if status != "Scheduled": continue
            st = _parse_iso(f.get("start_time") or f.get("Start Time") or f.get("Start"))
            if (st is None) or (st <= now_dt): eligible.append(r)

        if not eligible:
            return {"ok": False, "type":"Prospect", "error":"No eligible campaigns", "queued":0, "sent":0, "completed":False, "retries":0, "errors":["No eligible campaigns"]}
        campaign = eligible[0]

    cf = campaign.get("fields", {})
    campaign_name = cf.get("Name") or cf.get("name") or "Unnamed"

    # Prospect source: read table & view (defaults)
    table_name = cf.get("Prospect Table") or "(P1)"
    view_name  = cf.get("View/Segment") or None

    leads_tbl = get_table(LEADS_BASE_ENV, table_name)
    if not leads_tbl:
        return {"ok": False, "type":"Prospect", "error":f"Missing prospect table {table_name}", "queued":0, "sent":0, "completed":False, "retries":0, "errors":[f"Missing prospect table {table_name}"]}

    # Template(s)
    template_ids = []
    if isinstance(cf.get("templates"), list):
        template_ids = [t for t in cf["templates"] if isinstance(t, str)]
    elif isinstance(cf.get("Templates"), list):
        template_ids = [t for t in cf["Templates"] if isinstance(t, str)]
    elif isinstance(cf.get("Template"), list):
        template_ids = [t for t in cf["Template"] if isinstance(t, str)]
    elif isinstance(cf.get("Template"), str):
        template_ids = [cf["Template"]]

    if not template_ids:
        return {"ok": False, "type":"Prospect", "error":"No template linked", "queued":0, "sent":0, "completed":False, "retries":0, "errors":["No template linked"]}

    try:
        tmpl_row = templates.get(template_ids[0])
        template_text = (tmpl_row or {}).get("fields", {}).get("Message")
    except Exception:
        traceback.print_exc(); template_text = None
    if not template_text:
        return {"ok": False, "type":"Prospect", "error":"Template message missing", "queued":0, "sent":0, "completed":False, "retries":0, "errors":["Template message missing"]}

    print(f"🚀 Launching Campaign: {campaign_name} | Table: {table_name} | View: {view_name or 'ALL'}")

    # 2) leads (no formula)
    try:
        leads = leads_tbl.all(view=view_name, max_records=limit) if view_name else leads_tbl.all(max_records=limit)
    except Exception:
        traceback.print_exc()
        return {"ok": False, "type":"Prospect", "error":"Failed to read leads", "queued":0, "sent":0, "completed":False, "retries":0, "errors":["Failed to read leads"]}

    now_iso = utcnow().isoformat()
    queued = 0

    for lead in leads:
        lf = lead.get("fields", {})
        phone = get_phone(lf)
        if not phone: continue

        _, from_number = pick_number()
        if not from_number:
            print("⚠️ No numbers available with quota")
            break

        msg = format_template(template_text, lf)

        dq_tbl = get_table(LEADS_BASE_ENV,"Drip Queue")
        dq_payload = _remap_existing_only(dq_tbl, {
            "Prospect": [lead["id"]],
            "Leads": [lead["id"]],
            "Campaign": [campaign["id"]],
            "Template": [template_ids[0]],
            "phone": phone,
            "from_number": from_number,
            "message_preview": msg,
            "status": "QUEUED",
            "next_send_date": now_iso,
            "Property ID": lf.get("Property ID") or lf.get("property_id"),
        })
        try:
            dq_tbl.create(dq_payload)
            queued += 1
            print(f"📥 Queued → {phone} | {campaign_name}")
        except Exception:
            traceback.print_exc()

    # 3) logs (best effort)
    runs,kpis = get_perf_tables()
    if runs:
        try:
            runs.create(_remap_existing_only(runs,{
                "Type":"OUTBOUND_CAMPAIGN","Campaign":campaign_name,"Queued":queued,
                "Timestamp":now_iso,"Template Used":template_ids[0],
                "View Used": view_name or "ALL", "Processed By":"OutboundBatcher"
            }))
        except Exception: traceback.print_exc()
    if kpis:
        try:
            kpis.create(_remap_existing_only(kpis,{
                "Campaign":campaign_name,"Metric":"MESSAGES_QUEUED","Value":float(queued),
                "Date": utcnow().date().isoformat()
            }))
        except Exception: traceback.print_exc()

    # 4) update campaign
    try:
        campaigns.update(
            campaign["id"],
            _remap_existing_only(campaigns, {
                "Last Run Result": f"Queued {queued}",
                "Messages Queued": queued,
                "Status": "Running" if queued else (cf.get("status") or cf.get("Status") or "Scheduled"),
                "Last Run": now_iso,
                "last_run_at": now_iso,
            })
        )
    except Exception: traceback.print_exc()

    return {"ok": True, "campaign": campaign_name, "type":"Prospect",
            "queued": queued, "sent": queued, "completed": False, "retries": 0, "errors": []}