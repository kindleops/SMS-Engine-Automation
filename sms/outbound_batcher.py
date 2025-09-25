# sms/outbound_batcher.py
import os
import traceback
from datetime import datetime, timezone
from pyairtable import Table
from sms.quota_reset import reset_daily_quotas

# --- Config ---
NUMBERS_TABLE = os.getenv("NUMBERS_TABLE", "Numbers")
_last_reset_date = None  # safeguard for daily quota reset


# -------------------------
# Airtable Helpers
# -------------------------
def get_table(base_env: str, table_name: str) -> Table | None:
    """Generic Airtable table initializer."""
    api_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv(base_env) or os.getenv(f"AIRTABLE_{base_env}_ID")
    if not api_key or not base_id:
        print(f"⚠️ Missing Airtable config for {base_env}")
        return None
    try:
        return Table(api_key, base_id, table_name)
    except Exception:
        print(f"❌ Failed to init {table_name} in {base_env}")
        traceback.print_exc()
        return None


def get_perf_tables():
    """Return Runs + KPIs tables for logging."""
    key = os.getenv("AIRTABLE_REPORTING_KEY") or os.getenv("AIRTABLE_API_KEY")
    base = os.getenv("PERFORMANCE_BASE")
    if not key or not base:
        return None, None
    try:
        runs = Table(key, base, "Runs/Logs")
        kpis = Table(key, base, "KPIs")
        return runs, kpis
    except Exception:
        print("⚠️ Failed to init Performance base")
        traceback.print_exc()
        return None, None


def ensure_today_rows():
    """Auto-reset daily quotas once per day across all numbers."""
    global _last_reset_date
    today = datetime.now(timezone.utc).date().isoformat()
    if _last_reset_date != today:
        print(f"⚡ Auto-resetting quotas for {today}")
        reset_daily_quotas()
        _last_reset_date = today


# -------------------------
# Template Personalization
# -------------------------
def format_template(template: str, lead_fields: dict) -> str:
    """Safely format template with lead data."""
    full_name = lead_fields.get("Owner Name") or lead_fields.get("First") or ""
    first = full_name.split(" ")[0] if full_name else "there"
    address = lead_fields.get("Address") or lead_fields.get("Property Address") or "your property"

    try:
        return template.format(First=first, Address=address)
    except Exception:
        return template  # fallback if placeholders mismatch


# -------------------------
# Number Selection
# -------------------------
def pick_number(limit: int = 1):
    """Pick first available number with quota."""
    numbers = get_table("CAMPAIGN_CONTROL_BASE", NUMBERS_TABLE)
    if not numbers:
        return None, None

    available = numbers.all(formula="{Remaining} > 0", max_records=limit)
    if not available:
        return None, None

    row = available[0]
    return row["id"], row["fields"].get("Number")


# -------------------------
# Campaign Runner
# -------------------------
def send_batch(campaign_id: str | None = None, limit: int = 500):
    """
    Run a campaign:
      - Finds scheduled campaign if no ID provided
      - Fetches template + leads
      - Queues messages with personalization
      - Respects number quotas
      - Logs KPIs + updates campaign status
    """
    ensure_today_rows()

    campaigns = get_table("LEADS_CONVOS_BASE", "Campaigns")
    templates = get_table("LEADS_CONVOS_BASE", "Templates")
    leads_tbl = get_table("LEADS_CONVOS_BASE", "Leads")
    drip = get_table("LEADS_CONVOS_BASE", "Drip Queue")

    if not (campaigns and templates and leads_tbl and drip):
        return {"ok": False, "error": "Missing Airtable tables"}

    # 1. Select campaign
    if campaign_id:
        campaign = campaigns.get(campaign_id)
    else:
        now_iso = datetime.now(timezone.utc).isoformat()
        eligible = campaigns.all(
            formula=f"AND({{Status}}='Scheduled', IS_BEFORE({{Start Time}}, '{now_iso}'))"
        )
        if not eligible:
            return {"ok": False, "error": "No eligible campaigns"}
        campaign = eligible[0]

    c_fields = campaign["fields"]
    campaign_name = c_fields.get("Name", "Unnamed")
    view = c_fields.get("View/Segment")
    template_id = (c_fields.get("Template") or [None])[0]

    print(f"🚀 Launching Campaign: {campaign_name}")

    # 2. Fetch template
    template_row = templates.get(template_id) if template_id else None
    template_text = template_row["fields"].get("Message") if template_row else None
    if not template_text:
        return {"ok": False, "error": "No template found"}

    # 3. Fetch leads
    lead_records = leads_tbl.all(view=view, max_records=limit) if view else leads_tbl.all(max_records=limit)
    queued = 0
    now_iso = datetime.now(timezone.utc).isoformat()

    for lead in lead_records:
        lf = lead.get("fields", {})
        phone = lf.get("phone")
        if not phone:
            continue

        num_id, from_number = pick_number()
        if not from_number:
            print("⚠️ No numbers available with quota")
            break

        # 4. Personalize message
        personalized_text = format_template(template_text, lf)

        # 5. Queue into Drip Queue
        try:
            drip_payload = {
                "Leads": [lead["id"]],
                "Campaign": [campaign["id"]],
                "Template": [template_id],
                "phone": phone,
                "message_preview": personalized_text,
                "status": "QUEUED",
                "from_number": from_number,
                "next_send_date": now_iso,
            }

            # 🔗 Property linkage
            property_id = lf.get("Property ID")
            if property_id:
                drip_payload["Property ID"] = property_id

            record = drip.create(drip_payload)
            queued += 1

            print(
                f"📥 Queued → {phone} | "
                f"Campaign: {campaign_name} | "
                f"Property ID: {property_id or 'N/A'}"
            )

        except Exception as e:
            print(f"❌ Failed to queue message for {phone}: {e}")
            traceback.print_exc()

    # 6. Log Performance + KPIs
    runs, kpis = get_perf_tables()
    if runs:
        try:
            run_record = runs.create({
                "Type": "OUTBOUND_CAMPAIGN",
                "Campaign": campaign_name,
                "Queued": queued,
                "Timestamp": now_iso,
                "Template Used": template_id,
                "View Used": view or "ALL",
                "Processed By": "OutboundBatcher"
            })
            if kpis:
                kpis.create({
                    "Campaign": campaign_name,
                    "Metric": "MESSAGES_QUEUED",
                    "Value": queued,
                    "Date": datetime.now(timezone.utc).date().isoformat()
                })
            print(f"📊 Logged campaign run → {run_record['id']}")
        except Exception:
            print("⚠️ Failed to log performance run")
            traceback.print_exc()

    # 7. Update campaign status
    try:
        campaigns.update(campaign["id"], {
            "Last Run Result": f"Queued {queued}",
            "Messages Queued": queued,
            "Status": "Running",
            "Last Run": now_iso
        })
    except Exception:
        print("⚠️ Failed to update campaign status")
        traceback.print_exc()

    return {"ok": True, "campaign": campaign_name, "queued": queued}