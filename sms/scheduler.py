"""Campaign scheduler that hydrates Drip Queue from Airtable campaigns."""

from __future__ import annotations

import os
import time
import random
import requests
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from collections import defaultdict
from dotenv import load_dotenv

# Internal Imports
from sms.airtable_schema import (
    campaign_field_map,
    drip_field_map,
    prospects_field_map,
    template_field_map,
)
from sms.datastore import CONNECTOR, create_record, list_records, update_record
from sms.runtime import get_logger, iso_now, last_10_digits, normalize_phone

logger = get_logger(__name__)

# ───────────────────────────────────────────────────────────────
# ENV / BASE CONFIG
# ───────────────────────────────────────────────────────────────
ROOT_DIR = Path(__file__).resolve().parents[1]
load_dotenv(ROOT_DIR / ".env")

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY", "")
if not AIRTABLE_API_KEY:
    logger.warning("⚠️ Missing AIRTABLE_API_KEY — API requests will fail.")

TEST_MODE = os.getenv("TEST_MODE", "false").lower() in {"1", "true", "yes"}

# =========================
# Airtable Bases
# =========================
LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE", "appMn2MKocaJ9I3rW")
CAMPAIGN_CONTROL_BASE = os.getenv("CAMPAIGN_CONTROL_BASE", "appyhhWYmrM86H35a")
PERFORMANCE_BASE = os.getenv("PERFORMANCE_BASE", "appzRWrpFggxlRBgL")
DEVOPS_BASE = os.getenv("DEVOPS_BASE", "applqOU9LSAJ47gMy")

CAMPAIGNS_BASE_ID = LEADS_CONVOS_BASE  # back-compat alias

# =========================
# Numbers table (Campaign Control base)
# =========================
NUMBERS_TABLE_ID = "tblWG3Z2bkZF6k16n"  # table id (stable even if name changes)

# Field names (exact) and IDs (fallbacks) from your schema
NUMBERS_PHONE_FIELD = os.getenv("NUMBERS_PHONE_FIELD", "Number")  # fld8QxC0pEXwAKnu8
NUMBERS_STATUS_FIELD = os.getenv("NUMBERS_STATUS_FIELD", "Status")  # fldG4Wh9Q10Md8yMm
NUMBERS_MARKET_FIELD = os.getenv("NUMBERS_MARKET_FIELD", "Market")  # fldS2vH4eZnqBWl5g
NUMBERS_ACTIVE_FIELD = os.getenv("NUMBERS_ACTIVE_FIELD", "Active")  # fldYKsnDZpjObLYmS

NUMBERS_MARKET_FIELD_ID = "fldS2vH4eZnqBWl5g"
NUMBERS_ACTIVE_FIELD_ID = "fldYKsnDZpjObLYmS"
NUMBERS_STATUS_FIELD_ID = "fldG4Wh9Q10Md8yMm"
NUMBERS_PHONE_FIELD_ID = "fld8QxC0pEXwAKnu8"

# ───────────────────────────────────────────────────────────────
# FIELD MAPS (from your connectors)
# ───────────────────────────────────────────────────────────────
CAMPAIGN_FIELDS = campaign_field_map()
DRIP_FIELDS = drip_field_map()
PROSPECT_FIELDS = prospects_field_map()
TEMPLATE_FIELDS = template_field_map()

# Campaign Fields
CAMPAIGN_STATUS_FIELD     = CAMPAIGN_FIELDS.get("Status", "Status")
CAMPAIGN_MARKET_FIELD     = CAMPAIGN_FIELDS.get("Market", "Market")
CAMPAIGN_START_FIELD      = CAMPAIGN_FIELDS.get("Start Time", "Start Time")
CAMPAIGN_LAST_RUN_FIELD   = CAMPAIGN_FIELDS.get("Last Run At", "Last Run At")
CAMPAIGN_PROSPECTS_LINK   = CAMPAIGN_FIELDS.get("Prospects", "Prospects")
CAMPAIGN_TEMPLATES_LINK   = CAMPAIGN_FIELDS.get("Templates", "Templates")

# Drip Fields
DRIP_STATUS_FIELD         = DRIP_FIELDS.get("Status", "Status")
DRIP_MARKET_FIELD         = DRIP_FIELDS.get("Market", "Market")
DRIP_SELLER_PHONE_FIELD   = DRIP_FIELDS.get("Seller Phone Number", "Seller Phone Number")
DRIP_FROM_NUMBER_FIELD    = DRIP_FIELDS.get("TextGrid Phone Number", "TextGrid Phone Number")
DRIP_PROSPECT_LINK_FIELD  = DRIP_FIELDS.get("Prospect", "Prospect")
DRIP_CAMPAIGN_LINK_FIELD  = DRIP_FIELDS.get("Campaign", "Campaign")
DRIP_NEXT_SEND_DATE_FIELD = DRIP_FIELDS.get("Next Send Date", "Next Send Date")
DRIP_UI_FIELD             = DRIP_FIELDS.get("UI", "UI")
DRIP_PROCESSOR_FIELD      = DRIP_FIELDS.get("Processor", "Processor")
DRIP_MESSAGE_PREVIEW_FIELD= DRIP_FIELDS.get("Message Preview", "Message Preview")

SCHEDULER_PROCESSOR_LABEL = "Campaign Scheduler"

# ───────────────────────────────────────────────────────────────
# HELPERS
# ───────────────────────────────────────────────────────────────
def _parse_iso(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def _campaign_start(fields: Dict[str, Any]) -> datetime:
    return _parse_iso(fields.get(CAMPAIGN_START_FIELD)) or datetime.now(timezone.utc)

def _prospect_best_phone(fields: Dict[str, Any]) -> Optional[str]:
    """Return best normalized phone number."""
    candidates = [
        "Phone 1 (from Linked Owner)",              # ✅ explicit per your schema
        PROSPECT_FIELDS.get("PHONE_PRIMARY"),
        PROSPECT_FIELDS.get("PHONE_PRIMARY_LINKED"),
        PROSPECT_FIELDS.get("PHONE_SECONDARY"),
        PROSPECT_FIELDS.get("PHONE_SECONDARY_LINKED"),
        "Phone", "phone",
    ]
    for key in [c for c in candidates if c]:
        val = fields.get(key)
        if isinstance(val, list):
            for v in val:
                p = normalize_phone(v)
                if p: return p
        elif isinstance(val, str):
            p = normalize_phone(val)
            if p: return p
    return None

def _coerce_market(value: Any) -> str:
    if isinstance(value, str): return value.strip()
    if isinstance(value, list) and value: return _coerce_market(value[0])
    if isinstance(value, dict):
        for k in ("name", "label", "value", "Market"):
            if isinstance(value.get(k), str):
                return value[k].strip()
    return str(value or "").strip()

def _campaign_market(fields: Dict[str, Any]) -> str:
    return _coerce_market(fields.get(CAMPAIGN_MARKET_FIELD))

def _prospect_market(pf: Dict[str, Any], campaign_market: str) -> str:
    """Prefer prospect-level market; fall back to Property City; then campaign market."""
    return (
        _coerce_market(pf.get("Market"))
        or _coerce_market(pf.get("Property City"))
        or campaign_market
        or ""
    )

# ───────────────────────────────────────────────────────────────
# MESSAGE PLACEHOLDER RENDERING
# ───────────────────────────────────────────────────────────────
def _render_message(template: str, pf: Dict[str, Any]) -> str:
    """Render placeholders with correct field names and clean first names."""
    # First name from "Phone 1 Name (Primary) (from Linked Owner)"
    raw_name = pf.get("Phone 1 Name (Primary) (from Linked Owner)") or ""
    first_name = ""
    if isinstance(raw_name, str):
        parts = [p for p in raw_name.strip().replace(".", "").split(" ") if p]
        if parts:
            first_name = parts[0]

    # Single-selects can come through as strings or 1-element lists
    def _one(val: Any) -> str:
        if isinstance(val, list) and val: return str(val[0]).strip()
        if isinstance(val, str): return val.strip()
        return ""

    prop_address = _one(pf.get("Property Address"))
    prop_city    = _one(pf.get("Property City"))

    mapping = {"First": first_name or "there", "Address": prop_address, "Property City": prop_city}
    out = template
    for k, v in mapping.items():
        out = out.replace(f"{{{k}}}", v)
    return out.strip()

# ======================================================
# NUMBERS LOOKUP (MARKET-ISOLATED ROTATION — FINAL)
# ======================================================
_numbers_cache: Dict[str, List[str]] = {}
_rotation_index: Dict[str, int] = {}

def _mk_key(raw: Optional[str]) -> str:
    return (raw or "").strip().lower().replace(",", "").replace(".", "")

def _rotate(market_key: str, numbers: List[str]) -> Optional[str]:
    if not numbers:
        return None
    i = _rotation_index.get(market_key, 0)
    choice = numbers[i % len(numbers)]
    _rotation_index[market_key] = i + 1
    return choice

def _numbers_endpoint() -> str:
    return f"https://api.airtable.com/v0/{CAMPAIGN_CONTROL_BASE}/{NUMBERS_TABLE_ID}"

def _list_numbers(filter_formula: str) -> List[Dict[str, Any]]:
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}
    r = requests.get(_numbers_endpoint(), headers=headers, params={"filterByFormula": filter_formula, "pageSize": 100}, timeout=12)
    if r.status_code != 200:
        logger.warning("⚠️ Numbers fetch failed: %s %s", r.status_code, r.text[:200])
        return []
    return (r.json() or {}).get("records", []) or []

def _active_pool(records: List[Dict[str, Any]]) -> List[str]:
    pool: List[str] = []
    for rec in records:
        f = rec.get("fields", {}) or {}
        num    = f.get(NUMBERS_PHONE_FIELD) or f.get(NUMBERS_PHONE_FIELD_ID)
        status = (f.get(NUMBERS_STATUS_FIELD) or f.get(NUMBERS_STATUS_FIELD_ID) or "").strip().lower()
        active = f.get(NUMBERS_ACTIVE_FIELD, f.get(NUMBERS_ACTIVE_FIELD_ID, False))
        if isinstance(num, str) and num.strip() and active and status == "active":
            pool.append(num.strip())
    return pool

def _fetch_textgrid_number_for_market(market_raw: Optional[str]) -> Optional[str]:
    """Retrieve an active TextGrid number for a market; rotate per seller."""
    if not market_raw:
        logger.warning("⚠️ Missing market input for number fetch.")
        return None

    market_key = _mk_key(market_raw)
    if not market_key:
        return None

    # Use cached pool if available
    cached = _numbers_cache.get(market_key)
    if cached:
        chosen = _rotate(market_key, cached)
        logger.info("🔁 (cache) %s -> %s", market_raw, chosen)
        return chosen

    # 1) Exact single-select match by field **name**
    formula_name = f"AND({{{NUMBERS_MARKET_FIELD}}}='{market_raw}', OR({{{NUMBERS_ACTIVE_FIELD}}}=1, {{{NUMBERS_ACTIVE_FIELD}}}='true'), LOWER({{{NUMBERS_STATUS_FIELD}}})='active')"
    recs = _list_numbers(formula_name)

    # 2) If empty, try field **ID** (bulletproof to renames)
    if not recs:
        formula_id = f"AND({{{NUMBERS_MARKET_FIELD_ID}}}='{market_raw}', OR({{{NUMBERS_ACTIVE_FIELD_ID}}}=1, {{{NUMBERS_ACTIVE_FIELD_ID}}}='true'), LOWER({{{NUMBERS_STATUS_FIELD_ID}}})='active')"
        recs = _list_numbers(formula_id)

    pool = _active_pool(recs)

    # 3) If still empty, log & bail (no global fallback to wrong market)
    if not pool:
        logger.warning("⚠️ No matching numbers found for '%s'", market_raw)
        return None

    _numbers_cache[market_key] = pool
    chosen = _rotate(market_key, pool)
    logger.info("📞 %s -> %s (pool=%d)", market_raw, chosen, len(pool))
    return chosen

# ───────────────────────────────────────────────────────────────
# MAIN SCHEDULER
# ───────────────────────────────────────────────────────────────
def run_scheduler(limit: Optional[int] = None) -> Dict[str, Any]:
    logger.info("🚀 Scheduler start")
    summary: Dict[str, Any] = {"queued": 0, "campaigns": {}, "errors": [], "ok": True}

    if TEST_MODE:
        summary["note"] = "TEST_MODE active; no writes performed."
        return summary

    try:
        campaigns_h = CONNECTOR.campaigns()
        prospects_h = CONNECTOR.prospects()
        drip_h = CONNECTOR.drip_queue()
        templates_h = CONNECTOR.templates()

        campaigns = list_records(campaigns_h, page_size=100)
        existing = list_records(drip_h, page_size=100)

        existing_pairs = {
            (
                (f.get("fields", {}) or {}).get(DRIP_CAMPAIGN_LINK_FIELD, [None])[0],
                last_10_digits((f.get("fields", {}) or {}).get(DRIP_SELLER_PHONE_FIELD)),
            )
            for f in existing if f.get("fields")
        }

        for camp in campaigns:
            cfields = camp.get("fields", {}) or {}
            status = str(cfields.get(CAMPAIGN_STATUS_FIELD, "")).strip().lower()
            if status != "scheduled":
                continue

            campaign_id = camp.get("id")
            campaign_market = _campaign_market(cfields)
            if not campaign_market:
                logger.warning("⚠️ Campaign %s missing Market; skipping", campaign_id)
                continue

            # Pull templates
            template_ids = cfields.get(CAMPAIGN_TEMPLATES_LINK) or []
            messages: List[str] = []
            if template_ids:
                for tid in template_ids:
                    resp = templates_h.table.api.request(
                        "get", templates_h.table.url,
                        params={"filterByFormula": f"RECORD_ID()='{tid}'"},
                    )
                    for rec in (resp or {}).get("records", []):
                        msg = (rec.get("fields", {}) or {}).get("Message")
                        if isinstance(msg, str) and msg.strip():
                            messages.append(msg.strip())
            if not messages:
                logger.warning("⚠️ Campaign %s has no templates; skipping", campaign_id)
                continue

            # Prospects linked to campaign
            linked = cfields.get(CAMPAIGN_PROSPECTS_LINK) or []
            if not linked:
                logger.info("⏭️ Campaign %s has no linked prospects; skipping", campaign_id)
                continue

            # Fetch all linked prospects in chunks
            prospects: List[Dict[str, Any]] = []
            for i in range(0, len(linked), 100):
                chunk = linked[i:i+100]
                formula = "OR(" + ",".join([f"RECORD_ID()='{rid}'" for rid in chunk]) + ")"
                resp = prospects_h.table.api.request("get", prospects_h.table.url, params={"filterByFormula": formula})
                prospects.extend((resp or {}).get("records", []))
                time.sleep(0.12)

            start_time = _campaign_start(cfields)
            queued = skipped = processed = 0
            skip_reasons: Dict[str, int] = defaultdict(int)

            def _next_send(j: int) -> str:
                ts = start_time.timestamp() + random.randint(0, 90)
                return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

            for idx, pr in enumerate(prospects):
                processed += 1
                pf = pr.get("fields", {}) or {}

                # Seller phone
                phone = _prospect_best_phone(pf)
                if not phone:
                    skipped += 1; skip_reasons["missing_phone"] = skip_reasons.get("missing_phone", 0) + 1
                    continue

                digits = last_10_digits(phone)
                if (campaign_id, digits) in existing_pairs:
                    skipped += 1; skip_reasons["duplicate_phone"] = skip_reasons.get("duplicate_phone", 0) + 1
                    continue

                # Prospect-level market (falls back to campaign market)
                prospect_market = _prospect_market(pf, campaign_market)

                # 🔑 PER-SELLER NUMBER ROTATION (do NOT select once per campaign)
                from_number = _fetch_textgrid_number_for_market(prospect_market)
                if not from_number:
                    skipped += 1; skip_reasons["missing_textgrid_number"] = skip_reasons.get("missing_textgrid_number", 0) + 1
                    continue

                # Render the message preview with placeholders filled
                message_text = random.choice(messages)
                rendered = _render_message(message_text, pf)

                payload = {
                    DRIP_STATUS_FIELD: "QUEUED",
                    DRIP_MARKET_FIELD: prospect_market or campaign_market,
                    DRIP_SELLER_PHONE_FIELD: phone,
                    DRIP_FROM_NUMBER_FIELD: from_number,
                    DRIP_PROCESSOR_FIELD: SCHEDULER_PROCESSOR_LABEL,
                    DRIP_NEXT_SEND_DATE_FIELD: _next_send(idx),
                    DRIP_CAMPAIGN_LINK_FIELD: [campaign_id],
                    DRIP_PROSPECT_LINK_FIELD: [pr["id"]],
                    DRIP_UI_FIELD: "⏳",
                    DRIP_MESSAGE_PREVIEW_FIELD: rendered,
                }

                try:
                    created = create_record(drip_h, payload)
                    if created:
                        existing_pairs.add((campaign_id, digits))
                        queued += 1
                    else:
                        skipped += 1; skip_reasons["create_failed"] = skip_reasons.get("create_failed", 0) + 1
                except Exception as exc:
                    logger.warning("Create failed for %s: %s", digits, exc)
                    skipped += 1; skip_reasons["create_failed"] = skip_reasons.get("create_failed", 0) + 1

            if queued:
                try:
                    update_record(campaigns_h, campaign_id, {
                        CAMPAIGN_STATUS_FIELD: "Active",
                        CAMPAIGN_LAST_RUN_FIELD: iso_now(),
                    })
                except Exception:
                    pass

            summary["queued"] += queued
            summary["campaigns"][campaign_id] = {
                "queued": queued, "skipped": skipped, "processed": processed,
                "skip_reasons": dict(skip_reasons)
            }
            logger.info("✅ Campaign %s queued=%d skipped=%d processed=%d", campaign_id, queued, skipped, processed)

        summary["ok"] = not summary["errors"]
        logger.info("🏁 Scheduler done. Total queued: %s", summary["queued"])
        return summary

    except Exception as exc:
        logger.exception("💥 Scheduler fatal: %s", exc)
        summary["ok"] = False
        summary["errors"].append(str(exc))
        return summary
