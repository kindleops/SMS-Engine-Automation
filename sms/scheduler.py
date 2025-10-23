"""Campaign scheduler that hydrates Drip Queue from Airtable campaigns (production)."""

from __future__ import annotations

import os
import time
import random
import requests
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import quote
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

# For compatibility with any legacy code that referenced CAMPAIGNS_BASE_ID
CAMPAIGNS_BASE_ID = LEADS_CONVOS_BASE

# Numbers table (confirmed from your schema)
NUMBERS_TABLE_ID = os.getenv("NUMBERS_TABLE_ID", "tblWG3Z2bkZF6k16n")  # safer than name
NUMBERS_MARKET_FIELD = os.getenv("NUMBERS_MARKET_FIELD", "Market")
NUMBERS_PHONE_FIELD = os.getenv("NUMBERS_PHONE_FIELD", "Number")
NUMBERS_STATUS_FIELD = os.getenv("NUMBERS_STATUS_FIELD", "Status")
NUMBERS_ACTIVE_FIELD = os.getenv("NUMBERS_ACTIVE_FIELD", "Active")

# Prequeue behavior & pacing
PREQUEUE_JITTER_MAX_SEC = int(os.getenv("PREQUEUE_JITTER_MAX_SEC", "90"))  # spread a bit
CHUNK_SLEEP_SEC = float(os.getenv("CHUNK_SLEEP_SEC", "0.12"))              # Airtable friendliness

# ───────────────────────────────────────────────────────────────
# FIELD MAPS
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

def _prospect_best_phone(fields: Dict[str, Any]) -> Optional[str]:
    """
    Return best normalized phone number from common fields (string or list).
    Includes Linked Owner variants to maximize hit rate.
    """
    candidates: List[str] = [
        PROSPECT_FIELDS.get("PHONE_PRIMARY"),
        PROSPECT_FIELDS.get("PHONE_PRIMARY_LINKED"),
        PROSPECT_FIELDS.get("PHONE_SECONDARY"),
        PROSPECT_FIELDS.get("PHONE_SECONDARY_LINKED"),
        "Phone 1 (from Linked Owner)",
        "Phone 2 (from Linked Owner)",
        "Phone 1",
        "Phone 2",
        "Phones",
        "Owner Phone",
        "Primary Phone",
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

# ───────────────────────────────────────────────────────────────
# MESSAGE PLACEHOLDER RENDERING (your exact fields)
# ───────────────────────────────────────────────────────────────
def _render_message(template: str, pf: Dict[str, Any]) -> str:
    """Render {First}, {Address}, {Property City} using exact column names."""
    # First name from linked-owner name, strip middle initials like "N."
    raw_name = pf.get("Phone 1 Name (Primary) (from Linked Owner)") or ""
    first_name = ""
    if isinstance(raw_name, str) and raw_name.strip():
        first_name = raw_name.strip().split()[0].replace(".", "")

    # Single-selects generally arrive as strings in the API, but guard lists
    addr_val = pf.get("Property Address")
    city_val = pf.get("Property City")
    if isinstance(addr_val, list) and addr_val: addr_val = addr_val[0]
    if isinstance(city_val, list) and city_val: city_val = city_val[0]
    prop_address = (addr_val or "").strip() if isinstance(addr_val, str) else ""
    prop_city    = (city_val or "").strip() if isinstance(city_val, str) else ""

    msg = (template or "")
    msg = msg.replace("{First}", first_name)
    msg = msg.replace("{Address}", prop_address)
    msg = msg.replace("{Property City}", prop_city)
    return msg.strip()

# ======================================================
# NUMBERS LOOKUP (MARKET-ISOLATED ROTATION, per-prospect selection)
# ======================================================
_numbers_cache: Dict[str, List[str]] = {}
_rotation_index: Dict[str, int] = {}

def _market_key(raw: Optional[str]) -> str:
    return (raw or "").strip().lower().replace(",", "").replace(".", "")

def _choose_rotating(market_key: str, pool: List[str]) -> Optional[str]:
    if not pool: return None
    idx = _rotation_index.get(market_key, 0)
    choice = pool[idx % len(pool)]
    _rotation_index[market_key] = idx + 1
    return choice

def _fetch_textgrid_number_pool(market_raw: str) -> List[str]:
    """
    Fetch (and cache) the pool of active numbers for a market.
    Returns [] if none for that market. Uses exact equality for single-select.
    """
    mk = _market_key(market_raw)
    if mk in _numbers_cache:
        return _numbers_cache[mk]

    url = f"https://api.airtable.com/v0/{CAMPAIGN_CONTROL_BASE}/{NUMBERS_TABLE_ID}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}

    exact = (
        f"AND("
        f"{{{NUMBERS_MARKET_FIELD}}}='{market_raw}',"
        f"OR({{{NUMBERS_ACTIVE_FIELD}}}=1,{{{NUMBERS_ACTIVE_FIELD}}}='true'),"
        f"LOWER({{{NUMBERS_STATUS_FIELD}}})='active'"
        f")"
    )
    resp = requests.get(url, headers=headers, params={"filterByFormula": exact, "pageSize": 100}, timeout=12)
    recs = (resp.json() or {}).get("records", []) if resp.status_code == 200 else []

    pool: List[str] = []
    for r in recs:
        f = r.get("fields", {}) or {}
        num = f.get(NUMBERS_PHONE_FIELD)
        if isinstance(num, str) and num.strip():
            pool.append(num.strip())

    _numbers_cache[mk] = pool
    return pool

def _fetch_textgrid_number_global_pool() -> List[str]:
    """Global fallback pool of active numbers (any market)."""
    if "_global" in _numbers_cache:
        return _numbers_cache["_global"]

    url = f"https://api.airtable.com/v0/{CAMPAIGN_CONTROL_BASE}/{NUMBERS_TABLE_ID}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}
    fb = f"AND(OR({{{NUMBERS_ACTIVE_FIELD}}}=1,{{{NUMBERS_ACTIVE_FIELD}}}='true'),LOWER({{{NUMBERS_STATUS_FIELD}}})='active')"
    resp = requests.get(url, headers=headers, params={"filterByFormula": fb, "pageSize": 100}, timeout=12)
    recs = (resp.json() or {}).get("records", []) if resp.status_code == 200 else []

    pool: List[str] = []
    for r in recs:
        f = r.get("fields", {}) or {}
        num = f.get(NUMBERS_PHONE_FIELD)
        if isinstance(num, str) and num.strip():
            pool.append(num.strip())

    _numbers_cache["_global"] = pool
    return pool

def _choose_number_for_market(market_raw: str) -> Optional[str]:
    """
    Choose a number for a prospect in this market (round-robin).
    Falls back to global pool if the market has no active numbers.
    """
    mk = _market_key(market_raw)
    pool = _fetch_textgrid_number_pool(market_raw)
    if not pool:
        logger.warning("⚠️ Market %s has no active numbers; using global pool.", market_raw)
        gpool = _fetch_textgrid_number_global_pool()
        return _choose_rotating("_global", gpool) if gpool else None
    return _choose_rotating(mk, pool)

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

        # Pull ALL campaigns, then process all with Status == Scheduled
        campaigns = list_records(campaigns_h, page_size=1000)
        # Sort by Start Time so multiple scheduled campaigns run predictably
        def _start(f): 
            return _parse_iso((f.get("fields") or {}).get(CAMPAIGN_START_FIELD)) or datetime.now(timezone.utc)
        campaigns_sorted = sorted(campaigns, key=_start)

        # Build de-dupe set from existing drip (campaign + last10)
        existing = list_records(drip_h, page_size=1000)
        existing_pairs = {
            (
                ((f.get("fields") or {}).get(DRIP_CAMPAIGN_LINK_FIELD) or [None])[0],
                last_10_digits((f.get("fields") or {}).get(DRIP_SELLER_PHONE_FIELD)),
            )
            for f in existing
            if f.get("fields")
        }

        now_utc = datetime.now(timezone.utc)

        for camp in campaigns_sorted:
            cfields = camp.get("fields", {}) or {}
            status = str(cfields.get(CAMPAIGN_STATUS_FIELD, "")).strip().lower()
            if status != "scheduled":
                continue

            campaign_id = camp.get("id")
            start_time = _campaign_start(cfields)
            market = _campaign_market(cfields)
            if not market:
                logger.warning("⚠️ Campaign %s missing Market; skipping", campaign_id)
                continue

            # Templates
            template_ids = cfields.get(CAMPAIGN_TEMPLATES_LINK) or []
            messages: List[str] = []
            if template_ids:
                for tid in template_ids:
                    resp = templates_h.table.api.request(
                        "get",
                        templates_h.table.url,
                        params={"filterByFormula": f"RECORD_ID()='{tid}'"},
                    )
                    for rec in (resp or {}).get("records", []):
                        msg = (rec.get("fields", {}) or {}).get("Message")
                        if isinstance(msg, str) and msg.strip():
                            messages.append(msg.strip())
            if not messages:
                logger.warning("⚠️ Campaign %s has no templates; skipping", campaign_id)
                continue

            # Linked prospects
            linked = cfields.get(CAMPAIGN_PROSPECTS_LINK) or []
            if not linked:
                logger.info("⏭️ Campaign %s has no linked prospects; skipping", campaign_id)
                continue

            # Fetch prospects in chunks of 100
            prospects: List[Dict[str, Any]] = []
            for i in range(0, len(linked), 100):
                chunk = linked[i:i+100]
                formula = "OR(" + ",".join([f"RECORD_ID()='{rid}'" for rid in chunk]) + ")"
                resp = prospects_h.table.api.request("get", prospects_h.table.url, params={"filterByFormula": formula})
                prospects.extend((resp or {}).get("records", []))
                time.sleep(CHUNK_SLEEP_SEC)

            queued = skipped = processed = 0
            skip_reasons: Dict[str, int] = defaultdict(int)

            def _next_send(_j: int) -> str:
                # Prequeue everything with jitter around the Start Time
                base_ts = start_time.timestamp()
                ts = base_ts + random.randint(0, max(0, PREQUEUE_JITTER_MAX_SEC))
                return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

            # IMPORTANT: we choose a number **per prospect** for true rotation
            for idx, pr in enumerate(prospects):
                processed += 1
                pf = pr.get("fields", {}) or {}

                phone = _prospect_best_phone(pf)
                if not phone:
                    skipped += 1; skip_reasons["missing_phone"] += 1
                    continue

                digits = last_10_digits(phone)
                if (campaign_id, digits) in existing_pairs:
                    skipped += 1; skip_reasons["duplicate_phone"] += 1
                    continue

                from_number = _choose_number_for_market(market)
                if not from_number:
                    skipped += 1; skip_reasons["missing_textgrid_number"] += 1
                    continue

                template_text = random.choice(messages)
                rendered = _render_message(template_text, pf)

                payload = {
                    DRIP_STATUS_FIELD: "QUEUED",
                    DRIP_MARKET_FIELD: market,
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
                        skipped += 1; skip_reasons["create_failed"] += 1
                except Exception as exc:
                    logger.warning("Create failed for %s: %s", digits, exc)
                    skipped += 1; skip_reasons["create_failed"] += 1

            # Flip Status → Active only when we're at/after Start Time and we queued at least one
            if queued and now_utc >= start_time:
                try:
                    update_record(campaigns_h, campaign_id, {
                        CAMPAIGN_STATUS_FIELD: "Active",
                        CAMPAIGN_LAST_RUN_FIELD: iso_now(),
                    })
                except Exception:
                    pass
            else:
                # Keep as Scheduled (prequeued), but update Last Run At for traceability
                try:
                    update_record(campaigns_h, campaign_id, {
                        CAMPAIGN_LAST_RUN_FIELD: iso_now(),
                    })
                except Exception:
                    pass

            summary["queued"] += queued
            summary["campaigns"][campaign_id] = {
                "queued": queued,
                "skipped": skipped,
                "processed": processed,
                "skip_reasons": dict(skip_reasons),
                "market": market,
                "start_time": start_time.isoformat(),
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
