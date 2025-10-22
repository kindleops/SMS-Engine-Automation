"""Campaign scheduler that hydrates Drip Queue from Airtable campaigns."""

from __future__ import annotations

import os
import time
import random
import requests
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import quote
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict
from dotenv import load_dotenv

# Internal
from sms.airtable_schema import (
    campaign_field_map,
    drip_field_map,
    prospects_field_map,
    template_field_map,
)
from sms.datastore import CONNECTOR, create_record, list_records, update_record
from sms.runtime import get_logger, iso_now, last_10_digits, normalize_phone

logger = get_logger(__name__)

# ───────────────────────────────────────────────────────────────────────────────
# ENV / BASE CONFIG
# ───────────────────────────────────────────────────────────────────────────────
ROOT_DIR = Path(__file__).resolve().parents[1]
load_dotenv(ROOT_DIR / ".env")

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY", "")
if not AIRTABLE_API_KEY:
    logger.warning("⚠️ Missing AIRTABLE_API_KEY — API requests will fail.")

TEST_MODE = os.getenv("TEST_MODE", "false").lower() in {"1", "true", "yes"}

# Bases
CAMPAIGNS_BASE_ID = os.getenv("CAMPAIGNS_BASE_ID", "appMn2MKocaJ9I3rW")
CAMPAIGN_CONTROL_BASE = os.getenv("CAMPAIGN_CONTROL_BASE", "appyhhWYmrM86H35a")

# Numbers table (matches your screenshot exactly)
NUMBERS_TABLE = os.getenv("NUMBERS_TABLE", "Numbers")
NUMBERS_MARKET_FIELD = os.getenv("NUMBERS_MARKET_FIELD", "Market")
NUMBERS_PHONE_FIELD = os.getenv("NUMBERS_PHONE_FIELD", "A Number")
NUMBERS_STATUS_FIELD = os.getenv("NUMBERS_STATUS_FIELD", "Status")
NUMBERS_ACTIVE_FIELD = os.getenv("NUMBERS_ACTIVE_FIELD", "Active")

# ───────────────────────────────────────────────────────────────────────────────
# FIELD MAPS (use your canonical names with safe fallbacks)
# ───────────────────────────────────────────────────────────────────────────────
CAMPAIGN_FIELDS = campaign_field_map()
DRIP_FIELDS = drip_field_map()
PROSPECT_FIELDS = prospects_field_map()
TEMPLATE_FIELDS = template_field_map()

# Campaigns
CAMPAIGN_STATUS_FIELD     = CAMPAIGN_FIELDS.get("Status", "Status")
CAMPAIGN_MARKET_FIELD     = CAMPAIGN_FIELDS.get("Market", "Market")
CAMPAIGN_START_FIELD      = CAMPAIGN_FIELDS.get("Start Time", "Start Time")
CAMPAIGN_LAST_RUN_FIELD   = CAMPAIGN_FIELDS.get("Last Run At", "Last Run At")
CAMPAIGN_PROSPECTS_LINK   = CAMPAIGN_FIELDS.get("PROSPECTS_LINK", CAMPAIGN_FIELDS.get("Prospects", "Prospects"))
CAMPAIGN_TEMPLATES_LINK   = CAMPAIGN_FIELDS.get("TEMPLATES_LINK", CAMPAIGN_FIELDS.get("Templates", "Templates"))

# Drip Queue (NO "Message" field here)
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

# ───────────────────────────────────────────────────────────────────────────────
# HELPERS
# ───────────────────────────────────────────────────────────────────────────────
def _parse_iso(value: Any) -> Optional[datetime]:
    if not value: return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def _campaign_start(fields: Dict[str, Any]) -> datetime:
    return _parse_iso(fields.get(CAMPAIGN_START_FIELD)) or datetime.now(timezone.utc)

def _prospect_best_phone(fields: Dict[str, Any]) -> Optional[str]:
    # Try primary/linked/secondary in a sensible order
    candidates = [
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

# ───────────────────────────────────────────────────────────────────────────────
# NUMBERS LOOKUP / ROTATION
# ───────────────────────────────────────────────────────────────────────────────
_numbers_cache: Dict[str, List[str]] = {}
_rotation_index: Dict[str, int] = {}

def _market_key(m: str) -> str:
    return (m or "").strip().lower()

def _choose_rotating(market: str, numbers: List[str]) -> str:
    k = _market_key(market)
    idx = _rotation_index.get(k, 0)
    chosen = numbers[idx % len(numbers)]
    _rotation_index[k] = idx + 1
    return chosen

def _fetch_textgrid_number_for_market(market_raw: Optional[str]) -> Optional[str]:
    """
    Pull active numbers for a market from the Numbers table, cache, and rotate.
    Accepts exact market strings like 'Los Angeles, CA'.
    """
    if not market_raw: return None
    mkey = _market_key(market_raw)
    if not mkey: return None

    # cache hit → rotate
    cached = _numbers_cache.get(mkey)
    if cached:
        return _choose_rotating(market_raw, cached)

    # fetch all numbers (pagination-safe)
    try:
        url = f"https://api.airtable.com/v0/{CAMPAIGN_CONTROL_BASE}/{quote(NUMBERS_TABLE, safe='')}"
        headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}
        params: Dict[str, Any] = {"pageSize": 100}
        # formula: must match market + Active TRUE + Status 'Active'
        formula = (
            f"AND("
            f"LOWER({{{NUMBERS_MARKET_FIELD}}})='{mkey}',"
            f"{{{NUMBERS_ACTIVE_FIELD}}}=TRUE(),"
            f"{{{NUMBERS_STATUS_FIELD}}}='Active'"
            f")"
        )
        params["filterByFormula"] = formula

        records: List[Dict[str, Any]] = []
        offset = None
        while True:
            if offset: params["offset"] = offset
            r = requests.get(url, headers=headers, params=params, timeout=12)
            if r.status_code != 200:
                logger.warning("⚠️ Numbers fetch failed (%s): %s", r.status_code, r.text[:200])
                break
            data = r.json() or {}
            records.extend(data.get("records", []))
            offset = data.get("offset")
            if not offset: break

        nums: List[str] = []
        for rec in records:
            f = rec.get("fields", {}) or {}
            if f.get(NUMBERS_ACTIVE_FIELD) not in (True, "true", "TRUE", 1):  # double-guard
                continue
            num = f.get(NUMBERS_PHONE_FIELD) or f.get("Number") or f.get("Phone")
            if isinstance(num, str) and num.strip():
                nums.append(num.strip())

        if not nums:
            logger.warning("⚠️ No active TextGrid numbers found for market '%s'", market_raw)
            return None

        _numbers_cache[mkey] = nums
        chosen = _choose_rotating(market_raw, nums)
        logger.info("📞 %d numbers for %s — using %s", len(nums), market_raw, chosen)
        return chosen

    except Exception as exc:
        logger.error("❌ Numbers fetch error for %s: %s", market_raw, exc, exc_info=True)
        return None

# ───────────────────────────────────────────────────────────────────────────────
# MAIN SCHEDULER
# ───────────────────────────────────────────────────────────────────────────────
def run_scheduler(limit: Optional[int] = None) -> Dict[str, Any]:
    logger.info("🚀 Scheduler start")
    summary: Dict[str, Any] = {"queued": 0, "campaigns": {}, "errors": [], "ok": True}

    if TEST_MODE:
        summary["note"] = "TEST_MODE active; no writes performed."
        return summary

    try:
        # Handles
        campaigns_h = CONNECTOR.campaigns()
        prospects_h = CONNECTOR.prospects()
        drip_h = CONNECTOR.drip_queue()
        templates_h = CONNECTOR.templates()

        # Pull campaigns (paginated internally)
        campaigns = list_records(campaigns_h, page_size=100)

        # Build de-dupe set from existing drip
        existing = list_records(drip_h, page_size=100)
        existing_pairs = {
            ( (f.get("fields", {}) or {}).get(DRIP_CAMPAIGN_LINK_FIELD, [None])[0],
              last_10_digits((f.get("fields", {}) or {}).get(DRIP_SELLER_PHONE_FIELD)) )
            for f in existing if f.get("fields")
        }

        for camp in campaigns:
            cfields = camp.get("fields", {}) or {}
            status = str(cfields.get(CAMPAIGN_STATUS_FIELD, "")).strip().lower()
            if status != "scheduled":
                continue

            campaign_id = camp.get("id")
            market = _campaign_market(cfields)
            if not market:
                logger.warning("⚠️ Campaign %s missing Market; skipping", campaign_id)
                continue

            # templates
            template_ids = cfields.get(CAMPAIGN_TEMPLATES_LINK) or []
            messages: List[str] = []
            if template_ids:
                # fetch exact records by RECORD_ID()
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
                logger.warning("⚠️ Campaign %s has no template messages; skipping", campaign_id)
                continue

            # prospects (linked IDs)
            linked = cfields.get(CAMPAIGN_PROSPECTS_LINK) or []
            if not linked:
                logger.info("⏭️ Campaign %s has no linked prospects; skipping", campaign_id)
                continue

            # get a sending number for THIS market (rotates per call)
            from_number = _fetch_textgrid_number_for_market(market)
            if not from_number:
                logger.warning("⚠️ Campaign %s: no active TextGrid number for market '%s'", campaign_id, market)
                summary["campaigns"][campaign_id] = {
                    "queued": 0, "skipped": len(linked), "processed": 0,
                    "skip_reasons": {"missing_textgrid_number": len(linked)},
                    "from_number": None,
                }
                continue

            # hydrate prospects by chunks of 100
            prospects: List[Dict[str, Any]] = []
            for i in range(0, len(linked), 100):
                chunk = linked[i:i+100]
                formula = "OR(" + ",".join([f"RECORD_ID()='{rid}'" for rid in chunk]) + ")"
                resp = prospects_h.table.api.request("get", prospects_h.table.url, params={"filterByFormula": formula})
                prospects.extend((resp or {}).get("records", []))
                time.sleep(0.12)  # be nice to API

            start_time = _campaign_start(cfields)

            queued = skipped = processed = 0
            skip_reasons: Dict[str, int] = defaultdict(int)

            # Optional pacing: spread by random seconds to avoid burst
            def _next_send(j: int) -> str:
                # jitter 0–90s per record
                ts = start_time.timestamp() + random.randint(0, 90)
                return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

            for idx, pr in enumerate(prospects):
                processed += 1
                pf = pr.get("fields", {}) or {}

                phone = _prospect_best_phone(pf)
                if not phone:
                    skipped += 1; skip_reasons["missing_phone"] = skip_reasons.get("missing_phone", 0) + 1
                    continue

                digits = last_10_digits(phone)
                if (campaign_id, digits) in existing_pairs:
                    skipped += 1; skip_reasons["duplicate_phone"] = skip_reasons.get("duplicate_phone", 0) + 1
                    continue

                message_text = random.choice(messages)

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
                    DRIP_MESSAGE_PREVIEW_FIELD: message_text,  # ✅ preview only; NOT the actual send field
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

            # update campaign status if anything queued
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
                "queued": queued,
                "skipped": skipped,
                "processed": processed,
                "skip_reasons": dict(skip_reasons),
                "from_number": from_number,
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
