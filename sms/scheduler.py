"""Campaign scheduler that hydrates Drip Queue records from Airtable campaigns."""

from __future__ import annotations
import os
import re
import time
import random
import json
import requests
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote
from collections import defaultdict
from dotenv import load_dotenv

# Internal imports
from sms.airtable_schema import (
    campaign_field_map,
    drip_field_map,
    prospects_field_map,
    template_field_map,
)
from sms.config import settings
from sms.datastore import CONNECTOR, create_record, list_records, update_record
from sms.runtime import get_logger, iso_now, last_10_digits, normalize_phone

logger = get_logger(__name__)

# ======================================================
# LOAD ENV
# ======================================================
ROOT_DIR = Path(__file__).resolve().parents[1]
load_dotenv(ROOT_DIR / ".env")

TEST_MODE = os.getenv("TEST_MODE", "false").lower() in {"1", "true", "yes"}

# ======================================================
# BASE CONFIG
# ======================================================
# Core bases
CAMPAIGNS_BASE_ID = os.getenv("CAMPAIGNS_BASE_ID", "appMn2MKocaJ9I3rW")
CAMPAIGN_CONTROL_BASE = os.getenv("CAMPAIGN_CONTROL_BASE", "appyhhWYmrM86H35a")

# Numbers table
NUMBERS_TABLE = os.getenv("NUMBERS_TABLE", "Numbers")
NUMBERS_MARKET_FIELD = os.getenv("NUMBERS_MARKET_FIELD", "Market")
NUMBERS_PHONE_FIELD = os.getenv("NUMBERS_PHONE_FIELD", "TextGrid Phone Number")

# Airtable key
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
if not AIRTABLE_API_KEY:
    logger.warning("⚠️ Missing AIRTABLE_API_KEY — API requests will fail.")

# ======================================================
# FIELD MAPS
# ======================================================
CAMPAIGN_FIELDS = campaign_field_map()
DRIP_FIELDS = drip_field_map()
PROSPECT_FIELDS = prospects_field_map()
TEMPLATE_FIELDS = template_field_map()

# Important field constants (safe fallbacks)
CAMPAIGN_STATUS_FIELD = CAMPAIGN_FIELDS.get("Status", "Status")
CAMPAIGN_MARKET_FIELD = CAMPAIGN_FIELDS.get("Market", "Market")
CAMPAIGN_START_FIELD = CAMPAIGN_FIELDS.get("Start Time", "Start Time")
CAMPAIGN_LAST_RUN_FIELD = CAMPAIGN_FIELDS.get("Last Run At", "Last Run At")

DRIP_STATUS_FIELD = DRIP_FIELDS.get("Status", "Status")
DRIP_MARKET_FIELD = DRIP_FIELDS.get("Market", "Market")
DRIP_SELLER_PHONE_FIELD = DRIP_FIELDS.get("Seller Phone Number", "Seller Phone Number")
DRIP_FROM_NUMBER_FIELD = DRIP_FIELDS.get("TextGrid Phone Number", "TextGrid Phone Number")
DRIP_PROSPECT_LINK_FIELD = DRIP_FIELDS.get("Prospect", "Prospect")
DRIP_CAMPAIGN_LINK_FIELD = DRIP_FIELDS.get("Campaign", "Campaign")
DRIP_NEXT_SEND_DATE_FIELD = DRIP_FIELDS.get("Next Send Date", "Next Send Date")
DRIP_UI_FIELD = DRIP_FIELDS.get("UI", "UI")
DRIP_PROCESSOR_FIELD = DRIP_FIELDS.get("Processor", "Processor")

SCHEDULER_PROCESSOR_LABEL = "Campaign Scheduler"

# ======================================================
# UTILITIES
# ======================================================
def _parse_datetime(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def _campaign_start(fields: Dict[str, Any]) -> datetime:
    dt = _parse_datetime(fields.get(CAMPAIGN_START_FIELD)) if CAMPAIGN_START_FIELD else None
    return dt.astimezone(timezone.utc) if dt else datetime.now(timezone.utc)


def _prospect_phone(fields: Dict[str, Any]) -> Optional[str]:
    phone_fields = [
        PROSPECT_FIELDS.get("PHONE_PRIMARY"),
        PROSPECT_FIELDS.get("PHONE_SECONDARY"),
        PROSPECT_FIELDS.get("PHONE_PRIMARY_LINKED"),
        PROSPECT_FIELDS.get("PHONE_SECONDARY_LINKED"),
    ]
    for f in phone_fields:
        if not f:
            continue
        val = fields.get(f)
        if isinstance(val, list):
            for v in val:
                normalized = normalize_phone(v)
                if normalized:
                    return normalized
        elif isinstance(val, str):
            normalized = normalize_phone(val)
            if normalized:
                return normalized
    return None


def _coerce_market(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list) and value:
        return _coerce_market(value[0])
    if isinstance(value, dict):
        for k in ("name", "label", "value", "Market"):
            if isinstance(value.get(k), str):
                return value[k].strip()
    return str(value or "").strip()


def _campaign_market(fields: Dict[str, Any]) -> Tuple[str, str]:
    raw = _coerce_market(fields.get(CAMPAIGN_MARKET_FIELD))
    return raw, raw.lower().strip() if raw else ""


# ======================================================
# NUMBERS LOOKUP (CROSS-BASE)
# ======================================================
_numbers_cache: Dict[str, List[str]] = {}

def _fetch_textgrid_number_for_market(market_raw: Optional[str]) -> Optional[str]:
    """
    Fetch the TextGrid number(s) from the Numbers table for a given market.
    """
    if not market_raw:
        return None

    market_key = str(market_raw).strip().lower()
    if not market_key:
        return None

    # Cache hit
    if market_key in _numbers_cache and _numbers_cache[market_key]:
        return random.choice(_numbers_cache[market_key])

    url = f"https://api.airtable.com/v0/{CAMPAIGN_CONTROL_BASE}/{quote(NUMBERS_TABLE, safe='')}"
    formula = f"LOWER({{{NUMBERS_MARKET_FIELD}}})='{market_key}'"
    params = {"pageSize": 100, "filterByFormula": formula}
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=12)
        if resp.status_code != 200:
            logger.warning("⚠️ Numbers fetch for market %s failed: %s %s", market_raw, resp.status_code, resp.text[:200])
            return None

        recs = (resp.json() or {}).get("records", [])
        numbers: List[str] = []
        for rec in recs:
            fields = rec.get("fields", {}) or {}
            val = fields.get(NUMBERS_PHONE_FIELD) or fields.get("Phone") or fields.get("Number")
            if isinstance(val, str) and val.strip():
                numbers.append(val.strip())

        if not numbers:
            logger.warning("⚠️ No TextGrid numbers found for market %s", market_raw)
            return None

        _numbers_cache[market_key] = numbers
        chosen = random.choice(numbers)
        logger.info("🧭 Using TextGrid number %s for market %s (found %d candidates)", chosen, market_raw, len(numbers))
        return chosen

    except Exception as exc:
        logger.error("❌ Error fetching numbers for market %s: %s", market_raw, exc, exc_info=True)
        return None


# ======================================================
# MAIN SCHEDULER LOGIC
# ======================================================
def run_scheduler(limit: Optional[int] = None) -> Dict[str, Any]:
    logger.info("🚀 Starting campaign scheduler run")

    summary: Dict[str, Any] = {"queued": 0, "campaigns": {}, "errors": [], "ok": True}
    market_counts = defaultdict(int)
    if TEST_MODE:
        summary["note"] = "TEST_MODE active"
        return summary

    try:
        campaigns_handle = CONNECTOR.campaigns()
        drip_handle = CONNECTOR.drip_queue()
        prospects_handle = CONNECTOR.prospects()
        templates_handle = CONNECTOR.templates()

        campaigns = list_records(campaigns_handle, page_size=100)
        existing_drip = list_records(drip_handle, page_size=100)

        existing_pairs = {
            (cid, digits)
            for f in existing_drip
            if f.get("fields")
            for cid, digits in [
                (
                    str((f.get("fields") or {}).get(DRIP_CAMPAIGN_LINK_FIELD, [None])[0]),
                    last_10_digits((f.get("fields") or {}).get(DRIP_SELLER_PHONE_FIELD)),
                )
            ]
            if cid and digits
        }

        for campaign in campaigns:
            fields = campaign.get("fields", {}) or {}
            status = str(fields.get(CAMPAIGN_STATUS_FIELD, "")).lower()
            if status != "scheduled":
                continue

            campaign_id = campaign.get("id")
            linked_field = CAMPAIGN_FIELDS.get("PROSPECTS_LINK")
            linked_ids = fields.get(linked_field) if linked_field else []
            if not linked_ids:
                logger.info("Skipping campaign %s (no linked prospects)", campaign_id)
                continue

            # Fetch prospects
            prospect_records = []
            for chunk_start in range(0, len(linked_ids), 100):
                chunk = linked_ids[chunk_start:chunk_start + 100]
                formula = "OR(" + ",".join([f"RECORD_ID()='{rid}'" for rid in chunk]) + ")"
                resp = prospects_handle.table.api.request("get", prospects_handle.table.url, params={"filterByFormula": formula})
                prospect_records.extend(resp.get("records", []))
                time.sleep(0.15)

            if not prospect_records:
                continue

            campaign_market_raw, _ = _campaign_market(fields)
            from_number = _fetch_textgrid_number_for_market(campaign_market_raw)
            if not from_number:
                logger.warning("⚠️ Skipping campaign %s: no TextGrid number found for market '%s'", campaign_id, campaign_market_raw)
                summary["campaigns"][campaign_id] = {
                    "queued": 0,
                    "skipped": len(prospect_records),
                    "processed": len(prospect_records),
                    "skip_reasons": {"missing_textgrid_number": len(prospect_records)},
                }
                continue

            start_time = _campaign_start(fields)
            queued = skipped = processed = 0
            skip_reasons = defaultdict(int)

            # Template fetch
            template_link_field = CAMPAIGN_FIELDS.get("TEMPLATES_LINK")
            template_ids = fields.get(template_link_field) if template_link_field else []
            template_messages = []
            if template_ids:
                for tid in template_ids:
                    formula = f"RECORD_ID()='{tid}'"
                    resp = templates_handle.table.api.request("get", templates_handle.table.url, params={"filterByFormula": formula})
                    for rec in resp.get("records", []):
                        msg = (rec.get("fields", {}) or {}).get("Message")
                        if msg:
                            template_messages.append(msg)
            if not template_messages:
                logger.warning("⚠️ Campaign %s has no templates", campaign_id)
                continue

            for prospect in prospect_records:
                processed += 1
                pf = prospect.get("fields", {}) or {}
                phone = _prospect_phone(pf)
                if not phone:
                    skip_reasons["missing_phone"] += 1
                    skipped += 1
                    continue

                digits = last_10_digits(phone)
                if (campaign_id, digits) in existing_pairs:
                    skip_reasons["duplicate_phone"] += 1
                    skipped += 1
                    continue

                message_text = random.choice(template_messages)
                payload = {
                    DRIP_STATUS_FIELD: "QUEUED",
                    DRIP_MARKET_FIELD: campaign_market_raw,
                    DRIP_SELLER_PHONE_FIELD: phone,
                    DRIP_FROM_NUMBER_FIELD: from_number,
                    DRIP_PROCESSOR_FIELD: SCHEDULER_PROCESSOR_LABEL,
                    DRIP_NEXT_SEND_DATE_FIELD: start_time.isoformat(),
                    DRIP_CAMPAIGN_LINK_FIELD: [campaign_id],
                    DRIP_PROSPECT_LINK_FIELD: [prospect["id"]],
                    DRIP_UI_FIELD: "⏳",
                    "Message": message_text,
                    "Message Preview": message_text,
                }

                if create_record(drip_handle, payload):
                    existing_pairs.add((campaign_id, digits))
                    queued += 1
                else:
                    skipped += 1
                    skip_reasons["create_failed"] += 1

            if queued > 0:
                update_record(campaigns_handle, campaign_id, {
                    CAMPAIGN_STATUS_FIELD: "Active",
                    CAMPAIGN_LAST_RUN_FIELD: iso_now(),
                })

            summary["queued"] += queued
            summary["campaigns"][campaign_id] = {
                "queued": queued,
                "skipped": skipped,
                "processed": processed,
                "skip_reasons": dict(skip_reasons),
                "from_number": from_number,
            }

            logger.info("✅ Campaign %s queued=%d skipped=%d processed=%d", campaign_id, queued, skipped, processed)

        summary["market_counts"] = dict(market_counts)
        summary["ok"] = not summary["errors"]
        logger.info("🏁 Campaign scheduler finished: %d queued total", summary["queued"])
        return summary

    except Exception as exc:
        logger.exception("Scheduler fatal error: %s", exc)
        summary["ok"] = False
        summary["errors"].append(str(exc))
        return summary
