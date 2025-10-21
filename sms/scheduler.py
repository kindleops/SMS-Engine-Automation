"""Campaign scheduler that hydrates Drip Queue records from Airtable campaigns."""

from __future__ import annotations
import os
import re
import time
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from collections import defaultdict
import requests
from dotenv import load_dotenv

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # type: ignore

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
TEST_MODE = os.getenv("TEST_MODE", "false").lower() in {"1", "true", "yes"}

ROOT_DIR = Path(__file__).resolve().parents[1]
_ENV_LOADED = False
_LOGGED_ENV = False

def _load_env_once() -> None:
    global _ENV_LOADED
    if _ENV_LOADED:
        return
    for candidate in (ROOT_DIR / ".env", ROOT_DIR / "config" / ".env"):
        if candidate.exists():
            try:
                load_dotenv(candidate, override=False)
            except Exception as exc:
                logger.debug("Failed loading %s: %s", candidate, exc)
    _ENV_LOADED = True

_load_env_once()

# =====================================================================
# FIELD MAPS
# =====================================================================

CAMPAIGN_FIELDS = campaign_field_map()
DRIP_FIELDS = drip_field_map()
PROSPECT_FIELDS = prospects_field_map()
TEMPLATE_FIELDS = template_field_map()

# Campaign fields
CAMPAIGN_STATUS_FIELD = CAMPAIGN_FIELDS["STATUS"]
CAMPAIGN_MARKET_FIELD = CAMPAIGN_FIELDS["MARKET"]
CAMPAIGN_START_FIELD = CAMPAIGN_FIELDS.get("START_TIME")
CAMPAIGN_LAST_RUN_FIELD = CAMPAIGN_FIELDS.get("LAST_RUN_AT")
CAMPAIGN_TEMPLATES_LINK = CAMPAIGN_FIELDS.get("TEMPLATES_LINK")
CAMPAIGN_PROSPECTS_LINK = CAMPAIGN_FIELDS.get("PROSPECTS_LINK")

# Drip fields
DRIP_STATUS_FIELD = DRIP_FIELDS["STATUS"]
DRIP_MARKET_FIELD = DRIP_FIELDS.get("MARKET", "Market")
DRIP_SELLER_PHONE_FIELD = DRIP_FIELDS.get("SELLER_PHONE", "phone")
DRIP_FROM_NUMBER_FIELD = DRIP_FIELDS.get("FROM_NUMBER", "From Number")
DRIP_PROSPECT_LINK_FIELD = DRIP_FIELDS.get("PROSPECT_LINK", "Prospect")
DRIP_CAMPAIGN_LINK_FIELD = DRIP_FIELDS.get("CAMPAIGN_LINK", "Campaign")
DRIP_NEXT_SEND_DATE_FIELD = DRIP_FIELDS.get("NEXT_SEND_DATE", "next_send_date")
DRIP_UI_FIELD = DRIP_FIELDS.get("UI", "UI")
DRIP_PROCESSOR_FIELD = DRIP_FIELDS.get("PROCESSOR", "processor")
DRIP_PROPERTY_ID_FIELD = DRIP_FIELDS.get("PROPERTY_ID", "Property ID")
DRIP_MESSAGE_FIELD = DRIP_FIELDS.get("MESSAGE", "Message")
DRIP_TEMPLATE_LINK_FIELD = DRIP_FIELDS.get("TEMPLATE_LINK", "Template")
DRIP_MESSAGE_PREVIEW_FIELD = DRIP_FIELDS.get("MESSAGE_PREVIEW", "Message Preview")

# Prospect fields
PROSPECT_MARKET_FIELD = PROSPECT_FIELDS.get("MARKET", "Market")
PROSPECT_PROPERTY_ID_FIELD = PROSPECT_FIELDS.get("PROPERTY_ID")
PROSPECT_PHONE_FIELDS = [
    PROSPECT_FIELDS.get("PHONE_PRIMARY"),
    PROSPECT_FIELDS.get("PHONE_PRIMARY_LINKED"),
    PROSPECT_FIELDS.get("PHONE_SECONDARY"),
    PROSPECT_FIELDS.get("PHONE_SECONDARY_LINKED"),
]

# Template fields
TEMPLATE_MESSAGE_FIELD = TEMPLATE_FIELDS["MESSAGE"]
TEMPLATE_NAME_FIELD = TEMPLATE_FIELDS.get("NAME", "Name")

SCHEDULER_PROCESSOR_LABEL = "Campaign Scheduler"
TEXTGRID_NUMBER_FIELD = os.getenv("CAMPAIGN_TEXTGRID_NUMBER_FIELD", "TextGrid Phone Number")
TEXTGRID_NUMBER_FALLBACK_FIELD = os.getenv("CAMPAIGN_TEXTGRID_NUMBER_FALLBACK_FIELD", "TextGrid Number")

# =====================================================================
# HELPERS
# =====================================================================

def _extract_record_ids(value: Any) -> List[str]:
    ids = []
    if isinstance(value, list):
        for item in value:
            if isinstance(item, str):
                ids.append(item.strip())
            elif isinstance(item, dict) and item.get("id"):
                ids.append(item["id"])
    elif isinstance(value, str):
        ids.append(value.strip())
    return ids

def _get_linked_prospects(fields: Dict[str, Any]) -> List[str]:
    return _extract_record_ids(fields.get(CAMPAIGN_PROSPECTS_LINK))

def _escape_formula_value(value: str) -> str:
    return str(value).replace("'", "\\'")

def _fetch_linked_records(table_handle, record_ids: List[str], chunk_size: int, label: str, campaign_id: str) -> List[Dict[str, Any]]:
    if not record_ids:
        return []
    table = table_handle.table
    records = []
    for start in range(0, len(record_ids), chunk_size):
        chunk = record_ids[start:start+chunk_size]
        formula = "OR(" + ",".join([f"RECORD_ID()='{_escape_formula_value(r)}'" for r in chunk]) + ")"
        offset = None
        while True:
            params = {"pageSize": chunk_size, "filterByFormula": formula}
            if offset:
                params["offset"] = offset
            response = table.api.request("get", table.url, params=params)
            batch = response.get("records", [])
            records.extend(batch)
            offset = response.get("offset")
            if not offset:
                break
            time.sleep(0.2)
    logger.info(f"Fetched {len(records)} linked {label.lower()} for campaign {campaign_id}")
    return records

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

@dataclass(frozen=True)
class QuietHours:
    enforced: bool
    start_hour: int
    end_hour: int
    tz: timezone

def _quiet_hours() -> QuietHours:
    cfg = settings()
    tz = ZoneInfo(cfg.QUIET_TZ or "America/Chicago") if ZoneInfo else timezone.utc
    return QuietHours(bool(cfg.QUIET_HOURS_ENFORCED), int(cfg.QUIET_START_HOUR or 21), int(cfg.QUIET_END_HOUR or 9), tz)

def _apply_quiet_hours(desired: datetime, quiet: QuietHours) -> datetime:
    if not quiet.enforced:
        return desired
    local = desired.astimezone(quiet.tz)
    start = local.replace(hour=quiet.start_hour, minute=0)
    end = local.replace(hour=quiet.end_hour, minute=0)
    in_quiet = start <= local < end if start < end else not (end <= local < start)
    next_allowed = end if in_quiet else local
    return next_allowed.astimezone(timezone.utc)

def _prospect_phone(fields: Dict[str, Any]) -> Optional[str]:
    for k in PROSPECT_PHONE_FIELDS:
        if not k:
            continue
        val = fields.get(k)
        phones = val if isinstance(val, list) else [val]
        for ph in phones:
            normalized = normalize_phone(ph)
            if normalized:
                return normalized
    return None

def _campaign_market(fields: Dict[str, Any]) -> str:
    val = fields.get(CAMPAIGN_MARKET_FIELD)
    if isinstance(val, list) and val:
        val = val[0]
    return str(val or "").strip()

def _existing_pairs(drip_records: List[Dict[str, Any]]) -> Set[Tuple[str, str]]:
    pairs = set()
    for r in drip_records:
        f = r.get("fields", {}) or {}
        cid = None
        if isinstance(f.get(DRIP_CAMPAIGN_LINK_FIELD), list):
            cid = f[DRIP_CAMPAIGN_LINK_FIELD][0]
        digits = last_10_digits(f.get(DRIP_SELLER_PHONE_FIELD))
        if cid and digits:
            pairs.add((cid, digits))
    return pairs

# =====================================================================
# ✅ Resolve TextGrid Number (direct value or Numbers base lookup)
# =====================================================================

def _fetch_number_from_numbers_base(record_id: str) -> Optional[str]:
    base_id = (
        os.getenv("CAMPAIGN_CONTROL_BASE")
        or os.getenv("AIRTABLE_CAMPAIGN_CONTROL_BASE_ID")
        or "appyhhWYmrM86H35a"
    )
    api_key = os.getenv("AIRTABLE_API_KEY")
    if not (record_id and base_id and api_key):
        return None

    url = f"https://api.airtable.com/v0/{base_id}/Numbers/{record_id}"
    headers = {"Authorization": f"Bearer {api_key}"}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
    except Exception as exc:
        logger.error("❌ Failed to fetch number %s: %s", record_id, exc, exc_info=True)
        return None

    if resp.status_code != 200:
        logger.warning(
            "⚠️ Numbers lookup failed for %s status=%s body=%s",
            record_id,
            resp.status_code,
            resp.text,
        )
        return None

    fields = resp.json().get("fields", {})
    for candidate in ("TextGrid Phone Number", "Phone", "Number"):
        value = fields.get(candidate)
        if value and str(value).strip():
            return str(value).strip()
    return None


def _fetch_textgrid_number(campaign_fields: Dict[str, Any]) -> Optional[str]:
    raw_value = (
        campaign_fields.get(TEXTGRID_NUMBER_FIELD)
        or campaign_fields.get(TEXTGRID_NUMBER_FALLBACK_FIELD)
    )
    if not raw_value:
        return None

    def _resolve(value: Any) -> Optional[str]:
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return None
            if stripped.lower().startswith("rec"):
                return _fetch_number_from_numbers_base(stripped)
            return stripped
        if isinstance(value, dict):
            rid = value.get("id")
            if isinstance(rid, str) and rid.strip().lower().startswith("rec"):
                return _fetch_number_from_numbers_base(rid.strip())
            for candidate in ("TextGrid Phone Number", "Phone", "Number"):
                val = value.get(candidate)
                if val and str(val).strip():
                    return str(val).strip()
        if isinstance(value, list) and value:
            return _resolve(value[0])
        return None

    number = _resolve(raw_value)
    if number and number.strip():
        logger.info("🧭 Using TextGrid number %s", number.strip())
        return number.strip()

    return None

# =====================================================================
# MAIN
# =====================================================================

def run_scheduler(limit: Optional[int] = None) -> Dict[str, Any]:
    logger.info("🚀 Starting campaign scheduler run")

    summary = {"queued": 0, "campaigns": {}, "errors": [], "ok": True}
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
        existing_pairs = _existing_pairs(existing_drip)
        quiet = _quiet_hours()

        for campaign in campaigns:
            fields = campaign.get("fields", {}) or {}
            if str(fields.get(CAMPAIGN_STATUS_FIELD, "")).lower() != "scheduled":
                continue

            campaign_id = campaign.get("id")
            linked_ids = _get_linked_prospects(fields)
            if not linked_ids:
                summary["campaigns"][campaign_id] = {"queued": 0, "skipped": 0, "processed": 0}
                continue

            linked_prospects = _fetch_linked_records(prospects_handle, linked_ids, 100, "prospects", campaign_id)
            if not linked_prospects:
                continue

            from_number = _fetch_textgrid_number(fields)
            if not from_number:
                summary["campaigns"][campaign_id] = {
                    "queued": 0,
                    "skipped": len(linked_prospects),
                    "processed": len(linked_prospects),
                    "skip_reasons": {"missing_textgrid_number": len(linked_prospects)},
                }
                continue

            template_ids = _extract_record_ids(fields.get(CAMPAIGN_TEMPLATES_LINK))
            templates = _fetch_linked_records(templates_handle, template_ids, 100, "templates", campaign_id)
            template_choices = []
            for t in templates:
                msg = t.get("fields", {}).get(TEMPLATE_MESSAGE_FIELD)
                if msg and str(msg).strip():
                    template_choices.append((t["id"], str(msg).strip()))
            if not template_choices:
                continue

            start_time = _apply_quiet_hours(_campaign_start(fields), quiet)
            queued = skipped = processed = 0
            skip_reasons = defaultdict(int)
            campaign_market = _campaign_market(fields)

            for prospect in linked_prospects:
                processed += 1
                pf = prospect.get("fields", {}) or {}
                phone = _prospect_phone(pf)
                if not phone:
                    skipped += 1
                    skip_reasons["missing_phone"] += 1
                    continue
                digits = last_10_digits(phone)
                if (campaign_id, digits) in existing_pairs:
                    skipped += 1
                    skip_reasons["duplicate_phone"] += 1
                    continue

                template_id, message_text = random.choice(template_choices)

                payload = {
                    DRIP_STATUS_FIELD: "QUEUED",
                    DRIP_MARKET_FIELD: campaign_market,
                    DRIP_SELLER_PHONE_FIELD: phone,
                    DRIP_FROM_NUMBER_FIELD: from_number,
                    DRIP_PROCESSOR_FIELD: SCHEDULER_PROCESSOR_LABEL,
                    DRIP_NEXT_SEND_DATE_FIELD: start_time.isoformat(),
                    DRIP_CAMPAIGN_LINK_FIELD: [campaign_id],
                    DRIP_PROSPECT_LINK_FIELD: [prospect["id"]],
                    DRIP_MESSAGE_FIELD: message_text,
                    DRIP_MESSAGE_PREVIEW_FIELD: message_text,
                    DRIP_UI_FIELD: "⏳",
                    DRIP_TEMPLATE_LINK_FIELD: [template_id],
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

            logger.info(f"✅ Campaign {campaign_id}: queued={queued}, skipped={skipped}, processed={processed}")

        summary["market_counts"] = dict(market_counts)
        summary["ok"] = not summary["errors"]
        logger.info(f"🏁 Scheduler finished: {summary['queued']} queued total")
        return summary

    except Exception as exc:
        logger.exception("Scheduler fatal error: %s", exc)
        summary["ok"] = False
        summary["errors"].append(str(exc))
        return summary
