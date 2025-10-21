"""Campaign scheduler that hydrates Drip Queue records from Airtable campaigns."""

from __future__ import annotations
import os
import re
import time
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import quote
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
ENV_SOURCES: List[str] = []
_ENV_LOADED = False
_LOGGED_ENV = False
API_KEY_ENV_PRIORITY = (
    "AIRTABLE_API_KEY",
    "AIRTABLE_ACQUISITIONS_KEY",
    "AIRTABLE_COMPLIANCE_KEY",
    "AIRTABLE_REPORTING_KEY",
)
BASE_ID_PATTERN = re.compile(r"^app[a-zA-Z0-9]{14}$")

# =====================================================================
# ENV + CONFIG LOADING
# =====================================================================

def _load_env_once() -> None:
    global _ENV_LOADED
    if _ENV_LOADED:
        return
    for candidate in (ROOT_DIR / ".env", ROOT_DIR / "config" / ".env"):
        if candidate.exists():
            try:
                load_dotenv(candidate, override=False)
                ENV_SOURCES.append(str(candidate))
            except Exception as exc:
                logger.debug("Failed loading %s: %s", candidate, exc)
    _ENV_LOADED = True

_load_env_once()

def _resolve_api_key() -> Tuple[Optional[str], Optional[str]]:
    for name in API_KEY_ENV_PRIORITY:
        value = os.getenv(name)
        if value:
            return value, name
    return None, None

def _mask_token(token: Optional[str]) -> str:
    if not token:
        return "<missing>"
    trimmed = token.strip()
    return f"{trimmed[:4]}...{trimmed[-4:]}" if len(trimmed) > 8 else "*" * len(trimmed)

def _log_scheduler_env() -> None:
    global _LOGGED_ENV
    if _LOGGED_ENV:
        return
    cfg = settings()
    token, source = _resolve_api_key()
    logger.info(
        "Scheduler Airtable env: campaigns_base=%s, leads_base=%s, control_base=%s, table=%s, api_key=%s (from %s), env_files=%s",
        cfg.CAMPAIGNS_BASE_ID or "<missing>",
        cfg.LEADS_CONVOS_BASE or "<missing>",
        cfg.CAMPAIGN_CONTROL_BASE or "<missing>",
        cfg.CAMPAIGNS_TABLE or "<missing>",
        _mask_token(token),
        source or "<none>",
        ", ".join(ENV_SOURCES) or "<none>",
    )
    _LOGGED_ENV = True

# =====================================================================
# AIRTABLE HELPERS
# =====================================================================

CAMPAIGN_FIELDS = campaign_field_map()
DRIP_FIELDS = drip_field_map()
PROSPECT_FIELDS = prospects_field_map()

CAMPAIGN_STATUS_FIELD = CAMPAIGN_FIELDS["STATUS"]
CAMPAIGN_MARKET_FIELD = CAMPAIGN_FIELDS["MARKET"]
CAMPAIGN_START_FIELD = CAMPAIGN_FIELDS.get("START_TIME")
CAMPAIGN_LAST_RUN_FIELD = CAMPAIGN_FIELDS.get("LAST_RUN_AT")

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

PROSPECT_MARKET_FIELD = PROSPECT_FIELDS.get("MARKET", "Market")
PROSPECT_PROPERTY_ID_FIELD = PROSPECT_FIELDS.get("PROPERTY_ID")
PROSPECT_PHONE_FIELDS = [
    PROSPECT_FIELDS.get("PHONE_PRIMARY"),
    PROSPECT_FIELDS.get("PHONE_PRIMARY_LINKED"),
    PROSPECT_FIELDS.get("PHONE_SECONDARY"),
    PROSPECT_FIELDS.get("PHONE_SECONDARY_LINKED"),
]

SCHEDULER_PROCESSOR_LABEL = "Campaign Scheduler"

TEXTGRID_NUMBER_FIELD = os.getenv("CAMPAIGN_TEXTGRID_NUMBER_FIELD", "TextGrid Phone Number")
TEXTGRID_NUMBER_FALLBACK_FIELD = os.getenv("CAMPAIGN_TEXTGRID_NUMBER_FALLBACK_FIELD", "TextGrid Number")

TEMPLATE_FIELDS = template_field_map()
TEMPLATE_MESSAGE_FIELD = TEMPLATE_FIELDS["MESSAGE"]
TEMPLATE_NAME_FIELD = TEMPLATE_FIELDS.get("NAME", "Name")

# =====================================================================
# LINKED PROSPECTS FETCHING (FIXED + PAGINATED)
# =====================================================================

def _extract_record_ids(value: Any) -> List[str]:
    ids: List[str] = []
    if isinstance(value, list):
        for item in value:
            if isinstance(item, str) and item.strip():
                ids.append(item.strip())
            elif isinstance(item, dict):
                rid = item.get("id") or item.get("record_id") or item.get("Record ID")
                if isinstance(rid, str) and rid.strip():
                    ids.append(rid.strip())
    elif isinstance(value, dict):
        rid = value.get("id") or value.get("record_id") or value.get("Record ID")
        if isinstance(rid, str) and rid.strip():
            ids.append(rid.strip())
    elif isinstance(value, str) and value.strip():
        ids.append(value.strip())
    return ids


def _get_linked_prospects(fields: Dict[str, Any]) -> List[str]:
    linked_field = CAMPAIGN_FIELDS.get("PROSPECTS_LINK")
    return _extract_record_ids(fields.get(linked_field))

def _escape_formula_value(value: str) -> str:
    return value.replace("'", "\\'")

def _fetch_linked_prospect_records(prospects_handle, campaign_id: str, prospect_ids: List[str]) -> List[Dict[str, Any]]:
    """Fetch all linked prospect records from Airtable with pagination."""
    if not prospect_ids:
        return []
    table = prospects_handle.table
    records: List[Dict[str, Any]] = []

    for start in range(0, len(prospect_ids), 100):
        chunk = prospect_ids[start : start + 100]
        formula = "OR(" + ",".join(
            [f"RECORD_ID()='{_escape_formula_value(rid)}'" for rid in chunk]
        ) + ")"
        offset = None
        while True:
            params = {"pageSize": 100, "filterByFormula": formula}
            if offset:
                params["offset"] = offset
            try:
                response = table.api.request("get", table.url, params=params)
                batch = response.get("records", [])
                records.extend(batch)
                offset = response.get("offset")
                if not offset:
                    break
            except Exception as exc:
                logger.error("❌ Error fetching prospects for campaign %s: %s", campaign_id, exc, exc_info=True)
                break
            time.sleep(0.15)

    logger.info("✅ Fetched %s linked prospects for campaign %s", len(records), campaign_id)
    return records


def _fetch_linked_records(table_handle, record_ids: List[str], chunk_size: int, label: str, campaign_id: str) -> List[Dict[str, Any]]:
    if not record_ids:
        return []
    table = table_handle.table
    records: List[Dict[str, Any]] = []
    for start in range(0, len(record_ids), chunk_size):
        chunk = record_ids[start : start + chunk_size]
        formula = "OR(" + ",".join(
            [f"RECORD_ID()='{_escape_formula_value(rid)}'" for rid in chunk]
        ) + ")"
        offset = None
        while True:
            params = {"pageSize": chunk_size, "filterByFormula": formula}
            if offset:
                params["offset"] = offset
            try:
                response = table.api.request("get", table.url, params=params)
            except requests.RequestException as exc:
                logger.error(
                    "Failed to fetch %s chunk for campaign %s: %s",
                    label,
                    campaign_id,
                    exc,
                    exc_info=True,
                )
                break
            except Exception as exc:
                logger.error(
                    "Unexpected error fetching %s chunk for campaign %s: %s",
                    label,
                    campaign_id,
                    exc,
                    exc_info=True,
                )
                break
            chunk_records = response.get("records", [])
            records.extend(chunk_records)
            offset = response.get("offset")
            if not offset:
                break
            time.sleep(0.25)
    logger.info("Fetched %s linked %s for campaign %s", len(records), label.lower(), campaign_id)
    return records


def _fetch_textgrid_number(campaign_fields: Dict[str, Any]) -> Optional[str]:
    """
    Fetch the TextGrid number directly from the Campaign record.
    This field is NOT a linked record — it’s a plain text field in Airtable.
    """
    # Try all possible variations of the field name for safety
    possible_fields = [
        "TextGrid Phone Number",
        "Textgrid Phone Number",
        "TextGrid Number",
        "Textgrid Number",
        TEXTGRID_NUMBER_FIELD,
        TEXTGRID_NUMBER_FALLBACK_FIELD,
    ]
    
    for field_name in possible_fields:
        if field_name in campaign_fields and campaign_fields[field_name]:
            value = str(campaign_fields[field_name]).strip()
            if value:
                logger.info(f"🧭 Found TextGrid number '{value}' in field '{field_name}'")
                return value
    
    logger.warning("⚠️ No valid TextGrid number found in campaign fields.")
    return None


def _resolve_template_messages(fields: Dict[str, Any], templates_handle, campaign_id: str) -> List[Tuple[str, str]]:
    template_ids = _extract_record_ids(fields.get(CAMPAIGN_FIELDS.get("TEMPLATES_LINK")))
    if not template_ids:
        return []
    records = _fetch_linked_records(templates_handle, template_ids, 100, "templates", campaign_id)
    out: List[Tuple[str, str]] = []
    for record in records:
        tf = record.get("fields", {}) or {}
        message = tf.get(TEMPLATE_MESSAGE_FIELD)
        if message and str(message).strip():
            out.append((record.get("id"), str(message).strip()))
    return out

# =====================================================================
# TIME, MARKET, PHONE HELPERS
# =====================================================================

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
    return QuietHours(
        enforced=bool(cfg.QUIET_HOURS_ENFORCED),
        start_hour=int(cfg.QUIET_START_HOUR or 21),
        end_hour=int(cfg.QUIET_END_HOUR or 9),
        tz=tz,
    )

def _apply_quiet_hours(desired: datetime, quiet: QuietHours) -> datetime:
    if not quiet.enforced:
        return desired
    local = desired.astimezone(quiet.tz)
    start = local.replace(hour=quiet.start_hour, minute=0, second=0, microsecond=0)
    end = local.replace(hour=quiet.end_hour, minute=0, second=0, microsecond=0)
    in_quiet = start <= local < end if start < end else not (end <= local < start)
    next_allowed = end if in_quiet else local
    return next_allowed.astimezone(timezone.utc)

def _prospect_phone(fields: Dict[str, Any]) -> Optional[str]:
    for key in PROSPECT_PHONE_FIELDS:
        if not key:
            continue
        val = fields.get(key)
        phones = val if isinstance(val, list) else [val]
        for ph in phones:
            normalized = normalize_phone(ph)
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

def _prospect_market(fields: Dict[str, Any]) -> Tuple[str, str]:
    raw = _coerce_market(fields.get(PROSPECT_MARKET_FIELD))
    return raw, raw.lower().strip() if raw else ""

# =====================================================================
# MAIN CAMPAIGN SCHEDULER LOGIC
# =====================================================================

def run_scheduler(limit: Optional[int] = None) -> Dict[str, Any]:
    """Queue scheduled campaigns into the Drip Queue."""
    logger.info("🚀 Starting campaign scheduler run")
    _log_scheduler_env()

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

        # ✅ Load *all* campaigns (no limit)
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

        quiet = _quiet_hours()
        for campaign in campaigns:
            fields = campaign.get("fields", {}) or {}
            if str(fields.get(CAMPAIGN_STATUS_FIELD, "")).lower() != "scheduled":
                continue

            campaign_id = campaign.get("id")
            linked_ids = _get_linked_prospects(fields)
            if not linked_ids:
                logger.info("Skipping campaign %s (no linked prospects)", campaign_id)
                continue

            # ✅ Fetch ALL linked prospects, not just first chunk
            linked_prospects = _fetch_linked_prospect_records(prospects_handle, campaign_id, linked_ids)
            if not linked_prospects:
                logger.warning("No linked prospect records fetched for campaign %s", campaign_id)
                continue

            from_number = _fetch_textgrid_number(fields)
            if not from_number:
                logger.warning("⚠️ Skipping campaign %s: no TextGrid number found", campaign_id)
                summary["campaigns"][campaign_id] = {
                    "queued": 0,
                    "skipped": 0,
                    "processed": 0,
                    "skip_reasons": {"missing_textgrid_number": len(linked_prospects)},
                }
                continue
            logger.info("🧭 Using TextGrid number %s for campaign %s", from_number, campaign_id)

            template_choices = _resolve_template_messages(fields, templates_handle, campaign_id)
            if not template_choices:
                logger.warning("⚠️ Skipping campaign %s: no linked template message found", campaign_id)
                summary["campaigns"][campaign_id] = {
                    "queued": 0,
                    "skipped": 0,
                    "processed": 0,
                    "skip_reasons": {"missing_template": len(linked_prospects)},
                }
                continue

            start_time = _apply_quiet_hours(_campaign_start(fields), quiet)
            queued = skipped = processed = 0
            skip_reasons = defaultdict(int)

            for prospect in linked_prospects:
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

                template_id, message_text = random.choice(template_choices)

                payload = {
                    DRIP_STATUS_FIELD: "QUEUED",
                    DRIP_MARKET_FIELD: _campaign_market(fields)[0],
                    DRIP_SELLER_PHONE_FIELD: phone,
                    DRIP_PROCESSOR_FIELD: SCHEDULER_PROCESSOR_LABEL,
                    DRIP_NEXT_SEND_DATE_FIELD: start_time.isoformat(),
                    DRIP_CAMPAIGN_LINK_FIELD: [campaign_id],
                    DRIP_UI_FIELD: "⏳",
                    DRIP_PROSPECT_LINK_FIELD: [prospect["id"]],
                    DRIP_MESSAGE_FIELD: message_text,
                    "Message": message_text,
                    "Message Preview": message_text,
                }
                if DRIP_FROM_NUMBER_FIELD:
                    payload[DRIP_FROM_NUMBER_FIELD] = from_number
                if PROSPECT_PROPERTY_ID_FIELD and DRIP_PROPERTY_ID_FIELD:
                    pid = pf.get(PROSPECT_PROPERTY_ID_FIELD)
                    if pid:
                        payload[DRIP_PROPERTY_ID_FIELD] = pid
                if template_id and DRIP_TEMPLATE_LINK_FIELD:
                    payload[DRIP_TEMPLATE_LINK_FIELD] = [template_id]

                if create_record(drip_handle, payload):
                    existing_pairs.add((campaign_id, digits))
                    queued += 1
                else:
                    skipped += 1
                    skip_reasons["create_failed"] += 1

            # ✅ Always mark campaign as Active once processed
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

            logger.info("✅ Queued %s messages for campaign %s", queued, campaign_id)
            logger.info("✅ Campaign %s: processed=%s queued=%s skipped=%s", campaign_id, processed, queued, skipped)

        summary["market_counts"] = dict(market_counts)
        summary["ok"] = not summary["errors"]
        if market_counts:
            logger.info("🏙 Global market prospect summary:")
            for mkt, count in market_counts.items():
                logger.info("   %s → %s prospects", mkt, count)
        logger.info("🏁 Campaign scheduler finished: %s queued", summary["queued"])
        return summary

    except Exception as exc:
        logger.exception("Scheduler fatal error: %s", exc)
        summary["ok"] = False
        summary["errors"].append(str(exc))
        return summary
