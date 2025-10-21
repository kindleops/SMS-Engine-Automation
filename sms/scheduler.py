"""Campaign scheduler that hydrates Drip Queue records from Airtable campaigns."""

from __future__ import annotations
import os
import re
import time
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

from sms.airtable_schema import campaign_field_map, drip_field_map, prospects_field_map
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

# =====================================================================
# LINKED PROSPECTS FETCHING (FIXED + PAGINATED)
# =====================================================================

def _get_linked_prospects(fields: Dict[str, Any]) -> List[str]:
    linked_field = CAMPAIGN_FIELDS.get("PROSPECTS_LINK")
    linked = fields.get(linked_field)
    if isinstance(linked, list):
        return [str(i) for i in linked if i]
    return [linked.strip()] if isinstance(linked, str) and linked.strip() else []

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
            params = {"pageSize": 100, "formula": formula}  # ✅ modern param
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

        # ✅ Load *all* campaigns (no limit)
        campaigns = list_records(campaigns_handle, page_size=100)
        existing_drip = list_records(drip_handle, page_size=100)
        existing_pairs = {
            (str(f["fields"].get(DRIP_CAMPAIGN_LINK_FIELD, [None])[0]), last_10_digits(f["fields"].get(DRIP_SELLER_PHONE_FIELD)))
            for f in existing_drip if f.get("fields")
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
            linked_prospects = []
            for i in range(0, len(linked_ids), 100):
                subset = linked_ids[i : i + 100]
                batch = _fetch_linked_prospect_records(prospects_handle, campaign_id, subset)
                linked_prospects.extend(batch)
                time.sleep(0.2)

            if not linked_prospects:
                logger.warning("No linked prospect records fetched for campaign %s", campaign_id)
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
                    skip_reasons["duplicate"] += 1
                    skipped += 1
                    continue

                payload = {
                    DRIP_STATUS_FIELD: "QUEUED",
                    DRIP_MARKET_FIELD: _campaign_market(fields)[0],
                    DRIP_SELLER_PHONE_FIELD: phone,
                    DRIP_PROCESSOR_FIELD: SCHEDULER_PROCESSOR_LABEL,
                    DRIP_NEXT_SEND_DATE_FIELD: start_time.isoformat(),
                    DRIP_CAMPAIGN_LINK_FIELD: [campaign_id],
                    DRIP_UI_FIELD: "⏳",
                    DRIP_PROSPECT_LINK_FIELD: [prospect["id"]],
                }
                if PROSPECT_PROPERTY_ID_FIELD and DRIP_PROPERTY_ID_FIELD:
                    pid = pf.get(PROSPECT_PROPERTY_ID_FIELD)
                    if pid:
                        payload[DRIP_PROPERTY_ID_FIELD] = pid

                if create_record(drip_handle, payload):
                    existing_pairs.add((campaign_id, digits))
                    queued += 1
                else:
                    skipped += 1
                    skip_reasons["create_failed"] += 1

            # ✅ Always mark campaign as Active once processed
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
            }

            logger.info("✅ Campaign %s: processed=%s queued=%s skipped=%s", campaign_id, processed, queued, skipped)

        summary["ok"] = not summary["errors"]
        logger.info("🏁 Campaign scheduler finished: %s queued", summary["queued"])
        return summary

    except Exception as exc:
        logger.exception("Scheduler fatal error: %s", exc)
        summary["ok"] = False
        summary["errors"].append(str(exc))
        return summary
