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

import requests
from dotenv import load_dotenv
from collections import defaultdict

try:  # Python 3.9+
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover - fallback for <3.9
    ZoneInfo = None  # type: ignore

from sms.airtable_schema import (
    campaign_field_map,
    drip_field_map,
    prospects_field_map,
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


def _load_env_once() -> None:
    """Load environment variables from project-level .env files, once."""
    global _ENV_LOADED
    if _ENV_LOADED:
        return

    for candidate in (ROOT_DIR / ".env", ROOT_DIR / "config" / ".env"):
        try:
            if candidate.exists():
                load_dotenv(candidate, override=False)
                ENV_SOURCES.append(str(candidate))
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("Failed loading %s: %s", candidate, exc)

    _ENV_LOADED = True


_load_env_once()


def _resolve_api_key() -> Tuple[Optional[str], Optional[str]]:
    """Return the first configured Airtable API key along with its env var name."""
    for name in API_KEY_ENV_PRIORITY:
        value = os.getenv(name)
        if value:
            return value, name
    return None, None


def _mask_token(token: Optional[str]) -> str:
    if not token:
        return "<missing>"
    trimmed = token.strip()
    if len(trimmed) <= 4:
        return "*" * len(trimmed)
    if len(trimmed) <= 8:
        return f"{trimmed[:2]}...{trimmed[-2:]}"
    return f"{trimmed[:4]}...{trimmed[-4:]}"


def _log_scheduler_env() -> None:
    """Print a one-time snapshot of key Airtable env configuration."""
    global _LOGGED_ENV
    if _LOGGED_ENV:
        return

    cfg = settings()
    token, source = _resolve_api_key()
    present_sources = [name for name in API_KEY_ENV_PRIORITY if os.getenv(name)]

    logger.info(
        "Scheduler Airtable env: campaigns_base=%s, leads_base=%s, control_base=%s, campaigns_table=%s, api_key=%s (from %s), alt_keys=%s, env_files=%s",
        cfg.CAMPAIGNS_BASE_ID or "<missing>",
        cfg.LEADS_CONVOS_BASE or "<missing>",
        cfg.CAMPAIGN_CONTROL_BASE or "<missing>",
        cfg.CAMPAIGNS_TABLE or "<missing>",
        _mask_token(token),
        source or "<none>",
        ", ".join(name for name in present_sources if name != source) or "<none>",
        ", ".join(ENV_SOURCES) or "<none>",
    )
    _LOGGED_ENV = True


def _safe_response_preview(response: requests.Response) -> str:
    try:
        body = response.text or ""
    except Exception:
        body = repr(response)
    body = body.replace("\n", " ").strip()
    return body[:500]


def _list_available_tables(api_key: str, base_id: str) -> Optional[List[str]]:
    url = f"https://api.airtable.com/v0/meta/bases/{base_id}/tables"
    headers = {"Authorization": f"Bearer {api_key}"}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
    except requests.RequestException as exc:
        logger.debug("Unable to list Airtable tables for base %s: %s", base_id, exc)
        return None
    if resp.status_code != 200:
        logger.debug(
            "Listing tables via metadata API failed for base %s (status=%s)",
            base_id,
            resp.status_code,
        )
        return None

    try:
        data = resp.json()
    except ValueError:
        return None

    tables = [
        tbl.get("name")
        for tbl in data.get("tables", [])
        if isinstance(tbl, dict) and tbl.get("name")
    ]
    return tables or None


def _get_token_scopes(api_key: str) -> Optional[List[str]]:
    url = "https://api.airtable.com/v0/meta/whoami"
    headers = {"Authorization": f"Bearer {api_key}"}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
    except requests.RequestException as exc:
        logger.debug("Unable to fetch Airtable token scopes: %s", exc)
        return None

    if resp.status_code != 200:
        logger.debug("Airtable whoami endpoint returned status %s", resp.status_code)
        return None

    try:
        data = resp.json()
    except ValueError:
        return None

    scopes = data.get("scopes")
    if isinstance(scopes, list):
        return [str(scope) for scope in scopes]
    return None


def _probe_airtable_table(api_key: str, base_id: str, table_name: str) -> Tuple[bool, Dict[str, Any]]:
    url = f"https://api.airtable.com/v0/{base_id}/{quote(table_name, safe='')}"
    headers = {"Authorization": f"Bearer {api_key}"}
    params = {"maxRecords": 1}
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
    except requests.RequestException as exc:
        message = (
            f"Unable to reach Airtable for base '{base_id}' table '{table_name}': {exc}"
        )
        return False, {"message": message, "exception": str(exc)}

    if resp.status_code == 200:
        scopes = _get_token_scopes(api_key)
        info: Dict[str, Any] = {}
        if scopes:
            info["scopes"] = scopes
        return True, info

    preview = _safe_response_preview(resp)

    if resp.status_code == 404:
        tables = _list_available_tables(api_key, base_id)
        suggestion = None
        if tables:
            suggestion = next(
                (name for name in tables if name.lower() == table_name.lower()),
                None,
            )
        message = (
            f"Airtable table '{table_name}' not found in base '{base_id}'."
            " Check for typos or case mismatches."
        )
        if suggestion and suggestion != table_name:
            message += f" Did you mean '{suggestion}'?"
        if tables:
            message += f" Available tables: {', '.join(tables)}."
        return False, {
            "message": message,
            "status": resp.status_code,
            "response": preview,
            "available_tables": tables,
            "suggested_table": suggestion,
        }

    if resp.status_code == 403:
        scopes = _get_token_scopes(api_key)
        message = (
            f"Airtable denied access to base '{base_id}'."
            " Confirm the API key has been granted that base with read/write permissions"
            " and includes the data.records:read/write scopes."
        )
        info = {
            "message": message,
            "status": resp.status_code,
            "response": preview,
        }
        if scopes:
            info["scopes"] = scopes
        return False, info

    message = (
        f"Airtable responded with unexpected status {resp.status_code} for"
        f" base '{base_id}' table '{table_name}'."
    )
    return False, {
        "message": message,
        "status": resp.status_code,
        "response": preview,
    }


def _validate_airtable_access() -> Optional[Dict[str, Any]]:
    cfg = settings()
    base_id = cfg.CAMPAIGNS_BASE_ID or cfg.LEADS_CONVOS_BASE
    table_name = cfg.CAMPAIGNS_TABLE
    token, source = _resolve_api_key()
    details: Dict[str, Any] = {
        "base_id": base_id,
        "campaigns_base": cfg.CAMPAIGNS_BASE_ID,
        "leads_base": cfg.LEADS_CONVOS_BASE,
        "control_base": cfg.CAMPAIGN_CONTROL_BASE,
        "campaigns_table": table_name,
        "env_files": ENV_SOURCES or [],
        "available_key_envs": [
            name for name in API_KEY_ENV_PRIORITY if os.getenv(name)
        ],
    }
    if source:
        details["api_key_source"] = source

    if not base_id:
        message = "CAMPAIGNS_BASE_ID (or LEADS_CONVOS_BASE) is not configured."
        logger.error(message)
        return {"ok": False, "error": message, "details": details}
    if not BASE_ID_PATTERN.match(base_id):
        message = f"Campaigns base '{base_id}' does not look like a valid Airtable base id."
        logger.error(message)
        return {"ok": False, "error": message, "details": details}
    if not table_name:
        message = "CAMPAIGNS_TABLE is not configured."
        logger.error(message)
        return {"ok": False, "error": message, "details": details}
    if not token:
        message = (
            "No Airtable API key found (checked AIRTABLE_API_KEY, AIRTABLE_ACQUISITIONS_KEY,"
            " AIRTABLE_COMPLIANCE_KEY, AIRTABLE_REPORTING_KEY)."
        )
        logger.error(message)
        return {"ok": False, "error": message, "details": details}

    ok, info = _probe_airtable_table(token, base_id, table_name)
    if not ok:
        message = info.get("message") or "Unable to verify Airtable access."
        logger.error(message)
        info.pop("message", None)
        details.update(info)
        return {"ok": False, "error": message, "details": details}

    scopes = info.get("scopes") if isinstance(info, dict) else None
    if scopes:
        details["scopes"] = scopes
        missing = [
            scope
            for scope in ("data.records:read", "data.records:write")
            if scope not in scopes
        ]
        if missing:
            message = (
                "Airtable API key is missing required scopes: "
                + ", ".join(missing)
                + "."
            )
            logger.error(message)
            details["missing_scopes"] = missing
            return {"ok": False, "error": message, "details": details}

    return None

CAMPAIGN_FIELDS = campaign_field_map()
DRIP_FIELDS = drip_field_map()
PROSPECT_FIELDS = prospects_field_map()

CAMPAIGN_STATUS_FIELD = CAMPAIGN_FIELDS["STATUS"]
CAMPAIGN_MARKET_FIELD = CAMPAIGN_FIELDS["MARKET"]
CAMPAIGN_VIEW_FIELD = CAMPAIGN_FIELDS.get("VIEW_SEGMENT")
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

PROSPECT_MARKET_FIELD = PROSPECT_FIELDS.get("MARKET")
PROSPECT_SOURCE_LIST_FIELD = PROSPECT_FIELDS.get("SOURCE_LIST")
PROSPECT_PROPERTY_ID_FIELD = PROSPECT_FIELDS.get("PROPERTY_ID")
PROSPECT_PHONE_FIELDS = [
    PROSPECT_FIELDS.get("PHONE_PRIMARY"),
    PROSPECT_FIELDS.get("PHONE_PRIMARY_LINKED"),
    PROSPECT_FIELDS.get("PHONE_SECONDARY"),
    PROSPECT_FIELDS.get("PHONE_SECONDARY_LINKED"),
]
if PROSPECT_MARKET_FIELD is None:
    PROSPECT_MARKET_FIELD = "Market"

SCHEDULER_PROCESSOR_LABEL = "Campaign Scheduler"


def _get_linked_prospects(fields: Dict[str, Any]) -> List[str]:
    linked_field = CAMPAIGN_FIELDS.get("PROSPECTS_LINK")
    linked = fields.get(linked_field)
    if isinstance(linked, list):
        return [str(item) for item in linked if item]
    if isinstance(linked, str) and linked.strip():
        return [linked.strip()]
    return []


def _escape_formula_value(value: str) -> str:
    return value.replace("'", "\\'")


def _fetch_linked_prospect_records(prospects_handle, campaign_id: str, prospect_ids: List[str]) -> List[Dict[str, Any]]:
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
            except requests.RequestException as exc:
                logger.error(
                    "Failed to fetch prospects chunk for campaign %s: %s",
                    campaign_id,
                    exc,
                    exc_info=True,
                )
                break
            except Exception as exc:
                logger.error(
                    "Unexpected error fetching prospects chunk for campaign %s: %s",
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
    logger.info("Fetched %s linked prospects for campaign %s", len(records), campaign_id)
    if records:
        sample = [
            (
                rec.get("id"),
                _prospect_market(rec.get("fields", {}) or {})[0],
                type((rec.get("fields", {}) or {}).get(PROSPECT_MARKET_FIELD)).__name__,
            )
            for rec in records[:5]
        ]
        logger.debug("Sample fetched prospects for %s: %s", campaign_id, sample)
    return records


def _parse_datetime(value: Any) -> Optional[datetime]:
    if not value:
        return None
    text = str(value)
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)
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
    enforced = bool(cfg.QUIET_HOURS_ENFORCED)
    start_hour = int(cfg.QUIET_START_HOUR or 21)
    end_hour = int(cfg.QUIET_END_HOUR or 9)
    tz_name = cfg.QUIET_TZ or "America/Chicago"
    tz: timezone
    if ZoneInfo is not None:
        try:
            tz = ZoneInfo(tz_name)
        except Exception:
            tz = timezone.utc
    else:  # pragma: no cover - legacy Python
        tz = timezone.utc
    return QuietHours(enforced=enforced, start_hour=start_hour, end_hour=end_hour, tz=tz)


def _apply_quiet_hours(desired: datetime, quiet: QuietHours) -> datetime:
    if not quiet.enforced:
        return desired

    desired_utc = desired.astimezone(timezone.utc)
    local_now = desired_utc.astimezone(quiet.tz)

    start = local_now.replace(hour=quiet.start_hour, minute=0, second=0, microsecond=0)
    end = local_now.replace(hour=quiet.end_hour, minute=0, second=0, microsecond=0)

    if start <= end:
        in_quiet = start <= local_now < end
        next_allowed = end if in_quiet else local_now
    else:
        in_quiet = not (end <= local_now < start)
        if not in_quiet:
            next_allowed = local_now
        elif local_now < end:
            next_allowed = end
        else:
            next_allowed = end + timedelta(days=1)

    return next_allowed.astimezone(timezone.utc)


def _prospect_phone(fields: Dict[str, Any]) -> Optional[str]:
    for key in PROSPECT_PHONE_FIELDS:
        if not key:
            continue
        value = fields.get(key)
        if isinstance(value, list):
            options = value
        else:
            options = [value]
        for candidate in options:
            phone = normalize_phone(candidate)
            if phone:
                return phone
    return None


market_counts = defaultdict(int)


def _coerce_market(value: Any) -> str:
    """Normalize market values from Airtable (single select, linked, or text)."""
    if not value:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (list, tuple)) and value:
        first = value[0]
        if isinstance(first, str):
            return first.strip()
        if isinstance(first, dict):
            for key in ("name", "Name", "label", "Label", "value", "Value", "Market", "market"):
                v = first.get(key)
                if isinstance(v, str) and v.strip():
                    return v.strip()
            return ""
    if isinstance(value, dict):
        for key in ("name", "Name", "label", "Label", "value", "Value", "Market", "market"):
            v = value.get(key)
            if isinstance(v, str) and v.strip():
                return v.strip()
        return ""
    return str(value or "").strip()


def _campaign_market(fields: Dict[str, Any]) -> Tuple[str, str]:
    raw = _coerce_market(fields.get(CAMPAIGN_MARKET_FIELD))
    return raw, raw.lower().strip() if raw else ""


def _prospect_market(fields: Dict[str, Any]) -> Tuple[str, str]:
    raw = _coerce_market(fields.get(PROSPECT_MARKET_FIELD)) if PROSPECT_MARKET_FIELD else ""
    return raw, raw.lower().strip() if raw else ""


def _existing_pairs(drip_records: List[Dict[str, Any]]) -> Set[Tuple[str, str]]:
    pairs: Set[Tuple[str, str]] = set()
    for record in drip_records:
        fields = record.get("fields", {}) or {}
        campaign_links = fields.get(DRIP_CAMPAIGN_LINK_FIELD)
        if isinstance(campaign_links, list):
            campaign_id = campaign_links[0] if campaign_links else None
        else:
            campaign_id = campaign_links
        phone = fields.get(DRIP_SELLER_PHONE_FIELD)
        digits = last_10_digits(phone)
        if campaign_id and digits:
            pairs.add((str(campaign_id), digits))
    return pairs


def _schedule_campaign(
    campaign: Dict[str, Any],
    prospects_handle,
    drip_handle,
    existing_pairs: Set[Tuple[str, str]],
    quiet: QuietHours,
) -> Tuple[int, int, int, Dict[str, int]]:
    fields = campaign.get("fields", {}) or {}
    campaign_id = campaign.get("id")
    campaign_market_raw, campaign_market_norm = _campaign_market(fields)

    if not campaign_market_raw:
        logger.info("Skipping campaign %s – missing market", campaign_id)
        return 0, 0, 0, {}

    linked_ids = _get_linked_prospects(fields)
    if not linked_ids:
        logger.info("Skipping campaign %s – no linked prospects", campaign_id)
        return 0, 0, 0, {}

    linked_prospects = _fetch_linked_prospect_records(prospects_handle, campaign_id, linked_ids)
    logger.info("📎 Campaign %s using %s linked prospects", campaign_id, len(linked_prospects))
    if not linked_prospects:
        logger.warning("No linked prospect records fetched for campaign %s", campaign_id)
        return 0, 0, 0, {}

    start_time = _apply_quiet_hours(_campaign_start(fields), quiet)
    queued = 0
    skipped = 0
    processed = 0
    skip_reasons: Dict[str, int] = defaultdict(int)

    logger.info("🏁 Starting campaign %s for market '%s'", campaign_id, campaign_market_raw)

    local_market_counts = defaultdict(int)

    for prospect in linked_prospects:
        processed += 1
        pf = prospect.get("fields", {}) or {}
        prospect_id = prospect.get("id")
        prospect_market_raw, prospect_market_norm = _prospect_market(pf)

        if prospect_market_norm:
            market_counts[prospect_market_norm] += 1
            local_market_counts[prospect_market_norm] += 1

        logger.debug(
            "Market check: campaign=%s (%s) prospect=%s (%s)",
            campaign_market_norm,
            campaign_market_raw,
            prospect_market_norm,
            prospect_market_raw,
        )

        if campaign_market_norm and prospect_market_norm and campaign_market_norm != prospect_market_norm:
            logger.debug(
                "Market mismatch (ignored) campaign=%s prospect=%s",
                campaign_market_norm,
                prospect_market_norm,
            )
        elif campaign_market_norm and not prospect_market_norm:
            logger.debug(
                "Prospect %s missing market (ignored) for campaign %s",
                prospect_id,
                campaign_id,
            )

        phone = _prospect_phone(pf)
        if not phone:
            skip_reasons["missing_phone"] += 1
            skipped += 1
            continue
        digits = last_10_digits(phone)
        if not digits:
            skip_reasons["invalid_phone"] += 1
            skipped += 1
            continue

        key = (campaign["id"], digits)
        if key in existing_pairs:
            skip_reasons["duplicate_phone"] += 1
            skipped += 1
            continue

        payload: Dict[str, Any] = {
            DRIP_STATUS_FIELD: "QUEUED",
            DRIP_MARKET_FIELD: campaign_market_raw,
            DRIP_SELLER_PHONE_FIELD: phone,
            DRIP_PROCESSOR_FIELD: SCHEDULER_PROCESSOR_LABEL,
            DRIP_NEXT_SEND_DATE_FIELD: start_time.isoformat(),
            DRIP_CAMPAIGN_LINK_FIELD: [campaign["id"]],
            DRIP_UI_FIELD: "⏳",
        }

        if PROSPECT_PROPERTY_ID_FIELD and pf.get(PROSPECT_PROPERTY_ID_FIELD) and DRIP_PROPERTY_ID_FIELD:
            payload[DRIP_PROPERTY_ID_FIELD] = pf.get(PROSPECT_PROPERTY_ID_FIELD)
        if DRIP_PROSPECT_LINK_FIELD:
            payload[DRIP_PROSPECT_LINK_FIELD] = [prospect["id"]]

        created = create_record(drip_handle, payload)
        if created:
            existing_pairs.add(key)
            queued += 1
            logger.debug("✅ Queued prospect %s for campaign %s (%s)", prospect_id, campaign_id, campaign_market_raw)
        else:
            skip_reasons["create_failed"] += 1
            skipped += 1

    logger.info("🏙 Market prospect summary for campaign %s:", campaign_id)
    for mkt, count in local_market_counts.items():
        logger.info("   %s → %s prospects", mkt, count)

    logger.info(
        "✅ Finished campaign %s: processed=%s queued=%s skipped=%s reasons=%s",
        campaign_id,
        processed,
        queued,
        skipped,
        dict(skip_reasons),
    )
    return queued, skipped, processed, dict(skip_reasons)


def _list_with_retry(handle, label: str, attempts: int = 3, delay: float = 2.0) -> Tuple[List[Dict[str, Any]], bool]:
    last_error: Optional[Dict[str, Any]] = None
    for attempt in range(attempts):
        rows = list_records(handle, max_records=100, page_size=100)
        if rows or not handle.last_error:
            return rows, False
        last_error = handle.last_error
        logger.warning(
            "Airtable fetch issue for %s [%s/%s] attempt %s/%s: %s",
            label,
            handle.base_id or "memory",
            handle.table_name,
            attempt + 1,
            attempts,
            last_error,
        )
        time.sleep(delay)
    logger.error(
        "⚠️ %s fetch failed after %s attempts [%s/%s]: %s",
        label,
        attempts,
        handle.base_id or "memory",
        handle.table_name,
        last_error,
    )
    return [], True


def run_scheduler(limit: Optional[int] = None) -> Dict[str, Any]:
    """Queue scheduled campaigns into the Drip Queue."""

    logger.info("Starting campaign scheduler run")
    _log_scheduler_env()

    summary: Dict[str, Any] = {"queued": 0, "campaigns": {}, "errors": [], "ok": True}

    market_counts.clear()

    if TEST_MODE:
        logger.info("TEST_MODE enabled – returning mock scheduler result")
        summary["note"] = "test mode"
        return summary

    if not TEST_MODE:
        validation_error = _validate_airtable_access()
        if validation_error:
            return validation_error

    try:
        campaigns_handle = CONNECTOR.campaigns()
        drip_handle = CONNECTOR.drip_queue()
        prospects_handle = CONNECTOR.prospects()

        logger.info(
            "Scheduler using campaigns base=%s table=%s",
            campaigns_handle.base_id,
            campaigns_handle.table_name,
        )

        if not TEST_MODE:
            for handle, label in (
                (campaigns_handle, "Campaigns"),
                (prospects_handle, "Prospects"),
                (drip_handle, "Drip Queue"),
            ):
                if handle.in_memory:
                    logger.error(
                        "%s handle using in-memory fallback (base=%s). Check Airtable credentials and permissions.",
                        label,
                        handle.base_id,
                    )
                    return {
                        "ok": False,
                        "queued": 0,
                        "campaigns": {},
                        "errors": [f"{label} table unavailable"],
                    }

        campaigns, campaigns_failed = _list_with_retry(campaigns_handle, "Campaigns")
        if campaigns_failed:
            summary["errors"].append("Failed to load campaigns after retries")
            summary["ok"] = False
            return summary

        existing_drip, drip_failed = _list_with_retry(drip_handle, "Drip Queue")
        pairs = _existing_pairs(existing_drip) if not drip_failed else None

        quiet = _quiet_hours()
        processed = 0
        sample_campaign_markets = [
            (
                _campaign_market(c.get("fields", {}) or {})[0],
                type((c.get("fields", {}) or {}).get(CAMPAIGN_MARKET_FIELD)).__name__,
            )
            for c in campaigns[:5]
        ]
        logger.debug("Sample campaign markets (raw, type): %s", sample_campaign_markets)

        for campaign in campaigns:
            if limit is not None and processed >= limit:
                break
            fields = campaign.get("fields", {}) or {}
            status = str(fields.get(CAMPAIGN_STATUS_FIELD) or "").strip().lower()
            if status != "scheduled":
                logger.debug(
                    "Skipping campaign %s due to status '%s'",
                    campaign.get("id"),
                    status,
                )
                continue

            try:
                if pairs is None:
                    logger.warning(
                        "⚠️ Skipped Drip Queue batch due to repeated connection resets: %s",
                        campaign.get("id"),
                    )
                    summary["campaigns"][campaign["id"]] = {"queued": 0, "skipped": 0, "error": "Drip Queue unavailable"}
                    summary["errors"].append(f"Drip Queue unavailable for {campaign.get('id')}")
                    summary["ok"] = False
                    continue

                queued, skipped, processed_count, skip_summary = _schedule_campaign(
                    campaign, prospects_handle, drip_handle, pairs, quiet
                )
                summary["campaigns"][campaign["id"]] = {
                    "queued": queued,
                    "skipped": skipped,
                    "processed": processed_count,
                    "skip_reasons": skip_summary,
                }
                summary["queued"] += queued
                processed += 1
                if queued > 0:
                    patch = {CAMPAIGN_STATUS_FIELD: "Active"}
                    if CAMPAIGN_LAST_RUN_FIELD:
                        patch[CAMPAIGN_LAST_RUN_FIELD] = iso_now()
                    update_record(campaigns_handle, campaign["id"], patch)
            except Exception as exc:  # pragma: no cover - defensive path
                logger.exception("Campaign scheduling failed for %s", campaign.get("id"))
                summary["errors"].append({"campaign": campaign.get("id"), "error": str(exc)})
                summary["ok"] = False
    except Exception as exc:
        logger.exception("Scheduler fatal error")
        summary["errors"].append(str(exc))
        summary["ok"] = False
        return summary

    logger.info("🏙 Market prospect summary (global):")
    for mkt, count in market_counts.items():
        logger.info("   %s → %s prospects", mkt, count)

    summary["ok"] = not summary["errors"]
    logger.info("Campaign scheduler finished: %s queued entries", summary["queued"])
    return summary


__all__ = ["run_scheduler"]
