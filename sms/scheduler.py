"""Campaign scheduler that hydrates Drip Queue records from Airtable campaigns."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

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

SCHEDULER_PROCESSOR_LABEL = "Campaign Scheduler"


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


def _matches_segment(prospect_fields: Dict[str, Any], campaign_fields: Dict[str, Any]) -> bool:
    view = (campaign_fields.get(CAMPAIGN_VIEW_FIELD) or "").strip() if CAMPAIGN_VIEW_FIELD else ""
    if not view:
        return True
    prospect_lists = prospect_fields.get(PROSPECT_SOURCE_LIST_FIELD)
    if not prospect_lists:
        return False
    if isinstance(prospect_lists, list):
        values = prospect_lists
    else:
        values = [prospect_lists]
    return any(str(v).strip() == view for v in values)


def _campaign_market(fields: Dict[str, Any]) -> str:
    return str(fields.get(CAMPAIGN_MARKET_FIELD) or "").strip()


def _prospect_market(fields: Dict[str, Any]) -> str:
    return str(fields.get(PROSPECT_MARKET_FIELD) or "").strip() if PROSPECT_MARKET_FIELD else ""


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
    prospects: List[Dict[str, Any]],
    drip_handle,
    existing_pairs: Set[Tuple[str, str]],
    quiet: QuietHours,
) -> Tuple[int, int]:
    fields = campaign.get("fields", {}) or {}
    market = _campaign_market(fields)
    if not market:
        logger.info("Skipping campaign %s – missing market", campaign.get("id"))
        return 0, 0

    start_time = _apply_quiet_hours(_campaign_start(fields), quiet)
    queued = 0
    skipped = 0
    for prospect in prospects:
        pf = prospect.get("fields", {}) or {}
        if _prospect_market(pf) != market:
            continue
        if not _matches_segment(pf, fields):
            continue
        phone = _prospect_phone(pf)
        if not phone:
            skipped += 1
            continue
        digits = last_10_digits(phone)
        if not digits:
            skipped += 1
            continue
        key = (campaign["id"], digits)
        if key in existing_pairs:
            skipped += 1
            continue

        payload: Dict[str, Any] = {
            DRIP_STATUS_FIELD: "QUEUED",
            DRIP_MARKET_FIELD: market,
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
        else:
            skipped += 1

    return queued, skipped


def run_scheduler(limit: Optional[int] = None) -> Dict[str, Any]:
    """Queue scheduled campaigns into the Drip Queue."""

    logger.info("Starting campaign scheduler run")
    campaigns_handle = CONNECTOR.campaigns()
    drip_handle = CONNECTOR.drip_queue()
    prospects_handle = CONNECTOR.prospects()

    campaigns = list_records(campaigns_handle)
    prospects = list_records(prospects_handle)
    existing_drip = list_records(drip_handle)

    quiet = _quiet_hours()
    summary: Dict[str, Any] = {"queued": 0, "campaigns": {}, "errors": [], "ok": True}
    processed = 0
    pairs = _existing_pairs(existing_drip)

    for campaign in campaigns:
        if limit is not None and processed >= limit:
            break
        fields = campaign.get("fields", {}) or {}
        status = str(fields.get(CAMPAIGN_STATUS_FIELD) or "").strip().lower()
        if status != "scheduled":
            continue

        try:
            queued, skipped = _schedule_campaign(campaign, prospects, drip_handle, pairs, quiet)
            summary["campaigns"][campaign["id"]] = {"queued": queued, "skipped": skipped}
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

    logger.info("Campaign scheduler finished: %s queued entries", summary["queued"])
    return summary


__all__ = ["run_scheduler"]
