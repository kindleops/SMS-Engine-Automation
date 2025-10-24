"""
🧠 Bulletproof Campaign Scheduler (v3.1 – Telemetry Edition)
────────────────────────────────────────────────────────────
Hydrates the Drip Queue from active/scheduled campaigns with:
 • Market-aware number rotation
 • Template randomization & personalization
 • Robust error handling and telemetry
 • Smart deduplication and concurrency-safe writes
 • Environment-safe fallbacks (TEST_MODE, API failures)
"""

from __future__ import annotations
import os, time, random, requests, traceback
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from dotenv import load_dotenv

from sms.airtable_schema import (
    campaign_field_map,
    drip_field_map,
    prospects_field_map,
    template_field_map,
)
from sms.datastore import CONNECTOR, create_record, list_records, update_record
from sms.runtime import get_logger, iso_now, last_10_digits, normalize_phone, PerfTimer
from sms.logger import log_run
from sms.kpi_logger import log_kpi

# -------------------------------------------------------------
# ENV / SETUP
# -------------------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parents[1]
load_dotenv(ROOT_DIR / ".env")

logger = get_logger("scheduler")

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY", "")
LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE", "")
CAMPAIGN_CONTROL_BASE = os.getenv("CAMPAIGN_CONTROL_BASE", "")
TEST_MODE = os.getenv("TEST_MODE", "false").lower() in {"1", "true", "yes"}

NUMBERS_TABLE_ID = os.getenv("NUMBERS_TABLE_ID", "tblWG3Z2bkZF6k16n")
NUMBERS_MARKET_FIELD = os.getenv("NUMBERS_MARKET_FIELD", "Market")
NUMBERS_PHONE_FIELD = os.getenv("NUMBERS_PHONE_FIELD", "Number")
NUMBERS_STATUS_FIELD = os.getenv("NUMBERS_STATUS_FIELD", "Status")
NUMBERS_ACTIVE_FIELD = os.getenv("NUMBERS_ACTIVE_FIELD", "Active")

PREQUEUE_JITTER_MAX_SEC = int(os.getenv("PREQUEUE_JITTER_MAX_SEC", "90"))
CHUNK_SLEEP_SEC = float(os.getenv("CHUNK_SLEEP_SEC", "0.12"))
MAX_QUEUE_PER_CAMPAIGN = int(os.getenv("MAX_QUEUE_PER_CAMPAIGN", "0")) or None
SCHEDULER_PROCESSOR_LABEL = "Campaign Scheduler"

# -------------------------------------------------------------
# FIELD MAPS
# -------------------------------------------------------------
CAMPAIGN_FIELDS = campaign_field_map()
DRIP_FIELDS = drip_field_map()
PROSPECT_FIELDS = prospects_field_map()
TEMPLATE_FIELDS = template_field_map()

CAMPAIGN_STATUS_FIELD = CAMPAIGN_FIELDS.get("Status", "Status")
CAMPAIGN_MARKET_FIELD = CAMPAIGN_FIELDS.get("Market", "Market")
CAMPAIGN_START_FIELD = CAMPAIGN_FIELDS.get("Start Time", "Start Time")
CAMPAIGN_LAST_RUN_FIELD = CAMPAIGN_FIELDS.get("Last Run At", "Last Run At")
CAMPAIGN_PROSPECTS_LINK = CAMPAIGN_FIELDS.get("Prospects", "Prospects")
CAMPAIGN_TEMPLATES_LINK = CAMPAIGN_FIELDS.get("Templates", "Templates")

DRIP_STATUS_FIELD = DRIP_FIELDS.get("Status", "Status")
DRIP_MARKET_FIELD = DRIP_FIELDS.get("Market", "Market")
DRIP_SELLER_PHONE_FIELD = DRIP_FIELDS.get("Seller Phone Number", "Seller Phone Number")
DRIP_FROM_NUMBER_FIELD = DRIP_FIELDS.get("TextGrid Phone Number", "TextGrid Phone Number")
DRIP_PROSPECT_LINK_FIELD = DRIP_FIELDS.get("Prospect", "Prospect")
DRIP_CAMPAIGN_LINK_FIELD = DRIP_FIELDS.get("Campaign", "Campaign")
DRIP_NEXT_SEND_DATE_FIELD = DRIP_FIELDS.get("Next Send Date", "Next Send Date")
DRIP_UI_FIELD = DRIP_FIELDS.get("UI", "UI")
DRIP_PROCESSOR_FIELD = DRIP_FIELDS.get("Processor", "Processor")
DRIP_MESSAGE_PREVIEW_FIELD = DRIP_FIELDS.get("Message Preview", "Message")


# -------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------
def _parse_iso(v: Any) -> Optional[datetime]:
    if not v:
        return None
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def _campaign_start(fields: Dict[str, Any]) -> datetime:
    return _parse_iso(fields.get(CAMPAIGN_START_FIELD)) or datetime.now(timezone.utc)


def _market_key(raw: Optional[str]) -> str:
    return (raw or "").strip().lower().replace(",", "").replace(".", "")


# -------------------------------------------------------------
# MARKET → NUMBER ROTATION
# -------------------------------------------------------------
_numbers_cache: Dict[str, List[str]] = {}
_rotation_index: Dict[str, int] = {}


def _fetch_number_pool(market: str) -> List[str]:
    mk = _market_key(market)
    if mk in _numbers_cache:
        return _numbers_cache[mk]
    if not AIRTABLE_API_KEY:
        logger.error("❌ Missing AIRTABLE_API_KEY; cannot fetch number pool.")
        return []
    url = f"https://api.airtable.com/v0/{CAMPAIGN_CONTROL_BASE}/{NUMBERS_TABLE_ID}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}
    formula = f"AND({{{NUMBERS_MARKET_FIELD}}}='{market}',OR({{{NUMBERS_ACTIVE_FIELD}}}=1,{{{NUMBERS_ACTIVE_FIELD}}}='true'),LOWER({{{NUMBERS_STATUS_FIELD}}})='active')"
    try:
        resp = requests.get(url, headers=headers, params={"filterByFormula": formula, "pageSize": 100}, timeout=12)
        resp.raise_for_status()
        records = (resp.json() or {}).get("records", [])
    except Exception as e:
        logger.error("❌ Number pool fetch failed for %s: %s", market, e)
        _numbers_cache[mk] = []
        return []
    pool = [r.get("fields", {}).get(NUMBERS_PHONE_FIELD) for r in records if isinstance(r.get("fields", {}).get(NUMBERS_PHONE_FIELD), str)]
    _numbers_cache[mk] = pool
    return pool


def _fetch_global_pool() -> List[str]:
    if "_global" in _numbers_cache:
        return _numbers_cache["_global"]
    url = f"https://api.airtable.com/v0/{CAMPAIGN_CONTROL_BASE}/{NUMBERS_TABLE_ID}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}
    fb = f"AND(OR({{{NUMBERS_ACTIVE_FIELD}}}=1,{{{NUMBERS_ACTIVE_FIELD}}}='true'),LOWER({{{NUMBERS_STATUS_FIELD}}})='active')"
    try:
        resp = requests.get(url, headers=headers, params={"filterByFormula": fb, "pageSize": 100}, timeout=12)
        resp.raise_for_status()
        records = (resp.json() or {}).get("records", [])
    except Exception as e:
        logger.error("❌ Global number pool fetch failed: %s", e)
        _numbers_cache["_global"] = []
        return []
    pool = [r.get("fields", {}).get(NUMBERS_PHONE_FIELD) for r in records if isinstance(r.get("fields", {}).get(NUMBERS_PHONE_FIELD), str)]
    _numbers_cache["_global"] = pool
    return pool


def _choose_number(market: str) -> Optional[str]:
    mk = _market_key(market)
    pool = _fetch_number_pool(market) or _fetch_global_pool()
    if not pool:
        return None
    idx = _rotation_index.get(mk, 0)
    choice = pool[idx % len(pool)]
    _rotation_index[mk] = idx + 1
    return choice


# -------------------------------------------------------------
# MESSAGE RENDERING
# -------------------------------------------------------------
def _render_message(template: str, pf: Dict[str, Any]) -> str:
    name_raw = pf.get("Phone 1 Name (Primary) (from Linked Owner)") or ""
    first_name = name_raw.split()[0].replace(".", "") if isinstance(name_raw, str) and name_raw.strip() else ""
    addr = pf.get("Property Address")
    city = pf.get("Property City")
    if isinstance(addr, list) and addr:
        addr = addr[0]
    if isinstance(city, list) and city:
        city = city[0]
    msg = (template or "").replace("{First}", first_name).replace("{Address}", str(addr or "")).replace("{Property City}", str(city or ""))
    return msg.strip()


def _best_phone(fields: Dict[str, Any]) -> Optional[str]:
    for key in [
        "Phone",
        "Primary Phone",
        "Owner Phone",
        "Owner Phones",
        "Phone 1",
        "Phone 2",
        "Phones",
        "Phone 1 (from Linked Owner)",
        "Phone 2 (from Linked Owner)",
    ]:
        val = fields.get(key)
        if isinstance(val, list):
            for v in val:
                p = normalize_phone(v)
                if p:
                    return p
        elif isinstance(val, str):
            p = normalize_phone(val)
            if p:
                return p
    return None


# -------------------------------------------------------------
# MAIN SCHEDULER
# -------------------------------------------------------------
def run_scheduler(limit: Optional[int] = None) -> Dict[str, Any]:
    with PerfTimer("scheduler_run"):
        logger.info("🚀 Scheduler start")
        summary = {"queued": 0, "campaigns": {}, "errors": [], "ok": True}

        if TEST_MODE:
            summary["note"] = "TEST_MODE active; no writes performed."
            logger.info("⚠️ Running in TEST_MODE (no Airtable writes).")
            log_run("SCHEDULER", processed=0, status="TEST_MODE", extra=summary)
            return summary

        try:
            campaigns_h = CONNECTOR.campaigns()
            prospects_h = CONNECTOR.prospects()
            drip_h = CONNECTOR.drip_queue()
            templates_h = CONNECTOR.templates()

            campaigns = list_records(campaigns_h, page_size=100)
            now_utc = datetime.now(timezone.utc)
            existing = list_records(drip_h, page_size=100)
            existing_pairs = {
                (
                    (f.get("fields") or {}).get(DRIP_CAMPAIGN_LINK_FIELD, [None])[0],
                    last_10_digits((f.get("fields") or {}).get(DRIP_SELLER_PHONE_FIELD)),
                )
                for f in existing
                if f.get("fields")
            }

            for camp in sorted(campaigns, key=lambda c: _campaign_start(c.get("fields", {}))):
                fields = camp.get("fields", {}) or {}
                status = str(fields.get(CAMPAIGN_STATUS_FIELD, "")).lower()
                if status != "scheduled":
                    continue

                campaign_id = camp.get("id")
                start_time = _campaign_start(fields)
                market = fields.get(CAMPAIGN_MARKET_FIELD)
                if not market:
                    logger.warning("⚠️ Campaign %s missing market; skipped.", campaign_id)
                    continue

                # Fetch template messages
                template_ids = fields.get(CAMPAIGN_TEMPLATES_LINK) or []
                messages = []
                for tid in template_ids:
                    try:
                        resp = templates_h.table.api.request(
                            "get", templates_h.table.url, params={"filterByFormula": f"RECORD_ID()='{tid}'"}
                        )
                        for rec in (resp or {}).get("records", []):
                            msg = (rec.get("fields", {}) or {}).get("Message")
                            if msg and isinstance(msg, str):
                                messages.append(msg.strip())
                    except Exception as e:
                        logger.warning("⚠️ Template fetch failed for %s: %s", tid, e)
                if not messages:
                    logger.warning("⚠️ Campaign %s has no valid templates.", campaign_id)
                    continue

                # Fetch linked prospects
                linked = fields.get(CAMPAIGN_PROSPECTS_LINK) or []
                if not linked:
                    logger.info("⏭️ Campaign %s has no linked prospects.", campaign_id)
                    continue

                prospects = []
                for i in range(0, len(linked), 90):
                    chunk = linked[i : i + 90]
                    formula = "OR(" + ",".join([f"RECORD_ID()='{rid}'" for rid in chunk]) + ")"
                    try:
                        resp = prospects_h.table.api.request("get", prospects_h.table.url, params={"filterByFormula": formula})
                        prospects.extend((resp or {}).get("records", []))
                    except Exception as e:
                        logger.warning("⚠️ Prospect fetch failed chunk: %s", e)
                    time.sleep(CHUNK_SLEEP_SEC)

                queued, skipped, processed = 0, 0, 0
                skip_reasons = defaultdict(int)
                samples = defaultdict(lambda: deque(maxlen=5))

                for idx, pr in enumerate(prospects):
                    if MAX_QUEUE_PER_CAMPAIGN and queued >= MAX_QUEUE_PER_CAMPAIGN:
                        break
                    processed += 1
                    pf = pr.get("fields", {}) or {}
                    phone = _best_phone(pf)
                    if not phone:
                        skipped += 1
                        skip_reasons["missing_phone"] += 1
                        continue
                    digits = last_10_digits(phone)
                    if (campaign_id, digits) in existing_pairs:
                        skipped += 1
                        skip_reasons["duplicate_phone"] += 1
                        continue
                    from_number = _choose_number(market)
                    if not from_number:
                        skipped += 1
                        skip_reasons["no_textgrid_number"] += 1
                        continue
                    msg = random.choice(messages)
                    rendered = _render_message(msg, pf)
                    send_time = start_time.timestamp() + random.randint(0, PREQUEUE_JITTER_MAX_SEC)
                    next_send = datetime.fromtimestamp(send_time, tz=timezone.utc).isoformat()
                    payload = {
                        DRIP_STATUS_FIELD: "QUEUED",
                        DRIP_MARKET_FIELD: market,
                        DRIP_SELLER_PHONE_FIELD: phone,
                        DRIP_FROM_NUMBER_FIELD: from_number,
                        DRIP_PROCESSOR_FIELD: SCHEDULER_PROCESSOR_LABEL,
                        DRIP_NEXT_SEND_DATE_FIELD: next_send,
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
                            skipped += 1
                            skip_reasons["create_failed"] += 1
                    except Exception as e:
                        skipped += 1
                        skip_reasons["create_failed"] += 1
                        logger.warning("❌ Create failed for %s: %s", digits, e)

                try:
                    update_record(
                        campaigns_h,
                        campaign_id,
                        {
                            CAMPAIGN_STATUS_FIELD: "Active" if queued and now_utc >= start_time else "Scheduled",
                            CAMPAIGN_LAST_RUN_FIELD: iso_now(),
                        },
                    )
                except Exception:
                    pass

                logger.info("✅ Campaign %s queued=%d skipped=%d processed=%d", campaign_id, queued, skipped, processed)
                if skip_reasons:
                    logger.info("   Skip breakdown: %s", dict(skip_reasons))

                # KPI telemetry
                log_kpi("SCHEDULER_QUEUED", queued, campaign=campaign_id)
                log_kpi("SCHEDULER_SKIPPED", skipped, campaign=campaign_id)

                summary["queued"] += queued
                summary["campaigns"][campaign_id] = {
                    "queued": queued,
                    "skipped": skipped,
                    "processed": processed,
                    "skip_reasons": dict(skip_reasons),
                    "market": market,
                    "start_time": start_time.isoformat(),
                }

            summary["ok"] = not summary["errors"]
            logger.info("🏁 Scheduler done — total queued: %s", summary["queued"])
            log_run("SCHEDULER", processed=summary["queued"], status="OK", extra=summary)
            return summary

        except Exception as e:
            tb = traceback.format_exc()
            logger.error("💥 Scheduler fatal: %s\n%s", e, tb)
            summary["ok"] = False
            summary["errors"].append(str(e))
            log_run("SCHEDULER", processed=0, status="ERROR", breakdown=tb)
            return summary
