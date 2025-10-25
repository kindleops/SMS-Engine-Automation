"""Schema-aware Airtable datastore with deterministic fallbacks (Optimized Final Version)."""

from __future__ import annotations

import itertools
import os
import re
import time
import traceback
from collections import defaultdict
from dataclasses import dataclass, field as dataclass_field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from sms.runtime import get_logger, iso_now, last_10_digits, normalize_phone, retry
from sms.airtable_schema import (
    CAMPAIGNS_TABLE,
    CONVERSATIONS_TABLE,
    DRIP_QUEUE_TABLE,
    LEADS_TABLE,
    NUMBERS_TABLE_DEF,
    PROSPECTS_TABLE,
    TEMPLATES_TABLE,
    ConversationDirection,
    LeadStatus,
    conversations_field_map,
    leads_field_map,
    prospects_field_map,
)

try:
    from pyairtable import Table as _Table
except Exception:
    _Table = None  # type: ignore

logger = get_logger(__name__)
DEBUG = os.getenv("DEBUG", "").lower() in {"1", "true", "yes"}

CONV_FIELDS = conversations_field_map()
CONVERSATION_MESSAGES_TABLE_NAME = "Conversation Messages"
MESSAGES_FIELD_MAP: Dict[str, str] = {
    "CONVERSATION_LINK": "Conversation",
    "DIRECTION": "Direction",
    "TO": "To",
    "FROM": "From",
    "BODY": "Body",
    "MESSAGE_STATUS": "Message Status",
    "PROVIDER_SID": "Provider SID",
    "PROVIDER_ERROR": "Provider Error",
    "TIMESTAMP": "Timestamp",
}
LEAD_FIELDS = leads_field_map()
PROSPECT_FIELDS = prospects_field_map()

PROSPECT_PHONE_COLUMNS = [
    PROSPECT_FIELDS["PHONE_PRIMARY"],
    PROSPECT_FIELDS["PHONE_PRIMARY_LINKED"],
    PROSPECT_FIELDS["PHONE_SECONDARY"],
    PROSPECT_FIELDS["PHONE_SECONDARY_LINKED"],
]

LEGACY_PHONE_COLUMNS = (
    "phone",
    "Phone",
    "Mobile",
    "Cell",
    "Phone Number",
    "Primary Phone",
)

_FIELD_NORMALISER = re.compile(r"[^a-z0-9]+")
_field_map_cache: Dict[str, Dict[str, str]] = {}


TABLES = {
    "campaigns": "Campaigns",
    "prospects": "Prospects",
    "templates": "Templates",
    "drip_queue": "Drip Queue",
    "performance": "Performance",
}

_TABLE_BASE_PRIORITY: Dict[str, Tuple[str, ...]] = {
    "Campaigns": (
        "CAMPAIGNS_BASE_ID",
        "LEADS_CONVOS_BASE",
        "AIRTABLE_LEADS_CONVOS_BASE_ID",
        "CAMPAIGN_CONTROL_BASE",
        "AIRTABLE_CAMPAIGN_CONTROL_BASE_ID",
    ),
    "Prospects": (
        "LEADS_CONVOS_BASE",
        "AIRTABLE_LEADS_CONVOS_BASE_ID",
    ),
    "Templates": (
        "LEADS_CONVOS_BASE",
        "AIRTABLE_LEADS_CONVOS_BASE_ID",
    ),
    "Drip Queue": (
        "LEADS_CONVOS_BASE",
        "AIRTABLE_LEADS_CONVOS_BASE_ID",
    ),
    "Performance": (
        "PERFORMANCE_BASE",
        "AIRTABLE_PERFORMANCE_BASE_ID",
    ),
}


def _normalise(name: str | None) -> str:
    return _FIELD_NORMALISER.sub("", (name or "").lower())


class InMemoryTable:
    """Minimal Airtable drop-in replacement used for local tests."""

    def __init__(self, name: str):
        self.name = name
        self._records: Dict[str, Dict[str, Any]] = {}
        self._sequence = itertools.count(1)

    def create(self, fields: Dict[str, Any]):
        record_id = f"rec_{next(self._sequence)}"
        record = {"id": record_id, "fields": dict(fields)}
        self._records[record_id] = record
        return record

    def update(self, record_id: str, fields: Dict[str, Any]):
        if record_id not in self._records:
            raise KeyError(f"Unknown record id {record_id} in {self.name}")
        self._records[record_id]["fields"].update(fields)
        return self._records[record_id]

    def get(self, record_id: str):
        return self._records.get(record_id)

    def all(self, **kwargs):
        records = list(self._records.values())
        formula = kwargs.get("formula")
        max_records = kwargs.get("max_records")
        if formula:
            records = [rec for rec in records if _formula_match(rec, formula)]
        if max_records is not None:
            records = records[: int(max_records)]
        return records


class _NullPerformanceTable:
    """No-op table used when Airtable permissions are missing."""

    name = "Performance"

    def first(self, *args, **kwargs):  # pragma: no cover - trivial
        return None

    def all(self, *args, **kwargs):  # pragma: no cover - trivial
        return []

    def create(self, *args, **kwargs):  # pragma: no cover - trivial
        return None

    def update(self, *args, **kwargs):  # pragma: no cover - trivial
        return None


def _formula_match(record: Dict[str, Any], formula: str) -> bool:
    pattern = re.compile(r"\{([^}]+)\}\s*=\s*'([^']*)'")
    matches = pattern.findall(formula)
    if not matches:
        return False
    fields = record.get("fields", {})
    for field_name, expected in matches:
        if str(fields.get(field_name)) != expected:
            return False
    return True


def _first_non_empty(*names: str) -> Optional[str]:
    for name in names:
        v = os.getenv(name)
        if v:
            return v
    return None


@dataclass
class TableHandle:
    table: Any
    in_memory: bool
    base_id: Optional[str]
    table_name: str
    field_cache: Dict[str, str] = dataclass_field(default_factory=dict)
    last_error: Optional[Dict[str, Any]] = None


# ============================================================
# CONNECTOR
# ============================================================


class DataConnector:
    """Lazy pyairtable connector with in-memory fallback."""

    def __init__(self) -> None:
        self._tables: Dict[Tuple[str, str], TableHandle] = {}

    def _table(self, base: Optional[str], table_name: str) -> TableHandle:
        key = (base or "memory", table_name)
        if key in self._tables:
            return self._tables[key]

        if os.getenv("SMS_FORCE_IN_MEMORY", "").lower() in {"1", "true", "yes"}:
            handle = TableHandle(InMemoryTable(table_name), True, base, table_name)
            self._tables[key] = handle
            return handle

        api_key = _first_non_empty(
            "AIRTABLE_API_KEY",
            "AIRTABLE_ACQUISITIONS_KEY",
            "AIRTABLE_COMPLIANCE_KEY",
            "AIRTABLE_REPORTING_KEY",
        )

        if base and api_key and _Table is not None:
            try:
                table = _Table(api_key, base, table_name)
                handle = TableHandle(table, False, base, table_name)
                self._tables[key] = handle
                return handle
            except Exception:
                logger.warning("Falling back to in-memory table for %s", table_name, exc_info=True)

        handle = TableHandle(InMemoryTable(table_name), True, base, table_name)
        self._tables[key] = handle
        return handle

    def table_handle(self, table_name: str) -> TableHandle:
        env_keys = _TABLE_BASE_PRIORITY.get(table_name, ("LEADS_CONVOS_BASE", "AIRTABLE_LEADS_CONVOS_BASE_ID"))
        base = _first_non_empty(*env_keys) if env_keys else None
        return self._table(base, table_name)

    def conversations(self):
        return self._table(_first_non_empty("LEADS_CONVOS_BASE", "AIRTABLE_LEADS_CONVOS_BASE_ID"), CONVERSATIONS_TABLE.name())

    def leads(self):
        return self._table(_first_non_empty("LEADS_CONVOS_BASE", "AIRTABLE_LEADS_CONVOS_BASE_ID"), LEADS_TABLE.name())

    def prospects(self):
        return self.table_handle(TABLES["prospects"])

    def templates(self):
        return self.table_handle(TABLES["templates"])

    def drip_queue(self):
        return self.table_handle(TABLES["drip_queue"])

    def campaigns(self):
        return self.table_handle(TABLES["campaigns"])

    def performance(self):
        handle = self.table_handle(TABLES["performance"])
        if getattr(handle, "_performance_verified", False):
            return handle

        if handle.in_memory:
            setattr(handle, "_performance_verified", True)
            return handle

        table = getattr(handle, "table", None)
        probe = getattr(table, "first", None)
        if callable(probe):
            try:
                probe()
                setattr(handle, "_performance_verified", True)
            except Exception as exc:
                if "INVALID_PERMISSIONS_OR_MODEL_NOT_FOUND" in str(exc):
                    logger.warning(
                        "Performance table not accessible: %s", exc
                    )
                    null_handle = TableHandle(
                        _NullPerformanceTable(), True, handle.base_id, handle.table_name
                    )
                    key = (handle.base_id or "memory", handle.table_name)
                    self._tables[key] = null_handle
                    return null_handle
        else:
            setattr(handle, "_performance_verified", True)
        return handle

    def numbers(self):
        return self._table(_first_non_empty("CAMPAIGN_CONTROL_BASE", "AIRTABLE_CAMPAIGN_CONTROL_BASE_ID"), NUMBERS_TABLE_DEF.name())

    def conversation_messages(self):
        return self._table(
            _first_non_empty("LEADS_CONVOS_BASE", "AIRTABLE_LEADS_CONVOS_BASE_ID"),
            CONVERSATION_MESSAGES_TABLE_NAME,
        )


CONNECTOR = DataConnector()


# ============================================================
# LOW LEVEL HELPERS
# ============================================================


def _compact(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in (payload or {}).items() if v not in (None, "", [], {}, ())}


def _ensure_record_list(value: Any) -> List[str]:
    if value in (None, "", [], (), {}):
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        record_id = value.get("id")
        return [record_id] if record_id else []
    if isinstance(value, (list, tuple, set)):
        result: List[str] = []
        for item in value:
            if isinstance(item, str):
                result.append(item)
            elif isinstance(item, dict) and item.get("id"):
                result.append(item["id"])
        return result
    return [str(value)]


def _normalize_conversation_fields(fields: Dict[str, Any]) -> Dict[str, Any]:
    if not fields:
        return {}

    status_field = CONV_FIELDS.get("STATUS", "Status")
    stage_field = CONV_FIELDS.get("STAGE", "Stage")
    ai_intent_field = CONV_FIELDS.get("AI_INTENT", "AI Intent")
    lead_link_field = CONV_FIELDS.get("LEAD_LINK", "Lead")
    prospect_link_field = CONV_FIELDS.get("PROSPECT_LINK", "Prospect")

    normalized: Dict[str, Any] = {}
    for key, value in fields.items():
        if key == "status":
            normalized[status_field] = value
        elif key == "stage":
            normalized[stage_field] = value
        elif key == "ai_intent":
            normalized[ai_intent_field] = value
        elif key == "lead_id":
            normalized[lead_link_field] = _ensure_record_list(value)
        elif key == "prospect_id":
            normalized[prospect_link_field] = _ensure_record_list(value)
        else:
            normalized[key] = value
    return normalized


def _auto_field_map(handle: TableHandle) -> Dict[str, str]:
    cache_key = f"{handle.base_id or 'mem'}::{handle.table_name}"
    if cache_key in _field_map_cache:
        return _field_map_cache[cache_key]

    try:
        sample = handle.table.all(max_records=1)
    except TypeError:
        sample = handle.table.all()
    except Exception:
        sample = []
    record = sample[0] if sample else {}
    fields = record.get("fields", {}) if isinstance(record, dict) else {}
    mapping = {_normalise(name): name for name in fields.keys()}
    handle.field_cache = mapping
    _field_map_cache[cache_key] = mapping
    return mapping


def _remap_existing_only(handle: TableHandle, payload: Dict[str, Any]) -> Dict[str, Any]:
    mapping = _auto_field_map(handle)
    if not mapping:
        return dict(payload)
    result: Dict[str, Any] = {}
    for k, v in payload.items():
        result[mapping.get(_normalise(k), k)] = v
    return result


def _log_airtable_exception(handle: TableHandle, exc: Exception, action: str) -> None:
    if not DEBUG:
        logger.error("Airtable %s failed [%s]: %s", action, handle.table_name, exc)
        handle.last_error = {"action": action, "error": str(exc), "timestamp": iso_now()}
        return

    response = getattr(exc, "response", None)
    payload = {"action": action, "error": str(exc), "timestamp": iso_now()}
    if response is not None:
        try:
            body = response.text
        except Exception:
            try:
                body = response.content.decode("utf-8", "ignore")
            except Exception:
                body = repr(response)
        status = getattr(response, "status_code", "unknown")
        payload.update({"status": status, "body": body})
        logger.error("Airtable %s failed [%s] status=%s body=%s", action, handle.table_name, status, body)
    else:
        logger.error("Airtable %s failed [%s]: %s", action, handle.table_name, exc)
    handle.last_error = payload
    if DEBUG:
        traceback.print_exc()


# ============================================================
# SAFE WRAPPERS
# ============================================================


def _safe_all(handle: TableHandle, **kwargs) -> List[Dict[str, Any]]:
    if "page_size" not in kwargs:
        kwargs["page_size"] = 100
    if "max_records" not in kwargs:
        kwargs["max_records"] = 100
    for attempt in range(3):
        try:
            return list(handle.table.all(**kwargs))
        except (requests.exceptions.ConnectionError, ConnectionResetError) as exc:
            logger.warning("Airtable connection reset [%s] retry %s: %s", handle.table_name, attempt + 1, exc)
            time.sleep((2**attempt) * 0.5)
            continue
        except Exception as exc:
            _log_airtable_exception(handle, exc, "all")
            if "429" in str(exc) and attempt < 2:
                time.sleep((2**attempt) * 0.5)
                continue
            break
        finally:
            time.sleep(0.25)
    return []


def _safe_get(handle: TableHandle, record_id: str):
    if not record_id:
        return None
    try:
        return handle.table.get(record_id)
    except Exception as exc:
        _log_airtable_exception(handle, exc, "get")
        return None


def _safe_create(handle: TableHandle, fields: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    body = _compact(fields)
    if not body:
        return None
    payload = _remap_existing_only(handle, body)
    try:
        return retry(lambda: handle.table.create(payload), retries=3, base_delay=0.6, logger=logger)
    except Exception as exc:
        if handle.in_memory:
            return handle.table.create(payload)
        _log_airtable_exception(handle, exc, "create")
        return None


def _safe_update(handle: TableHandle, record_id: str, fields: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not record_id:
        return None
    body = _compact(fields)
    if not body:
        return None
    payload = _remap_existing_only(handle, body)
    try:
        return retry(lambda: handle.table.update(record_id, payload), retries=3, base_delay=0.6, logger=logger)
    except Exception as exc:
        if handle.in_memory:
            return handle.table.update(record_id, payload)
        _log_airtable_exception(handle, exc, "update")
        return None


# ============================================================
# REPOSITORY
# ============================================================


    payload_fields = (
        _normalize_conversation_fields(fields)
        if handle.table_name == CONVERSATIONS_TABLE.name()
        else fields
    )
    body = _compact(payload_fields)
    payload_fields = (
        _normalize_conversation_fields(fields)
        if handle.table_name == CONVERSATIONS_TABLE.name()
        else fields
    )
    body = _compact(payload_fields)

    def __init__(self) -> None:
        self._conversation_index: Dict[str, str] = {}
        self._prospect_phone_index: Dict[str, str] = {}
        self._lead_phone_index: Dict[str, str] = {}
        self._number_counters: Dict[str, Dict[str, Any]] = defaultdict(dict)

    # Conversations
    def find_conversation_by_sid(self, sid: str) -> Optional[Dict[str, Any]]:
        if not sid:
            return None
        rid = self._conversation_index.get(sid)
        h = CONNECTOR.conversations()
        if rid:
            r = _safe_get(h, rid)
            if r:
                return r
        textgrid_field = CONVERSATIONS_TABLE.field_name("TEXTGRID_ID")
        records = _safe_all(h, formula=f"{{{textgrid_field}}}='{sid}'", max_records=1)
        if records:
            self._conversation_index[sid] = records[0]["id"]
            return records[0]
        return None

    def create_or_update_conversation(self, sid: Optional[str], fields: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        h = CONNECTOR.conversations()
        if sid:
            existing = self.find_conversation_by_sid(sid)
            if existing:
                return _safe_update(h, existing["id"], fields) or existing
        rec = _safe_create(h, fields)
        if rec and sid:
            self._conversation_index[sid] = rec["id"]
        return rec

    # Leads & Prospects
    def _refresh_lead_index(self):
        h = CONNECTOR.leads()
        for r in _safe_all(h):
            f = r.get("fields", {}) or {}
            d = last_10_digits(f.get(LEAD_FIELDS["PHONE"]))
            if d:
                self._lead_phone_index[d] = r["id"]

    def _refresh_prospect_index(self):
        h = CONNECTOR.prospects()
        for r in _safe_all(h):
            f = r.get("fields", {}) or {}
            for c in (*PROSPECT_PHONE_COLUMNS, *LEGACY_PHONE_COLUMNS):
                d = last_10_digits(f.get(c))
                if d and d not in self._prospect_phone_index:
                    self._prospect_phone_index[d] = r["id"]

    def find_lead_by_phone(self, phone: str) -> Optional[Dict[str, Any]]:
        d = last_10_digits(phone)
        if not d:
            return None
        h = CONNECTOR.leads()
        rid = self._lead_phone_index.get(d)
        if rid:
            r = _safe_get(h, rid)
            if r:
                return r
        self._refresh_lead_index()
        rid = self._lead_phone_index.get(d)
        return _safe_get(h, rid) if rid else None

    def find_prospect_by_phone(self, phone: str) -> Optional[Dict[str, Any]]:
        d = last_10_digits(phone)
        if not d:
            return None
        h = CONNECTOR.prospects()
        rid = self._prospect_phone_index.get(d)
        if rid:
            r = _safe_get(h, rid)
            if r:
                return r
        self._refresh_prospect_index()
        rid = self._prospect_phone_index.get(d)
        return _safe_get(h, rid) if rid else None

    def ensure_prospect(self, phone: str) -> Optional[Dict[str, Any]]:
        if not phone:
            return None
        existing = self.find_prospect_by_phone(phone)
        if existing:
            return existing
        normalized = normalize_phone(phone) or phone
        payload = {
            PROSPECT_FIELDS["PHONE_PRIMARY"]: normalized,
            PROSPECT_FIELDS.get("NAME", "Name"): normalized,
            PROSPECT_FIELDS.get("LAST_ACTIVITY", "Last Activity"): iso_now(),
        }
        rec = _safe_create(CONNECTOR.prospects(), payload)
        d = last_10_digits(normalized)
        if rec and d:
            self._prospect_phone_index[d] = rec["id"]
        return rec

    def ensure_lead(self, phone: str, *, source: str, initial_fields: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        if not phone:
            return None
        existing = self.find_lead_by_phone(phone)
        if existing:
            return existing
        normalized = normalize_phone(phone) or phone
        prospect = self.find_prospect_by_phone(phone)
        pf = (prospect or {}).get("fields", {}) if prospect else {}
        prop_id = pf.get(PROSPECT_FIELDS.get("PROPERTY_ID"))
        payload = {
            LEAD_FIELDS["PHONE"]: normalized,
            LEAD_FIELDS["STATUS"]: LeadStatus.NEW.value,
            LEAD_FIELDS["SOURCE"]: source,
            LEAD_FIELDS["LAST_DIRECTION"]: ConversationDirection.INBOUND.value,
            LEAD_FIELDS["LAST_ACTIVITY"]: iso_now(),
            LEAD_FIELDS["REPLY_COUNT"]: 0,
            LEAD_FIELDS["SENT_COUNT"]: 0,
        }
        if prop_id:
            payload[LEAD_FIELDS["PROPERTY_ID"]] = prop_id
        if initial_fields:
            payload.update(initial_fields)
        rec = _safe_create(CONNECTOR.leads(), payload)
        d = last_10_digits(normalized)
        if rec and d:
            self._lead_phone_index[d] = rec["id"]
        return rec


REPOSITORY = Repository()


# ============================================================
# PUBLIC HELPERS
# ============================================================


def reset_state():
    CONNECTOR._tables.clear()
    REPOSITORY._conversation_index.clear()
    REPOSITORY._prospect_phone_index.clear()
    REPOSITORY._lead_phone_index.clear()
    REPOSITORY._number_counters.clear()
    _field_map_cache.clear()
    logger.info("🧹 Datastore state and caches cleared.")


def ensure_prospect_or_lead(phone: str):
    lead = REPOSITORY.find_lead_by_phone(phone)
    if lead:
        return lead, None
    prospect = REPOSITORY.find_prospect_by_phone(phone)
    if prospect:
        return None, prospect
    created = REPOSITORY.ensure_prospect(phone)
    return None, created


def create_conversation(sid: Optional[str], fields: Dict[str, Any]):
    return REPOSITORY.create_or_update_conversation(sid, fields)


def update_record(handle: TableHandle, record_id: str, fields: Dict[str, Any]):
    return _safe_update(handle, record_id, fields)


def list_records(handle: TableHandle, **kwargs):
    return _safe_all(handle, **kwargs)
def log_message(
    *,
    conversation_id: str,
    direction: str,
    to_phone: str,
    from_phone: str,
    body: str,
    status: str,
    provider_sid: Optional[str] = None,
    provider_error: Optional[str] = None,
    timestamp: Optional[datetime] = None,
) -> Optional[Dict[str, Any]]:
    if not conversation_id:
        logger.warning("[messages] Missing conversation id; skipping log entry.")
        return None

    try:
        handle = CONNECTOR.conversation_messages()
    except Exception as exc:  # pragma: no cover - defensive safeguard
        logger.warning("[messages] Unable to resolve Conversation Messages table: %s", exc)
        return None

    table = getattr(handle, "table", None)
    if table is None:
        logger.warning("[messages] Conversation Messages table unavailable; skipping persist.")
        return None

    # Normalize timestamp to ISO-8601 string
    if timestamp is None:
        ts_value = iso_now()
    elif isinstance(timestamp, datetime):
        ts = timestamp if timestamp.tzinfo else timestamp.replace(tzinfo=timezone.utc)
        ts_value = ts.astimezone(timezone.utc).isoformat()
    else:
        ts_value = str(timestamp)

    fields = {
        MESSAGES_FIELD_MAP.get("CONVERSATION_LINK", "Conversation"): [conversation_id],
        MESSAGES_FIELD_MAP.get("DIRECTION", "Direction"): direction,
        MESSAGES_FIELD_MAP.get("TO", "To"): to_phone,
        MESSAGES_FIELD_MAP.get("FROM", "From"): from_phone,
        MESSAGES_FIELD_MAP.get("BODY", "Body"): body,
        MESSAGES_FIELD_MAP.get("MESSAGE_STATUS", "Message Status"): status,
        MESSAGES_FIELD_MAP.get("TIMESTAMP", "Timestamp"): ts_value,
    }

    if provider_sid:
        fields[MESSAGES_FIELD_MAP.get("PROVIDER_SID", "Provider SID")] = provider_sid
    if provider_error:
        fields[MESSAGES_FIELD_MAP.get("PROVIDER_ERROR", "Provider Error")] = provider_error

    try:
        return _safe_create(handle, fields)
    except Exception as exc:  # pragma: no cover - defensive safeguard
        logger.warning("[messages] Failed to persist message record: %s", exc)
        return None


