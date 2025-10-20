"""Schema-aware Airtable datastore with deterministic fallbacks."""

from __future__ import annotations

import itertools
import os
import re
import traceback
from collections import defaultdict
from dataclasses import dataclass, field as dataclass_field
from typing import Any, Dict, List, Optional, Tuple

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

try:  # Optional dependency – tests run without Airtable credentials.
    from pyairtable import Table as _Table
except Exception:  # pragma: no cover - dependency not installed during tests
    _Table = None  # type: ignore


logger = get_logger(__name__)

CONV_FIELDS = conversations_field_map()
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


def _normalise(name: str | None) -> str:
    if not name:
        return ""
    return _FIELD_NORMALISER.sub("", name.lower())


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
        value = os.getenv(name)
        if value:
            return value
    return None


@dataclass
class TableHandle:
    table: Any
    in_memory: bool
    base_id: Optional[str]
    table_name: str
    field_cache: Dict[str, str] = dataclass_field(default_factory=dict)


class DataConnector:
    """Lazy pyairtable connector with in-memory fallback."""

    def __init__(self) -> None:
        self._tables: Dict[Tuple[str, str], TableHandle] = {}

    def _table(self, base: Optional[str], table_name: str) -> TableHandle:
        key = (base or "memory", table_name)
        if key in self._tables:
            return self._tables[key]

        if os.getenv("SMS_FORCE_IN_MEMORY", "").lower() in {"1", "true", "yes"}:
            handle = TableHandle(
                table=InMemoryTable(table_name),
                in_memory=True,
                base_id=base,
                table_name=table_name,
            )
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
                handle = TableHandle(
                    table=table,
                    in_memory=False,
                    base_id=base,
                    table_name=table_name,
                )
                self._tables[key] = handle
                return handle
            except Exception:  # pragma: no cover - network failure path
                logger.warning("Falling back to in-memory table for %s", table_name, exc_info=True)

        handle = TableHandle(
            table=InMemoryTable(table_name),
            in_memory=True,
            base_id=base,
            table_name=table_name,
        )
        self._tables[key] = handle
        return handle

    def conversations(self) -> TableHandle:
        base = _first_non_empty("LEADS_CONVOS_BASE", "AIRTABLE_LEADS_CONVOS_BASE_ID")
        return self._table(base, CONVERSATIONS_TABLE.name())

    def leads(self) -> TableHandle:
        base = _first_non_empty("LEADS_CONVOS_BASE", "AIRTABLE_LEADS_CONVOS_BASE_ID")
        return self._table(base, LEADS_TABLE.name())

    def prospects(self) -> TableHandle:
        base = _first_non_empty("LEADS_CONVOS_BASE", "AIRTABLE_LEADS_CONVOS_BASE_ID")
        return self._table(base, PROSPECTS_TABLE.name())

    def templates(self) -> TableHandle:
        base = _first_non_empty("LEADS_CONVOS_BASE", "AIRTABLE_LEADS_CONVOS_BASE_ID")
        return self._table(base, TEMPLATES_TABLE.name())

    def drip_queue(self) -> TableHandle:
        base = _first_non_empty("LEADS_CONVOS_BASE", "AIRTABLE_LEADS_CONVOS_BASE_ID")
        return self._table(base, DRIP_QUEUE_TABLE.name())

    def campaigns(self) -> TableHandle:
        base = _first_non_empty(
            "LEADS_CONVOS_BASE",
            "AIRTABLE_LEADS_CONVOS_BASE_ID",
            "CAMPAIGN_CONTROL_BASE",
            "AIRTABLE_CAMPAIGN_CONTROL_BASE_ID",
        )
        return self._table(base, CAMPAIGNS_TABLE.name())

    def numbers(self) -> TableHandle:
        base = _first_non_empty("CAMPAIGN_CONTROL_BASE", "AIRTABLE_CAMPAIGN_CONTROL_BASE_ID")
        return self._table(base, NUMBERS_TABLE_DEF.name())


CONNECTOR = DataConnector()


def _compact(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        key: value
        for key, value in (payload or {}).items()
        if value not in (None, "", [], {}, ())
    }


def _auto_field_map(handle: TableHandle) -> Dict[str, str]:
    if handle.field_cache:
        return handle.field_cache

    try:
        sample = handle.table.all(max_records=1)  # type: ignore[arg-type]
    except TypeError:
        sample = handle.table.all()  # type: ignore[call-arg]
    except Exception:
        sample = []
    record = sample[0] if sample else None
    fields = record.get("fields", {}) if isinstance(record, dict) else {}
    mapping = {_normalise(name): name for name in fields.keys()}
    handle.field_cache = mapping
    return mapping


def _remap_existing_only(handle: TableHandle, payload: Dict[str, Any]) -> Dict[str, Any]:
    if not payload:
        return {}

    mapping = _auto_field_map(handle)
    if not mapping:
        return dict(payload)

    result: Dict[str, Any] = {}
    values = set(mapping.values())

    for key, value in payload.items():
        if key in values:
            result[key] = value
            continue
        actual = mapping.get(_normalise(key))
        if actual:
            result[actual] = value
        else:
            result[key] = value

    return result or dict(payload)


def _log_airtable_exception(handle: TableHandle, exc: Exception, action: str) -> None:
    response = getattr(exc, "response", None)
    if response is not None:
        try:
            body = response.text  # type: ignore[attr-defined]
        except Exception:
            try:
                body = response.content.decode("utf-8", "ignore")  # type: ignore[attr-defined]
            except Exception:
                body = repr(response)
        status = getattr(response, "status_code", "unknown")  # type: ignore[attr-defined]
        logger.error(
            "Airtable %s failed [%s/%s] status=%s body=%s",
            action,
            handle.base_id or "memory",
            handle.table_name,
            status,
            body,
        )
    else:
        logger.error(
            "Airtable %s failed [%s/%s]: %s",
            action,
            handle.base_id or "memory",
            handle.table_name,
            exc,
        )
    traceback.print_exc()


def _safe_all(handle: TableHandle, **kwargs) -> List[Dict[str, Any]]:
    try:
        return list(handle.table.all(**kwargs))
    except Exception as exc:  # pragma: no cover - network failure path
        _log_airtable_exception(handle, exc, "all")
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

    def _op():
        return handle.table.create(payload)

    try:
        return retry(_op, retries=3, base_delay=0.6, logger=logger)
    except Exception as exc:  # pragma: no cover - network failure path
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

    def _op():
        return handle.table.update(record_id, payload)

    try:
        return retry(_op, retries=3, base_delay=0.6, logger=logger)
    except Exception as exc:  # pragma: no cover - network failure path
        if handle.in_memory:
            return handle.table.update(record_id, payload)
        _log_airtable_exception(handle, exc, "update")
        return None


class Repository:
    """High-level helpers for schema-aware Airtable interactions."""

    def __init__(self) -> None:
        self._conversation_index: Dict[str, str] = {}
        self._prospect_phone_index: Dict[str, str] = {}
        self._lead_phone_index: Dict[str, str] = {}
        self._number_counters: Dict[str, Dict[str, Any]] = defaultdict(dict)

    # ------------------------------------------------------------------ Conversations
    def find_conversation_by_sid(self, message_sid: str) -> Optional[Dict[str, Any]]:
        if not message_sid:
            return None

        record_id = self._conversation_index.get(message_sid)
        handle = CONNECTOR.conversations()
        if record_id:
            record = _safe_get(handle, record_id)
            if record:
                return record

        textgrid_field = CONVERSATIONS_TABLE.field_name("TEXTGRID_ID")
        records = _safe_all(handle, formula=f"{{{textgrid_field}}}='{message_sid}'", max_records=1)
        record = records[0] if records else None
        if record:
            self._conversation_index[message_sid] = record["id"]
        return record

    def create_or_update_conversation(self, message_sid: Optional[str], fields: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        handle = CONNECTOR.conversations()
        if message_sid:
            existing = self.find_conversation_by_sid(message_sid)
            if existing:
                updated = _safe_update(handle, existing["id"], fields)
                return updated or existing

        record = _safe_create(handle, fields)
        if record and message_sid:
            self._conversation_index[message_sid] = record["id"]
        return record

    # ------------------------------------------------------------------ Leads & prospects
    def _refresh_lead_index(self) -> None:
        handle = CONNECTOR.leads()
        for record in _safe_all(handle):
            fields = record.get("fields", {}) or {}
            digits = last_10_digits(fields.get(LEAD_FIELDS["PHONE"]))
            if digits:
                self._lead_phone_index[digits] = record["id"]

    def _refresh_prospect_index(self) -> None:
        handle = CONNECTOR.prospects()
        for record in _safe_all(handle):
            fields = record.get("fields", {}) or {}
            for column in (*PROSPECT_PHONE_COLUMNS, *LEGACY_PHONE_COLUMNS):
                digits = last_10_digits(fields.get(column))
                if digits and digits not in self._prospect_phone_index:
                    self._prospect_phone_index[digits] = record["id"]

    def find_lead_by_phone(self, phone: str) -> Optional[Dict[str, Any]]:
        digits = last_10_digits(phone)
        if not digits:
            return None

        handle = CONNECTOR.leads()
        record_id = self._lead_phone_index.get(digits)
        if record_id:
            record = _safe_get(handle, record_id)
            if record:
                return record

        self._refresh_lead_index()
        record_id = self._lead_phone_index.get(digits)
        if not record_id:
            return None
        return _safe_get(handle, record_id)

    def find_prospect_by_phone(self, phone: str) -> Optional[Dict[str, Any]]:
        digits = last_10_digits(phone)
        if not digits:
            return None

        handle = CONNECTOR.prospects()
        record_id = self._prospect_phone_index.get(digits)
        if record_id:
            record = _safe_get(handle, record_id)
            if record:
                return record

        self._refresh_prospect_index()
        record_id = self._prospect_phone_index.get(digits)
        if not record_id:
            return None
        return _safe_get(handle, record_id)

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
        record = _safe_create(CONNECTOR.prospects(), payload)
        digits = last_10_digits(normalized)
        if record and digits:
            self._prospect_phone_index[digits] = record["id"]
        return record

    def ensure_lead(
        self,
        phone: str,
        *,
        source: str,
        initial_fields: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        if not phone:
            return None

        existing = self.find_lead_by_phone(phone)
        if existing:
            return existing

        normalized = normalize_phone(phone) or phone
        prospect = self.find_prospect_by_phone(phone)
        prospect_fields = (prospect or {}).get("fields", {}) if prospect else {}
        property_id = prospect_fields.get(PROSPECT_FIELDS.get("PROPERTY_ID"))

        payload = {
            LEAD_FIELDS["PHONE"]: normalized,
            LEAD_FIELDS["STATUS"]: LeadStatus.NEW.value,
            LEAD_FIELDS["SOURCE"]: source,
            LEAD_FIELDS["LAST_DIRECTION"]: ConversationDirection.INBOUND.value,
            LEAD_FIELDS["LAST_ACTIVITY"]: iso_now(),
            LEAD_FIELDS["REPLY_COUNT"]: 0,
            LEAD_FIELDS["SENT_COUNT"]: 0,
        }
        if property_id:
            payload[LEAD_FIELDS["PROPERTY_ID"]] = property_id
        if initial_fields:
            payload.update(initial_fields)

        record = _safe_create(CONNECTOR.leads(), payload)
        digits = last_10_digits(normalized)
        if record and digits:
            self._lead_phone_index[digits] = record["id"]
        return record

    def touch_lead_activity(
        self,
        lead_id: str,
        *,
        body: Optional[str],
        direction: str,
        delivery_status: Optional[str] = None,
    ) -> None:
        if not lead_id:
            return

        handle = CONNECTOR.leads()
        record = _safe_get(handle, lead_id) or {"fields": {}}
        fields = record.get("fields", {}) or {}

        payload: Dict[str, Any] = {
            LEAD_FIELDS["LAST_ACTIVITY"]: iso_now(),
            LEAD_FIELDS["LAST_DIRECTION"]: direction,
        }
        if body:
            payload[LEAD_FIELDS["LAST_MESSAGE"]] = body[:500]

        now = iso_now()
        if direction == ConversationDirection.INBOUND.value:
            reply_count = int(fields.get(LEAD_FIELDS["REPLY_COUNT"]) or 0) + 1
            payload[LEAD_FIELDS["REPLY_COUNT"]] = reply_count
            payload[LEAD_FIELDS["LAST_INBOUND"]] = now
        elif direction == ConversationDirection.OUTBOUND.value:
            sent_count = int(fields.get(LEAD_FIELDS["SENT_COUNT"]) or 0) + 1
            payload[LEAD_FIELDS["SENT_COUNT"]] = sent_count
            payload[LEAD_FIELDS["LAST_OUTBOUND"]] = now

        if delivery_status:
            payload[LEAD_FIELDS["LAST_DELIVERY_STATUS"]] = delivery_status

        _safe_update(handle, lead_id, payload)

    def update_lead_delivery_totals(
        self,
        lead_id: str,
        *,
        delivered_delta: int = 0,
        failed_delta: int = 0,
        status: Optional[str] = None,
    ) -> None:
        if not lead_id:
            return
        handle = CONNECTOR.leads()
        record = _safe_get(handle, lead_id)
        fields = record.get("fields", {}) if record else {}

        payload: Dict[str, Any] = {}
        if delivered_delta:
            delivered = int(fields.get(LEAD_FIELDS["DELIVERED_COUNT"]) or 0) + delivered_delta
            payload[LEAD_FIELDS["DELIVERED_COUNT"]] = delivered
        if failed_delta:
            failed = int(fields.get(LEAD_FIELDS["FAILED_COUNT"]) or 0) + failed_delta
            payload[LEAD_FIELDS["FAILED_COUNT"]] = failed
        if delivered_delta or failed_delta:
            sent = int(fields.get(LEAD_FIELDS["SENT_COUNT"]) or 0) + delivered_delta + failed_delta
            payload[LEAD_FIELDS["SENT_COUNT"]] = sent
        if status:
            payload[LEAD_FIELDS["LAST_DELIVERY_STATUS"]] = status

        if payload:
            payload[LEAD_FIELDS["LAST_ACTIVITY"]] = iso_now()
            _safe_update(handle, lead_id, payload)

    # ------------------------------------------------------------------ Numbers & campaigns
    def increment_number_counters(self, number: str, **deltas: int) -> Dict[str, Any]:
        normalized = normalize_phone(number) or number
        counters = self._number_counters[normalized]
        for field, delta in deltas.items():
            counters[field] = int(counters.get(field, 0)) + int(delta)
        return counters

    def all_campaigns(self) -> List[Dict[str, Any]]:
        return _safe_all(CONNECTOR.campaigns())

    def update_campaign(self, record_id: str, fields: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        return _safe_update(CONNECTOR.campaigns(), record_id, fields)

    def find_template(self, template_id: str) -> Optional[Dict[str, Any]]:
        handle = CONNECTOR.templates()
        record_id_field = TEMPLATES_TABLE.field_name("RECORD_ID")
        records = _safe_all(handle, formula=f"{{{record_id_field}}}='{template_id}'", max_records=1)
        return records[0] if records else None


REPOSITORY = Repository()


def reset_state() -> None:
    """Reset in-memory caches (used by tests)."""

    CONNECTOR._tables.clear()  # type: ignore[attr-defined]
    REPOSITORY._conversation_index.clear()
    REPOSITORY._prospect_phone_index.clear()
    REPOSITORY._lead_phone_index.clear()
    REPOSITORY._number_counters.clear()


def ensure_prospect_or_lead(phone: str) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    lead = REPOSITORY.find_lead_by_phone(phone)
    if lead:
        return lead, None
    prospect = REPOSITORY.find_prospect_by_phone(phone)
    if prospect:
        return None, prospect
    created = REPOSITORY.ensure_prospect(phone)
    return None, created


def create_conversation(message_sid: Optional[str], fields: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    return REPOSITORY.create_or_update_conversation(message_sid, fields)


def update_conversation(record_id: str, fields: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    return _safe_update(CONNECTOR.conversations(), record_id, fields)


def conversation_by_sid(message_sid: str) -> Optional[Dict[str, Any]]:
    return REPOSITORY.find_conversation_by_sid(message_sid)


def promote_if_needed(
    phone: str,
    conversation_fields: Dict[str, Any],
    stage: Optional[str],
    *,
    source: str = "Lead Promoter",
) -> Optional[Dict[str, Any]]:
    initial: Dict[str, Any] = {}
    body = conversation_fields.get(CONV_FIELDS.get("BODY"))
    if body:
        initial[LEAD_FIELDS["LAST_MESSAGE"]] = str(body)[:500]
    if stage:
        initial[LEAD_FIELDS["STATUS"]] = LeadStatus.ACTIVE_COMMUNICATION.value
    return REPOSITORY.ensure_lead(phone, source=source, initial_fields=initial)


def promote_to_lead(
    phone: str,
    *,
    source: str,
    conversation_fields: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[str], Optional[str]]:
    initial: Dict[str, Any] = {}
    if conversation_fields:
        body = conversation_fields.get(CONV_FIELDS.get("BODY"))
        if body:
            initial[LEAD_FIELDS["LAST_MESSAGE"]] = str(body)[:500]
    record = REPOSITORY.ensure_lead(phone, source=source, initial_fields=initial)
    if not record:
        return None, None
    fields = record.get("fields", {}) or {}
    property_id = fields.get(LEAD_FIELDS.get("PROPERTY_ID"))
    return record.get("id"), property_id


def touch_lead(
    lead_id: str,
    *,
    body: Optional[str],
    direction: str,
    status: Optional[str] = None,
) -> None:
    REPOSITORY.touch_lead_activity(lead_id, body=body, direction=direction, delivery_status=status)


def update_lead_totals(lead_id: str, *, delivered: int = 0, failed: int = 0, status: Optional[str] = None) -> None:
    REPOSITORY.update_lead_delivery_totals(lead_id, delivered_delta=delivered, failed_delta=failed, status=status)


def create_record(handle: TableHandle, fields: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Public helper for safe create on arbitrary tables."""

    return _safe_create(handle, fields)


def update_record(handle: TableHandle, record_id: str, fields: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Public helper for safe update on arbitrary tables."""

    return _safe_update(handle, record_id, fields)


def list_records(handle: TableHandle, **kwargs) -> List[Dict[str, Any]]:
    """Safe wrapper for Table.all()."""

    return _safe_all(handle, **kwargs)


def get_record(handle: TableHandle, record_id: str) -> Optional[Dict[str, Any]]:
    """Safe wrapper for Table.get()."""

    return _safe_get(handle, record_id)
