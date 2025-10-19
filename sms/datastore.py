"""Airtable-aware data access layer with an in-memory fallback.

The production system persists into Airtable, but automated tests inside this
repository run without live Airtable credentials.  This module provides a thin
adapter that mirrors Airtable's interface (`create`, `update`, `all`, `get`)
while maintaining deterministic behaviour locally.

Each table lazily initialises either a pyairtable Table (when the required
environment variables are present) or an :class:`InMemoryTable` that emulates
the parts of the API the engine relies on.  Both implementations expose the
same subset of methods which keeps the business logic in other modules clean
and testable.

The adapter also offers convenience methods for common lookup operations used
throughout the engine – e.g. locating a lead/prospect by phone, enforcing
idempotency on MessageSid/TextGrid IDs, and maintaining lightweight counters.
"""

from __future__ import annotations

import itertools
import os
import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

from . import spec

try:  # pyairtable is optional during testing
    from pyairtable import Table as _Table
except Exception:  # pragma: no cover - dependency optional
    _Table = None  # type: ignore


# ---------------------------------------------------------------------------
# In-memory table that mimics Airtable's minimal behaviour
# ---------------------------------------------------------------------------


class InMemoryTable:
    def __init__(self, name: str):
        self.name = name
        self._records: Dict[str, Dict[str, Any]] = {}
        self._sequence = itertools.count(1)

    # Airtable compatibility -------------------------------------------------
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

    # Convenience ------------------------------------------------------------
    def first(self, field: str, value: Any) -> Optional[Dict[str, Any]]:
        for record in self._records.values():
            if record["fields"].get(field) == value:
                return record
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


# ---------------------------------------------------------------------------
# Airtable connector with fallback
# ---------------------------------------------------------------------------


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


class DataConnector:
    """Lazy table factory that falls back to in-memory storage."""

    def __init__(self) -> None:
        self._tables: Dict[Tuple[str, str], TableHandle] = {}

    def _table(self, base: Optional[str], table_name: str) -> TableHandle:
        key = (base or "memory", table_name)
        if key in self._tables:
            return self._tables[key]

        if os.getenv("SMS_FORCE_IN_MEMORY", "").lower() in {"1", "true", "yes"}:
            table = InMemoryTable(table_name)
            handle = TableHandle(table=table, in_memory=True)
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
                handle = TableHandle(table=table, in_memory=False)
                self._tables[key] = handle
                return handle
            except Exception:
                # Failed to talk to Airtable → fall back to memory
                pass

        table = InMemoryTable(table_name)
        handle = TableHandle(table=table, in_memory=True)
        self._tables[key] = handle
        return handle

    # Public factories ------------------------------------------------------
    def conversations(self) -> TableHandle:
        base = _first_non_empty("LEADS_CONVOS_BASE", "AIRTABLE_LEADS_CONVOS_BASE_ID")
        table_name = os.getenv("CONVERSATIONS_TABLE", "Conversations")
        return self._table(base, table_name)

    def leads(self) -> TableHandle:
        base = _first_non_empty("LEADS_CONVOS_BASE", "AIRTABLE_LEADS_CONVOS_BASE_ID")
        table_name = os.getenv("LEADS_TABLE", "Leads")
        return self._table(base, table_name)

    def prospects(self) -> TableHandle:
        base = _first_non_empty("LEADS_CONVOS_BASE", "AIRTABLE_LEADS_CONVOS_BASE_ID")
        table_name = os.getenv("PROSPECTS_TABLE", "Prospects")
        return self._table(base, table_name)

    def numbers(self) -> TableHandle:
        base = _first_non_empty("CAMPAIGN_CONTROL_BASE", "AIRTABLE_CAMPAIGN_CONTROL_BASE_ID")
        table_name = os.getenv("NUMBERS_TABLE", "Numbers")
        return self._table(base, table_name)

    def campaigns(self) -> TableHandle:
        base = _first_non_empty("CAMPAIGN_CONTROL_BASE", "AIRTABLE_CAMPAIGN_CONTROL_BASE_ID")
        table_name = os.getenv("CAMPAIGNS_TABLE", "Campaigns")
        return self._table(base, table_name)

    def templates(self) -> TableHandle:
        base = _first_non_empty("LEADS_CONVOS_BASE", "AIRTABLE_LEADS_CONVOS_BASE_ID")
        table_name = os.getenv("TEMPLATES_TABLE", "Templates")
        return self._table(base, table_name)


CONNECTOR = DataConnector()


# ---------------------------------------------------------------------------
# Convenience service layer
# ---------------------------------------------------------------------------


def _record_or_none(record) -> Optional[Dict[str, Any]]:
    if not record:
        return None
    if isinstance(record, list):
        return record[0] if record else None
    return record


def _all_records(handle: TableHandle, **kwargs) -> List[Dict[str, Any]]:
    try:
        return list(handle.table.all(**kwargs))
    except Exception:
        return []


def _create_record(handle: TableHandle, fields: Dict[str, Any]):
    try:
        return handle.table.create(fields)
    except Exception:
        if handle.in_memory:
            return handle.table.create(fields)
        raise


def _update_record(handle: TableHandle, record_id: str, fields: Dict[str, Any]):
    try:
        return handle.table.update(record_id, fields)
    except Exception:
        if handle.in_memory:
            return handle.table.update(record_id, fields)
        raise


def _get_record(handle: TableHandle, record_id: str):
    try:
        return handle.table.get(record_id)
    except Exception:
        return None


class Repository:
    """High-level Airtable helper with opinionated semantics."""

    def __init__(self) -> None:
        self._conversation_index: Dict[str, str] = {}
        self._prospect_phone_index: Dict[str, str] = {}
        self._lead_phone_index: Dict[str, str] = {}
        self._number_cache: Dict[str, Dict[str, Any]] = defaultdict(dict)

    # Conversations ---------------------------------------------------------
    def find_conversation_by_sid(self, message_sid: str) -> Optional[Dict[str, Any]]:
        record_id = self._conversation_index.get(message_sid)
        if record_id:
            return _get_record(CONNECTOR.conversations(), record_id)
        # fall back to formula search
        convo_handle = CONNECTOR.conversations()
        field = spec.CONVERSATION_FIELDS.textgrid_id
        records = _all_records(convo_handle, formula=f"{{{field}}}='{message_sid}'", max_records=1)
        record = _record_or_none(records)
        if record:
            self._conversation_index[message_sid] = record["id"]
        return record

    def create_or_update_conversation(self, message_sid: Optional[str], fields: Dict[str, Any]) -> Dict[str, Any]:
        if message_sid:
            existing = self.find_conversation_by_sid(message_sid)
            if existing:
                updated = _update_record(CONNECTOR.conversations(), existing["id"], fields)
                return updated or existing

        record = _create_record(CONNECTOR.conversations(), fields)
        if message_sid:
            self._conversation_index[message_sid] = record["id"]
        return record

    # Leads & prospects -----------------------------------------------------
    def _refresh_lead_index(self) -> None:
        leads = _all_records(CONNECTOR.leads())
        for record in leads:
            fields = record.get("fields", {})
            phone = fields.get(spec.LEAD_FIELDS.phone)
            if phone:
                digits = spec.last_10_digits(str(phone))
                if digits:
                    self._lead_phone_index[digits] = record["id"]

    def _refresh_prospect_index(self) -> None:
        prospects = _all_records(CONNECTOR.prospects())
        for record in prospects:
            fields = record.get("fields", {})
            for candidate in spec.PHONE_FIELD_CANDIDATES:
                value = fields.get(candidate)
                digits = spec.last_10_digits(str(value)) if value else None
                if digits:
                    self._prospect_phone_index.setdefault(digits, record["id"])

    def find_lead_by_phone(self, phone: str) -> Optional[Dict[str, Any]]:
        digits = spec.last_10_digits(phone)
        if not digits:
            return None
        record_id = self._lead_phone_index.get(digits)
        if not record_id:
            self._refresh_lead_index()
            record_id = self._lead_phone_index.get(digits)
        if not record_id:
            return None
        return _get_record(CONNECTOR.leads(), record_id)

    def find_prospect_by_phone(self, phone: str) -> Optional[Dict[str, Any]]:
        digits = spec.last_10_digits(phone)
        if not digits:
            return None
        record_id = self._prospect_phone_index.get(digits)
        if not record_id:
            self._refresh_prospect_index()
            record_id = self._prospect_phone_index.get(digits)
        if not record_id:
            return None
        return _get_record(CONNECTOR.prospects(), record_id)

    def ensure_prospect(self, phone: str) -> Dict[str, Any]:
        existing = self.find_prospect_by_phone(phone)
        if existing:
            return existing
        payload = {
            spec.CONVERSATION_FIELDS.seller_phone: phone,
            "Created At": spec.iso_now(),
        }
        record = _create_record(CONNECTOR.prospects(), payload)
        digits = spec.last_10_digits(phone)
        if digits:
            self._prospect_phone_index[digits] = record["id"]
        return record

    def promote_to_lead(self, phone: str, conversation_fields: Dict[str, Any], stage: Optional[str]) -> Dict[str, Any]:
        existing = self.find_lead_by_phone(phone)
        if existing:
            return existing

        payload = {
            spec.LEAD_FIELDS.phone: phone,
            spec.LEAD_FIELDS.last_activity: spec.iso_now(),
            spec.LEAD_FIELDS.last_direction: "INBOUND",
            spec.LEAD_FIELDS.last_message: conversation_fields.get(spec.CONVERSATION_FIELDS.message_body),
            spec.LEAD_FIELDS.lead_status: "ACTIVE COMMUNICATION",
            spec.LEAD_FIELDS.reply_count: 1,
        }
        if stage:
            payload[spec.LEAD_FIELDS.lead_status] = "ACTIVE COMMUNICATION"
        record = _create_record(CONNECTOR.leads(), payload)
        digits = spec.last_10_digits(phone)
        if digits:
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
        record = _get_record(CONNECTOR.leads(), lead_id) or {"fields": {}}
        fields = record.get("fields", {})
        payload = {
            spec.LEAD_FIELDS.last_activity: spec.iso_now(),
            spec.LEAD_FIELDS.last_direction: direction,
        }
        if body:
            payload[spec.LEAD_FIELDS.last_message] = body[:500]
        if direction == "INBOUND":
            payload[spec.LEAD_FIELDS.last_inbound] = spec.iso_now()
            reply_count = int(fields.get(spec.LEAD_FIELDS.reply_count) or 0) + 1
            payload[spec.LEAD_FIELDS.reply_count] = reply_count
        elif direction == "OUTBOUND":
            payload[spec.LEAD_FIELDS.last_outbound] = spec.iso_now()
            sent_count = int(fields.get(spec.LEAD_FIELDS.sent_count) or 0) + 1
            payload[spec.LEAD_FIELDS.sent_count] = sent_count
        if delivery_status:
            payload[spec.LEAD_FIELDS.last_delivery_status] = delivery_status
        _update_record(CONNECTOR.leads(), lead_id, payload)

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
        record = _get_record(handle, lead_id)
        fields = record.get("fields", {}) if record else {}
        payload = {}
        sent_count = int(fields.get(spec.LEAD_FIELDS.sent_count) or 0)
        if delivered_delta:
            sent_count += delivered_delta
            payload[spec.LEAD_FIELDS.last_delivery_status] = status or "DELIVERED"
        if failed_delta:
            sent_count += failed_delta
            payload[spec.LEAD_FIELDS.last_delivery_status] = status or "FAILED"
        if delivered_delta or failed_delta:
            payload[spec.LEAD_FIELDS.sent_count] = sent_count
        if payload:
            payload[spec.LEAD_FIELDS.last_activity] = spec.iso_now()
            _update_record(handle, lead_id, payload)

    # Numbers ---------------------------------------------------------------
    def increment_number_counters(self, number: str, **deltas: int) -> Dict[str, Any]:
        digits = spec.normalize_phone(number) or number
        key = digits or number
        counters = self._number_cache[key]
        for field, value in deltas.items():
            counters[field] = counters.get(field, 0) + int(value)
        return counters

    # Campaigns -------------------------------------------------------------
    def all_campaigns(self) -> List[Dict[str, Any]]:
        return _all_records(CONNECTOR.campaigns())

    def update_campaign(self, record_id: str, fields: Dict[str, Any]):
        return _update_record(CONNECTOR.campaigns(), record_id, fields)

    # Templates -------------------------------------------------------------
    def find_template(self, template_id: str) -> Optional[Dict[str, Any]]:
        handle = CONNECTOR.templates()
        records = _all_records(handle, formula=f"{{Record ID}}='{template_id}'", max_records=1)
        return _record_or_none(records)


REPOSITORY = Repository()


def reset_state() -> None:
    """Reset in-memory caches (used by tests)."""
    CONNECTOR._tables.clear()  # type: ignore[attr-defined]
    REPOSITORY._conversation_index.clear()
    REPOSITORY._prospect_phone_index.clear()
    REPOSITORY._lead_phone_index.clear()
    REPOSITORY._number_cache.clear()


# ---------------------------------------------------------------------------
# Helper functions for convenience
# ---------------------------------------------------------------------------


def ensure_prospect_or_lead(phone: str) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    lead = REPOSITORY.find_lead_by_phone(phone)
    if lead:
        return lead, None
    prospect = REPOSITORY.find_prospect_by_phone(phone)
    if prospect:
        return None, prospect
    prospect = REPOSITORY.ensure_prospect(phone)
    return None, prospect


def create_conversation(message_sid: Optional[str], fields: Dict[str, Any]) -> Dict[str, Any]:
    return REPOSITORY.create_or_update_conversation(message_sid, fields)


def update_conversation(record_id: str, fields: Dict[str, Any]) -> Dict[str, Any]:
    return _update_record(CONNECTOR.conversations(), record_id, fields)


def conversation_by_sid(message_sid: str) -> Optional[Dict[str, Any]]:
    return REPOSITORY.find_conversation_by_sid(message_sid)


def promote_if_needed(phone: str, conversation_fields: Dict[str, Any], stage: Optional[str]) -> Optional[Dict[str, Any]]:
    return REPOSITORY.promote_to_lead(phone, conversation_fields, stage)


def touch_lead(lead_id: str, *, body: Optional[str], direction: str, status: Optional[str] = None) -> None:
    REPOSITORY.touch_lead_activity(lead_id, body=body, direction=direction, delivery_status=status)


def update_lead_totals(lead_id: str, *, delivered: int = 0, failed: int = 0, status: Optional[str] = None) -> None:
    REPOSITORY.update_lead_delivery_totals(lead_id, delivered_delta=delivered, failed_delta=failed, status=status)


