# ops/airtable_sync.py
from __future__ import annotations
import os
import time
from typing import Any, Dict, Optional

try:
    from pyairtable import Api
except ImportError:
    Api = None  # handled at runtime


class AirtableSync:
    """
    Tiny wrapper around Airtable for structured, resilient writes.
    Uses Api.table() per pyairtable's modern style.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_id: Optional[str] = None,
        servers_table: str = "Servers",
        deployments_table: str = "Deployments",
        issues_table: str = "Issues",
        logs_table: str = "Logs",
        sms_events_table: str = "SMS_Events",
        max_retries: int = 2,
        retry_backoff: float = 0.75,
    ):
        self.api_key = api_key or os.getenv("AIRTABLE_API_KEY")
        self.base_id = base_id or os.getenv("DEVOPS_BASE") or os.getenv("PERFORMANCE_BASE")
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff

        self.servers_table = servers_table or os.getenv("SERVERS_TABLE", "Servers")
        self.deployments_table = deployments_table or os.getenv("DEPLOYMENTS_TABLE", "Deployments")
        self.issues_table = issues_table or os.getenv("ISSUES_TABLE", "Issues")
        self.logs_table = logs_table or os.getenv("LOGS_TABLE", "Logs")
        self.sms_events_table = sms_events_table or os.getenv("SMS_EVENTS_TABLE", "SMS_Events")

        self.api = Api(self.api_key) if (self.api_key and self.base_id and Api) else None

    # ---------- internals ----------
    def _table(self, name: str):
        if not self.api:
            return None
        return self.api.table(self.base_id, name)

    def _create(self, table_name: str, fields: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        t = self._table(table_name)
        if not t:
            return None
        last_err = None
        for attempt in range(self.max_retries + 1):
            try:
                return t.create(fields)
            except Exception as e:  # network/transient
                last_err = e
                time.sleep(self.retry_backoff * (attempt + 1))
        print(f"⚠️ Airtable create failed [{table_name}]: {last_err}")
        return None

    # ---------- public writers ----------
    def log_server(self, name: str, status: str, latency_ms: Optional[int] = None, meta: Dict[str, Any] | None = None):
        payload = {
            "Service": name,
            "Status": status.upper(),
            "Latency (ms)": latency_ms,
            "Meta": (meta and str(meta)) or None,
        }
        return self._create(self.servers_table, payload)

    def log_deploy(self, service: str, env: str, git_sha: str, outcome: str, meta: Dict[str, Any] | None = None):
        payload = {
            "Service": service,
            "Environment": env,
            "Git SHA": git_sha,
            "Outcome": outcome.upper(),
            "Meta": (meta and str(meta)) or None,
        }
        return self._create(self.deployments_table, payload)

    def log_issue(self, source: str, title: str, severity: str = "INFO", url: Optional[str] = None, meta: Dict[str, Any] | None = None):
        payload = {
            "Source": source,
            "Title": title,
            "Severity": severity.upper(),
            "URL": url,
            "Meta": (meta and str(meta)) or None,
        }
        return self._create(self.issues_table, payload)

    def log_error(self, service: str, message: str, severity: str = "ERROR", meta: Dict[str, Any] | None = None):
        payload = {
            "Service": service,
            "Severity": severity.upper(),
            "Message": message[:1000],
            "Meta": (meta and str(meta)) or None,
        }
        return self._create(self.logs_table, payload)

    def log_sms_event(self, event_type: str, phone: str, status: str | None = None, meta: Dict[str, Any] | None = None):
        payload = {
            "Event": event_type,   # e.g. OUTBOUND, INBOUND, OPTOUT, DELIVERY
            "Phone": phone,
            "Status": (status or "").upper() if status else None,
            "Meta": (meta and str(meta)) or None,
        }
        return self._create(self.sms_events_table, payload)