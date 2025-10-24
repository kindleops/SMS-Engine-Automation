from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional, Dict, Any
from datetime import datetime, timezone

# -----------------------------
# Direct Airtable Schema Imports
# -----------------------------
from sms.airtable_schema import (
    CONVERSATIONS_TABLE,
    LEADS_TABLE,
    CAMPAIGNS_TABLE,
    TEMPLATES_TABLE,
    PROSPECTS_TABLE,
    DEALS_TABLE,
    CAMPAIGN_MANAGER_TABLE,
    NUMBERS_TABLE_DEF,
    OPTOUTS_TABLE,
    MARKETS_TABLE,
    LOGS_TABLE,
    KPIS_TABLE_DEF,
    DEVOPS_SERVICES_TABLE,
    DEVOPS_DEPLOYMENTS_TABLE,
    DEVOPS_SYSTEM_LOGS_TABLE,
    DEVOPS_INTEGRATIONS_TABLE,
    DEVOPS_HEALTH_CHECKS_TABLE,
    DEVOPS_METRICS_TABLE,
    conversations_field_map,
    leads_field_map,
    campaign_field_map,
    template_field_map,
    prospects_field_map,
    deals_field_map,
    campaign_manager_field_map,
    numbers_field_map,
    optouts_field_map,
    markets_field_map,
    logs_field_map,
    kpi_field_map,
    devops_services_field_map,
    devops_deployments_field_map,
    devops_system_logs_field_map,
    devops_integrations_field_map,
    devops_health_checks_field_map,
    devops_metrics_field_map,
)

# -----------------------------
# .env Loader
# -----------------------------
try:
    from dotenv import load_dotenv

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    ENV_PATH = os.path.join(BASE_DIR, "..", ".env")
    load_dotenv(dotenv_path=ENV_PATH, override=True)
except Exception:
    pass

# -----------------------------
# Timezone
# -----------------------------
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # type: ignore


# -----------------------------
# Env helpers
# -----------------------------
def env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "on")


def env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, default))
    except Exception:
        return default


def env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, default))
    except Exception:
        return default


def env_str(key: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(key)
    return v if (v and str(v).strip() != "") else default


# -----------------------------
# Static Field Maps
# -----------------------------
CONVERSATIONS_FIELDS = CONVERSATIONS_TABLE.field_names()
CONV_FIELDS = conversations_field_map()

LEADS_FIELDS = LEADS_TABLE.field_names()
LEAD_FIELDS = leads_field_map()

CAMPAIGN_FIELDS = CAMPAIGNS_TABLE.field_names()
CAMPAIGN_FIELD_MAP = campaign_field_map()

TEMPLATE_FIELDS = TEMPLATES_TABLE.field_names()
TEMPLATE_FIELD_MAP = template_field_map()

PROSPECT_FIELDS = PROSPECTS_TABLE.field_names()
PROSPECT_FIELD_MAP = prospects_field_map()

DEALS_FIELDS = DEALS_TABLE.field_names()
DEALS_FIELD_MAP = deals_field_map()

CAMPAIGN_MANAGER_FIELDS = CAMPAIGN_MANAGER_TABLE.field_names()
CAMPAIGN_MANAGER_FIELD_MAP = campaign_manager_field_map()

NUMBERS_FIELDS = NUMBERS_TABLE_DEF.field_names()
NUMBERS_FIELD_MAP = numbers_field_map()

OPTOUT_FIELDS = OPTOUTS_TABLE.field_names()
OPTOUT_FIELD_MAP = optouts_field_map()

MARKET_FIELDS = MARKETS_TABLE.field_names()
MARKET_FIELD_MAP = markets_field_map()

LOG_FIELDS = LOGS_TABLE.field_names()
LOG_FIELD_MAP = logs_field_map()

KPI_FIELDS = KPIS_TABLE_DEF.field_names()
KPI_FIELD_MAP = kpi_field_map()

DEVOPS_SERVICE_FIELDS = DEVOPS_SERVICES_TABLE.field_names()
DEVOPS_SERVICE_FIELD_MAP = devops_services_field_map()

DEVOPS_DEPLOYMENT_FIELDS = DEVOPS_DEPLOYMENTS_TABLE.field_names()
DEVOPS_DEPLOYMENT_FIELD_MAP = devops_deployments_field_map()

DEVOPS_SYSTEM_LOG_FIELDS = DEVOPS_SYSTEM_LOGS_TABLE.field_names()
DEVOPS_SYSTEM_LOG_FIELD_MAP = devops_system_logs_field_map()

DEVOPS_INTEGRATION_FIELDS = DEVOPS_INTEGRATIONS_TABLE.field_names()
DEVOPS_INTEGRATION_FIELD_MAP = devops_integrations_field_map()

DEVOPS_HEALTH_FIELDS = DEVOPS_HEALTH_CHECKS_TABLE.field_names()
DEVOPS_HEALTH_FIELD_MAP = devops_health_checks_field_map()

DEVOPS_METRIC_FIELDS = DEVOPS_METRICS_TABLE.field_names()
DEVOPS_METRIC_FIELD_MAP = devops_metrics_field_map()

# ✅ Canonical Drip Queue Field Map (self-contained)
DRIP_FIELD_MAP: dict[str, str] = {
    "STATUS": "Status",
    "CAMPAIGN_LINK": "Campaign",
    "TEMPLATE_LINK": "Template",
    "PROSPECT_LINK": "Prospect",
    "SELLER_PHONE": "Seller Phone Number",
    "FROM_NUMBER": "TextGrid Phone Number",  # ✅ Fix for outbound_batcher KeyError
    "MARKET": "Market",
    "MESSAGE_PREVIEW": "Message",
    "PROPERTY_ID": "Property ID",
    "NEXT_SEND_DATE": "Next Send Date",
    "NEXT_SEND_AT": "Next Send At",
    "NEXT_SEND_AT_UTC": "Next Send At (UTC)",
    "UI": "UI",
    "LAST_SENT": "Last Sent",
    "SENT_AT": "Sent At",
    "SENT_FLAG": "Sent Flag",
    "FAILED_FLAG": "Failed Flag",
    "DECLINED_FLAG": "Declined Flag",
    "LAST_ERROR": "Last Error",
}

PHONE_FIELDS = [
    "phone",
    "Phone",
    "Mobile",
    "Cell",
    "Phone Number",
    "Primary Phone",
    "Phone 1",
    "Phone 2",
    "Phone 3",
    "Owner Phone",
    "Owner Phone 1",
    "Owner Phone 2",
    "Phone 1 (from Linked Owner)",
    "Phone 2 (from Linked Owner)",
    "Phone 3 (from Linked Owner)",
]


# -----------------------------
# Settings Object
# -----------------------------
@dataclass(frozen=True)
class Settings:
    AIRTABLE_API_KEY: Optional[str]
    AIRTABLE_REPORTING_KEY: Optional[str]
    LEADS_CONVOS_BASE: Optional[str]
    CAMPAIGN_CONTROL_BASE: Optional[str]
    PERFORMANCE_BASE: Optional[str]
    PROSPECTS_TABLE: str
    LEADS_TABLE: str
    CONVERSATIONS_TABLE: str
    TEMPLATES_TABLE: str
    DRIP_QUEUE_TABLE: str
    CAMPAIGNS_TABLE: str
    CAMPAIGN_MANAGER_TABLE: str
    OPTOUTS_TABLE: str
    MARKETS_TABLE: str
    LOGS_TABLE: str
    KPIS_TABLE: str
    DEVOPS_SERVICES_TABLE: str
    DEVOPS_DEPLOYMENTS_TABLE: str
    DEVOPS_SYSTEM_LOGS_TABLE: str
    DEVOPS_INTEGRATIONS_TABLE: str
    DEVOPS_HEALTH_CHECKS_TABLE: str
    DEVOPS_METRICS_TABLE: str
    DEALS_TABLE: str
    NUMBERS_TABLE: str
    DAILY_LIMIT_DEFAULT: int
    RATE_PER_NUMBER_PER_MIN: int
    GLOBAL_RATE_PER_MIN: int
    SLEEP_BETWEEN_SENDS_SEC: float
    QUIET_TZ: str
    QUIET_START_HOUR: int
    QUIET_END_HOUR: int
    QUIET_HOURS_ENFORCED: bool
    DEDUPE_HOURS: int
    MESSAGES_PER_MIN: int
    QUEUE_JITTER_SECONDS: int
    REDIS_URL: Optional[str]
    REDIS_TLS: bool
    RUNNER_SEND_AFTER_QUEUE_DEFAULT: bool
    AUTO_BACKFILL_FROM_NUMBER: bool
    CRON_TOKEN: Optional[str]
    CAMPAIGNS_BASE_ID: Optional[str]


@lru_cache(maxsize=1)
def settings() -> Settings:
    return Settings(
        AIRTABLE_API_KEY=env_str("AIRTABLE_API_KEY"),
        AIRTABLE_REPORTING_KEY=env_str("AIRTABLE_REPORTING_KEY"),
        LEADS_CONVOS_BASE=env_str("LEADS_CONVOS_BASE") or env_str("AIRTABLE_LEADS_CONVOS_BASE_ID"),
        CAMPAIGN_CONTROL_BASE=env_str("CAMPAIGN_CONTROL_BASE") or env_str("AIRTABLE_CAMPAIGN_CONTROL_BASE_ID"),
        PERFORMANCE_BASE=env_str("PERFORMANCE_BASE") or env_str("AIRTABLE_PERFORMANCE_BASE_ID"),
        PROSPECTS_TABLE="Prospects",
        LEADS_TABLE="Leads",
        CONVERSATIONS_TABLE="Conversations",
        TEMPLATES_TABLE="Templates",
        DRIP_QUEUE_TABLE="Drip Queue",
        CAMPAIGNS_TABLE="Campaigns",
        CAMPAIGN_MANAGER_TABLE="Campaigns Manager",
        OPTOUTS_TABLE="Opt-Outs",
        MARKETS_TABLE="Markets",
        LOGS_TABLE="Logs",
        KPIS_TABLE="KPIs",
        DEVOPS_SERVICES_TABLE="Services",
        DEVOPS_DEPLOYMENTS_TABLE="Deployments",
        DEVOPS_SYSTEM_LOGS_TABLE="System Logs",
        DEVOPS_INTEGRATIONS_TABLE="Integrations",
        DEVOPS_HEALTH_CHECKS_TABLE="Health Checks",
        DEVOPS_METRICS_TABLE="Metrics",
        DEALS_TABLE="Deals",
        NUMBERS_TABLE="Numbers",
        DAILY_LIMIT_DEFAULT=750,
        RATE_PER_NUMBER_PER_MIN=20,
        GLOBAL_RATE_PER_MIN=5000,
        SLEEP_BETWEEN_SENDS_SEC=0.03,
        QUIET_TZ="America/Chicago",
        QUIET_START_HOUR=21,
        QUIET_END_HOUR=9,
        QUIET_HOURS_ENFORCED=True,
        DEDUPE_HOURS=72,
        MESSAGES_PER_MIN=20,
        QUEUE_JITTER_SECONDS=2,
        REDIS_URL=None,
        REDIS_TLS=True,
        RUNNER_SEND_AFTER_QUEUE_DEFAULT=False,
        AUTO_BACKFILL_FROM_NUMBER=True,
        CRON_TOKEN=env_str("CRON_TOKEN"),
        CAMPAIGNS_BASE_ID=env_str("CAMPAIGNS_BASE_ID"),
    )


# -----------------------------
# Time helpers
# -----------------------------
def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def tz_now() -> datetime:
    tz = ZoneInfo(settings().QUIET_TZ) if ZoneInfo else timezone.utc
    return datetime.now(tz)


def in_quiet_hours() -> bool:
    s = settings()
    if not s.QUIET_HOURS_ENFORCED:
        return False
    h = tz_now().hour
    return (h >= s.QUIET_START_HOUR) or (h < s.QUIET_END_HOUR)


# -----------------------------
# Airtable Helpers
# -----------------------------
try:
    from pyairtable import Api  # type: ignore
except Exception:
    Api = None  # type: ignore


@lru_cache(maxsize=1)
def api_main():
    s = settings()
    return Api(s.AIRTABLE_API_KEY) if (Api and s.AIRTABLE_API_KEY and s.LEADS_CONVOS_BASE) else None


@lru_cache(maxsize=1)
def api_control():
    s = settings()
    return Api(s.AIRTABLE_API_KEY) if (Api and s.AIRTABLE_API_KEY and s.CAMPAIGN_CONTROL_BASE) else None


@lru_cache(maxsize=1)
def api_perf():
    s = settings()
    key = s.AIRTABLE_REPORTING_KEY or s.AIRTABLE_API_KEY
    return Api(key) if (Api and key and s.PERFORMANCE_BASE) else None


def table_main(table_name: str):
    a = api_main()
    b = settings().LEADS_CONVOS_BASE
    return a.table(b, table_name) if a else None


def table_control(table_name: str):
    a = api_control()
    b = settings().CAMPAIGN_CONTROL_BASE
    return a.table(b, table_name) if a else None


def table_perf(table_name: str):
    a = api_perf()
    b = settings().PERFORMANCE_BASE
    return a.table(b, table_name) if a else None


# -----------------------------
# Shortcuts
# -----------------------------
@lru_cache(maxsize=None)
def drip_queue():
    return table_main(settings().DRIP_QUEUE_TABLE)
