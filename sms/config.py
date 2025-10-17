# sms/config.py
from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional, Dict, Any, Tuple
from datetime import datetime, timezone

# .env loader (safe if missing)
try:
    from dotenv import load_dotenv  # type: ignore

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    ENV_PATH = os.path.join(BASE_DIR, "..", ".env")
    load_dotenv(dotenv_path=ENV_PATH, override=True)
except Exception:
    pass

# Optional zoneinfo
try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:
    ZoneInfo = None  # type: ignore


# -----------------------------
# Contract constants (README2 authoritative)
# -----------------------------


class EnvVars:
    """Authoritative environment variable names from README2."""

    AIRTABLE_API_KEY = "AIRTABLE_API_KEY"
    LEADS_CONVOS_BASE = "LEADS_CONVOS_BASE"
    AIRTABLE_LEADS_CONVOS_BASE_ID = "AIRTABLE_LEADS_CONVOS_BASE_ID"
    CONVERSATIONS_TABLE = "CONVERSATIONS_TABLE"
    LEADS_TABLE = "LEADS_TABLE"
    CAMPAIGNS_TABLE = "CAMPAIGNS_TABLE"
    TEMPLATES_TABLE = "TEMPLATES_TABLE"
    PROSPECTS_TABLE = "PROSPECTS_TABLE"
    NUMBERS_TABLE = "NUMBERS_TABLE"
    WEBHOOK_TOKEN = "WEBHOOK_TOKEN"
    CRON_TOKEN = "CRON_TOKEN"
    QUIET_HOURS_ENFORCED = "QUIET_HOURS_ENFORCED"
    QUIET_START_HOUR_LOCAL = "QUIET_START_HOUR_LOCAL"
    QUIET_END_HOUR_LOCAL = "QUIET_END_HOUR_LOCAL"
    RATE_PER_NUMBER_PER_MIN = "RATE_PER_NUMBER_PER_MIN"
    GLOBAL_RATE_PER_MIN = "GLOBAL_RATE_PER_MIN"
    DAILY_LIMIT = "DAILY_LIMIT"
    JITTER_SECONDS = "JITTER_SECONDS"
    WORKER_INTERVAL_SEC = "WORKER_INTERVAL_SEC"
    SEND_BATCH_LIMIT = "SEND_BATCH_LIMIT"
    RETRY_LIMIT = "RETRY_LIMIT"
    TEXTGRID_ACCOUNT_SID = "TEXTGRID_ACCOUNT_SID"
    TEXTGRID_AUTH_TOKEN = "TEXTGRID_AUTH_TOKEN"
    UPSTASH_REDIS_REST_URL = "UPSTASH_REDIS_REST_URL"
    UPSTASH_REDIS_REST_TOKEN = "UPSTASH_REDIS_REST_TOKEN"
    LOG_LEVEL = "LOG_LEVEL"
    AIRTABLE_CAMPAIGN_CONTROL_BASE_ID = "AIRTABLE_CAMPAIGN_CONTROL_BASE_ID"
    AIRTABLE_PERFORMANCE_BASE_ID = "AIRTABLE_PERFORMANCE_BASE_ID"
    CAMPAIGN_CONTROL_BASE = "CAMPAIGN_CONTROL_BASE"
    PERFORMANCE_BASE = "PERFORMANCE_BASE"
    AIRTABLE_REPORTING_KEY = "AIRTABLE_REPORTING_KEY"
    AIRTABLE_CAMPAIGN_CONTROL_KEY = "AIRTABLE_COMPLIANCE_KEY"
    AIRTABLE_ACQUISITIONS_KEY = "AIRTABLE_ACQUISITIONS_KEY"
    AIRTABLE_PERFORMANCE_KEY = "AIRTABLE_REPORTING_KEY"


EXPECTED_ENV_VARS: Tuple[str, ...] = (
    EnvVars.AIRTABLE_API_KEY,
    EnvVars.LEADS_CONVOS_BASE,
    EnvVars.AIRTABLE_LEADS_CONVOS_BASE_ID,
    EnvVars.CONVERSATIONS_TABLE,
    EnvVars.LEADS_TABLE,
    EnvVars.CAMPAIGNS_TABLE,
    EnvVars.TEMPLATES_TABLE,
    EnvVars.PROSPECTS_TABLE,
    EnvVars.NUMBERS_TABLE,
    EnvVars.WEBHOOK_TOKEN,
    EnvVars.CRON_TOKEN,
    EnvVars.QUIET_HOURS_ENFORCED,
    EnvVars.QUIET_START_HOUR_LOCAL,
    EnvVars.QUIET_END_HOUR_LOCAL,
    EnvVars.RATE_PER_NUMBER_PER_MIN,
    EnvVars.GLOBAL_RATE_PER_MIN,
    EnvVars.DAILY_LIMIT,
    EnvVars.JITTER_SECONDS,
    EnvVars.WORKER_INTERVAL_SEC,
    EnvVars.SEND_BATCH_LIMIT,
    EnvVars.RETRY_LIMIT,
    EnvVars.TEXTGRID_ACCOUNT_SID,
    EnvVars.TEXTGRID_AUTH_TOKEN,
    EnvVars.UPSTASH_REDIS_REST_URL,
    EnvVars.UPSTASH_REDIS_REST_TOKEN,
    EnvVars.LOG_LEVEL,
)


LEGACY_ENV_VAR_SYNONYMS = {
    "QUIET_START_HOUR": EnvVars.QUIET_START_HOUR_LOCAL,
    "QUIET_END_HOUR": EnvVars.QUIET_END_HOUR_LOCAL,
    "REDIS_URL": EnvVars.UPSTASH_REDIS_REST_URL,
    "UPSTASH_REDIS_URL": EnvVars.UPSTASH_REDIS_REST_URL,
}


def _check_legacy_envs() -> None:
    legacy_hits = [k for k in LEGACY_ENV_VAR_SYNONYMS if os.getenv(k)]
    if legacy_hits:
        mapped = ", ".join(
            f"{key}→{LEGACY_ENV_VAR_SYNONYMS[key]}" for key in sorted(legacy_hits)
        )
        print(
            "⚠️ Legacy environment variable names detected. Update deployment to use "
            f"README2 contract: {mapped}."
        )


_check_legacy_envs()


@dataclass(frozen=True)
class ConversationFields:
    STAGE: str = "Stage"
    PROCESSED_BY: str = "Processed By"
    INTENT_DETECTED: str = "Intent Detected"
    DIRECTION: str = "Direction"
    DELIVERY_STATUS: str = "Delivery Status"
    AI_INTENT: str = "AI Intent"
    TEXTGRID_PHONE_NUMBER: str = "TextGrid Phone Number"
    TEXTGRID_ID: str = "TextGrid ID"
    TEMPLATE_RECORD_ID: str = "Template Record ID"
    SELLER_PHONE_NUMBER: str = "Seller Phone Number"
    PROSPECT_RECORD_ID: str = "Prospect Record ID"
    LEAD_LINK: str = "Lead"
    LEAD_RECORD_ID: str = "Lead Record ID"
    CAMPAIGN_RECORD_ID: str = "Campaign Record ID"
    SENT_COUNT: str = "Sent Count"
    REPLY_COUNT: str = "Reply Count"
    MESSAGE_SUMMARY: str = "Message Summary (AI)"
    MESSAGE_LONG_TEXT: str = "Message Long text"
    TEMPLATE_LINK: str = "Template"
    PROSPECTS_LINK: str = "Prospects"
    PROSPECT_LINK: str = "Prospect"
    LEAD_STATUS_LOOKUP: str = "Lead Status (from Lead)"
    CAMPAIGN_LINK: str = "Campaign"
    RESPONSE_TIME_MINUTES: str = "Response Time (Minutes)"
    RECORD_ID: str = "Record ID"
    RECEIVED_TIME: str = "Received Time"
    PROCESSED_TIME: str = "Processed Time"
    LAST_SENT_TIME: str = "Last Sent Time"
    LAST_RETRY_TIME: str = "Last Retry Time"
    AI_RESPONSE_TRIGGER: str = "AI Response Trigger"


@dataclass(frozen=True)
class LeadFields:
    LEAD_STATUS: str = "Lead Status"
    LAST_ACTIVITY: str = "Last Activity"
    LAST_DIRECTION: str = "Last Direction"
    LAST_MESSAGE: str = "Last Message"
    LAST_INBOUND: str = "Last Inbound"
    LAST_OUTBOUND: str = "Last Outbound"
    REPLY_COUNT: str = "Reply Count"
    SENT_COUNT: str = "Sent Count"
    RESPONSE_TIME: str = "Response Time (Minutes)"
    PHONE: str = "Phone"
    CAMPAIGNS_LINK: str = "Campaigns"
    CONVERSATIONS_LINK: str = "Conversations"
    RECORD_ID: str = "Record ID"


@dataclass(frozen=True)
class NumbersFields:
    NUMBER: str = "Number"
    FRIENDLY_NAME: str = "Friendly Name"
    MARKET: str = "Market"
    MARKETS_MULTI: str = "Markets"
    ACTIVE: str = "Active"
    STATUS: str = "Status"
    SENT_TODAY: str = "Sent Today"
    DELIVERED_TODAY: str = "Delivered Today"
    FAILED_TODAY: str = "Failed Today"
    OPTOUT_TODAY: str = "Opt-Outs Today"
    SENT_TOTAL: str = "Sent Total"
    DELIVERED_TOTAL: str = "Delivered Total"
    FAILED_TOTAL: str = "Failed Total"
    OPTOUT_TOTAL: str = "Opt-Outs Total"
    REMAINING: str = "Remaining"
    DAILY_RESET: str = "Daily Reset"
    LAST_USED: str = "Last Used"


CONVERSATION_FIELDS = ConversationFields()
LEAD_FIELDS = LeadFields()
NUMBERS_FIELDS = NumbersFields()


# -----------------------------
# Env helpers
# -----------------------------
def _env_get(key: str) -> Optional[str]:
    return os.getenv(key)


def env_bool(key: str, default: bool = False) -> bool:
    v = _env_get(key)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "on")


def env_int(key: str, default: int) -> int:
    v = _env_get(key)
    try:
        return int(v) if v is not None else default
    except Exception:
        return default


def env_float(key: str, default: float) -> float:
    v = _env_get(key)
    try:
        return float(v) if v is not None else default
    except Exception:
        return default


def env_str(key: str, default: Optional[str] = None) -> Optional[str]:
    v = _env_get(key)
    return v if (v is not None and str(v).strip() != "") else default


# -----------------------------
# Static field name maps
# -----------------------------
CONV_FIELDS = {
    "FROM": env_str("CONV_FROM_FIELD", CONVERSATION_FIELDS.SELLER_PHONE_NUMBER),
    "TO": env_str("CONV_TO_FIELD", CONVERSATION_FIELDS.TEXTGRID_PHONE_NUMBER),
    "BODY": env_str("CONV_MESSAGE_FIELD", CONVERSATION_FIELDS.MESSAGE_LONG_TEXT),
    "STATUS": env_str("CONV_STATUS_FIELD", CONVERSATION_FIELDS.DELIVERY_STATUS),
    "DIRECTION": env_str("CONV_DIRECTION_FIELD", CONVERSATION_FIELDS.DIRECTION),
    "TEXTGRID_ID": env_str("CONV_TEXTGRID_ID_FIELD", CONVERSATION_FIELDS.TEXTGRID_ID),
    "RECEIVED_AT": env_str("CONV_RECEIVED_AT_FIELD", CONVERSATION_FIELDS.RECEIVED_TIME),
    "INTENT": env_str("CONV_INTENT_FIELD", CONVERSATION_FIELDS.INTENT_DETECTED),
    "PROCESSED_BY": env_str("CONV_PROCESSED_BY_FIELD", CONVERSATION_FIELDS.PROCESSED_BY),
    "SENT_AT": env_str("CONV_SENT_AT_FIELD", CONVERSATION_FIELDS.LAST_SENT_TIME),
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
    "Seller Phone Number",
]


# -----------------------------
# Settings object
# -----------------------------
@dataclass(frozen=True)
class Settings:
    # Keys
    AIRTABLE_API_KEY: Optional[str]
    AIRTABLE_REPORTING_KEY: Optional[str]

    # Bases
    LEADS_CONVOS_BASE: Optional[str]
    CAMPAIGN_CONTROL_BASE: Optional[str]
    PERFORMANCE_BASE: Optional[str]

    # Tables (main base)
    PROSPECTS_TABLE: str
    LEADS_TABLE: str
    CONVERSATIONS_TABLE: str
    TEMPLATES_TABLE: str
    DRIP_QUEUE_TABLE: str
    CAMPAIGNS_TABLE: str

    # Tables (control base)
    NUMBERS_TABLE: str

    # Messaging limits / pacing
    DAILY_LIMIT_DEFAULT: int
    RATE_PER_NUMBER_PER_MIN: int
    GLOBAL_RATE_PER_MIN: int
    SLEEP_BETWEEN_SENDS_SEC: float

    # Quiet hours (America/Chicago by default)
    QUIET_TZ: str
    QUIET_START_HOUR_LOCAL: int
    QUIET_END_HOUR_LOCAL: int
    QUIET_HOURS_ENFORCED: bool

    # Queue dedupe lookback
    DEDUPE_HOURS: int

    # Campaign queue pacing
    MESSAGES_PER_MIN: int
    QUEUE_JITTER_SECONDS: int

    # Redis
    REDIS_REST_URL: Optional[str]
    REDIS_REST_TOKEN: Optional[str]
    REDIS_TLS: bool

    # Feature toggles
    RUNNER_SEND_AFTER_QUEUE_DEFAULT: bool
    AUTO_BACKFILL_FROM_NUMBER: bool

    # Security / API
    CRON_TOKEN: Optional[str]
    WEBHOOK_TOKEN: Optional[str]
    LOG_LEVEL: str


@lru_cache(maxsize=1)
def settings() -> Settings:
    return Settings(
        AIRTABLE_API_KEY=env_str(EnvVars.AIRTABLE_API_KEY),
        AIRTABLE_REPORTING_KEY=env_str(EnvVars.AIRTABLE_REPORTING_KEY),
        LEADS_CONVOS_BASE=env_str(EnvVars.LEADS_CONVOS_BASE)
        or env_str(EnvVars.AIRTABLE_LEADS_CONVOS_BASE_ID),
        CAMPAIGN_CONTROL_BASE=env_str(EnvVars.CAMPAIGN_CONTROL_BASE)
        or env_str(EnvVars.AIRTABLE_CAMPAIGN_CONTROL_BASE_ID),
        PERFORMANCE_BASE=env_str(EnvVars.PERFORMANCE_BASE)
        or env_str(EnvVars.AIRTABLE_PERFORMANCE_BASE_ID),
        PROSPECTS_TABLE=env_str(EnvVars.PROSPECTS_TABLE, "Prospects"),
        LEADS_TABLE=env_str(EnvVars.LEADS_TABLE, "Leads"),
        CONVERSATIONS_TABLE=env_str(EnvVars.CONVERSATIONS_TABLE, "Conversations"),
        TEMPLATES_TABLE=env_str(EnvVars.TEMPLATES_TABLE, "Templates"),
        DRIP_QUEUE_TABLE=env_str("DRIP_QUEUE_TABLE", "Drip Queue"),
        CAMPAIGNS_TABLE=env_str(EnvVars.CAMPAIGNS_TABLE, "Campaigns"),
        NUMBERS_TABLE=env_str(EnvVars.NUMBERS_TABLE, "Numbers"),
        DAILY_LIMIT_DEFAULT=env_int(EnvVars.DAILY_LIMIT, 750),
        RATE_PER_NUMBER_PER_MIN=env_int(EnvVars.RATE_PER_NUMBER_PER_MIN, 20),
        GLOBAL_RATE_PER_MIN=env_int(EnvVars.GLOBAL_RATE_PER_MIN, 5000),
        SLEEP_BETWEEN_SENDS_SEC=env_float("SLEEP_BETWEEN_SENDS_SEC", 0.03),
        QUIET_TZ=env_str("QUIET_TZ", "America/Chicago") or "America/Chicago",
        QUIET_START_HOUR_LOCAL=env_int(EnvVars.QUIET_START_HOUR_LOCAL, 21),
        QUIET_END_HOUR_LOCAL=env_int(EnvVars.QUIET_END_HOUR_LOCAL, 9),
        QUIET_HOURS_ENFORCED=env_bool(EnvVars.QUIET_HOURS_ENFORCED, True),
        DEDUPE_HOURS=env_int("DEDUPE_HOURS", 72),
        MESSAGES_PER_MIN=env_int("MESSAGES_PER_MIN", 20),
        QUEUE_JITTER_SECONDS=env_int(EnvVars.JITTER_SECONDS, 2),
        REDIS_REST_URL=env_str(EnvVars.UPSTASH_REDIS_REST_URL),
        REDIS_REST_TOKEN=env_str(EnvVars.UPSTASH_REDIS_REST_TOKEN),
        REDIS_TLS=env_bool("REDIS_TLS", True),
        RUNNER_SEND_AFTER_QUEUE_DEFAULT=env_bool("RUNNER_SEND_AFTER_QUEUE", False),
        AUTO_BACKFILL_FROM_NUMBER=env_bool("AUTO_BACKFILL_FROM_NUMBER", True),
        CRON_TOKEN=env_str(EnvVars.CRON_TOKEN),
        WEBHOOK_TOKEN=env_str(EnvVars.WEBHOOK_TOKEN),
        LOG_LEVEL=env_str(EnvVars.LOG_LEVEL, "info") or "info",
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
    return (h >= s.QUIET_START_HOUR_LOCAL) or (h < s.QUIET_END_HOUR_LOCAL)


# -----------------------------
# Airtable table shorthands
# (These return pyairtable.Table if configured, else None)
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
    return a.table(b, table_name) if a else None  # type: ignore[union-attr]


def table_control(table_name: str):
    a = api_control()
    b = settings().CAMPAIGN_CONTROL_BASE
    return a.table(b, table_name) if a else None  # type: ignore[union-attr]


def table_perf(table_name: str):
    a = api_perf()
    b = settings().PERFORMANCE_BASE
    return a.table(b, table_name) if a else None  # type: ignore[union-attr]


# Shorthand resolvers (cached)
@lru_cache(maxsize=None)
def conversations():
    return table_main(settings().CONVERSATIONS_TABLE)


@lru_cache(maxsize=None)
def leads():
    return table_main(settings().LEADS_TABLE)


@lru_cache(maxsize=None)
def prospects():
    return table_main(settings().PROSPECTS_TABLE)


@lru_cache(maxsize=None)
def templates():
    return table_main(settings().TEMPLATES_TABLE)


@lru_cache(maxsize=None)
def drip_queue():
    return table_main(settings().DRIP_QUEUE_TABLE)


@lru_cache(maxsize=None)
def campaigns():
    return table_main(settings().CAMPAIGNS_TABLE)


@lru_cache(maxsize=None)
def numbers():
    return table_control(settings().NUMBERS_TABLE)


@lru_cache(maxsize=None)
def runs_logs():
    return table_perf("Runs/Logs")


@lru_cache(maxsize=None)
def kpis():
    return table_perf("KPIs")


# -----------------------------
# Field-safe mapping helpers
# -----------------------------
def norm(s: Any) -> Any:
    import re

    return re.sub(r"[^a-z0-9]+", "", s.strip().lower()) if isinstance(s, str) else s


def auto_field_map(tbl) -> Dict[str, str]:
    try:
        probe = tbl.all(max_records=1)  # type: ignore[attr-defined]
        keys = list((probe[0] or {}).get("fields", {}).keys()) if probe else []
    except Exception:
        keys = []
    return {norm(k): k for k in keys}


def remap_existing_only(tbl, payload: Dict[str, Any]) -> Dict[str, Any]:
    amap = auto_field_map(tbl)
    out: Dict[str, Any] = {}
    for k, v in (payload or {}).items():
        ak = amap.get(norm(k))
        if ak:
            out[ak] = v
    return out


# -----------------------------
# Back-compat module-level exports
# (Some older modules expect these names on sms.config)
# -----------------------------
S = settings()
AIRTABLE_API_KEY: Optional[str] = S.AIRTABLE_API_KEY
AIRTABLE_REPORTING_KEY: Optional[str] = S.AIRTABLE_REPORTING_KEY
LEADS_CONVOS_BASE: Optional[str] = S.LEADS_CONVOS_BASE
CAMPAIGN_CONTROL_BASE: Optional[str] = S.CAMPAIGN_CONTROL_BASE
PERFORMANCE_BASE: Optional[str] = S.PERFORMANCE_BASE

__all__ = [
    "EnvVars",
    "CONVERSATION_FIELDS",
    "LEAD_FIELDS",
    "NUMBERS_FIELDS",
    "settings",
    # time helpers
    "utcnow",
    "tz_now",
    "in_quiet_hours",
    # tables
    "conversations",
    "leads",
    "prospects",
    "templates",
    "drip_queue",
    "campaigns",
    "numbers",
    "runs_logs",
    "kpis",
    "table_main",
    "table_control",
    "table_perf",
    # helpers
    "CONV_FIELDS",
    "PHONE_FIELDS",
    "remap_existing_only",
    "auto_field_map",
    "norm",
    # back-compat names
    "AIRTABLE_API_KEY",
    "AIRTABLE_REPORTING_KEY",
    "LEADS_CONVOS_BASE",
    "CAMPAIGN_CONTROL_BASE",
    "PERFORMANCE_BASE",
]
