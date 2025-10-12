# sms/config.py
from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional, Dict, Any
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
# Env helpers
# -----------------------------
def env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "on")

def env_int(key: str, default: int) -> int:
    v = os.getenv(key)
    try:
        return int(v) if v is not None else default
    except Exception:
        return default

def env_float(key: str, default: float) -> float:
    v = os.getenv(key)
    try:
        return float(v) if v is not None else default
    except Exception:
        return default

def env_str(key: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(key)
    return v if (v is not None and str(v).strip() != "") else default


# -----------------------------
# Static field name maps
# -----------------------------
CONV_FIELDS = {
    "FROM": env_str("CONV_FROM_FIELD", "phone"),
    "TO": env_str("CONV_TO_FIELD", "to_number"),
    "BODY": env_str("CONV_MESSAGE_FIELD", "message"),
    "STATUS": env_str("CONV_STATUS_FIELD", "status"),
    "DIRECTION": env_str("CONV_DIRECTION_FIELD", "direction"),
    "TEXTGRID_ID": env_str("CONV_TEXTGRID_ID_FIELD", "TextGrid ID"),
    "RECEIVED_AT": env_str("CONV_RECEIVED_AT_FIELD", "received_at"),
    "INTENT": env_str("CONV_INTENT_FIELD", "intent_detected"),
    "PROCESSED_BY": env_str("CONV_PROCESSED_BY_FIELD", "processed_by"),
    "SENT_AT": env_str("CONV_SENT_AT_FIELD", "sent_at"),
}

PHONE_FIELDS = [
    "phone","Phone","Mobile","Cell","Phone Number","Primary Phone",
    "Phone 1","Phone 2","Phone 3",
    "Owner Phone","Owner Phone 1","Owner Phone 2",
    "Phone 1 (from Linked Owner)","Phone 2 (from Linked Owner)","Phone 3 (from Linked Owner)",
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
    QUIET_START_HOUR: int
    QUIET_END_HOUR: int
    QUIET_HOURS_ENFORCED: bool

    # Queue dedupe lookback
    DEDUPE_HOURS: int

    # Campaign queue pacing
    MESSAGES_PER_MIN: int
    QUEUE_JITTER_SECONDS: int

    # Redis
    REDIS_URL: Optional[str]
    REDIS_TLS: bool

    # Feature toggles
    RUNNER_SEND_AFTER_QUEUE_DEFAULT: bool
    AUTO_BACKFILL_FROM_NUMBER: bool

    # Security / API
    CRON_TOKEN: Optional[str]


@lru_cache(maxsize=1)
def settings() -> Settings:
    return Settings(
        AIRTABLE_API_KEY = env_str("AIRTABLE_API_KEY"),
        AIRTABLE_REPORTING_KEY = env_str("AIRTABLE_REPORTING_KEY"),

        LEADS_CONVOS_BASE = env_str("LEADS_CONVOS_BASE") or env_str("AIRTABLE_LEADS_CONVOS_BASE_ID"),
        CAMPAIGN_CONTROL_BASE = env_str("CAMPAIGN_CONTROL_BASE") or env_str("AIRTABLE_CAMPAIGN_CONTROL_BASE_ID"),
        PERFORMANCE_BASE = env_str("PERFORMANCE_BASE") or env_str("AIRTABLE_PERFORMANCE_BASE_ID"),

        PROSPECTS_TABLE = env_str("PROSPECTS_TABLE", "Prospects"),
        LEADS_TABLE = env_str("LEADS_TABLE", "Leads"),
        CONVERSATIONS_TABLE = env_str("CONVERSATIONS_TABLE", "Conversations"),
        TEMPLATES_TABLE = env_str("TEMPLATES_TABLE", "Templates"),
        DRIP_QUEUE_TABLE = env_str("DRIP_QUEUE_TABLE", "Drip Queue"),
        CAMPAIGNS_TABLE = env_str("CAMPAIGNS_TABLE", "Campaigns"),

        NUMBERS_TABLE = env_str("NUMBERS_TABLE", "Numbers"),

        DAILY_LIMIT_DEFAULT = env_int("DAILY_LIMIT", 750),
        RATE_PER_NUMBER_PER_MIN = env_int("RATE_PER_NUMBER_PER_MIN", 20),
        GLOBAL_RATE_PER_MIN = env_int("GLOBAL_RATE_PER_MIN", 5000),
        SLEEP_BETWEEN_SENDS_SEC = env_float("SLEEP_BETWEEN_SENDS_SEC", 0.03),

        QUIET_TZ = env_str("QUIET_TZ", "America/Chicago") or "America/Chicago",
        QUIET_START_HOUR = env_int("QUIET_START_HOUR", 21),
        QUIET_END_HOUR = env_int("QUIET_END_HOUR", 9),
        QUIET_HOURS_ENFORCED = env_bool("QUIET_HOURS_ENFORCED", True),

        DEDUPE_HOURS = env_int("DEDUPE_HOURS", 72),

        MESSAGES_PER_MIN = env_int("MESSAGES_PER_MIN", 20),
        QUEUE_JITTER_SECONDS = env_int("JITTER_SECONDS", 2),

        REDIS_URL = env_str("REDIS_URL") or env_str("UPSTASH_REDIS_URL") or env_str("UPSTASH_REDIS_REST_URL"),
        REDIS_TLS = env_bool("REDIS_TLS", True),

        RUNNER_SEND_AFTER_QUEUE_DEFAULT = env_bool("RUNNER_SEND_AFTER_QUEUE", False),
        AUTO_BACKFILL_FROM_NUMBER = env_bool("AUTO_BACKFILL_FROM_NUMBER", True),

        CRON_TOKEN = env_str("CRON_TOKEN"),
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
def conversations(): return table_main(settings().CONVERSATIONS_TABLE)
@lru_cache(maxsize=None)
def leads():         return table_main(settings().LEADS_TABLE)
@lru_cache(maxsize=None)
def prospects():     return table_main(settings().PROSPECTS_TABLE)
@lru_cache(maxsize=None)
def templates():     return table_main(settings().TEMPLATES_TABLE)
@lru_cache(maxsize=None)
def drip_queue():    return table_main(settings().DRIP_QUEUE_TABLE)
@lru_cache(maxsize=None)
def campaigns():     return table_main(settings().CAMPAIGNS_TABLE)
@lru_cache(maxsize=None)
def numbers():       return table_control(settings().NUMBERS_TABLE)
@lru_cache(maxsize=None)
def runs_logs():     return table_perf("Runs/Logs")
@lru_cache(maxsize=None)
def kpis():          return table_perf("KPIs")


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
AIRTABLE_API_KEY: Optional[str]      = S.AIRTABLE_API_KEY
AIRTABLE_REPORTING_KEY: Optional[str]= S.AIRTABLE_REPORTING_KEY
LEADS_CONVOS_BASE: Optional[str]     = S.LEADS_CONVOS_BASE
CAMPAIGN_CONTROL_BASE: Optional[str] = S.CAMPAIGN_CONTROL_BASE
PERFORMANCE_BASE: Optional[str]      = S.PERFORMANCE_BASE

__all__ = [
    "settings",
    # time helpers
    "utcnow", "tz_now", "in_quiet_hours",
    # tables
    "conversations","leads","prospects","templates","drip_queue","campaigns","numbers","runs_logs","kpis",
    "table_main","table_control","table_perf",
    # helpers
    "CONV_FIELDS","PHONE_FIELDS","remap_existing_only","auto_field_map","norm",
    # back-compat names
    "AIRTABLE_API_KEY","AIRTABLE_REPORTING_KEY","LEADS_CONVOS_BASE","CAMPAIGN_CONTROL_BASE","PERFORMANCE_BASE",
]