"""
🧠 SMS Engine Runtime Core
----------------------------------------------------
Centralized utilities for logging, retries, timezone handling,
phone normalization, and environment introspection.
"""

from __future__ import annotations
import asyncio
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Awaitable, Callable, Iterable, Optional, TypeVar

T = TypeVar("T")

# Internal state flags
_LOGGING_CONFIGURED = False
_GLOBAL_HOOK_INSTALLED = False
_CORE_ENV_LOGGED = False
_DIGIT_PATTERN = re.compile(r"\d+")

# ────────────────────────────────────────────────
# ENV MASKING + LOGGING CONFIG
# ────────────────────────────────────────────────
def _mask_env_value(value: Optional[str]) -> str:
    """Mask sensitive env values (API keys, tokens, etc.)."""
    if not value:
        return "<missing>"
    trimmed = value.strip()
    if len(trimmed) <= 4:
        return "*" * len(trimmed)
    if len(trimmed) <= 8:
        return f"{trimmed[:2]}...{trimmed[-2:]}"
    return f"{trimmed[:4]}...{trimmed[-4:]}"

def _normalize_level(value: int | str | None) -> int:
    """Normalize string or int log level."""
    if value is None:
        env_level = os.getenv("SMS_LOG_LEVEL")
        if env_level:
            value = env_level
        else:
            return logging.INFO
    if isinstance(value, int):
        return value
    level = logging.getLevelName(value.upper())
    return level if isinstance(level, int) else logging.INFO

def configure_logging(level: int | str | None = None) -> None:
    """Initialize root logging configuration once."""
    global _LOGGING_CONFIGURED
    if _LOGGING_CONFIGURED:
        return
    logging.basicConfig(
        level=_normalize_level(level),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    _LOGGING_CONFIGURED = True
    _log_core_env()

def get_logger(name: str = "sms") -> logging.Logger:
    """Return module-specific logger."""
    if not _LOGGING_CONFIGURED:
        configure_logging()
    return logging.getLogger(name)

# ────────────────────────────────────────────────
# GLOBAL EXCEPTION HOOK
# ────────────────────────────────────────────────
def install_global_exception_hook() -> None:
    """Install a catch-all global exception hook (prints full traceback)."""
    global _GLOBAL_HOOK_INSTALLED
    if _GLOBAL_HOOK_INSTALLED:
        return

    def _hook(exc_type, exc, tb):
        logger = get_logger("uncaught")
        logger.error("Uncaught exception (%s): %s", exc_type.__name__, exc, exc_info=(exc_type, exc, tb))

    sys.excepthook = _hook
    _GLOBAL_HOOK_INSTALLED = True
    _log_core_env()

# ────────────────────────────────────────────────
# CORE ENV LOGGING
# ────────────────────────────────────────────────
def _log_core_env() -> None:
    """Logs masked environment variables for observability."""
    global _CORE_ENV_LOGGED
    if _CORE_ENV_LOGGED:
        return
    logger = get_logger("env")
    logger.info(
        "Core env: AIRTABLE_API_KEY=%s | LEADS_CONVOS_BASE=%s | PERFORMANCE_BASE=%s | TEST_MODE=%s",
        _mask_env_value(os.getenv("AIRTABLE_API_KEY")),
        os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID") or "<missing>",
        os.getenv("PERFORMANCE_BASE") or "<missing>",
        os.getenv("TEST_MODE", "false"),
    )
    _CORE_ENV_LOGGED = True

# ────────────────────────────────────────────────
# TIME UTILITIES
# ────────────────────────────────────────────────
def utc_now() -> datetime:
    """Return UTC datetime (always timezone-aware)."""
    return datetime.now(timezone.utc)

def iso_now() -> str:
    """Return ISO8601 UTC timestamp (Z suffix)."""
    return utc_now().isoformat(timespec="seconds").replace("+00:00", "Z")

# ────────────────────────────────────────────────
# PHONE UTILITIES
# ────────────────────────────────────────────────
def only_digits(value: str | None) -> str:
    """Extract all digits from a string."""
    if value is None:
        return ""
    return "".join(_DIGIT_PATTERN.findall(str(value)))

def last_10_digits(value: str | None) -> Optional[str]:
    """Return the last 10 digits from a phone number-like string."""
    digits = only_digits(value)
    return digits[-10:] if len(digits) >= 10 else None

def normalize_phone(value: str | None) -> Optional[str]:
    """Normalize US phone numbers to +E.164 format."""
    if not value:
        return None
    digits = only_digits(value)
    if not digits:
        return None
    if len(digits) == 10:
        digits = "1" + digits
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    if value.startswith("+") and digits:
        return f"+{digits}"
    return None

# ────────────────────────────────────────────────
# RETRY UTILITIES
# ────────────────────────────────────────────────
def retry(
    func: Callable[[], T],
    *,
    retries: int = 3,
    base_delay: float = 0.5,
    backoff: float = 2.0,
    exceptions: Iterable[type[BaseException]] = (Exception,),
    logger: Optional[logging.Logger] = None,
) -> T:
    """Retry a callable with exponential backoff."""
    log = logger or get_logger(__name__)
    attempt = 0
    while True:
        try:
            return func()
        except exceptions as exc:
            if attempt >= retries:
                log.error("Retry exhausted after %s attempts: %s", attempt + 1, exc, exc_info=exc)
                raise
            delay = base_delay * (backoff ** attempt)
            log.warning("Retryable error (%s/%s): %s — sleeping %.2fs", attempt + 1, retries + 1, exc, delay)
            time.sleep(delay)
            attempt += 1

async def retry_async(
    func: Callable[[], Awaitable[T]],
    *,
    retries: int = 3,
    base_delay: float = 0.5,
    backoff: float = 2.0,
    exceptions: Iterable[type[BaseException]] = (Exception,),
    logger: Optional[logging.Logger] = None,
) -> T:
    """Retry an async callable with exponential backoff."""
    log = logger or get_logger(__name__)
    attempt = 0
    while True:
        try:
            return await func()
        except exceptions as exc:
            if attempt >= retries:
                log.error("Async retry exhausted after %s attempts: %s", attempt + 1, exc, exc_info=exc)
                raise
            delay = base_delay * (backoff ** attempt)
            log.warning("Retryable async error (%s/%s): %s — sleeping %.2fs", attempt + 1, retries + 1, exc, delay)
            await asyncio.sleep(delay)
            attempt += 1

# ────────────────────────────────────────────────
# INIT (auto install global hook)
# ────────────────────────────────────────────────
install_global_exception_hook()