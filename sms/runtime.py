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

_LOGGING_CONFIGURED = False
_DIGIT_PATTERN = re.compile(r"\d+")
_GLOBAL_HOOK_INSTALLED = False
_CORE_ENV_LOGGED = False


def _mask_env_value(value: Optional[str]) -> str:
    if not value:
        return "<missing>"
    trimmed = value.strip()
    if len(trimmed) <= 4:
        return "*" * len(trimmed)
    if len(trimmed) <= 8:
        return f"{trimmed[:2]}...{trimmed[-2:]}"
    return f"{trimmed[:4]}...{trimmed[-4:]}"


def _normalise_level(value: int | str | None) -> int:
    if value is None:
        env_level = os.getenv("SMS_LOG_LEVEL")
        if env_level:
            value = env_level
        else:
            return logging.INFO
    if isinstance(value, int):
        return value
    level = logging.getLevelName(value.upper())
    if isinstance(level, int):
        return level
    return logging.INFO


def configure_logging(level: int | str | None = None) -> None:
    """Initialise the root logging configuration once."""

    global _LOGGING_CONFIGURED
    if _LOGGING_CONFIGURED:
        return

    logging.basicConfig(
        level=_normalise_level(level),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    _LOGGING_CONFIGURED = True
    _log_core_env()


def get_logger(name: str = "sms") -> logging.Logger:
    """Return a module-specific logger, configuring logging on first use."""

    if not _LOGGING_CONFIGURED:
        configure_logging()
    return logging.getLogger(name)


def install_global_exception_hook() -> None:
    global _GLOBAL_HOOK_INSTALLED
    if _GLOBAL_HOOK_INSTALLED:
        return

    def _hook(exc_type, exc, tb):
        logger = get_logger("uncaught")
        logger.error("Uncaught exception (%s): %s", exc_type.__name__, exc, exc_info=(exc_type, exc, tb))

    sys.excepthook = _hook
    _GLOBAL_HOOK_INSTALLED = True
    _log_core_env()


def _log_core_env() -> None:
    global _CORE_ENV_LOGGED
    if _CORE_ENV_LOGGED:
        return
    logger = get_logger("env")
    logger.info(
        "Core env: AIRTABLE_API_KEY=%s, LEADS_CONVOS_BASE=%s, PERFORMANCE_BASE=%s, TEST_MODE=%s",
        _mask_env_value(os.getenv("AIRTABLE_API_KEY")),
        os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID") or "<missing>",
        os.getenv("PERFORMANCE_BASE") or "<missing>",
        os.getenv("TEST_MODE", "false"),
    )
    _CORE_ENV_LOGGED = True


def utc_now() -> datetime:
    """UTC timestamp helper to centralise timezone handling."""

    return datetime.now(timezone.utc)


def iso_now() -> str:
    """Return an ISO-8601 timestamp in UTC."""

    return utc_now().isoformat(timespec="seconds").replace("+00:00", "Z")


def only_digits(value: str | None) -> str:
    """Extract all digits from an arbitrary string."""

    if value is None:
        return ""
    return "".join(_DIGIT_PATTERN.findall(str(value)))


def last_10_digits(value: str | None) -> Optional[str]:
    """Return the final 10 digits of a phone-like string."""

    digits = only_digits(value)
    if len(digits) < 10:
        return None
    return digits[-10:]


def normalize_phone(value: str | None) -> Optional[str]:
    """Return an E.164 formatted phone number where possible."""

    if value is None:
        return None
    digits = only_digits(value)
    if not digits:
        return None
    if len(digits) == 10:
        digits = "1" + digits
    if len(digits) == 11 and digits.startswith("1"):
        return "+{}".format(digits)
    if value.startswith("+") and len(digits) > 0:
        return "+{}".format(digits)
    return None


def retry(
    func: Callable[[], T],
    *,
    retries: int = 3,
    base_delay: float = 0.5,
    backoff: float = 2.0,
    exceptions: Iterable[type[BaseException]] | tuple[type[BaseException], ...] = (Exception,),
    logger: Optional[logging.Logger] = None,
) -> T:
    """Retry a synchronous callable with exponential backoff."""

    log = logger or get_logger(__name__)
    attempt = 0
    while True:
        try:
            return func()
        except exceptions as exc:  # type: ignore[arg-type]
            if attempt >= retries:
                log.error("Retry exhausted after %s attempts: %s", attempt + 1, exc, exc_info=exc)
                raise
            delay = base_delay * (backoff ** attempt)
            log.warning("Retryable error (%s/%s): %s", attempt + 1, retries + 1, exc)
            time.sleep(delay)
            attempt += 1


async def retry_async(
    func: Callable[[], Awaitable[T]],
    *,
    retries: int = 3,
    base_delay: float = 0.5,
    backoff: float = 2.0,
    exceptions: Iterable[type[BaseException]] | tuple[type[BaseException], ...] = (Exception,),
    logger: Optional[logging.Logger] = None,
) -> T:
    """Retry an async callable with exponential backoff."""

    log = logger or get_logger(__name__)
    attempt = 0
    while True:
        try:
            return await func()
        except exceptions as exc:  # type: ignore[arg-type]
            if attempt >= retries:
                log.error("Async retry exhausted after %s attempts: %s", attempt + 1, exc, exc_info=exc)
                raise
            delay = base_delay * (backoff ** attempt)
            log.warning("Retryable async error (%s/%s): %s", attempt + 1, retries + 1, exc)
            await asyncio.sleep(delay)
            attempt += 1


install_global_exception_hook()
