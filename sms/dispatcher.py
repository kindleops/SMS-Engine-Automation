# sms/dispatcher.py
"""
Unified SMS Engine Dispatcher (Async-Optimized)
------------------------------------------------
Central orchestration layer for outbound, retry, and inbound flows.
Now async-ready and integrated with datastore + unified logging.
"""

from __future__ import annotations
import os, time, traceback, asyncio
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import Any, Dict, Optional, TYPE_CHECKING

from sms.runtime import get_logger

if TYPE_CHECKING:
    from zoneinfo import ZoneInfo
else:
    try:
        from zoneinfo import ZoneInfo
    except Exception:
        ZoneInfo = None  # type: ignore

logger = get_logger("dispatcher")


# ---------------------------------------------------------------------------
@dataclass
class DispatchPolicy:
    quiet_tz: Optional["ZoneInfo"]
    quiet_start_hour: int
    quiet_end_hour: int
    quiet_enforced: bool
    rate_per_number_per_min: int
    global_rate_per_min: int
    daily_limit: int
    jitter_seconds: int
    retry_limit: int

    @classmethod
    def load_from_env(cls):
        tz = ZoneInfo(os.getenv("QUIET_TZ", "America/Chicago")) if ZoneInfo else None
        return cls(
            quiet_tz=tz,
            quiet_start_hour=int(os.getenv("QUIET_START_HOUR", "21")),
            quiet_end_hour=int(os.getenv("QUIET_END_HOUR", "9")),
            quiet_enforced=os.getenv("QUIET_HOURS_ENFORCED", "true").lower() in ("1", "true", "yes"),
            rate_per_number_per_min=int(os.getenv("RATE_PER_NUMBER_PER_MIN", "20")),
            global_rate_per_min=int(os.getenv("GLOBAL_RATE_PER_MIN", "5000")),
            daily_limit=int(os.getenv("DAILY_LIMIT", "750")),
            jitter_seconds=int(os.getenv("JITTER_SECONDS", "2")),
            retry_limit=int(os.getenv("RETRY_LIMIT", "3")),
        )

    def now_local(self) -> datetime:
        return datetime.now(self.quiet_tz or timezone.utc)

    def is_quiet(self, when: Optional[datetime] = None) -> bool:
        if not self.quiet_enforced:
            return False
        ref = when or self.now_local()
        return (ref.hour >= self.quiet_start_hour) or (ref.hour < self.quiet_end_hour)

    def next_quiet_end(self, when: Optional[datetime] = None) -> Optional[datetime]:
        if not self.quiet_enforced:
            return None
        ref = when or self.now_local()
        if not self.is_quiet(ref):
            return ref
        end_hour = self.quiet_end_hour
        base = ref.replace(minute=0, second=0, microsecond=0)
        if ref.hour < end_hour:
            base = base.replace(hour=end_hour)
        else:
            base = (base + timedelta(days=1)).replace(hour=end_hour)
        return base

    def jitter(self) -> int:
        return self.jitter_seconds


# Global policy instance + accessors
_POLICY = DispatchPolicy.load_from_env()


def get_policy() -> DispatchPolicy:
    return _POLICY


def refresh_policy() -> DispatchPolicy:
    global _POLICY
    _POLICY = DispatchPolicy.load_from_env()
    return _POLICY


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _std_envelope(ok: bool, typ: str, payload: Dict[str, Any], started_at: float) -> Dict[str, Any]:
    return {
        "ok": ok,
        "type": typ,
        "duration_ms": int((time.time() - started_at) * 1000),
        **(payload or {}),
    }


# ---------------------------------------------------------------------------
# Core Engine
# ---------------------------------------------------------------------------


async def run_engine(mode: str, **kwargs) -> Dict[str, Any]:
    """Unified dispatcher entrypoint."""
    started = time.time()
    mode = (mode or "").lower().strip()
    policy = get_policy()

    try:
        # Outbound Campaign Engine
        if mode == "prospects":
            from sms.campaign_runner import run_campaigns_sync

            send_after_queue = kwargs.get("send_after_queue", True)
            limit = kwargs.get("limit", 50)

            if policy.is_quiet():
                send_after_queue = False
                logger.info(f"🌙 Quiet hours active — delaying send until {policy.next_quiet_end()}")
            result = run_campaigns_sync(limit=limit, send_after_queue=send_after_queue)

            return _std_envelope(
                True,
                "Prospect",
                {
                    "result": result,
                    "quiet_hours": policy.is_quiet(),
                },
                started,
            )

        # Lead Retry Engine
        elif mode == "leads":
            from sms.retry_runner import run_retry

            retry_limit = _safe_int(kwargs.get("retry_limit", 100))
            retry_result = run_retry(limit=retry_limit)

            followups = {}
            try:
                from sms.followup_flow import run_followups

                followups = run_followups()
            except Exception:
                logger.warning("Followups failed or not enabled")

            return _std_envelope(
                True,
                "Lead",
                {
                    "retries": retry_result,
                    "followups": followups or None,
                },
                started,
            )

        # Inbound Autoresponder
        elif mode == "inbounds":
            from sms.autoresponder import run_autoresponder

            limit = _safe_int(kwargs.get("limit", 50))
            view = kwargs.get("view", "Unprocessed Inbounds")
            result = run_autoresponder(limit=limit, view=view)
            return _std_envelope(True, "Inbound", {"result": result}, started)

        else:
            return _std_envelope(
                False, "Unknown", {"error": f"Unknown mode: {mode}", "supported": ["prospects", "leads", "inbounds"]}, started
            )

    except Exception as e:
        logger.error(f"Dispatcher error in mode={mode}: {e}")
        traceback.print_exc()
        return _std_envelope(
            False,
            mode,
            {
                "error": str(e),
                "stack": traceback.format_exc(),
            },
            started,
        )
