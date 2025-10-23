# sms/dispatcher.py
"""
Unified SMS Engine Dispatcher
--------------------------------
Central orchestration layer for the automation system.

Responsible for:
  • Coordinating Prospects → Drip Queue → Outbound sends
  • Handling Lead retry and follow-up flows
  • Running inbound autoresponder (24/7 safe)
  • Respecting quiet-hour policy, rate limits, and retry budgets

All modules import this file as the canonical entrypoint for timed jobs.
"""

from __future__ import annotations

import os
import time
import traceback
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import Any, Dict, Optional

# ---------------------------------------------------------------------------
# Optional zoneinfo + follow-up modules
# ---------------------------------------------------------------------------

try:
    from zoneinfo import ZoneInfo
except Exception:
    from typing import Any as ZoneInfo  # type: ignore

try:
    from sms.followup_flow import run_followups  # noqa
    _HAS_FOLLOWUPS = True
except Exception:
    _HAS_FOLLOWUPS = False


# ---------------------------------------------------------------------------
# Dispatch policy configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class DispatchPolicy:
    """Authoritative runtime configuration for send cadence and quiet hours."""

    quiet_tz: Optional[ZoneInfo]
    quiet_start_hour: int
    quiet_end_hour: int
    quiet_enforced: bool
    rate_per_number_per_min: int
    global_rate_per_min: int
    daily_limit: int
    jitter_seconds: int
    send_batch_limit: int
    retry_limit: int

    @classmethod
    def load_from_env(cls) -> DispatchPolicy:
        tz = ZoneInfo(os.getenv("QUIET_TZ", "America/Chicago")) if ZoneInfo else None
        return cls(
            quiet_tz=tz,
            quiet_start_hour=int(os.getenv("QUIET_START_HOUR", "21")),
            quiet_end_hour=int(os.getenv("QUIET_END_HOUR", "9")),
            quiet_enforced=os.getenv("QUIET_HOURS_ENFORCED", "true").lower()
            in ("1", "true", "yes"),
            rate_per_number_per_min=int(os.getenv("RATE_PER_NUMBER_PER_MIN", "20")),
            global_rate_per_min=int(os.getenv("GLOBAL_RATE_PER_MIN", "5000")),
            daily_limit=int(os.getenv("DAILY_LIMIT", "750")),
            jitter_seconds=int(os.getenv("JITTER_SECONDS", "2")),
            send_batch_limit=int(os.getenv("SEND_BATCH_LIMIT", "500")),
            retry_limit=int(os.getenv("RETRY_LIMIT", "3")),
        )

    # -----------------------------------------------------------------------
    # Quiet hours logic
    # -----------------------------------------------------------------------
    def now_local(self) -> datetime:
        if self.quiet_tz:
            return datetime.now(self.quiet_tz)
        return datetime.now(timezone.utc)

    def is_quiet(self, when: Optional[datetime] = None) -> bool:
        """Returns True if current time is within quiet hours (local time)."""
        if not self.quiet_enforced:
            return False
        ref = when or self.now_local()
        hour = ref.hour
        return (hour >= self.quiet_start_hour) or (hour < self.quiet_end_hour)

    def next_quiet_end(self, when: Optional[datetime] = None) -> Optional[datetime]:
        """Return datetime when quiet hours end, or None if not enforced."""
        if not self.quiet_enforced:
            return None
        ref = when or self.now_local()
        if not self.is_quiet(ref):
            return ref
        end_hour = self.quiet_end_hour
        local = ref.replace(minute=0, second=0, microsecond=0)
        if ref.hour < end_hour:
            local = local.replace(hour=end_hour)
        else:
            local = (local + timedelta(days=1)).replace(hour=end_hour)
        return local

    # -----------------------------------------------------------------------
    # Rate / jitter helpers
    # -----------------------------------------------------------------------
    def rate_limits(self) -> Dict[str, int]:
        return {
            "per_number_per_min": self.rate_per_number_per_min,
            "global_per_min": self.global_rate_per_min,
            "daily_limit": self.daily_limit,
        }

    def jitter(self) -> int:
        return self.jitter_seconds

    def retry_budget(self) -> int:
        return self.retry_limit


# ---------------------------------------------------------------------------
# Global policy cache (hot-reloadable)
# ---------------------------------------------------------------------------

_POLICY = DispatchPolicy.load_from_env()


def refresh_policy() -> DispatchPolicy:
    """Force a reload of DispatchPolicy from environment variables."""
    global _POLICY
    _POLICY = DispatchPolicy.load_from_env()
    return _POLICY


def get_policy() -> DispatchPolicy:
    """Return the active policy object."""
    return _POLICY


def _is_quiet_hours_outbound() -> bool:
    return get_policy().is_quiet()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _safe_int(v: Any, default: int) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _summarize_prospect_result(res: Dict[str, Any]) -> Dict[str, int]:
    """Aggregate campaign-runner result into standardized totals."""
    totals = {"processed_campaigns": 0, "queued": 0, "sent": 0, "retries": 0, "errors": 0}
    if not isinstance(res, dict):
        return totals
    totals["processed_campaigns"] = _safe_int(res.get("processed", 0), 0)
    for item in res.get("results", []) or []:
        if not isinstance(item, dict):
            continue
        totals["queued"] += _safe_int(item.get("queued", 0), 0)
        totals["sent"] += _safe_int(item.get("sent", 0), 0)
        totals["retries"] += _safe_int(item.get("retries", 0), 0)
    totals["errors"] = len(res.get("errors") or [])
    return totals


def _std_envelope(ok: bool, typ: str, payload: Dict[str, Any], started_at: float) -> Dict[str, Any]:
    """Uniform return envelope for API, CLI, or scheduler logging."""
    payload = payload or {}
    return {
        "ok": ok,
        "type": typ,
        "duration_ms": int((time.time() - started_at) * 1000),
        **payload,
    }


# ---------------------------------------------------------------------------
# Dispatcher Entrypoint
# ---------------------------------------------------------------------------

def run_engine(mode: str, **kwargs) -> dict:
    """
    Unified dispatcher for all SMS engines.

    Modes:
      • "prospects" → Outbound campaigns (Prospects → Drip Queue → optional immediate send)
      • "leads"     → Retry loop + optional follow-ups
      • "inbounds"  → Autoresponder (24/7 safe)

    kwargs:
      prospects:
        - limit: int | "ALL"
        - send_after_queue: bool
      leads:
        - retry_limit: int
      inbounds:
        - limit: int
        - view: str
    """
    started = time.time()
    mode = (mode or "").lower().strip()

    try:
        # -------------------------------------------------------------------
        # Prospect Outbound Engine
        # -------------------------------------------------------------------
        if mode == "prospects":
            send_after_queue = kwargs.get("send_after_queue", True)

            # Respect quiet hours
            if _is_quiet_hours_outbound():
                send_after_queue = False
                next_end = get_policy().next_quiet_end()
                print(f"🌙 Quiet hours active — delaying send until {next_end}")

            limit = kwargs.get("limit", "ALL")
            result = _get_run_campaigns()(limit=limit, send_after_queue=send_after_queue)

            totals = _summarize_prospect_result(result)
            return _std_envelope(
                True,
                "Prospect",
                {
                    "result": result,
                    "totals": totals,
                    "quiet_hours": _is_quiet_hours_outbound(),
                },
                started,
            )

        # -------------------------------------------------------------------
        # Lead Retry Engine
        # -------------------------------------------------------------------
        elif mode == "leads":
            retry_limit = _safe_int(kwargs.get("retry_limit", 100), 100)
            retry_result = _get_run_retry()(limit=retry_limit)

            followups: Dict[str, Any] = {}
            if _HAS_FOLLOWUPS:
                try:
                    followups = run_followups()
                except Exception:
                    traceback.print_exc()
                    followups = {"ok": False, "error": "followups_failed"}

            return _std_envelope(
                True,
                "Lead",
                {
                    "retries": retry_result,
                    "followups": followups if _HAS_FOLLOWUPS else None,
                    "processed": _safe_int(retry_result.get("retried", 0), 0),
                },
                started,
            )

        # -------------------------------------------------------------------
        # Inbound Handler
        # -------------------------------------------------------------------
        elif mode == "inbounds":
            limit = _safe_int(kwargs.get("limit", 50), 50)
            view = kwargs.get("view", "Unprocessed Inbounds")

            result = _get_run_autoresponder()(limit=limit, view=view)
            return _std_envelope(
                True,
                "Inbound",
                {
                    "result": result,
                    "processed": _safe_int(result.get("processed", 0), 0),
                },
                started,
            )

        # -------------------------------------------------------------------
        # Unknown Mode
        # -------------------------------------------------------------------
        else:
            return _std_envelope(
                False,
                "Unknown",
                {
                    "error": f"Unknown mode: {mode}",
                    "supported_modes": ["prospects", "leads", "inbounds"],
                },
                started,
            )

    except Exception as e:
        traceback.print_exc()
        return _std_envelope(
            False,
            mode or "unknown",
            {
                "error": str(e),
                "stack": traceback.format_exc(),
            },
            started,
        )


# ---------------------------------------------------------------------------
# Lazy imports to avoid circular dependencies
# ---------------------------------------------------------------------------

def _get_run_campaigns():
    from sms.campaign_runner import run_campaigns as _run_campaigns
    return _run_campaigns


def _get_run_autoresponder():
    from sms.autoresponder import run_autoresponder as _run_autoresponder
    return _run_autoresponder


def _get_run_retry():
    from sms.retry_runner import run_retry as _run_retry
    return _run_retry


# ---------------------------------------------------------------------------
# CLI harness
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print(run_engine("prospects", limit=10))
    print(run_engine("leads", retry_limit=50))
    print(run_engine("inbounds", limit=10))
