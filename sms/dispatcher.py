# sms/dispatcher.py
from __future__ import annotations

import os
import traceback
import time
from typing import Any, Dict, Optional
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

# Optional: followups (if present)
try:
    from sms.followup_flow import run_followups  # noqa

    _HAS_FOLLOWUPS = True
except Exception:
    _HAS_FOLLOWUPS = False

# -----------------------
# Policy configuration
# -----------------------


@dataclass(frozen=True)
class DispatchPolicy:
    """Authoritative policy for quiet hours, rate limits, retries, and jitter.

    README2.md establishes these as canonical across the project.  Modules should
    fetch configuration from this policy instead of re-reading environment
    variables so that adjustments can be made in one place.
    """

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
    def load_from_env(cls) -> "DispatchPolicy":
        tz = ZoneInfo(os.getenv("QUIET_TZ", "America/Chicago")) if ZoneInfo else None
        return cls(
            quiet_tz=tz,
            quiet_start_hour=int(os.getenv("QUIET_START_HOUR_LOCAL", os.getenv("QUIET_START_HOUR", "21"))),
            quiet_end_hour=int(os.getenv("QUIET_END_HOUR_LOCAL", os.getenv("QUIET_END_HOUR", "9"))),
            quiet_enforced=os.getenv("QUIET_HOURS_ENFORCED", "true").lower() in ("1", "true", "yes"),
            rate_per_number_per_min=int(os.getenv("RATE_PER_NUMBER_PER_MIN", "20")),
            global_rate_per_min=int(os.getenv("GLOBAL_RATE_PER_MIN", "5000")),
            daily_limit=int(os.getenv("DAILY_LIMIT", "750")),
            jitter_seconds=int(os.getenv("JITTER_SECONDS", os.getenv("SEND_JITTER_SECONDS", "2"))),
            send_batch_limit=int(os.getenv("SEND_BATCH_LIMIT", "500")),
            retry_limit=int(os.getenv("RETRY_LIMIT", os.getenv("MAX_RETRIES", "3"))),
        )

    # ---- Quiet hours -------------------------------------------------
    def now_local(self) -> datetime:
        if self.quiet_tz:
            return datetime.now(self.quiet_tz)
        return datetime.now(timezone.utc)

    def is_quiet(self, when: Optional[datetime] = None) -> bool:
        if not self.quiet_enforced:
            return False
        ref = when or self.now_local()
        hour = ref.hour
        return (hour >= self.quiet_start_hour) or (hour < self.quiet_end_hour)

    def next_quiet_end(self, when: Optional[datetime] = None) -> Optional[datetime]:
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

    # ---- Rate limit helpers -----------------------------------------
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


_POLICY = DispatchPolicy.load_from_env()


def refresh_policy() -> DispatchPolicy:
    """Reload the dispatch policy from environment.

    Useful for tests that patch environment variables.
    """

    global _POLICY
    _POLICY = DispatchPolicy.load_from_env()
    return _POLICY


def get_policy() -> DispatchPolicy:
    return _POLICY


def _is_quiet_hours_outbound() -> bool:
    return get_policy().is_quiet()


# -----------------------
# Helpers
# -----------------------
def _safe_int(v: Any, default: int) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _summarize_prospect_result(res: Dict[str, Any]) -> Dict[str, int]:
    """
    Normalize metrics from run_campaigns result into easy totals.
    """
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
    payload = payload or {}
    return {
        "ok": ok,
        "type": typ,
        "duration_ms": int((time.time() - started_at) * 1000),
        **payload,
    }


# -----------------------
# Dispatcher
# -----------------------
def run_engine(mode: str, **kwargs) -> dict:
    """
    Unified dispatcher for all SMS engines.

    Modes:
      - "prospects" → Outbound campaigns (Prospects → Drip Queue → (optional) immediate send)
      - "leads"     → Retry loop (and follow-ups if available)
      - "inbounds"  → Autoresponder (promote to leads, AI replies)

    kwargs:
      prospects:
        - limit: int | "ALL"            (campaigns to process)
        - send_after_queue: bool        (force immediate send; will be forced False in quiet hours)
      leads:
        - retry_limit: int              (retry batch size)
      inbounds:
        - limit: int                    (inbound rows to process)
        - view: str                     (Airtable view for Conversations)
    """
    started = time.time()
    mode = (mode or "").lower().strip()
    try:
        if mode == "prospects":
            # Respect quiet hours for outbound
            send_after_queue: Optional[bool] = kwargs.get("send_after_queue")
            if _is_quiet_hours_outbound():
                send_after_queue = False  # hard block immediate sends during quiet hours

            # Pass through limit; default to processing all
            limit = kwargs.get("limit", "ALL")
            result = _get_run_campaigns()(limit=limit, send_after_queue=send_after_queue)

            sums = _summarize_prospect_result(result)
            return _std_envelope(
                True,
                "Prospect",
                {
                    "result": result,
                    "totals": sums,
                    "quiet_hours": _is_quiet_hours_outbound(),
                },
                started,
            )

        elif mode == "leads":
            retry_limit = _safe_int(kwargs.get("retry_limit", 100), 100)
            retry_result = _get_run_retry()(limit=retry_limit)

            # Optionally run follow-ups if available
            followups: Dict[str, Any] = {}
            if _HAS_FOLLOWUPS:
                try:
                    followups = run_followups()
                except Exception:
                    # keep going even if followups fails
                    traceback.print_exc()
                    followups = {"ok": False, "error": "followups_failed"}

            payload = {
                "retries": retry_result,
                "followups": followups if _HAS_FOLLOWUPS else None,
                "processed": _safe_int(retry_result.get("retried", 0), 0),
            }
            return _std_envelope(True, "Lead", payload, started)

        elif mode == "inbounds":
            limit = _safe_int(kwargs.get("limit", 50), 50)
            view = kwargs.get("view", "Unprocessed Inbounds")
            result = _get_run_autoresponder()(limit=limit, view=view)

            payload = {
                "result": result,
                "processed": _safe_int(result.get("processed", 0), 0),
            }
            # Inbounds can run 24/7 (no quiet hour block)
            return _std_envelope(True, "Inbound", payload, started)

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
        print(f"❌ Dispatcher error in mode={mode}: {e}")
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


if __name__ == "__main__":
    # 🔧 Quick manual tests (won’t send during quiet hours)
    print(run_engine("prospects", limit=10))
    print(run_engine("leads", retry_limit=50))
    print(run_engine("inbounds", limit=10))
def _get_run_campaigns():
    from sms.campaign_runner import run_campaigns as _run_campaigns

    return _run_campaigns


def _get_run_autoresponder():
    from sms.autoresponder import run_autoresponder as _run_autoresponder

    return _run_autoresponder


def _get_run_retry():
    from sms.retry_runner import run_retry as _run_retry

    return _run_retry
