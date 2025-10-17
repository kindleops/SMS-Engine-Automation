# sms/dispatcher.py
from __future__ import annotations

import logging
import os
import traceback
import time
from typing import Any, Dict, Optional
from datetime import datetime

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover - fallback for very old Python
    ZoneInfo = None


LOGGER = logging.getLogger(__name__)

# Runners
from sms.campaign_runner import run_campaigns
from sms.autoresponder import run_autoresponder
from sms.retry_runner import run_retry

# Optional: followups (if present)
try:
    from sms.followup_flow import run_followups  # noqa

    _HAS_FOLLOWUPS = True
except Exception:
    _HAS_FOLLOWUPS = False

# -----------------------
# Quiet hours (Outbound)
# -----------------------
def _load_quiet_timezone() -> Optional[ZoneInfo]:
    """Attempt to load the quiet-hour timezone safely."""
    if not ZoneInfo:
        return None

    tz_name = os.getenv("QUIET_TZ", "America/Chicago")
    try:
        return ZoneInfo(tz_name)
    except Exception:  # pragma: no cover - depends on system tz database
        LOGGER.warning("Unable to load timezone '%s'; quiet hours disabled.", tz_name)
        return None


QUIET_TZ = _load_quiet_timezone()
QUIET_START_HOUR = int(os.getenv("QUIET_START_HOUR", "21"))  # 9pm local
QUIET_END_HOUR = int(os.getenv("QUIET_END_HOUR", "9"))  # 9am local


def _central_now() -> datetime:
    if QUIET_TZ:
        return datetime.now(QUIET_TZ)
    return datetime.utcnow()


def _is_quiet_hours_outbound() -> bool:
    h = _central_now().hour
    return (h >= QUIET_START_HOUR) or (h < QUIET_END_HOUR)


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


def _std_envelope(ok: bool, typ: str, payload: Any, started_at: float) -> Dict[str, Any]:
    if isinstance(payload, dict):
        payload_dict = dict(payload)
    else:
        payload_dict = {"payload": payload}
    return {
        "ok": ok,
        "type": typ,
        "duration_ms": int((time.time() - started_at) * 1000),
        **payload_dict,
    }


# -----------------------
# Dispatcher
# -----------------------
SUPPORTED_MODES = ("prospects", "leads", "inbounds")


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
    normalized_mode = str(mode or "").lower().strip()
    try:
        if normalized_mode == "prospects":
            # Respect quiet hours for outbound
            send_after_queue: Optional[bool] = kwargs.get("send_after_queue")
            quiet_hours = _is_quiet_hours_outbound()
            if quiet_hours:
                send_after_queue = False  # hard block immediate sends during quiet hours

            # Pass through limit; default to processing all
            limit = kwargs.get("limit", "ALL")
            result = run_campaigns(limit=limit, send_after_queue=send_after_queue)

            sums = _summarize_prospect_result(result)
            return _std_envelope(
                True,
                "Prospect",
                {
                    "result": result,
                    "totals": sums,
                    "quiet_hours": quiet_hours,
                },
                started,
            )

        elif normalized_mode == "leads":
            retry_limit = _safe_int(kwargs.get("retry_limit", 100), 100)
            retry_result = run_retry(limit=retry_limit)

            # Optionally run follow-ups if available
            followups: Dict[str, Any] = {}
            if _HAS_FOLLOWUPS:
                try:
                    followups = run_followups()
                except Exception:
                    # keep going even if followups fails
                    LOGGER.exception("Follow-up flow failed")
                    followups = {"ok": False, "error": "followups_failed"}

            payload = {
                "retries": retry_result,
                "followups": followups if _HAS_FOLLOWUPS else None,
                "processed": _safe_int(retry_result.get("retried", 0), 0),
            }
            return _std_envelope(True, "Lead", payload, started)

        elif normalized_mode == "inbounds":
            limit = _safe_int(kwargs.get("limit", 50), 50)
            view = kwargs.get("view", "Unprocessed Inbounds")
            result = run_autoresponder(limit=limit, view=view)

            payload = {
                "result": result,
                "processed": _safe_int(result.get("processed", 0), 0),
            }
            # Inbounds can run 24/7 (no quiet hour block)
            return _std_envelope(True, "Inbound", payload, started)

        else:
            mode_desc = normalized_mode or "<missing>"
            return _std_envelope(
                False,
                "Unknown",
                {
                    "error": f"Unknown mode: {mode_desc}",
                    "supported_modes": list(SUPPORTED_MODES),
                },
                started,
            )

    except Exception as e:
        LOGGER.exception("Dispatcher error in mode=%s", normalized_mode or "unknown")
        return _std_envelope(
            False,
            normalized_mode or "unknown",
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
