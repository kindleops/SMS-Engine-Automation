# sms/drip_admin.py
"""
Drip Queue Normalizer (Datastore Refactor)
-------------------------------------------
Ensures every queued or ready record has a valid UTC send time.

Key behaviors:
  • Auto-converts CT/UI date to UTC
  • Bumps past-due timestamps forward by jitter
  • Mirrors CT time for Airtable UI
  • Marks record Ready for send
"""

from __future__ import annotations
import random, traceback
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo
from sms.runtime import get_logger
from sms.datastore import CONNECTOR, update_record
from sms.airtable_schema import DripStatus

logger = get_logger("drip_admin")

QUIET_TZ = ZoneInfo("America/Chicago")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _to_ct_naive(dt_utc: datetime) -> str:
    """Convert UTC datetime → CT naive string for Airtable UI fields."""
    return dt_utc.astimezone(QUIET_TZ).replace(tzinfo=None).isoformat(timespec="seconds")


def _parse_datetime(value: Any) -> Optional[datetime]:
    """Parse any ISO/CT/naive string into UTC datetime."""
    if not value or not isinstance(value, str):
        return None
    try:
        s = value.strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        try:
            local = datetime.fromisoformat(value.strip()).replace(tzinfo=QUIET_TZ)
            return local.astimezone(timezone.utc)
        except Exception:
            return None


def _should_bump(dt_utc: Optional[datetime], now: datetime, force_now: bool) -> bool:
    """Decide if timestamp should be moved forward."""
    return force_now or dt_utc is None or dt_utc < now - timedelta(seconds=5)


def normalize_next_send_dates(
    dry_run: bool = True,
    force_now: bool = False,
    limit: int = 1000,
    jitter_seconds: tuple[int, int] = (2, 12),
) -> Dict[str, Any]:
    """Normalize queued/ready drip rows for safe execution."""
    table = CONNECTOR.drip_queue()
    if not table:
        return {"ok": False, "error": "Drip table unavailable"}

    try:
        rows = table.all()
    except Exception as e:
        traceback.print_exc()
        return {"ok": False, "error": str(e)}

    now = _utcnow()
    examined = updated = 0
    valid_statuses = {
        DripStatus.QUEUED.value,
        DripStatus.READY.value,
        DripStatus.RETRY.value,
        DripStatus.THROTTLED.value,
    }

    for r in rows:
        if updated >= limit:
            break
        fields = r.get("fields", {}) or {}
        status = str(fields.get("Status") or "").strip()
        if status not in valid_statuses:
            continue
        examined += 1

        # Extract and validate send time
        send_at_utc = (
            _parse_datetime(fields.get("Next Send At"))
            or _parse_datetime(fields.get("next_send_at_utc"))
            or _parse_datetime(fields.get("Next Send Date"))
        )
        if _should_bump(send_at_utc, now, force_now):
            send_at_utc = now + timedelta(seconds=random.randint(*jitter_seconds))

        ct_local_str = _to_ct_naive(send_at_utc)
        payload = {
            "Next Send At": send_at_utc.isoformat(),
            "next_send_at_utc": send_at_utc.isoformat(),
            "Next Send Date": ct_local_str,
            "Status": DripStatus.READY.value,
        }

        if dry_run:
            logger.info("DRY-RUN: id=%s | status=%s → %s CT=%s", r.get("id"), status, send_at_utc.isoformat(), ct_local_str)
        else:
            if update_record(table, r["id"], payload):
                updated += 1

    return {
        "ok": True,
        "examined": examined,
        "updated": 0 if dry_run else updated,
        "dry_run": dry_run,
        "force_now": force_now,
        "limit": limit,
    }
