# sms/drip_admin.py
"""
Drip Queue Normalizer (Hardened)
--------------------------------
Ensures every queued/retry/throttled record has a valid UTC send time and a CT UI mirror.

Fixes & Guards:
  • Proper table handle (.table)
  • Naive time parsed as CT (not UTC)
  • Separate fields for UTC vs CT UI (no overwrite)
  • Limit respected even in dry-run
  • Quiet hours defer (optional)
  • Campaign status gate: don't mark READY if Paused/Completed
"""

from __future__ import annotations
import random, traceback
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional, Tuple, List
from zoneinfo import ZoneInfo

from sms.runtime import get_logger
from sms.datastore import CONNECTOR, update_record
from sms.airtable_schema import DripStatus

logger = get_logger("drip_admin")

# ---- Config / Schema (tweak if your field names differ) ----
QUIET_TZ = ZoneInfo("America/Chicago")
QUIET_START = 21
QUIET_END   = 9

DRIP_STATUS_F = "Status"
DRIP_NEXT_SEND_CT_F  = "Next Send Date"      # UI-facing local time (CT), naive datetime
DRIP_NEXT_SEND_UTC_F = "next_send_at_utc"    # machine UTC timestamp (ISO8601)
DRIP_CAMPAIGN_LINK_F = "Campaign"

CAMPAIGN_STATUS_F = "Status"                 # in Campaigns table

# ---- Helpers ----
def _utcnow() -> datetime:
    return datetime.now(timezone.utc)

def _to_ct_naive(dt_utc: datetime) -> str:
    """Convert UTC → naive CT string for Airtable UI fields."""
    return dt_utc.astimezone(QUIET_TZ).replace(tzinfo=None).isoformat(timespec="seconds")

def _parse_datetime(value: Any) -> Optional[datetime]:
    """
    Parse ISO string into UTC datetime.
    If string is naive, interpret as CT (UI values) — not UTC.
    """
    if not value or not isinstance(value, str):
        return None
    s = value.strip()
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        # If it had a tz, normalize to UTC
        if dt.tzinfo is not None:
            return dt.astimezone(timezone.utc)
        # Naive → assume CT UI
        return dt.replace(tzinfo=QUIET_TZ).astimezone(timezone.utc)
    except Exception:
        return None

def _should_bump(dt_utc: Optional[datetime], now: datetime, force_now: bool) -> bool:
    return force_now or dt_utc is None or dt_utc < now - timedelta(seconds=5)

def _is_quiet_hours(now_ct: Optional[datetime] = None) -> bool:
    t = now_ct or datetime.now(QUIET_TZ)
    return (t.hour >= QUIET_START) or (t.hour < QUIET_END)

def _next_allowed_ct(now_ct: Optional[datetime] = None) -> datetime:
    """Return next allowed CT send time (QUIET_END) today or tomorrow."""
    base = now_ct or datetime.now(QUIET_TZ)
    target = base.replace(hour=QUIET_END, minute=0, second=0, microsecond=0)
    if base.hour >= QUIET_START:
        # after start of quiet hours → schedule next day at QUIET_END
        target = target + timedelta(days=1)
    elif base.hour < QUIET_END:
        # before QUIET_END same day → schedule today at QUIET_END
        pass
    return target

# Cache campaign statuses to avoid extra network calls per-row
_campaign_status_cache: Dict[str, str] = {}

def _campaign_is_active(camp_ids: List[str]) -> bool:
    """Return True if ANY linked campaign is Active; if none found, default True."""
    if not camp_ids:
        return True
    tbl = CONNECTOR.campaigns().table
    to_fetch = [cid for cid in camp_ids if cid not in _campaign_status_cache]
    if to_fetch:
        for i in range(0, len(to_fetch), 90):
            chunk = to_fetch[i:i+90]
            formula = "OR(" + ",".join([f"RECORD_ID()='{cid}'" for cid in chunk]) + ")"
            try:
                recs = tbl.all(formula=formula, page_size=100) or []
                for r in recs:
                    _campaign_status_cache[r["id"]] = str(r.get("fields", {}).get(CAMPAIGN_STATUS_F, "")).strip().lower()
            except Exception as e:
                logger.warning(f"Campaign status fetch failed: {e}")
                # If fetch fails, we won't block; treat as active
                for cid in chunk:
                    _campaign_status_cache.setdefault(cid, "active")
    # Consider active if any linked campaign is active
    for cid in camp_ids:
        if _campaign_status_cache.get(cid, "active") == "active":
            return True
    return False

# ---- Core ----
def normalize_next_send_dates(
    dry_run: bool = True,
    force_now: bool = False,
    limit: int = 1000,
    jitter_seconds: Tuple[int, int] = (2, 12),
    respect_quiet_hours: bool = True,
    campaign_status_gate: bool = True,
) -> Dict[str, Any]:
    """
    Normalize queued/retry/throttled drip rows.
    - Computes/bumps UTC time (DRIP_NEXT_SEND_UTC_F)
    - Mirrors CT UI (DRIP_NEXT_SEND_CT_F)
    - Marks READY only if allowed (not quiet hours, campaign active)
    """
    dtbl = CONNECTOR.drip_queue().table
    if not dtbl:
        return {"ok": False, "error": "Drip table unavailable"}

    # Filter to only interesting statuses
    formula = (
        f"OR("
        f"{{{DRIP_STATUS_F}}}='{DripStatus.QUEUED.value}',"
        f"{{{DRIP_STATUS_F}}}='{DripStatus.READY.value}',"
        f"{{{DRIP_STATUS_F}}}='{DripStatus.RETRY.value}',"
        f"{{{DRIP_STATUS_F}}}='{DripStatus.THROTTLED.value}'"
        f")"
    )
    try:
        rows = dtbl.all(formula=formula, page_size=100) or []
    except Exception as e:
        traceback.print_exc()
        return {"ok": False, "error": str(e)}

    now_utc = _utcnow()
    now_ct = datetime.now(QUIET_TZ)
    processed = updated = 0

    for r in rows:
        if processed >= limit:
            break
        processed += 1

        fields = r.get("fields", {}) or {}
        status = str(fields.get(DRIP_STATUS_F) or "").strip()
        # Prefer UTC field; fall back to CT UI field
        send_at_utc = (
            _parse_datetime(fields.get(DRIP_NEXT_SEND_UTC_F))
            or _parse_datetime(fields.get(DRIP_NEXT_SEND_CT_F))
        )

        # Decide target time
        if _should_bump(send_at_utc, now_utc, force_now):
            if respect_quiet_hours and not force_now and _is_quiet_hours(now_ct):
                # Defer to next allowed CT time with small jitter (convert to UTC)
                base_ct = _next_allowed_ct(now_ct)
                base_ct = base_ct + timedelta(seconds=random.randint(*jitter_seconds))
                send_at_utc = base_ct.astimezone(timezone.utc)
            else:
                send_at_utc = now_utc + timedelta(seconds=random.randint(*jitter_seconds))

        # Campaign status gate: don't mark READY if all linked campaigns are paused/completed
        make_ready = True
        if campaign_status_gate:
            camp_ids = fields.get(DRIP_CAMPAIGN_LINK_F) or []
            if not _campaign_is_active([c for c in camp_ids if isinstance(c, str)]):
                make_ready = False

        ct_local_str = _to_ct_naive(send_at_utc)
        payload = {
            DRIP_NEXT_SEND_UTC_F: send_at_utc.isoformat(),  # machine UTC
            DRIP_NEXT_SEND_CT_F: ct_local_str,              # UI CT
        }
        if make_ready and not (respect_quiet_hours and _is_quiet_hours(now_ct) and not force_now):
            payload[DRIP_STATUS_F] = DripStatus.READY.value
        else:
            # Keep current status (typically QUEUED/THROTTLED/RETRY), but still normalize timestamps
            pass

        if dry_run:
            logger.info(
                "DRY-RUN: id=%s | %s → UTC=%s | CT=%s | will_ready=%s",
                r.get("id"), status, send_at_utc.isoformat(), ct_local_str, bool(payload.get(DRIP_STATUS_F) == DripStatus.READY.value)
            )
        else:
            if update_record(dtbl, r["id"], payload):
                updated += 1

    return {
        "ok": True,
        "examined": processed,
        "updated": 0 if dry_run else updated,
        "dry_run": dry_run,
        "force_now": force_now,
        "limit": limit,
        "quiet_defer": respect_quiet_hours,
        "campaign_gate": campaign_status_gate,
    }
