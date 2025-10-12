# sms/retry_worker.py
from __future__ import annotations

import os
import traceback
from datetime import datetime, timezone, timedelta
from functools import lru_cache
from typing import Optional, Dict, Any, List

# ----------------- optional send backends -----------------
try:
    from sms.message_processor import MessageProcessor as _MP  # preferred (logs, DRY)
except Exception:
    _MP = None

try:
    from sms.textgrid_sender import send_message as _send_direct  # fallback
except Exception:
    _send_direct = None

# ----------------- pyairtable compatibility -----------------
_PyTable = None
_PyApi = None
try:
    from pyairtable import Table as _PyTable  # v1 style
except Exception:
    _PyTable = None
try:
    from pyairtable import Api as _PyApi  # v2 style
except Exception:
    _PyApi = None


def _make_table(api_key: Optional[str], base_id: Optional[str], table_name: str):
    """
    Return a table client exposing .all()/.get()/.update() across pyairtable versions,
    or None if not configured.
    """
    if not (api_key and base_id and table_name):
        return None
    try:
        if _PyTable:
            return _PyTable(api_key, base_id, table_name)
        if _PyApi:
            return _PyApi(api_key).table(base_id, table_name)
    except Exception:
        traceback.print_exc()
    return None


# ----------------- ENV / TABLES / FIELDS -----------------
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
CONVOS_TABLE_NAME = os.getenv("CONVERSATIONS_TABLE", "Conversations")

# Retry tuning
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
BASE_BACKOFF_MINUTES = int(os.getenv("BASE_BACKOFF_MINUTES", "30"))

# Field mapping (safe defaults, all env-driven)
PHONE_FIELD = os.getenv("CONV_FROM_FIELD", "phone")
MESSAGE_FIELD = os.getenv("CONV_MESSAGE_FIELD", "message")
STATUS_FIELD = os.getenv("CONV_STATUS_FIELD", "status")
DIRECTION_FIELD = os.getenv("CONV_DIRECTION_FIELD", "direction")

RETRY_COUNT_FIELD = os.getenv("CONV_RETRY_COUNT_FIELD", "retry_count")
RETRY_AFTER_FIELD = os.getenv("CONV_RETRY_AFTER_FIELD", "retry_after")
RETRIED_AT_FIELD = os.getenv("CONV_RETRIED_AT_FIELD", "retried_at")
LAST_ERROR_FIELD = os.getenv("CONV_LAST_ERROR_FIELD", "last_retry_error")
PERMANENT_FAIL_FIELD = os.getenv("CONV_PERM_FAIL_FIELD", "permanent_fail_reason")

FAILED_STATES = {"FAILED", "DELIVERY_FAILED", "UNDELIVERED", "UNDELIVERABLE", "THROTTLED", "NEEDS_RETRY"}


# ----------------- small helpers -----------------
def _now() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now().isoformat()


def _parse_dt(s: Any) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        return None


def _norm(s: str) -> str:
    import re

    return re.sub(r"[^a-z0-9]+", "", s.strip().lower()) if isinstance(s, str) else s


def _auto_field_map(tbl) -> Dict[str, str]:
    """normalized_field_name -> actual Airtable field name for this table."""
    keys: List[str] = []
    try:
        sample = None
        try:
            page = tbl.all(max_records=1)
            sample = page[0] if page else None
        except Exception:
            sample = None
        if sample:
            keys = list(sample.get("fields", {}).keys())
    except Exception:
        pass
    return {_norm(k): k for k in keys}


def _remap_existing_only(tbl, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Keep only keys that already exist on the table (prevents 422 UNKNOWN_FIELD_NAME)."""
    amap = _auto_field_map(tbl)
    if not amap:
        return dict(payload)  # optimistic if we can't probe
    out: Dict[str, Any] = {}
    for k, v in payload.items():
        ak = amap.get(_norm(k))
        if ak:
            out[ak] = v
    return out


def _is_retryable(f: Dict[str, Any]) -> bool:
    # OUT direction
    direction = str(f.get(DIRECTION_FIELD) or f.get("Direction") or "").strip().upper()
    if direction != "OUT":
        return False
    # Status must be one of failed/retryable
    status = str(f.get(STATUS_FIELD) or f.get("Status") or "").strip().upper()
    if status not in FAILED_STATES:
        return False
    # Retry count gate
    retries = int(f.get(RETRY_COUNT_FIELD) or f.get("retry_count") or 0)
    if retries >= MAX_RETRIES:
        return False
    # retry_after gate (if set, must be <= now)
    ra = f.get(RETRY_AFTER_FIELD) or f.get("retry_after")
    ra_dt = _parse_dt(ra)
    return (ra_dt is None) or (ra_dt <= _now())


def _backoff_delay(retry_count: int) -> timedelta:
    # 30m, 60m, 120m ... with BASE_BACKOFF_MINUTES as factor
    return timedelta(minutes=BASE_BACKOFF_MINUTES * max(1, 2 ** max(0, retry_count - 1)))


def _is_permanent_error(err: str) -> bool:
    text = (err or "").lower()
    signals = [
        "invalid",
        "not a valid",
        "unreachable",
        "blacklisted",
        "blocked",
        "landline",
        "disconnected",
        "undeliverable",
        "unknown subscriber",
        "unknown destination",
        "absent subscriber",
        "rejected by carrier",
    ]
    return any(sig in text for sig in signals)


def _send(phone: str, body: str) -> None:
    """Preferred send path → MessageProcessor, then direct sender, else MOCK."""
    if _MP:
        res = _MP.send(phone=phone, body=body, direction="OUT")
        if not res or res.get("status") != "sent":
            raise RuntimeError(res.get("error", "send_failed") if isinstance(res, dict) else "send_failed")
        return
    if _send_direct:
        _send_direct(phone, body)
        return
    print(f"[MOCK] send → {phone}: {body[:140]}")  # no raise in MOCK


# ----------------- Airtable client -----------------
@lru_cache(maxsize=1)
def _t_convos():
    tbl = _make_table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, CONVOS_TABLE_NAME)
    if not tbl:
        print("⚠️ RetryWorker: No Airtable config → MOCK mode")
    return tbl


# ----------------- candidate selection (python-side) -----------------
def _pick_candidates(convos, limit: int, view: Optional[str]) -> List[Dict]:
    # Prefer explicit view if provided → then Python filter
    if convos and view:
        try:
            rows = convos.all(view=view)
            cands = [r for r in rows if _is_retryable(r.get("fields", {}))]
            return cands[:limit]
        except Exception:
            traceback.print_exc()

    # Fallback: full scan + Python filter (safer than brittle formula)
    try:
        rows = convos.all() if convos else []
        cands = [r for r in rows if _is_retryable(r.get("fields", {}))]

        # Sort by earliest retry_after (or by sent_at) so the oldest waits go first
        def _sort_key(r):
            f = r.get("fields", {})
            return _parse_dt(f.get(RETRY_AFTER_FIELD) or f.get("retry_after") or f.get("sent_at")) or _now()

        cands.sort(key=_sort_key)
        return cands[:limit]
    except Exception:
        traceback.print_exc()
        return []


# ----------------- main -----------------
def run_retry(limit: int = 100, view: str | None = None) -> Dict[str, Any]:
    convos = _t_convos()
    if not convos:
        # MOCK result if Airtable not configured
        print("⚠️ RetryWorker: Skipping (no Airtable); returning MOCK result")
        return {"ok": False, "retried": 0, "failed_update_errors": 0, "permanent": 0, "limit": limit, "mock": True}

    candidates = _pick_candidates(convos, limit, view)

    retried = 0
    failed_updates = 0
    permanent = 0

    for r in candidates:
        rid = r.get("id")
        f = r.get("fields", {})
        phone = f.get(PHONE_FIELD) or f.get("Phone") or f.get("From")
        body = f.get(MESSAGE_FIELD) or f.get("Body") or f.get("message")
        retries_prev = int(f.get(RETRY_COUNT_FIELD) or f.get("retry_count") or 0)

        if not (rid and phone and body):
            continue

        # Mark as RETRYING (best effort) so concurrent workers don't double-send
        try:
            pre = _remap_existing_only(convos, {STATUS_FIELD: "RETRYING"})
            if pre:
                convos.update(rid, pre)
        except Exception:
            # non-fatal
            pass

        try:
            _send(phone, body)

            patch = {
                STATUS_FIELD: "SENT",
                RETRY_COUNT_FIELD: retries_prev + 1,
                RETRIED_AT_FIELD: _now_iso(),
                LAST_ERROR_FIELD: None,
                RETRY_AFTER_FIELD: None,
            }
            safe = _remap_existing_only(convos, patch)
            if not safe:
                # fallbacks (in case names differ in case)
                safe = _remap_existing_only(
                    convos,
                    {
                        "Status": "SENT",
                        "retry_count": retries_prev + 1,
                        "retried_at": _now_iso(),
                        "last_retry_error": None,
                        "retry_after": None,
                    },
                )
            if safe:
                convos.update(rid, safe)

            retried += 1
            print(f"📤 Retried → {phone} | attempt {retries_prev + 1}")

        except Exception as e:
            err = str(e)
            new_count = retries_prev + 1

            patch = {
                RETRY_COUNT_FIELD: new_count,
                LAST_ERROR_FIELD: err[:500],
            }

            if _is_permanent_error(err) or new_count >= MAX_RETRIES:
                patch[STATUS_FIELD] = "GAVE_UP"
                patch[PERMANENT_FAIL_FIELD] = err[:500]
                if _is_permanent_error(err):
                    permanent += 1
                print(f"🚫 Giving up → {phone} | reason: {err}")
            else:
                delay = _backoff_delay(new_count)
                patch[RETRY_AFTER_FIELD] = (_now() + delay).isoformat()
                patch[STATUS_FIELD] = "NEEDS_RETRY"
                print(f"⚠️ Retry failed → {phone} | next in {delay}: {err}")

            safe = _remap_existing_only(convos, patch)
            if not safe:
                # last-resort fallback keys
                fb = {
                    "retry_count": new_count,
                    "last_retry_error": err[:500],
                }
                # status + retry_after if table uses different casing
                if patch.get(STATUS_FIELD) or patch.get("Status"):
                    fb["Status"] = patch.get(STATUS_FIELD) or patch.get("Status")
                if patch.get(RETRY_AFTER_FIELD):
                    fb["retry_after"] = patch.get(RETRY_AFTER_FIELD)
                if patch.get(PERMANENT_FAIL_FIELD):
                    fb["permanent_fail_reason"] = patch.get(PERMANENT_FAIL_FIELD)
                safe = _remap_existing_only(convos, fb)

            try:
                if safe:
                    convos.update(rid, safe)
            except Exception:
                traceback.print_exc()
                failed_updates += 1

    print(f"🔁 Retry worker | ✅ retried={retried} | ❌ update_errors={failed_updates} | 🚫 permanent={permanent}")
    return {
        "ok": True,
        "retried": retried,
        "failed_update_errors": failed_updates,
        "permanent": permanent,
        "limit": limit,
        "count_candidates": len(candidates),
    }


if __name__ == "__main__":
    run_retry()
