"""
🔁 retry_worker.py (v3.1 — Telemetry Edition)
──────────────────────────────────────────────
Lightweight retry worker for failed outbound messages.
Adds:
 - Structured logging
 - KPI / Run telemetry
 - Duration + candidate count tracking
"""

from __future__ import annotations
import os, time, traceback, re
from datetime import datetime, timezone, timedelta
from functools import lru_cache
from typing import Optional, Dict, Any, List

from sms.config import CONV_FIELDS, CONVERSATIONS_FIELDS
from sms.runtime import get_logger

log = get_logger("retry_worker")

try:
    from sms.logger import log_run
except Exception:

    def log_run(*_a, **_k):
        pass


try:
    from sms.kpi_logger import log_kpi
except Exception:

    def log_kpi(*_a, **_k):
        pass


# ----------------- optional send backends -----------------
try:
    from sms.message_processor import MessageProcessor as _MP
except Exception:
    _MP = None

try:
    from sms.textgrid_sender import send_message as _send_direct
except Exception:
    _send_direct = None

# Import quiet hours checking - CRITICAL FIX for 1:11 AM sending bug
try:
    from sms.main import is_quiet_hours_local
except Exception:
    # Fallback - assume always quiet during import issues
    def is_quiet_hours_local():
        return True

# ----------------- pyairtable compatibility -----------------
_PyTable = _PyApi = None
try:
    from pyairtable import Table as _PyTable
except Exception:
    pass
try:
    from pyairtable import Api as _PyApi
except Exception:
    pass


def _make_table(api_key: Optional[str], base_id: Optional[str], table_name: str):
    if not (api_key and base_id and table_name):
        return None
    try:
        if _PyTable:
            return _PyTable(api_key, base_id, table_name)
        if _PyApi:
            return _PyApi(api_key).table(base_id, table_name)
    except Exception:
        log.error("Failed to init Airtable Conversations table", exc_info=True)
    return None


# ----------------- ENV / TABLES / FIELDS -----------------
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
CONVOS_TABLE_NAME = os.getenv("CONVERSATIONS_TABLE", "Conversations")

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
BASE_BACKOFF_MINUTES = int(os.getenv("BASE_BACKOFF_MINUTES", "30"))

PHONE_FIELD = CONV_FIELDS["FROM"]
TO_FIELD = CONV_FIELDS["TO"]
MESSAGE_FIELD = CONV_FIELDS["BODY"]
STATUS_FIELD = CONV_FIELDS["STATUS"]
DIRECTION_FIELD = CONV_FIELDS["DIRECTION"]

RETRY_COUNT_FIELD = CONVERSATIONS_FIELDS.get("RETRY_COUNT", "retry_count")
RETRY_AFTER_FIELD = CONVERSATIONS_FIELDS.get("RETRY_AFTER", "retry_after")
RETRIED_AT_FIELD = CONVERSATIONS_FIELDS.get("LAST_RETRY_AT", "retried_at")
LAST_ERROR_FIELD = CONVERSATIONS_FIELDS.get("LAST_ERROR", "last_retry_error")
PERMANENT_FAIL_FIELD = CONVERSATIONS_FIELDS.get("PERMANENT_FAIL", "permanent_fail_reason")

FAILED_STATES = {"FAILED", "DELIVERY_FAILED", "UNDELIVERED", "UNDELIVERABLE", "THROTTLED", "NEEDS_RETRY"}

# Get default from number
def _get_default_from_number() -> str:
    """Get default from number from environment variables."""
    return os.getenv("DEFAULT_FROM_NUMBER", "+18329063669")  # fallback to env default

def _is_valid_from_number(from_number: str) -> bool:
    """Validate that from_number is a known good number."""
    if not from_number:
        return False
    
    # Known good numbers (current valid TextGrid DIDs)
    valid_numbers = {
        "+18329063669",  # Current default
        "+19045124117",  # Known valid 904 number
        "+19045124118",  # Known valid 904 number
    }
    
    return from_number in valid_numbers


# ----------------- helpers -----------------
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
    return re.sub(r"[^a-z0-9]+", "", s.strip().lower()) if isinstance(s, str) else s


def _auto_field_map(tbl) -> Dict[str, str]:
    try:
        rows = tbl.all(max_records=1)
        keys = list(rows[0].get("fields", {}).keys()) if rows else []
    except Exception:
        keys = []
    return {_norm(k): k for k in keys}


def _remap_existing_only(tbl, payload: Dict[str, Any]) -> Dict[str, Any]:
    amap = _auto_field_map(tbl)
    if not amap:
        return dict(payload)
    return {amap[_norm(k)]: v for k, v in payload.items() if _norm(k) in amap}


def _is_retryable(f: Dict[str, Any]) -> bool:
    dirn = str(f.get(DIRECTION_FIELD) or f.get("Direction") or "").upper()
    if dirn not in ("OUT", "OUTBOUND"):
        return False
    status = str(f.get(STATUS_FIELD) or f.get("Status") or "").upper()
    if status not in FAILED_STATES:
        return False
    retries = int(f.get(RETRY_COUNT_FIELD) or f.get("retry_count") or 0)
    if retries >= MAX_RETRIES:
        return False
    ra = f.get(RETRY_AFTER_FIELD) or f.get("retry_after")
    ra_dt = _parse_dt(ra)
    return (ra_dt is None) or (ra_dt <= _now())


def _backoff_delay(retry_count: int) -> timedelta:
    return timedelta(minutes=BASE_BACKOFF_MINUTES * max(1, 2 ** max(0, retry_count - 1)))


def _is_permanent_error(err: str) -> bool:
    text = (err or "").lower()
    for sig in [
        "invalid",
        "blacklisted",
        "landline",
        "blocked",
        "disconnected",
        "unreachable",
        "unknown subscriber",
        "rejected by carrier",
    ]:
        if sig in text:
            return True
    return False


def _send(phone: str, body: str, from_number: Optional[str]) -> None:
    # CRITICAL FIX: Check quiet hours before sending any retry messages
    if is_quiet_hours_local():
        raise RuntimeError("quiet_hours_blocked")
    
    if not from_number:
        raise RuntimeError("missing_from_number")
    if _MP:
        res = _MP.send(phone=phone, body=body, direction="OUT", from_number=from_number)
        if not res or res.get("status") != "sent":
            raise RuntimeError(res.get("error", "send_failed"))
        return
    if _send_direct:
        _send_direct(from_number=from_number, to=phone, message=body)
        return
    log.info(f"[MOCK] send → {phone}: {body[:100]}")


# ----------------- Airtable client -----------------
@lru_cache(maxsize=1)
def _t_convos():
    tbl = _make_table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, CONVOS_TABLE_NAME)
    if not tbl:
        log.warning("⚠️ RetryWorker: No Airtable config → mock mode")
    return tbl


# ----------------- core -----------------
def _pick_candidates(convos, limit: int, view: Optional[str]) -> List[Dict]:
    try:
        rows = convos.all(view=view) if (convos and view) else convos.all()
        cands = [r for r in rows if _is_retryable(r.get("fields", {}))]
        cands.sort(key=lambda r: _parse_dt(r.get("fields", {}).get(RETRY_AFTER_FIELD)) or _now())
        return cands[:limit]
    except Exception:
        log.error("Candidate selection failed", exc_info=True)
        return []


# ----------------- main -----------------
def run_retry(limit: int = 100, view: Optional[str] = None) -> Dict[str, Any]:
    start = time.time()
    convos = _t_convos()
    if not convos:
        log.warning("RetryWorker: Skipping (no Airtable)")
        return {"ok": False, "mock": True, "retried": 0}

    candidates = _pick_candidates(convos, limit, view)
    retried, failed_updates, permanent = 0, 0, 0

    for r in candidates:
        rid, f = r.get("id"), r.get("fields", {})
        phone = f.get(PHONE_FIELD) or f.get("From")
        body = f.get(MESSAGE_FIELD) or f.get("Body")
        from_number = f.get(TO_FIELD) or f.get("TextGrid Phone Number") or f.get("To")
        # FIX: Validate and correct from_number - don't trust old/wrong data
        if not from_number or not _is_valid_from_number(from_number):
            if from_number:
                log.warning(f"Replacing invalid from_number '{from_number}' with default")
            from_number = _get_default_from_number()
            
        retries_prev = int(f.get(RETRY_COUNT_FIELD) or 0)
        if not (rid and phone and body):
            continue

        try:
            pre = _remap_existing_only(convos, {STATUS_FIELD: "RETRYING"})
            if pre:
                convos.update(rid, pre)
        except Exception:
            pass

        try:
            _send(phone, body, from_number)
            patch = {
                STATUS_FIELD: "SENT",
                RETRY_COUNT_FIELD: retries_prev + 1,
                RETRIED_AT_FIELD: _now_iso(),
                LAST_ERROR_FIELD: None,
                RETRY_AFTER_FIELD: None,
            }
            safe = _remap_existing_only(convos, patch)
            if safe:
                convos.update(rid, safe)
            retried += 1
            log.info(f"📤 Retried → {phone} | attempt {retries_prev + 1}")
        except Exception as e:
            err, new_count = str(e), retries_prev + 1
            patch = {RETRY_COUNT_FIELD: new_count, LAST_ERROR_FIELD: err[:500]}
            if _is_permanent_error(err) or new_count >= MAX_RETRIES:
                patch[STATUS_FIELD] = "GAVE_UP"
                patch[PERMANENT_FAIL_FIELD] = err[:500]
                permanent += 1
                log.warning(f"🚫 Giving up → {phone} | reason: {err}")
            else:
                delay = _backoff_delay(new_count)
                patch[RETRY_AFTER_FIELD] = (_now() + delay).isoformat()
                patch[STATUS_FIELD] = "NEEDS_RETRY"
                log.warning(f"⚠️ Retry failed → {phone} | next in {delay}: {err}")

            safe = _remap_existing_only(convos, patch)
            try:
                if safe:
                    convos.update(rid, safe)
            except Exception:
                failed_updates += 1
                log.error("Update failed", exc_info=True)

    duration = round(time.time() - start, 2)
    summary = {
        "ok": True,
        "retried": retried,
        "failed_update_errors": failed_updates,
        "permanent": permanent,
        "limit": limit,
        "count_candidates": len(candidates),
        "duration_sec": duration,
    }

    log_run("RETRY_WORKER", processed=retried, breakdown=summary)
    log_kpi("RETRY_RETRIED", retried)
    log_kpi("RETRY_PERMANENT_FAILS", permanent)

    log.info(f"✅ RetryWorker complete | retried={retried} | permanent={permanent} | update_errors={failed_updates} | duration={duration}s")

    return summary


if __name__ == "__main__":
    run_retry()
