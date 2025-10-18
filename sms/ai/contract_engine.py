# sms/contract_engine.py
from __future__ import annotations

import os
import re
import time
import hashlib
import traceback
from typing import Any, Dict, Optional

try:
    import requests
except Exception:
    requests = None  # handled below


# ───────────────────────────────────────────────────────────
# Config
# ───────────────────────────────────────────────────────────
DOCUSIGN_API = (os.getenv("DOCUSIGN_API") or "").rstrip("/")
DOCUSIGN_TOKEN = os.getenv("DOCUSIGN_TOKEN")
PURCHASE_TEMPLATE_ID = os.getenv("PURCHASE_AGREEMENT_TEMPLATE")
TEST_MODE = os.getenv("CONTRACTS_TEST_MODE", "false").lower() in ("1", "true", "yes")

DEFAULT_TIMEOUT = float(os.getenv("CONTRACTS_HTTP_TIMEOUT", "10"))
MAX_RETRIES = int(os.getenv("CONTRACTS_MAX_RETRIES", "3"))
BACKOFF_BASE = float(os.getenv("CONTRACTS_BACKOFF_BASE_SEC", "1.2"))  # 1.2, 2.4, 4.8...


# ───────────────────────────────────────────────────────────
# Helpers
# ───────────────────────────────────────────────────────────
def _log(msg: str) -> None:
    print(f"[ContractEngine] {msg}")


def _is_email(s: str) -> bool:
    if not isinstance(s, str):
        return False
    return re.match(r"^[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}$", s, re.I) is not None


def _fmt_money(v: Any) -> Optional[str]:
    try:
        f = float(v)
        if f <= 0:
            return None
        return f"${f:,.0f}"
    except Exception:
        return None


def _idempotency_key(*parts: Any) -> str:
    raw = "|".join(str(p) for p in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _env_ok() -> tuple[bool, Optional[str]]:
    if not requests:
        return False, "python-requests not available"
    if not DOCUSIGN_API:
        return False, "DOCUSIGN_API not set"
    if not DOCUSIGN_TOKEN:
        return False, "DOCUSIGN_TOKEN not set"
    if not PURCHASE_TEMPLATE_ID:
        return False, "PURCHASE_AGREEMENT_TEMPLATE not set"
    return True, None


def _headers(idem_key: str) -> Dict[str, str]:
    h = {
        "Authorization": f"Bearer {DOCUSIGN_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    # Many APIs honor an idempotency header; harmless if ignored
    h["Idempotency-Key"] = idem_key
    return h


def _post_with_retries(url: str, payload: Dict[str, Any], headers: Dict[str, str]) -> requests.Response:
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=DEFAULT_TIMEOUT)
            # Treat 2xx as success; 409 may indicate duplicate with idempotency key (still OK)
            if 200 <= resp.status_code < 300 or resp.status_code == 409:
                return resp
            # For 4xx non-409, don't hammer (likely a validation problem)
            if 400 <= resp.status_code < 500:
                return resp
            # Else (5xx etc), backoff + retry
            _log(f"HTTP {resp.status_code} from contract API (attempt {attempt}/{MAX_RETRIES})")
        except Exception as e:
            last_exc = e
            _log(f"POST failed (attempt {attempt}/{MAX_RETRIES}): {e}")

        if attempt < MAX_RETRIES:
            # exponential backoff with mild jitter
            delay = BACKOFF_BASE * (2 ** (attempt - 1))
            time.sleep(delay)
    # If we’re here with an exception, raise it; caller will handle
    if last_exc:
        raise last_exc
    # Otherwise, return the last response (even if not 2xx)
    return resp  # type: ignore


# ───────────────────────────────────────────────────────────
# Public API
# ───────────────────────────────────────────────────────────
def send_contract(
    seller_name: str,
    seller_email: str,
    address: str,
    offer_price: Any,
    *,
    subject_prefix: str = "Cash Offer",
    extra_merge_fields: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Create and send an e-sign contract.

    Returns:
      {
        "ok": bool,
        "envelope_id": Optional[str],
        "status_code": Optional[int],
        "error": Optional[str],
        "idempotency_key": str,
        "request_payload": {…},   # what we attempted to send (for observability)
        "response": {…}           # parsed JSON if any
      }
    """
    # Input validation
    if not seller_name or not isinstance(seller_name, str):
        return {"ok": False, "error": "Invalid seller_name"}
    if not _is_email(seller_email):
        return {"ok": False, "error": "Invalid seller_email"}
    if not address or not isinstance(address, str):
        return {"ok": False, "error": "Invalid address"}
    money_str = _fmt_money(offer_price)
    if not money_str:
        return {"ok": False, "error": "Invalid offer_price"}

    idem = _idempotency_key(seller_email.strip().lower(), address.strip().lower(), money_str, PURCHASE_TEMPLATE_ID or "")
    # Minimal, vendor-agnostic payload; your gateway/worker can adapt to DocuSign
    merge_fields = {
        "PropertyAddress": address,
        "OfferPrice": money_str,
        "SellerName": seller_name,
        **(extra_merge_fields or {}),
    }
    payload = {
        "templateId": PURCHASE_TEMPLATE_ID,
        "emailSubject": f"{subject_prefix} for {address}",
        "recipient": {"name": seller_name, "email": seller_email},
        "mergeFields": merge_fields,
        "metadata": {"idempotency_key": idem},
    }

    # TEST mode: don’t call external API
    if TEST_MODE:
        _log(f"[TEST_MODE] Would send contract to {seller_email} for {address} at {money_str}")
        return {
            "ok": True,
            "envelope_id": f"TEST-{idem[:10]}",
            "status_code": 200,
            "error": None,
            "idempotency_key": idem,
            "request_payload": payload,
            "response": {"note": "test mode – no external call"},
        }

    ok_env, err_env = _env_ok()
    if not ok_env:
        return {
            "ok": False,
            "error": f"Config error: {err_env}",
            "idempotency_key": idem,
            "request_payload": payload,
        }

    url = f"{DOCUSIGN_API}/envelopes"
    try:
        resp = _post_with_retries(url, payload, _headers(idem))
    except Exception as e:
        traceback.print_exc()
        return {
            "ok": False,
            "error": f"HTTP request failed: {e}",
            "idempotency_key": idem,
            "request_payload": payload,
        }

    status = resp.status_code
    try:
        body = resp.json()
    except Exception:
        body = {"raw": resp.text[:2000]}

    # Interpret response
    if 200 <= status < 300 or status == 409:
        # Common keys: envelopeId / id; support either
        envelope_id = body.get("envelopeId") or body.get("id") or body.get("envelope_id")
        return {
            "ok": True,
            "envelope_id": envelope_id,
            "status_code": status,
            "error": None,
            "idempotency_key": idem,
            "request_payload": payload,
            "response": body,
        }

    # 4xx/5xx – bubble up a concise message
    err_msg = body.get("message") or body.get("error_description") or body.get("error") or f"HTTP {status}"
    return {
        "ok": False,
        "envelope_id": body.get("envelopeId") or body.get("id"),
        "status_code": status,
        "error": err_msg,
        "idempotency_key": idem,
        "request_payload": payload,
        "response": body,
    }
