# sms/ai_closer.py
from __future__ import annotations

import os
import re
import math
import statistics
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional

# Re-use your existing sender (guarded, but expected to exist in this project)
try:
    from sms.message_processor import MessageProcessor
except Exception:
    MessageProcessor = None  # type: ignore

# Optional Airtable dependency (we don't require it at runtime)
try:
    from pyairtable import Table  # type: ignore
except Exception:
    Table = None  # type: ignore

# ---------- ENV / CONFIG ----------
CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")
LEADS_TABLE         = os.getenv("LEADS_TABLE", "Leads")
PROSPECTS_TABLE     = os.getenv("PROSPECTS_TABLE", "Prospects")

# Offer math (override in env if desired)
OFFER_MAO_PCT          = float(os.getenv("OFFER_MAO_PCT", "0.70"))   # 70% rule default
CLOSING_FEE_FLAT       = float(os.getenv("CLOSING_FEE_FLAT", "4000"))
WHOLESALE_FEE_FLAT     = float(os.getenv("WHOLESALE_FEE_FLAT", "8000"))
REPAIR_FLOOR_PER_SQFT  = float(os.getenv("REPAIR_FLOOR_PER_SQFT", "12"))  # min repair/sqft
REPAIR_CEIL_PER_SQFT   = float(os.getenv("REPAIR_CEIL_PER_SQFT", "55"))   # max repair/sqft
MIN_CASH_OFFER         = float(os.getenv("MIN_CASH_OFFER", "5000"))

DM_ENABLED     = bool(os.getenv("DEALMACHINE_API_KEY"))
ZILLOW_ENABLED = bool(os.getenv("ZILLOW_RAPIDAPI_KEY"))

# ---------- Airtable lazy clients (safe; may return None) ----------
@lru_cache(maxsize=None)
def _get_airtable(table_name: str) -> Optional[Any]:
    """
    Best-effort Table getter. Returns None if pyairtable not installed or env missing.
    """
    api_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
    if not (api_key and base_id and Table):
        return None
    try:
        return Table(api_key, base_id, table_name)  # type: ignore[call-arg]
    except Exception:
        return None

def _iso_ts() -> str:
    return datetime.now(timezone.utc).isoformat()

# ---------- Utilities ----------
# Accepts: 125k, $137,500, 1.25m, 97500, $98k, 98 K, etc.
_PRICE_RE = re.compile(
    r"""
    (?P<prefix>\$)?\s*
    (?P<num>
        (?:\d{1,3}(?:,\d{3})+|\d+(?:\.\d+)?)
    )
    \s*(?P<suff>[kKmM])?
    """,
    re.VERBOSE,
)

def _extract_ask_price(text: str) -> Optional[float]:
    """
    Extracts an 'ask' like '125k' or '$137,500' or '1.2m' from seller message.
    Returns float dollars if found and sane.
    """
    if not text:
        return None
    t = str(text)
    m = _PRICE_RE.search(t)
    if not m:
        return None
    raw = m.group("num").replace(",", "")
    suffix = (m.group("suff") or "").lower()

    try:
        val = float(raw)
        if suffix == "k":
            val *= 1_000
        elif suffix == "m":
            val *= 1_000_000
        # sanity floor
        if val < 5_000:
            return None
        return val
    except Exception:
        return None

def _median(nums: List[float]) -> Optional[float]:
    nums = [float(n) for n in nums if isinstance(n, (int, float)) and not math.isnan(float(n))]
    if not nums:
        return None
    try:
        return statistics.median(sorted(nums))
    except Exception:
        return None

def _soft_cap(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))

# ---------- “API” stubs (replace with real calls) ----------
def _fetch_dealmachine_comps(address: str, city: str = "", state: str = "", zipc: str = "") -> List[Dict[str, Any]]:
    """
    Stub: Replace with a real DealMachine API call.
    Return list of comps with at least: price, beds, baths, sqft, distance_mi, close_date.
    """
    if not DM_ENABLED:
        return []
    # TODO: Implement real HTTP call
    return []

def _fetch_zillow_comps(address: str, city: str = "", state: str = "", zipc: str = "") -> List[Dict[str, Any]]:
    """
    Stub: Replace with a real Zillow (RapidAPI) call.
    Return list of comps with: price, beds, baths, sqft, distance_mi, close_date.
    """
    if not ZILLOW_ENABLED:
        return []
    # TODO: Implement real HTTP call
    return []

# ---------- Normalization / ARV ----------
def _normalize_comps(*comp_lists: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Combine sources and filter to reasonable ranges."""
    merged: List[Dict[str, Any]] = []
    for lst in comp_lists:
        merged.extend(lst or [])
    # Basic sanity filter: price between 30k and 2M; sqft between 400 and 6000
    out = []
    for c in merged:
        try:
            price = float(c.get("price")) if c.get("price") is not None else None
            sqft  = float(c.get("sqft")) if c.get("sqft") is not None else None
        except Exception:
            continue
        if price is None or not (30_000 <= price <= 2_000_000):
            continue
        if sqft is not None and not (400 <= sqft <= 6000):
            continue
        out.append(c)
    return out

def _estimate_arv(comps: List[Dict[str, Any]], subject_sqft: Optional[float]) -> Optional[float]:
    """
    Simple ARV estimator:
    - Take the top 5 comps by similarity (closest distance and closest sqft if we have it)
    - Return median price of those top comps
    """
    if not comps:
        return None

    def similarity_score(c: Dict[str, Any]) -> float:
        # smaller is better
        try:
            dist = float(c.get("distance_mi", 0.5))
        except Exception:
            dist = 0.5
        if subject_sqft and c.get("sqft"):
            try:
                dsq = abs(float(c["sqft"]) - float(subject_sqft)) / max(float(subject_sqft), 1.0)
            except Exception:
                dsq = 0.15
        else:
            dsq = 0.15
        return dist * 0.7 + dsq * 0.3

    sorted_comps = sorted(comps, key=similarity_score)
    top = sorted_comps[:5] if len(sorted_comps) >= 5 else sorted_comps
    return _median([float(c["price"]) for c in top if c.get("price") is not None])

# ---------- Repairs ----------
def _infer_repair_severity(text: str) -> str:
    t = (text or "").lower()
    if any(w in t for w in ["burn", "fire", "foundation", "black mold", "blackmold", "unlivable"]):
        return "heavy"
    if any(w in t for w in ["roof", "hvac", "plumbing", "electrical", "water damage", "waterdamage"]):
        return "moderate"
    if any(w in t for w in ["needs work", "repairs", "old kitchen", "outdated"]):
        return "light"
    return "unknown"

def _estimate_repairs(sqft: Optional[float], seller_text: str) -> float:
    """
    Heuristic:
    - Start with per-sqft band based on severity (unknown → low-mid)
    - Clamp to reasonable floors/ceilings
    """
    severity = _infer_repair_severity(seller_text)
    base = {
        "heavy": 45.0,
        "moderate": 30.0,
        "light": 18.0,
        "unknown": 15.0,
    }.get(severity, 15.0)

    per_sqft = _soft_cap(base, REPAIR_FLOOR_PER_SQFT, REPAIR_CEIL_PER_SQFT)
    if not sqft:
        # If we don't know size, return a conservative mid figure
        return float(per_sqft * 1200.0)
    try:
        return float(per_sqft * float(sqft))
    except Exception:
        return float(per_sqft * 1200.0)

# ---------- Offer math ----------
def _compute_mao(arv: float, est_repairs: float) -> float:
    """
    MAO = ARV * PCT - Repairs - Fees (closing + wholesale)
    """
    return float(arv * OFFER_MAO_PCT - est_repairs - CLOSING_FEE_FLAT - WHOLESALE_FEE_FLAT)

# ---------- SMS copy ----------
def _format_offer_sms(offer: float) -> str:
    # Keep it crisp, no ARV mention
    # Round to nearest $500 for cleaner number psychology
    rounded = int(round(offer / 500.0) * 500)
    return (
        f"Thanks for your patience. After running the numbers and factoring in repairs & comps, "
        f"we’d be at ${rounded:,} cash. We cover all closing costs, no commissions, and buy as-is. "
        f"Does that work for you?"
    )

def _format_counterask_sms(ask: float, offer: float) -> str:
    gap = max(0.0, float(ask) - float(offer))
    if gap <= 2500:
        tgt = int(round((ask + offer) / 1000) * 1000)
        return (
            f"Appreciate it. We’re close — I could potentially get to about ${tgt:,} "
            f"if we can move quickly. Would that work?"
        )
    base = int(round(offer / 1000) * 1000)
    return (
        f"Thanks for sharing. Based on repairs and recent sales, we’re around ${base:,}. "
        f"Is there any flexibility on your ${int(ask):,} number?"
    )

# ---------- Public entry ----------
def run_ai_closer(from_phone: str, inbound_text: str, convo_fields: Dict[str, Any]) -> Dict[str, Any]:
    """
    Called by autoresponder when intent is price/condition stage.
    1) Gather context (subject address, sqft if available).
    2) Pull comps from DealMachine / Zillow (stubs now).
    3) Estimate ARV, repairs, then compute offer.
    4) Send offer via MessageProcessor.
    Returns a dict with breakdown for logging.
    """
    # Subject context (best-effort from Airtable fields present on the conversation row)
    address = (
        convo_fields.get("Property Address")
        or convo_fields.get("Address")
        or convo_fields.get("address")
        or ""
    )
    city    = convo_fields.get("City") or ""
    state   = convo_fields.get("State") or ""
    zipcode = convo_fields.get("Zip") or convo_fields.get("Zip Code") or ""

    subject_sqft: Optional[float] = None
    try:
        if convo_fields.get("Sqft") is not None:
            subject_sqft = float(convo_fields.get("Sqft"))
        elif convo_fields.get("SQFT") is not None:
            subject_sqft = float(convo_fields.get("SQFT"))
    except Exception:
        subject_sqft = None

    # Try to capture lead/property linkage if present on the convo
    lead_id = None
    prop_id = None
    try:
        _lead = convo_fields.get("lead_id") or convo_fields.get("Lead")
        if isinstance(_lead, list) and _lead:
            lead_id = _lead[0]
        elif isinstance(_lead, str):
            lead_id = _lead
    except Exception:
        pass
    try:
        _pid = convo_fields.get("Property ID") or convo_fields.get("property_id")
        if isinstance(_pid, list) and _pid:
            prop_id = _pid[0]
        elif isinstance(_pid, str):
            prop_id = _pid
    except Exception:
        pass

    # 1) Seller ask (if present in text)
    seller_ask = _extract_ask_price(inbound_text)

    # 2) Fetch comps
    dm_comps = _fetch_dealmachine_comps(address, city, state, zipcode)
    z_comps  = _fetch_zillow_comps(address, city, state, zipcode)
    comps    = _normalize_comps(dm_comps, z_comps)

    # 3) Estimate ARV (fallback to seller ask uplift if nothing available)
    arv = _estimate_arv(comps, subject_sqft)
    if arv is None:
        if seller_ask:
            arv = seller_ask * 1.05  # light uplift
        else:
            # No comps, no ask → bail softly with a follow-up
            note = "No comps/ask; deferring with follow-up"
            if MessageProcessor:
                try:
                    MessageProcessor.send(
                        phone=from_phone,
                        body="Thanks for the details. I’m double-checking the recent sales and will circle back shortly.",
                        lead_id=lead_id,
                        property_id=prop_id,
                        direction="OUT",
                    )
                except Exception:
                    pass
            return {
                "ok": False,
                "reason": note,
                "offer": None,
                "arv": None,
                "repairs": None,
                "comps_used": 0,
                "timestamp": _iso_ts(),
            }

    # 4) Repairs estimate
    est_repairs = _estimate_repairs(subject_sqft, inbound_text)

    # 5) Offer math (with floor)
    mao = _compute_mao(arv, est_repairs)
    if mao < MIN_CASH_OFFER:
        mao = MIN_CASH_OFFER

    # 6) Decide copy (if seller gave an ask, consider a counter style)
    if seller_ask:
        body = _format_counterask_sms(seller_ask, mao)
    else:
        body = _format_offer_sms(mao)

    # 7) Send SMS (graceful if MessageProcessor missing)
    send_ok = False
    if MessageProcessor:
        try:
            send_res = MessageProcessor.send(
                phone=from_phone,
                body=body,
                lead_id=lead_id,
                property_id=prop_id,
                direction="OUT",
            )
            send_ok = bool((send_res or {}).get("status") == "sent")
        except Exception:
            send_ok = False

    return {
        "ok": send_ok,
        "sent_body": body,
        "offer": float(mao),
        "arv": float(arv),
        "repairs": float(est_repairs),
        "comps_used": len(comps),
        "seller_ask": float(seller_ask) if seller_ask else None,
        "zillow_used": bool(z_comps),
        "dealmachine_used": bool(dm_comps),
        "timestamp": _iso_ts(),
    }