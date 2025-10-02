# sms/ai_closer.py
import os
import re
import math
import statistics
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

# Re-use your existing sender
from sms.message_processor import MessageProcessor

try:
    from pyairtable import Table
except ImportError:
    Table = None

# ---------- ENV / CONFIG ----------
CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")
LEADS_TABLE = os.getenv("LEADS_TABLE", "Leads")
PROSPECTS_TABLE = os.getenv("PROSPECTS_TABLE", "Prospects")

# Offer math (override in env if desired)
OFFER_MAO_PCT = float(os.getenv("OFFER_MAO_PCT", "0.70"))   # 70% rule default
CLOSING_FEE_FLAT = float(os.getenv("CLOSING_FEE_FLAT", "4000"))
WHOLESALE_FEE_FLAT = float(os.getenv("WHOLESALE_FEE_FLAT", "8000"))
REPAIR_FLOOR_PER_SQFT = float(os.getenv("REPAIR_FLOOR_PER_SQFT", "12"))  # min repair/sqft
REPAIR_CEIL_PER_SQFT = float(os.getenv("REPAIR_CEIL_PER_SQFT", "55"))    # max repair/sqft
DM_ENABLED = os.getenv("DEALMACHINE_API_KEY") is not None
ZILLOW_ENABLED = os.getenv("ZILLOW_RAPIDAPI_KEY") is not None

# ---------- Airtable lazy clients ----------
@lru_cache(maxsize=None)
def _get_airtable(table_name: str) -> Optional["Table"]:
    api_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
    if not (api_key and base_id and Table):
        return None
    try:
        return Table(api_key, base_id, table_name)
    except Exception:
        return None

def _iso_ts() -> str:
    return datetime.now(timezone.utc).isoformat()

# ---------- Utilities ----------
_PRICE_RE = re.compile(r"(\$?\s*\d{2,3}(?:,\d{3})?)(?:\s*(k|K))?")

def _extract_ask_price(text: str) -> Optional[float]:
    """
    Extracts a number like '125k' or '$137,500' from seller message.
    Returns float dollars if found.
    """
    if not text:
        return None
    m = _PRICE_RE.search(text.replace(" ", ""))
    if not m:
        return None
    raw, suff = m.group(1), m.group(2)
    raw = raw.replace("$", "").replace(",", "")
    try:
        val = float(raw)
        if suff and suff.lower() == "k":
            val *= 1000
        # sanity
        if val < 5000:
            return None
        return val
    except Exception:
        return None

def _median(nums: List[float]) -> Optional[float]:
    nums = [n for n in nums if isinstance(n, (int, float)) and not math.isnan(n)]
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
    # TODO: implement real HTTP call. For now, return empty to force Zillow / fallback.
    return []

def _fetch_zillow_comps(address: str, city: str = "", state: str = "", zipc: str = "") -> List[Dict[str, Any]]:
    """
    Stub: Replace with a real Zillow (RapidAPI) call.
    Return list of comps with: price, beds, baths, sqft, distance_mi, close_date.
    """
    if not ZILLOW_ENABLED:
        return []
    # TODO: implement real HTTP call. For now, return empty to use fallback.
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
        price = c.get("price")
        sqft = c.get("sqft") or 0
        if not price:
            continue
        if not (30000 <= float(price) <= 2000000):
            continue
        if sqft and not (400 <= float(sqft) <= 6000):
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
        dist = float(c.get("distance_mi", 0.5))
        if subject_sqft and c.get("sqft"):
            dsq = abs(float(c["sqft"]) - float(subject_sqft)) / max(subject_sqft, 1)
        else:
            dsq = 0.15
        return dist * 0.7 + dsq * 0.3

    sorted_comps = sorted(comps, key=similarity_score)
    top = sorted_comps[:5] if len(sorted_comps) >= 5 else sorted_comps
    return _median([float(c["price"]) for c in top if c.get("price")])

# ---------- Repairs ----------
def _infer_repair_severity(text: str) -> str:
    t = (text or "").lower()
    if any(w in t for w in ["burn", "fire", "foundation", "black mold", "unlivable"]):
        return "heavy"
    if any(w in t for w in ["roof", "hvac", "plumbing", "electrical", "water damage"]):
        return "moderate"
    if any(w in t for w in ["needs work", "repairs", "old kitchen", "outdated"]):
        return "light"
    return "unknown"

def _estimate_repairs(sqft: Optional[float], seller_text: str) -> float:
    """
    Very simple heuristic:
    - Start with per-sqft band based on severity (unknown → low-mid)
    - Clamp to reasonable floors/ceilings
    """
    severity = _infer_repair_severity(seller_text)
    base = {
        "heavy": 45,
        "moderate": 30,
        "light": 18,
        "unknown": 15,
    }.get(severity, 15)

    per_sqft = _soft_cap(base, REPAIR_FLOOR_PER_SQFT, REPAIR_CEIL_PER_SQFT)
    if not sqft:
        # If we don't know size, return a conservative mid figure
        return float(per_sqft * 1200)
    return float(per_sqft * float(sqft))

# ---------- Offer math ----------
def _compute_mao(arv: float, est_repairs: float) -> float:
    """
    MAO = ARV * PCT - Repairs - Fees (closing + wholesale)
    """
    return float(arv * OFFER_MAO_PCT - est_repairs - CLOSING_FEE_FLAT - WHOLESALE_FEE_FLAT)

# ---------- SMS copy ----------
def _format_offer_sms(offer: float) -> str:
    # Keep it crisp, no ARV mention per your constraint
    # Round to nearest $500 for cleaner number psychology
    rounded = int(round(offer / 500.0) * 500)
    return (
        f"Thanks for your patience. After running the numbers and factoring in repairs & comps, "
        f"we’d be at ${rounded:,} cash. We cover all closing costs, no commissions, and buy as-is. "
        f"Does that work for you?"
    )

def _format_counterask_sms(ask: float, offer: float) -> str:
    gap = max(0, ask - offer)
    if gap <= 2500:
        return (
            f"Appreciate it. We’re close — I could potentially get to about ${int(round((ask+offer)/1000)*1000):,} "
            f"if we can move quickly. Would that work?"
        )
    return (
        f"Thanks for sharing. Based on repairs and recent sales, we’re around ${int(round(offer/1000)*1000):,}. "
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
    city = convo_fields.get("City") or ""
    state = convo_fields.get("State") or ""
    zipcode = convo_fields.get("Zip") or convo_fields.get("Zip Code") or ""
    subject_sqft = None
    if convo_fields.get("Sqft"):
        try:
            subject_sqft = float(convo_fields["Sqft"])
        except Exception:
            pass

    # 1) Seller ask (if present in text)
    seller_ask = _extract_ask_price(inbound_text)

    # 2) Fetch comps
    dm_comps = _fetch_dealmachine_comps(address, city, state, zipcode)
    z_comps = _fetch_zillow_comps(address, city, state, zipcode)
    comps = _normalize_comps(dm_comps, z_comps)

    # 3) Estimate ARV
    arv = _estimate_arv(comps, subject_sqft)
    # If we totally fail to get comps, take a conservative default using seller ask if it exists
    if arv is None:
        if seller_ask:
            arv = seller_ask * 1.05  # light uplift
        else:
            # No comps, no ask → bail softly
            note = "No comps/ask; deferring with follow-up"
            MessageProcessor.send(
                phone=from_phone,
                body="Thanks for the details. I’m double-checking the recent sales and will circle back shortly.",
                lead_id=None,
                property_id=None,
                direction="OUT",
            )
            return {
                "ok": False,
                "reason": note,
                "offer": None,
                "arv": None,
                "repairs": None,
                "comps_used": 0,
            }

    # 4) Repairs estimate
    est_repairs = _estimate_repairs(subject_sqft, inbound_text)

    # 5) Offer math
    mao = _compute_mao(arv, est_repairs)
    if mao < 5000:
        mao = 5000.0  # never send a nonsensical lowball

    # 6) Decide copy (if seller gave an ask, consider a counter style)
    if seller_ask:
        body = _format_counterask_sms(seller_ask, mao)
    else:
        body = _format_offer_sms(mao)

    send_res = MessageProcessor.send(
        phone=from_phone,
        body=body,
        lead_id=None,        # if you have lead_id/property_id on convo_fields, you can wire them here
        property_id=None,
        direction="OUT",
    )

    return {
        "ok": (send_res or {}).get("status") == "sent",
        "sent_body": body,
        "offer": mao,
        "arv": arv,
        "repairs": est_repairs,
        "comps_used": len(comps),
        "seller_ask": seller_ask,
        "zillow_used": bool(z_comps),
        "dealmachine_used": bool(dm_comps),
        "timestamp": _iso_ts(),
    }