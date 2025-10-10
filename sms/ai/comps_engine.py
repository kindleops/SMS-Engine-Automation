# sms/comps_engine.py
from __future__ import annotations

import math
import statistics
from typing import Any, Dict, List, Optional, Tuple

# Optional deps: never crash if these modules are missing or shaped differently
try:
    from sms.ai import dealmachine_client  # expects get_comps(...)
except Exception:
    dealmachine_client = None  # type: ignore

try:
    from sms.ai import zillow_client  # expects get_zestimate(...)
except Exception:
    zillow_client = None  # type: ignore


# ────────────────────────────────────────────────────────────────────────────
# Utilities
# ────────────────────────────────────────────────────────────────────────────
def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip().replace(",", "").replace("$", "")
        if s.lower().endswith("k"):
            return float(s[:-1]) * 1_000.0
        return float(s)
    except Exception:
        return None


def _norm_comp_row(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Normalize a comp record from *any* source to a common shape:
        {
          "price": float,
          "sqft": Optional[float],
          "beds": Optional[float],
          "baths": Optional[float],
          "distance_mi": Optional[float],
          "close_date": Optional[str],   # as-is if provided
          "source": str                  # "dealmachine" | "zillow"
        }
    """
    # Common price keys we might see
    price_keys = [
        "sold_price", "sale_price", "price", "close_price", "closingPrice",
        "lastSoldPrice", "zestimate", "amount",
    ]
    sqft_keys = ["sqft", "square_feet", "livingArea", "living_area", "area"]
    beds_keys = ["beds", "bedrooms", "bed", "num_beds"]
    baths_keys = ["baths", "bathrooms", "bath", "num_baths"]
    dist_keys = ["distance_mi", "distance", "miles", "distanceMiles", "distance_miles"]
    date_keys = ["close_date", "sold_date", "sale_date", "closeDate", "soldDate"]

    # price is mandatory
    price = None
    for k in price_keys:
        v = raw.get(k)
        price = _safe_float(v)
        if price is not None:
            break
    if price is None:
        return None

    sqft = None
    for k in sqft_keys:
        sqft = _safe_float(raw.get(k))
        if sqft is not None:
            break

    beds = None
    for k in beds_keys:
        beds = _safe_float(raw.get(k))
        if beds is not None:
            break

    baths = None
    for k in baths_keys:
        baths = _safe_float(raw.get(k))
        if baths is not None:
            break

    distance = None
    for k in dist_keys:
        distance = _safe_float(raw.get(k))
        if distance is not None:
            break

    close_date = None
    for k in date_keys:
        if raw.get(k):
            close_date = str(raw.get(k))
            break

    src = str(raw.get("_source") or raw.get("source") or "").strip().lower()
    if not src:
        # best-effort: infer from available fields
        if "zestimate" in raw:
            src = "zillow"
        else:
            src = "dealmachine"

    return {
        "price": float(price),
        "sqft": sqft if sqft and sqft > 0 else None,
        "beds": beds if beds and beds > 0 else None,
        "baths": baths if baths and baths > 0 else None,
        "distance_mi": distance if distance and distance >= 0 else None,
        "close_date": close_date,
        "source": src,
    }


def _filter_reasonable(comps: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Keep comps within reasonable real-estate bounds.
    """
    out: List[Dict[str, Any]] = []
    for c in comps:
        price = _safe_float(c.get("price"))
        if not price or price < 30_000 or price > 2_000_000:
            continue
        sqft = _safe_float(c.get("sqft"))
        if sqft is not None and (sqft < 400 or sqft > 6_000):
            continue
        out.append(c)
    return out


def _trim_outliers_by_price(comps: List[Dict[str, Any]], keep_frac: float = 0.8) -> List[Dict[str, Any]]:
    """
    Trim symmetric price outliers (e.g., keep middle 80% by price).
    """
    if not comps:
        return comps
    keep_frac = min(max(keep_frac, 0.4), 1.0)  # clamp 40%..100%
    n = len(comps)
    if n < 5 or math.isclose(keep_frac, 1.0):
        return comps
    comps_sorted = sorted(comps, key=lambda c: c["price"])
    k = int(round(n * keep_frac))
    start = (n - k) // 2
    return comps_sorted[start:start + k]


def _median(values: List[float]) -> Optional[float]:
    vals = [v for v in values if isinstance(v, (int, float)) and not math.isnan(float(v))]
    if not vals:
        return None
    try:
        return float(statistics.median(sorted(vals)))
    except Exception:
        return None


def _select_top_by_distance(comps: List[Dict[str, Any]], top_n: int = 5) -> List[Dict[str, Any]]:
    if not comps:
        return []
    known = [c for c in comps if c.get("distance_mi") is not None]
    if known:
        known.sort(key=lambda c: (c["distance_mi"], abs((c.get("sqft") or 0) - (statistics.median([x.get("sqft") or 0 for x in comps]) or 0))))
        return known[:top_n]
    # distance unknown, just pick by price median vicinity (already trimmed)
    return comps[:min(top_n, len(comps))]


# ────────────────────────────────────────────────────────────────────────────
# Source fetchers (safe)
# ────────────────────────────────────────────────────────────────────────────
def _fetch_dealmachine(address: str, city: str, state: str, zip_code: str) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    if not dealmachine_client or not hasattr(dealmachine_client, "get_comps"):
        return [], "dealmachine_client missing or incompatible"
    try:
        raw = dealmachine_client.get_comps(address, city, state, zip_code)  # type: ignore[attr-defined]
        if not isinstance(raw, list):
            return [], "unexpected response type from DealMachine"
        norm = []
        for r in raw:
            n = _norm_comp_row({**r, "_source": "dealmachine"})
            if n:
                norm.append(n)
        return norm, None
    except Exception as e:
        return [], str(e)


def _fetch_zillow(address: str, city: str, state: str, zip_code: str) -> Tuple[Optional[float], Dict[str, Any], Optional[str]]:
    """
    Returns (zestimate, raw_payload, error_msg)
    """
    if not zillow_client or not hasattr(zillow_client, "get_zestimate"):
        return None, {}, "zillow_client missing or incompatible"
    try:
        data = zillow_client.get_zestimate(address, city, state, zip_code)  # type: ignore[attr-defined]
        if not isinstance(data, dict):
            return None, {}, "unexpected response type from Zillow"
        # zestimate might live under different keys depending on your client
        z = _safe_float(
            data.get("zestimate")
            or data.get("price")
            or (data.get("result", {}) if isinstance(data.get("result"), dict) else {}).get("zestimate")
        )
        return (float(z) if z else None), data, None
    except Exception as e:
        return None, {}, str(e)


# ────────────────────────────────────────────────────────────────────────────
# Public API
# ────────────────────────────────────────────────────────────────────────────
def run_comps(address: str, city: str, state: str, zip_code: str) -> Dict[str, Any]:
    """
    Robust comps engine:
      1) Pull comps from DealMachine (if available)
      2) Pull Zestimate from Zillow (if available)
      3) Clean + normalize + trim outliers
      4) Compute ARV as:
           - median of top-by-distance DealMachine comps (up to 5), then
           - if Zillow present, average with Zestimate
      5) Return full breakdown with confidence + errors (never raises)
    """
    results: Dict[str, Any] = {
        "input": {"address": address, "city": city, "state": state, "zip": zip_code},
        "ok": True,
        "errors": [],
        "dealmachine": {"count": 0, "used": 0},
        "zillow": {},
        "arv": None,
        "arv_method": None,
        "confidence": None,  # low | medium | high
    }

    # 1) DealMachine
    dm_norm, dm_err = _fetch_dealmachine(address, city, state, zip_code)
    if dm_err:
        results["errors"].append({"source": "dealmachine", "error": dm_err})
    dm_filtered = _filter_reasonable(dm_norm)
    dm_trimmed = _trim_outliers_by_price(dm_filtered, keep_frac=0.8)
    top_dm = _select_top_by_distance(dm_trimmed, top_n=5)

    results["dealmachine"]["count"] = len(dm_norm)
    results["dealmachine"]["used"] = len(top_dm)
    results["dealmachine"]["comps"] = top_dm  # already normalized & trimmed

    comps_median = _median([c["price"] for c in top_dm]) if top_dm else None

    # 2) Zillow
    zestimate, z_payload, z_err = _fetch_zillow(address, city, state, zip_code)
    if z_err:
        results["errors"].append({"source": "zillow", "error": z_err})
    if z_payload:
        results["zillow"] = {k: v for k, v in z_payload.items() if k != "comps"}  # keep payload minimal
    if zestimate:
        results["zillow"]["zestimate"] = float(zestimate)

    # 3) Reconciliation → ARV
    arv: Optional[float] = None
    if comps_median is not None and zestimate is not None:
        arv = (comps_median + zestimate) / 2.0
        results["arv_method"] = "avg(dm_median, zestimate)"
    elif comps_median is not None:
        arv = comps_median
        results["arv_method"] = "dm_median"
    elif zestimate is not None:
        arv = zestimate
        results["arv_method"] = "zestimate_only"
    else:
        results["ok"] = False
        results["arv_method"] = "none"
        results["errors"].append({"source": "engine", "error": "no valid comps or zestimate"})
        return results

    # 4) Round to cleaner number (nearest $500)
    if arv is not None:
        rounded = int(round(arv / 500.0) * 500)
        results["arv"] = float(rounded)

    # 5) Confidence heuristic
    #    - high: >=4 comps used
    #    - medium: 2-3 comps or 1 comp + zestimate
    #    - low: only zestimate or 1 comp
    used = results["dealmachine"]["used"]
    if used >= 4:
        conf = "high"
    elif used >= 2 or (used >= 1 and zestimate is not None):
        conf = "medium"
    else:
        conf = "low"
    results["confidence"] = conf

    return results