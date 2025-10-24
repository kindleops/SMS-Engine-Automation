"""
🚀 Airtable Client (Unified, Version-Agnostic)
---------------------------------------------
• Works with pyairtable v1/v2 (Table or Api.table)
• Safe CRUD with retries and field filtering
• Cached table getters for leads, campaigns, performance
"""

from __future__ import annotations

import os, re, traceback, time
from functools import lru_cache
from typing import Optional, Dict, Any, List

from sms.runtime import get_logger

logger = get_logger("airtable_client")

# ---------------- pyairtable imports ----------------
try:
    from pyairtable import Table as _TopLevelTable  # v1/v2 legacy
except Exception:
    _TopLevelTable = None  # type: ignore

try:
    from pyairtable.table import Table as _ModuleTable  # v2 internal
except Exception:
    _ModuleTable = None  # type: ignore

try:
    from pyairtable import Api as _Api  # v2 canonical
except Exception:
    _Api = None  # type: ignore


# ---------------- internal helpers ----------------
def _first_env(*names: str) -> Optional[str]:
    for n in names:
        v = os.getenv(n)
        if v:
            return v
    return None


def _mk_table(api_key: Optional[str], base_id: Optional[str], name: str):
    """Version-agnostic constructor fallback chain."""
    if not (api_key and base_id):
        logger.warning(f"⚠️ Missing config for '{name}' (api_key={bool(api_key)}, base={bool(base_id)})")
        return None

    for ctor_name, ctor in (
        ("TopLevelTable", _TopLevelTable),
        ("ModuleTable", _ModuleTable),
    ):
        if ctor:
            try:
                tbl = ctor(api_key, base_id, name)  # type: ignore
                logger.debug(f"✅ Using {ctor_name} for table '{name}'")
                return tbl
            except Exception as e:
                logger.warning(f"⚠️ {ctor_name} failed for {name}: {e}")
                continue

    # canonical v2 fallback
    if _Api:
        try:
            api = _Api(api_key)
            tbl = api.table(base_id, name)
            logger.debug(f"✅ Using Api().table for '{name}'")
            return tbl
        except Exception as e:
            logger.error(f"❌ Api().table failed for '{name}': {e}")
            traceback.print_exc()

    logger.error(f"⚠️ pyairtable not compatible → '{name}' in MOCK mode")
    return None


# ---------------- table getters (cached) ----------------
@lru_cache(maxsize=None)
def get_leads_table(name: str):
    base = _first_env("LEADS_CONVOS_BASE", "AIRTABLE_LEADS_CONVOS_BASE_ID")
    key = _first_env("AIRTABLE_ACQUISITIONS_KEY", "LEADS_CONVOS_KEY", "AIRTABLE_API_KEY")
    return _mk_table(key, base, name)


@lru_cache(maxsize=None)
def get_campaigns_table(name: str):
    base = _first_env("CAMPAIGN_CONTROL_BASE", "AIRTABLE_CAMPAIGN_CONTROL_BASE_ID")
    key = _first_env("AIRTABLE_COMPLIANCE_KEY", "CAMPAIGN_CONTROL_KEY", "AIRTABLE_API_KEY")
    return _mk_table(key, base, name)


@lru_cache(maxsize=None)
def get_performance_table(name: str):
    base = _first_env("PERFORMANCE_BASE", "AIRTABLE_PERFORMANCE_BASE_ID")
    key = _first_env("AIRTABLE_REPORTING_KEY", "PERFORMANCE_KEY", "AIRTABLE_API_KEY")
    return _mk_table(key, base, name)


# ---------------- shortcuts ----------------
@lru_cache(maxsize=None)
def get_convos():
    return get_leads_table(os.getenv("CONVERSATIONS_TABLE", "Conversations"))


@lru_cache(maxsize=None)
def get_leads():
    return get_leads_table(os.getenv("LEADS_TABLE", "Leads"))


@lru_cache(maxsize=None)
def get_prospects():
    return get_leads_table(os.getenv("PROSPECTS_TABLE", "Prospects"))


@lru_cache(maxsize=None)
def get_drip():
    return get_leads_table(os.getenv("DRIP_QUEUE_TABLE", "Drip Queue"))


@lru_cache(maxsize=None)
def get_campaigns():
    return get_campaigns_table(os.getenv("CAMPAIGNS_TABLE", "Campaigns"))


@lru_cache(maxsize=None)
def get_numbers():
    return get_campaigns_table(os.getenv("NUMBERS_TABLE", "Numbers"))


@lru_cache(maxsize=None)
def get_optouts():
    return get_campaigns_table(os.getenv("OPTOUTS_TABLE", "Opt-Outs"))


@lru_cache(maxsize=None)
def get_kpis():
    return get_performance_table(os.getenv("KPIS_TABLE", "KPIs"))


@lru_cache(maxsize=None)
def get_runs():
    return get_performance_table(os.getenv("RUNS_TABLE", "Logs"))


# ---------------- field utilities ----------------
_field_cache: Dict[str, Dict[str, str]] = {}


def _norm(s: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s).strip().lower())


def _auto_field_map(tbl) -> Dict[str, str]:
    """Caches Airtable field maps per table ID for faster safe updates."""
    tid = getattr(tbl, "name", None) or str(id(tbl))
    if tid in _field_cache:
        return _field_cache[tid]

    try:
        page = tbl.all(max_records=1)
        fields = list((page[0] if page else {"fields": {}}).get("fields", {}).keys())
        amap = {_norm(k): k for k in fields}
        _field_cache[tid] = amap
        return amap
    except Exception:
        traceback.print_exc()
        return {}


def remap_existing_only(tbl, payload: Dict[str, Any]) -> Dict[str, Any]:
    amap = _auto_field_map(tbl)
    if not amap:
        return payload
    out: Dict[str, Any] = {}
    for k, v in payload.items():
        ak = amap.get(_norm(k))
        if ak:
            out[ak] = v
    return out


# ---------------- safe CRUD with retry ----------------
def _with_retry(fn, *args, retries: int = 3, delay: float = 0.5, **kwargs):
    for attempt in range(retries):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            msg = str(e)
            if "429" in msg or "422" in msg:
                time.sleep(delay)
                delay *= 2
                continue
            traceback.print_exc()
            break
    return None


def safe_create(tbl, payload: Dict[str, Any]):
    if not (tbl and payload):
        return None
    return _with_retry(tbl.create, remap_existing_only(tbl, payload))


def safe_update(tbl, rec_id: str, payload: Dict[str, Any]):
    if not (tbl and rec_id and payload):
        return None
    return _with_retry(tbl.update, rec_id, remap_existing_only(tbl, payload))


def safe_get(tbl, rec_id: str):
    if not (tbl and rec_id):
        return None
    return _with_retry(tbl.get, rec_id)


def safe_all(tbl, **kwargs) -> List[Dict[str, Any]]:
    if not tbl:
        return []
    try:
        return list(tbl.all(**kwargs))
    except Exception:
        traceback.print_exc()
        return []


# ---------------- diagnostics ----------------
def config_summary() -> Dict[str, Any]:
    return {
        "pyairtable_top_Table": bool(_TopLevelTable),
        "pyairtable_mod_Table": bool(_ModuleTable),
        "pyairtable_Api": bool(_Api),
        "leads_base": bool(_first_env("LEADS_CONVOS_BASE")),
        "campaign_control_base": bool(_first_env("CAMPAIGN_CONTROL_BASE")),
        "performance_base": bool(_first_env("PERFORMANCE_BASE")),
        "some_api_key": bool(
            _first_env(
                "AIRTABLE_API_KEY",
                "AIRTABLE_ACQUISITIONS_KEY",
                "AIRTABLE_COMPLIANCE_KEY",
                "AIRTABLE_REPORTING_KEY",
                "LEADS_CONVOS_KEY",
                "CAMPAIGN_CONTROL_KEY",
                "PERFORMANCE_KEY",
            )
        ),
        "cached_field_maps": len(_field_cache),
    }
