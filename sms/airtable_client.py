# sms/airtable_client.py
from __future__ import annotations

import os, traceback, re
from functools import lru_cache
from typing import Optional, Dict, Any, List, Callable

# --- Multi-version compat imports (v1.x and v2.x) --------------------
try:
    # v1.x often exposes Table at top level
    from pyairtable import Table as _TopLevelTable  # type: ignore
except Exception:
    _TopLevelTable = None  # type: ignore

try:
    # v2.x exposes Table at pyairtable.table
    from pyairtable.table import Table as _ModuleTable  # type: ignore
except Exception:
    _ModuleTable = None  # type: ignore

try:
    # v2.x canonical client
    from pyairtable import Api as _Api  # type: ignore
except Exception:
    _Api = None  # type: ignore

# Public “Table” type for annotations (doesn’t crash if lib missing)
try:
    from typing import TYPE_CHECKING
    if TYPE_CHECKING:
        from pyairtable.table import Table  # type: ignore
except Exception:
    pass


__all__ = [
    "get_leads_table", "get_campaigns_table", "get_performance_table",
    "get_convos", "get_leads", "get_prospects", "get_templates", "get_drip",
    "get_campaigns", "get_numbers", "get_optouts", "get_kpis", "get_runs",
    "remap_existing_only", "safe_create", "safe_update", "safe_get", "safe_all",
    "config_summary",
]

# ---------------- env helpers ----------------
def _first_env(*names: str) -> Optional[str]:
    for n in names:
        v = os.getenv(n)
        if v:
            return v
    return None

def _log(msg: str) -> None:
    print(f"[AirtableClient] {msg}")

# ---------------- table factory (works across versions) ----------------
def _mk_table(api_key: Optional[str], base_id: Optional[str], name: str):
    """
    Try Table constructors in this order:
      1) pyairtable.Table(api_key, base_id, name)         (v1 & some v2)
      2) pyairtable.table.Table(api_key, base_id, name)   (v2)
      3) Api(api_key).table(base_id, name)                (v2 canonical)
    Return None if lib or env is missing.
    """
    if not (api_key and base_id):
        _log(f"⚠️ Missing config for '{name}' (api_key={bool(api_key)}, base={bool(base_id)})")
        return None

    # 1) Top-level Table
    if _TopLevelTable:
        try:
            return _TopLevelTable(api_key, base_id, name)  # type: ignore
        except Exception:
            traceback.print_exc()

    # 2) Module Table
    if _ModuleTable:
        try:
            return _ModuleTable(api_key, base_id, name)  # type: ignore
        except Exception:
            traceback.print_exc()

    # 3) Api().table
    if _Api:
        try:
            api = _Api(api_key)  # type: ignore
            return api.table(base_id, name)
        except Exception:
            traceback.print_exc()

    _log(f"⚠️ pyairtable not available/compatible → '{name}' in MOCK mode")
    return None

# ---------------- base-level getters (cached) ----------------
@lru_cache(maxsize=None)
def get_leads_table(name: str):
    base = _first_env("LEADS_CONVOS_BASE", "AIRTABLE_LEADS_CONVOS_BASE_ID")
    key  = _first_env("AIRTABLE_ACQUISITIONS_KEY", "LEADS_CONVOS_KEY", "AIRTABLE_API_KEY")
    return _mk_table(key, base, name)

@lru_cache(maxsize=None)
def get_campaigns_table(name: str):
    base = _first_env("CAMPAIGN_CONTROL_BASE", "AIRTABLE_CAMPAIGN_CONTROL_BASE_ID")
    key  = _first_env("AIRTABLE_COMPLIANCE_KEY", "CAMPAIGN_CONTROL_KEY", "AIRTABLE_API_KEY")
    return _mk_table(key, base, name)

@lru_cache(maxsize=None)
def get_performance_table(name: str):
    base = _first_env("PERFORMANCE_BASE", "AIRTABLE_PERFORMANCE_BASE_ID")
    key  = _first_env("AIRTABLE_REPORTING_KEY", "PERFORMANCE_KEY", "AIRTABLE_API_KEY")
    return _mk_table(key, base, name)

# ---------------- shortcuts (cached) ----------------
@lru_cache(maxsize=None)
def get_convos():      return get_leads_table(_first_env("CONVERSATIONS_TABLE") or "Conversations")
@lru_cache(maxsize=None)
def get_leads():       return get_leads_table(_first_env("LEADS_TABLE") or "Leads")
@lru_cache(maxsize=None)
def get_prospects():   return get_leads_table(_first_env("PROSPECTS_TABLE") or "Prospects")
@lru_cache(maxsize=None)
def get_templates():   return get_leads_table(_first_env("TEMPLATES_TABLE") or "Templates")
@lru_cache(maxsize=None)
def get_drip():        return get_leads_table(_first_env("DRIP_QUEUE_TABLE", "DRIP_TABLE") or "Drip Queue")
@lru_cache(maxsize=None)
def get_campaigns():   return get_campaigns_table(_first_env("CAMPAIGNS_TABLE") or "Campaigns")
@lru_cache(maxsize=None)
def get_numbers():     return get_campaigns_table(_first_env("NUMBERS_TABLE") or "Numbers")
@lru_cache(maxsize=None)
def get_optouts():     return get_campaigns_table(_first_env("OPTOUTS_TABLE") or "Opt-Outs")
@lru_cache(maxsize=None)
def get_kpis():        return get_performance_table(_first_env("KPIS_TABLE", "KPIS_TABLE_NAME") or "KPIs")
@lru_cache(maxsize=None)
def get_runs():        return get_performance_table(_first_env("RUNS_TABLE", "RUNS_TABLE_NAME") or "Runs/Logs")

# ---------------- field-safe helpers ----------------
def _norm(s: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s).strip().lower())

def _auto_field_map(tbl) -> Dict[str, str]:
    try:
        page = tbl.all(max_records=1)  # works in v1/v2
        fields = list((page[0] if page else {"fields": {}}).get("fields", {}).keys())
    except Exception:
        fields = []
    return {_norm(k): k for k in fields}

def remap_existing_only(tbl, payload: Dict[str, Any]) -> Dict[str, Any]:
    if not tbl:
        return dict(payload)
    amap = _auto_field_map(tbl)
    if not amap:
        return dict(payload)
    out: Dict[str, Any] = {}
    for k, v in payload.items():
        ak = amap.get(_norm(k))
        if ak:
            out[ak] = v
    return out

def safe_create(tbl, payload: Dict[str, Any]):
    if not (tbl and payload):
        return None
    try:
        return tbl.create(remap_existing_only(tbl, payload))
    except Exception:
        traceback.print_exc()
        return None

def safe_update(tbl, rec_id: str, payload: Dict[str, Any]):
    if not (tbl and rec_id and payload):
        return None
    try:
        return tbl.update(rec_id, remap_existing_only(tbl, payload))
    except Exception:
        traceback.print_exc()
        return None

def safe_get(tbl, rec_id: str):
    if not (tbl and rec_id):
        return None
    try:
        return tbl.get(rec_id)
    except Exception:
        traceback.print_exc()
        return None

def safe_all(tbl, **kwargs) -> List[Dict[str, Any]]:
    if not tbl:
        return []
    try:
        return list(tbl.all(**kwargs))
    except Exception:
        traceback.print_exc()
        return []

# ---------------- diagnostics ----------------
def config_summary() -> Dict[str, bool]:
    return {
        "pyairtable_top_Table": bool(_TopLevelTable),
        "pyairtable_mod_Table": bool(_ModuleTable),
        "pyairtable_Api": bool(_Api),
        "leads_base": bool(_first_env("LEADS_CONVOS_BASE", "AIRTABLE_LEADS_CONVOS_BASE_ID")),
        "campaign_control_base": bool(_first_env("CAMPAIGN_CONTROL_BASE", "AIRTABLE_CAMPAIGN_CONTROL_BASE_ID")),
        "performance_base": bool(_first_env("PERFORMANCE_BASE", "AIRTABLE_PERFORMANCE_BASE_ID")),
        "some_api_key": bool(_first_env(
            "AIRTABLE_API_KEY", "AIRTABLE_ACQUISITIONS_KEY",
            "AIRTABLE_COMPLIANCE_KEY", "AIRTABLE_REPORTING_KEY",
            "LEADS_CONVOS_KEY", "CAMPAIGN_CONTROL_KEY", "PERFORMANCE_KEY",
        )),
    }
