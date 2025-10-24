"""
🚀 SMS Engine Package Init
--------------------------
Provides backward compatibility for pyairtable v1 → v2.

Legacy usage:
    Table(API_KEY, BASE_ID, "TableName")

v2 usage:
    Api(API_KEY).table(BASE_ID, "TableName")

This shim preserves compatibility for all legacy imports.
"""

import sys

# ---- Optional: lightweight settings re-export ----
try:
    from .config import settings  # type: ignore
except Exception:
    settings = None  # type: ignore

# ---- pyairtable v1 compatibility shim ----
try:
    import pyairtable as _pyat
    from pyairtable import Api as _Api

    try:
        from pyairtable.api.table import Table as _RealTable  # type: ignore
    except Exception:
        _RealTable = None  # type: ignore

    # Simple cache to avoid re-instantiating Api for same key
    _api_cache = {}

    def _get_api(key: str):
        if key not in _api_cache:
            _api_cache[key] = _Api(key)
        return _api_cache[key]

    class _CompatTable:
        """Drop-in replacement for pyairtable.Table (v1-style constructor)."""

        def __new__(cls, api_key, base_or_id, table_name, *args, **kwargs):
            try:
                # Normal v1 pattern: (api_key, base_id, table_name)
                return _get_api(api_key).table(base_or_id, table_name)
            except Exception as e:
                # If base_or_id is already a Base object, fallback to direct Table init
                if _RealTable is not None and getattr(base_or_id, "__class__", None).__name__ == "Base":
                    try:
                        return _RealTable(None, base_or_id, table_name)  # type: ignore
                    except Exception:
                        pass

                sys.stderr.write(
                    f"[sms.init] ⚠️ pyairtable shim failed: {e.__class__.__name__}: {e}\n"
                )
                raise

    # Monkey-patch pyairtable.Table globally
    _pyat.Table = _CompatTable  # type: ignore[attr-defined]
    sys.stderr.write("[sms.init] ✅ pyairtable Table shim applied successfully.\n")

except Exception as e:
    sys.stderr.write(f"[sms.init] ⚠️ pyairtable not available or shim skipped: {e}\n")
    pass

__all__ = ["settings"]
