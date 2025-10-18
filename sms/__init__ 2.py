# sms/__init__.py
"""
Package init + pyairtable v1 -> v2 compatibility shim.

Many modules still do:
    from pyairtable import Table
    Table(API_KEY, BASE_ID, "TableName")

In pyairtable v2, that signature no longer works. We monkey-patch
pyairtable.Table so the old 3-arg form is translated to v2's:
    Api(API_KEY).table(BASE_ID, "TableName")

This runs before importing other sms.* modules (since FastAPI loads
`sms.main:app`, which imports this __init__ first).
"""

# Re-export anything you want from config (optional)
try:
    from .config import settings  # lightweight
except Exception:
    settings = None  # type: ignore

# ---- pyairtable v1 compatibility shim ----
try:
    import pyairtable as _pyat  # the package
    from pyairtable import Api as _Api

    try:
        # Real v2 Table class (for advanced fallback if needed)
        from pyairtable.api.table import Table as _RealTable  # type: ignore
    except Exception:
        _RealTable = None  # type: ignore

    class _CompatTable:
        """
        Drop-in replacement so existing code can continue to call:
            Table(api_key, base_id, table_name)
        Returns a v2 Table object from Api(api_key).table(base_id, table_name).
        """

        def __new__(cls, api_key, base_or_id, table_name, *args, **kwargs):
            # If the caller accidentally passes a Base object, try to use the real ctor.
            try:
                # Most legacy calls: (api_key:str, base_id:str, name:str)
                return _Api(api_key).table(base_or_id, table_name)
            except Exception:
                # Ultimate fallback: if v2 Table is available and base_or_id looks like a Base
                if _RealTable is not None and getattr(base_or_id, "__class__", None).__name__ == "Base":
                    try:
                        return _RealTable(None, base_or_id, table_name)  # type: ignore
                    except Exception:
                        pass
                raise  # surface the original error for visibility

    # Monkey-patch the package attribute
    _pyat.Table = _CompatTable  # type: ignore[attr-defined]

except Exception:
    # If pyairtable isn't installed yet, do nothing; imports will fail normally.
    pass

__all__ = ["settings"]
