import os
from fastapi import HTTPException
from pyairtable import Table
from datetime import datetime, timezone

def strict_health(
    mode: str = "prospects",
    api_key: str | None = None,
    base_id: str | None = None,
) -> dict:
    """
    Actively verifies Airtable connectivity per engine mode.
    Returns {ok: True} only if all required tables respond.
    Allows api_key/base_id override (for tests).
    """

    # 1. Validate mode
    valid_modes = {"prospects", "leads", "inbounds"}
    if mode not in valid_modes:
        raise HTTPException(
            status_code=400,
            detail={"ok": False, "error": f"Invalid mode '{mode}'"},
        )

    # 2. Resolve API creds (prefer explicit params, else env)
    api_key = api_key or os.getenv("AIRTABLE_API_KEY")
    base_id = base_id or os.getenv("LEADS_CONVOS_BASE")

    if not api_key or not base_id:
        raise HTTPException(
            status_code=500,
            detail={"ok": False, "errors": ["Missing Airtable API key or base ID"]},
        )

    try:
        # 3. Probe Airtable connectivity
        table = Table(api_key, base_id, mode.capitalize())
        table.all(max_records=1)

        return {
            "ok": True,
            "mode": mode,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    except Exception as err:
        raise HTTPException(
            status_code=500,
            detail={"ok": False, "errors": [str(err)]},
        )