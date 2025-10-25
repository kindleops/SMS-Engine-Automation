"""Utility helpers for Drip status normalization."""

from __future__ import annotations

ALLOWED_DRIP_STATUSES = {"Queued", "Sending", "Sent", "Failed", "Error"}


def _sanitize_status(val: str | None) -> str:
    """Normalize drip status values to a safe select option."""
    v = (val or "").strip().replace("…", "...")
    if v.endswith("..."):
        v = v[:-3]
    vt = v.title()
    return vt if vt in ALLOWED_DRIP_STATUSES else "Queued"


__all__ = ["ALLOWED_DRIP_STATUSES", "_sanitize_status"]
