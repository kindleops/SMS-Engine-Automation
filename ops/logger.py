# sms/logger.py
from __future__ import annotations
import sys
import traceback
from datetime import datetime, timezone

try:
    from ops.airtable_sync import AirtableSync  # local import

    SYNC = AirtableSync()
except Exception:
    SYNC = None


def ts():
    return datetime.now(timezone.utc).isoformat()


def info(msg: str, service: str = "sms"):
    print(f"[INFO] {ts()} [{service}] {msg}")


def warn(msg: str, service: str = "sms"):
    print(f"[WARN] {ts()} [{service}] {msg}", file=sys.stderr)
    if SYNC:
        SYNC.log_error(service=service, message=msg, severity="WARN")


def error(msg: str, service: str = "sms", exc: Exception | None = None):
    print(f"[ERROR] {ts()} [{service}] {msg}", file=sys.stderr)
    if exc:
        traceback.print_exc()
    if SYNC:
        SYNC.log_error(service=service, message=f"{msg} :: {exc or ''}", severity="ERROR")
