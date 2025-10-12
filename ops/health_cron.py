# ops/health_cron.py
from __future__ import annotations
import os
import time
import json
import httpx
from ops.airtable_sync import AirtableSync

SYNC = AirtableSync()

# HEALTHTARGETS is JSON array of objects: [{"name":"sms-api","url":"https://your.app/health"},{"name":"web","url":"..."}]
TARGETS = os.getenv("HEALTHTARGETS_JSON", "[]")


def check(target: dict) -> None:
    name = target.get("name", "unknown")
    url = target.get("url")
    if not url:
        return
    t0 = time.perf_counter()
    status = "DOWN"
    try:
        with httpx.Client(timeout=8.0) as client:
            r = client.get(url)
            latency_ms = int((time.perf_counter() - t0) * 1000)
            if r.status_code < 400:
                status = "UP"
            SYNC.log_server(name=name, status=status, latency_ms=latency_ms, meta={"code": r.status_code})
    except Exception as e:
        latency_ms = int((time.perf_counter() - t0) * 1000)
        SYNC.log_server(name=name, status="DOWN", latency_ms=latency_ms, meta={"error": str(e)})


def main():
    try:
        arr = json.loads(TARGETS)
    except Exception:
        arr = []
    for t in arr:
        check(t)


if __name__ == "__main__":
    main()
