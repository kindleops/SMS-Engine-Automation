import os, requests, traceback
from .common import tznow_iso, get_table, remap_existing_only
from .devops_logger import log_devops

RURL   = os.getenv("UPSTASH_REDIS_REST_URL")
RTOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN")

def rc(cmd: list[str]):
    r = requests.post(f"{RURL}/pipeline", headers={"Authorization": f"Bearer {RTOKEN}"}, json={"commands": [cmd]})
    r.raise_for_status()
    return r.json().get("result", [])

def run(limit_keys: int = 300):
    if not (RURL and RTOKEN):
        return {"ok": False, "err": "Missing Upstash env"}
    tbl = get_table("DEVOPS_BASE", "Redis Metrics")
    if not tbl:
        return {"ok": False, "err": "Missing Redis Metrics table"}

    out = {"count": 0}
    try:
        [keys] = rc(["KEYS", "sms:*"]) or [[]]
        keys = keys[:limit_keys]
        for k in keys:
            [val] = rc(["GET", k]) or [None]
            row = {
                "Key": k,
                "Value": str(val),
                "Type": "Quota" if ("quota" in k or "cooldown" in k) else "Cache",
                "Timestamp": tznow_iso(),
            }
            tbl.create(remap_existing_only(tbl, row))
            out["count"] += 1
        log_devops("Redis Sync", "Upstash", {"keys": out["count"]})
        return {"ok": True, **out}
    except Exception as e:
        traceback.print_exc()
        log_devops("Redis Sync", "Upstash", {"error": str(e)}, status="FAIL", severity="Error")
        return {"ok": False, "err": str(e)}

if __name__ == "__main__":
    print(run())