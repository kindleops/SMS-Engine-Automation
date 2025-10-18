import os, requests, traceback
from .common import tznow_iso, get_table, remap_existing_only
from .devops_logger import log_devops

FASTAPI_URL = os.getenv("FASTAPI_URL", "http://localhost:8000")


def run():
    tbl = get_table("DEVOPS_BASE", "Health Checks")
    if not tbl:
        return {"ok": False, "err": "Missing Health Checks table"}

    results = {}
    for mode in ["prospects", "leads", "inbounds"]:
        try:
            r = requests.get(f"{FASTAPI_URL}/health/strict", params={"mode": mode}, timeout=20)
            js = r.json() if r.content else {}
            row = {
                "Service": [mode.capitalize()],
                "Timestamp": tznow_iso(),
                "Status Code": r.status_code,
                "Response Time (ms)": js.get("latency", 0),
                "Status": "Healthy" if r.status_code == 200 else "Down",
                "Notes": str(js)[:10000],
            }
            tbl.create(remap_existing_only(tbl, row))
            log_devops("Health Check", "FastAPI", {"mode": mode, "status": r.status_code})
            results[mode] = {"status": r.status_code}
        except Exception as e:
            traceback.print_exc()
            log_devops("Health Check", "FastAPI", {"mode": mode, "error": str(e)}, status="FAIL", severity="Warn")
            results[mode] = {"status": "ERROR", "error": str(e)}
    return {"ok": True, "results": results}


if __name__ == "__main__":
    print(run())
