import os, traceback, requests
from .common import tznow_iso, get_table, remap_existing_only
from .devops_logger import log_devops

FASTAPI_URL = os.getenv("FASTAPI_URL", "http://localhost:8000")


def run():
    try:
        r = requests.post(f"{FASTAPI_URL}/aggregate-kpis", headers={"x-cron-token": os.getenv("CRON_TOKEN", "")}, timeout=60)
        js = r.json() if r.content else {}
        log_devops("KPI Aggregate", "FastAPI", js, status="OK" if r.ok else "FAIL")
        # Optionally mirror a summary into DevOps → Metrics / KPIs
        kpi_tbl = get_table("DEVOPS_BASE", "Metrics / KPIs")
        if kpi_tbl:
            kpi_tbl.create(
                remap_existing_only(
                    kpi_tbl,
                    {
                        "Date": tznow_iso()[:10],
                        "KPI": "DAILY_REFRESH",
                        "Value": 1,
                        "Notes": str(js)[:10000],
                    },
                )
            )
        return {"ok": r.ok, "resp": js}
    except Exception as e:
        traceback.print_exc()
        log_devops("KPI Aggregate", "FastAPI", {"error": str(e)}, status="FAIL", severity="Error")
        return {"ok": False, "err": str(e)}


if __name__ == "__main__":
    print(run())
