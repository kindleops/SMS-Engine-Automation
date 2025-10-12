import os, requests, traceback
from .common import tznow_iso, get_table, remap_existing_only
from .devops_logger import log_devops

RENDER_KEY = os.getenv("RENDER_API_KEY")


def run():
    headers = {"Authorization": f"Bearer {RENDER_KEY}"}
    svc_tbl = get_table("DEVOPS_BASE", "Services")
    dep_tbl = get_table("DEVOPS_BASE", "Deployments")
    if not (svc_tbl and dep_tbl):
        return {"ok": False, "err": "Missing Services/Deployments tables"}

    try:
        svcs = requests.get("https://api.render.com/v1/services", headers=headers, timeout=30).json()
        for s in svcs:
            # Normalize fields safely (Render API shapes can vary)
            name = s.get("service", {}).get("name") or s.get("name")
            status = s.get("service", {}).get("serviceDetails", {}).get("status") or s.get("status")
            url = s.get("service", {}).get("serviceDetails", {}).get("url") or s.get("dashboardUrl")
            row = {
                "Service Name": name,
                "Category": "Backend",
                "Status": status,
                "API URL / Host": url,
                "Last Ping": tznow_iso(),
            }
            svc_tbl.create(remap_existing_only(svc_tbl, row))

        log_devops("Render Sync", "Render", {"services": len(svcs)})

        # If you want deployment history, call deployments endpoint per service (optional)
        return {"ok": True, "services": len(svcs)}
    except Exception as e:
        traceback.print_exc()
        log_devops("Render Sync", "Render", {"error": str(e)}, status="FAIL", severity="Warn")
        return {"ok": False, "err": str(e)}


if __name__ == "__main__":
    print(run())
