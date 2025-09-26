# engine_runner.py
import traceback
from datetime import datetime, timezone
from sms.dispatcher import run_engine

def log_result(tag: str, result: dict):
    ts = datetime.now(timezone.utc).isoformat()
    print(f"\n[{ts}] ✅ {tag} completed")
    print(result)

def main():
    print("🚀 Engine Runner started")

    # --- Outbound (prospects) ---
    try:
        res_prospects = run_engine("prospects", limit=50)
        log_result("Prospects", res_prospects)
    except Exception:
        print("❌ Error in prospects run")
        traceback.print_exc()

    # --- Retry / Followups (leads) ---
    try:
        res_leads = run_engine("leads", retry_limit=100)
        log_result("Leads", res_leads)
    except Exception:
        print("❌ Error in leads run")
        traceback.print_exc()

    # --- Inbounds (autoresponder) ---
    try:
        res_inbounds = run_engine("inbounds", limit=25)
        log_result("Inbounds", res_inbounds)
    except Exception:
        print("❌ Error in inbounds run")
        traceback.print_exc()

    print("🏁 Engine Runner finished\n")

if __name__ == "__main__":
    main()