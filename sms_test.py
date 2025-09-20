# sms_test.py
import os
from dotenv import load_dotenv

# --- Load .env ---
load_dotenv()  # ensures .env is loaded when running this script

from sms.textgrid_sender import send_message

def debug_env():
    """Print critical env vars so we know what's loaded."""
    print("🔎 DEBUG ENV VARS:")
    print("AIRTABLE_API_KEY:", os.getenv("AIRTABLE_API_KEY")[:8] + "..." if os.getenv("AIRTABLE_API_KEY") else None)
    print("CAMPAIGN_CONTROL_BASE:", os.getenv("CAMPAIGN_CONTROL_BASE"))
    print("NUMBERS_TABLE:", os.getenv("NUMBERS_TABLE"))
    print("TEXTGRID_ACCOUNT_SID:", os.getenv("TEXTGRID_ACCOUNT_SID"))
    print("TEXTGRID_AUTH_TOKEN:", os.getenv("TEXTGRID_AUTH_TOKEN")[:8] + "..." if os.getenv("TEXTGRID_AUTH_TOKEN") else None)

def run_test():
    try:
        debug_env()
        res = send_message(
            to="+16128072000",  # 🔔 replace with your test number if needed
            body="🔥 Test message from Everline engine",
            market="houston"    # will pull a number from your pools
        )
        print("✅ RESULT:", res)
    except Exception as e:
        print("❌ ERROR:", e)

if __name__ == "__main__":
    run_test()