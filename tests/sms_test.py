# sms_test.py
import os
import sys
import json
from sms.textgrid_sender import send_message
from dotenv import load_dotenv

load_dotenv()


def debug_env():
    """Print critical env vars so we know what's loaded."""
    print("🔎 DEBUG ENV VARS:")

    def mask(val):
        return val[:8] + "..." if val else None

    print("AIRTABLE_API_KEY:", mask(os.getenv("AIRTABLE_API_KEY")))
    print("CAMPAIGN_CONTROL_BASE:", os.getenv("CAMPAIGN_CONTROL_BASE"))
    print("NUMBERS_TABLE:", os.getenv("NUMBERS_TABLE"))
    print("TEXTGRID_ACCOUNT_SID:", mask(os.getenv("TEXTGRID_ACCOUNT_SID")))
    print("TEXTGRID_AUTH_TOKEN:", mask(os.getenv("TEXTGRID_AUTH_TOKEN")))


def run_test():
    try:
        debug_env()
        res = send_message(
            to="+16128072000",  # 🔔 replace with your test number
            body="🔥 Test message from Everline engine",
            market="houston",  # pulls number from pools
        )
        print("✅ RESULT:")
        print(json.dumps(res, indent=2))
        sys.exit(0)
    except Exception as e:
        print("❌ ERROR:", e)
        sys.exit(1)


if __name__ == "__main__":
    run_test()
