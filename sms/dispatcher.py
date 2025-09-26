# sms/dispatcher.py
import traceback

from sms.outbound_batcher import send_batch
from sms.campaign_runner import run_campaigns
from sms.autoresponder import run_autoresponder
from sms.retry_runner import run_retry


def run_engine(mode: str, **kwargs) -> dict:
    """
    Unified dispatcher for all SMS engines.

    Modes:
      - "prospects" → Outbound campaigns (raw data → first touch)
      - "leads"     → Retry loop + follow-up flows
      - "inbounds"  → Autoresponder (promote to leads, AI replies)

    kwargs are passed down to the specific runner.
    """
    mode = (mode or "").lower().strip()
    result = {}

    try:
        if mode == "prospects":
            # First-touch outbound to raw prospect data
            result = run_campaigns(**kwargs)

        elif mode == "leads":
            # Retry failed messages, keep active convos alive
            retry_result = run_retry(limit=kwargs.get("retry_limit", 100))
            result = {
                "ok": retry_result.get("ok", True),
                "type": "Lead",
                "processed": retry_result.get("retried", 0),
                "retries": retry_result,
                "errors": []
            }

        elif mode == "inbounds":
            # Handle inbound messages with autoresponder
            result = run_autoresponder(limit=kwargs.get("limit", 50))

        else:
            return {
                "ok": False,
                "error": f"Unknown mode: {mode}",
                "supported_modes": ["prospects", "leads", "inbounds"]
            }

    except Exception as e:
        print(f"❌ Dispatcher error in mode={mode}: {e}")
        traceback.print_exc()
        return {
            "ok": False,
            "error": str(e),
            "mode": mode,
            "stack": traceback.format_exc()
        }

    return result


if __name__ == "__main__":
    # 🔧 Quick manual tests
    print(run_engine("prospects", limit=10))
    print(run_engine("leads", retry_limit=50))
    print(run_engine("inbounds", limit=10))