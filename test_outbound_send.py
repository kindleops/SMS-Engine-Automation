# test_outbound_send.py
import os
from sms.textgrid_sender import send_message

# Make sure these env vars are set in your terminal or .env before running:
# export TEXTGRID_ACCOUNT_SID="your_account_sid"
# export TEXTGRID_AUTH_TOKEN="your_auth_token"

TO_NUMBER = "+16128072000"        # destination number
FROM_NUMBER = "+18139558255"      # one of your registered TextGrid DIDs
BODY = "Test message from REI SMS Engine ✅"

print("🚀 Sending test outbound message...")

result = send_message(
    from_number=FROM_NUMBER,
    to=TO_NUMBER,
    message=BODY
)

print("\n✅ Send result:")
print(result)