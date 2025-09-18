import os
from dotenv import load_dotenv
from sms.textgrid_sender import send_message

# Load .env so we have keys
load_dotenv()

print("TEXTGRID_API_KEY:", os.getenv("TEXTGRID_API_KEY"))
print("TEXTGRID_CAMPAIGN_ID:", os.getenv("TEXTGRID_CAMPAIGN_ID"))

res = send_message(
    to="+16128072000",  # your test number
    body="🔥 Test message from Everline engine",
    from_number=None
)
print("RESULT:", res)
