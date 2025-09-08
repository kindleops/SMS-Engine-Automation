import os, requests

TEXTGRID_API_KEY = os.getenv("TEXTGRID_API_KEY")
TEXTGRID_CAMPAIGN_ID = os.getenv("TEXTGRID_CAMPAIGN_ID")
TEXTGRID_URL = "https://api.textgrid.com/v1/messages/send"

def send_message(to_number, body, from_number=None):
    headers = {"Authorization": f"Bearer {TEXTGRID_API_KEY}"}
    data = {
        "campaign_id": TEXTGRID_CAMPAIGN_ID,
        "to": to_number,
        "body": body,
    }
    if from_number:
        data["from"] = from_number

    r = requests.post(TEXTGRID_URL, json=data, headers=headers)
    if r.status_code != 200:
        print("❌ Error sending:", r.text)
    return r.json()