📲 REI SMS Engine

The REI SMS Engine is a Python-based system for outbound and inbound SMS automation using TextGrid API and Airtable for tracking leads, conversations, and quotas.

⸻

🚀 Features
	•	Outbound Messaging: Batch sends using daily quotas.
	•	Inbound Processing: Handles replies, classifies intent, auto-responds.
	•	Retry Logic: Retries failed SMS with exponential backoff.
	•	Autoresponder: Automatically replies to inbound messages with templates.
	•	Opt-Out Management: Detects “STOP” and removes from campaigns.
	•	Logging: All activity tracked in Airtable (Conversations, Leads, Numbers).

 📂 Project Structure
 rei-sms-engine/
│── sms/                     # Core engine modules
│   ├── airtable_client.py   # Airtable table connectors
│   ├── autoresponder.py     # Inbound autoresponder
│   ├── inbound_webhook.py   # FastAPI inbound webhook
│   ├── message_processor.py # Outbound processor
│   ├── outbound_batcher.py  # Batch sending + quotas
│   ├── quota_reset.py       # Resets daily quotas
│   ├── retry_runner.py      # Retry logic (script version)
│   ├── retry_worker.py      # Retry logic (worker version)
│   ├── templates.py         # Message templates
│   ├── textgrid_sender.py   # Sends SMS via TextGrid
│   └── utils/               # Helpers
│
├── .env                     # Environment variables
├── requirements.txt         # Python dependencies
├── sms_test.py              # Quick test script to send SMS
├── run.py                   # Main entry for production
├── render.yaml              # Deployment config (Render.com)
└── README.md                # This file

⚙️ Setup

1. Clone Repo
git clone <your-repo-url>
cd rei-sms-engine

2. Virtual Environment
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt --break-system-packages

3. Environment Variables
Create .env in the project root:
# --- TextGrid API ---
TEXTGRID_API_KEY=your_api_key_here
TEXTGRID_CAMPAIGN_ID=your_campaign_id_here

# --- Airtable ---
AIRTABLE_API_KEY=your_airtable_key_here
LEADS_CONVOS_BASE=appMn2MKocaJ9I3rW
CONVERSATIONS_TABLE=Conversations

# --- Conversations field mappings ---
CONV_FROM_FIELD=phone
CONV_TO_FIELD=to_number
CONV_MESSAGE_FIELD=message
CONV_STATUS_FIELD=status
CONV_DIRECTION_FIELD=direction
CONV_TEXTGRID_ID_FIELD=TextGrid ID
CONV_RECEIVED_AT_FIELD=received_at
CONV_INTENT_FIELD=intent_detected
CONV_PROCESSED_BY_FIELD=processed_by
CONV_SENT_AT_FIELD=sent_at
⚠️ Make sure there are no colons (:) in .env, only =.

🧪 Testing SMS

Run the quick test script:
python sms_test.py

This will:
	1.	Load .env
	2.	Send an SMS via TextGrid to your test number
	3.	Log the outbound message into Airtable

▶️ Running Engine

Start FastAPI app (inbound webhook, autoresponder, quotas, etc):
uvicorn sms.main:app --reload

Now your webhook is live at:
http://127.0.0.1:8000/inbound

Endpoints:
	•	POST /send → Run outbound batch
	•	POST /autoresponder → Run autoresponder
	•	POST /ai-closer → Run AI closer (same as autoresponder, different label)
	•	POST /manual-qa → Run manual QA on inbounds
	•	POST /reset-quotas → Reset number quotas

 📊 Airtable Tables
	•	Conversations → Logs all inbound/outbound SMS
	•	Leads → Linked to conversations by phone
	•	Numbers → Quota management
	•	Opt-Outs → Stores STOP/unsubscribe numbers

⸻

🛠️ Development Notes
	•	Delete duplicate .env files (only keep root one).
	•	Clear __pycache__/ if field names mismatch.
	•	Use print(os.environ) in debug if vars aren’t loading.
