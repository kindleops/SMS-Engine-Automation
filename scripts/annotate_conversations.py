cat > annotate_conversations.py <<'PY'
#!/usr/bin/env python3
import os, re, time
from sms.tables import get_table as _get

DRY_RUN = os.getenv("DRY_RUN","false").lower()=="true"
BASE = "LEADS_CONVOS_BASE" if os.getenv("LEADS_CONVOS_BASE") else "LEADS_CONVO_BASE"
C = _get("AIRTABLE_API_KEY", BASE, "CONVERSATIONS_TABLE", "Conversations")

# Simple keyword rules — tune as needed
INTENTS = [
    ("STOP", re.compile(r"\b(stop, unsubscribe, cancel, quit, end)\b", re.I)),
    ("PRICE", re.compile(r"\b(\$?\d{2,7}k\b|\$?\d{2,7}\b|price|offer|amount|how much)\b", re.I)),
    ("APPOINTMENT", re.compile(r"\b(meet|call|phone|time|schedule|tomorrow|today|this week|next week)\b", re.I)),
    ("NOT_INTERESTED", re.compile(r"\b(not interested|wrong number)\b", re.I)),
    ("YES", re.compile(r"\b(yes|yeah|yep|sure|ok)\b", re.I)),
    ("QUESTION", re.compile(r"\?|how|what|when|where|why", re.I)),
]
def classify(text):
    s = (text or "").strip()
    if not s: return "UNKNOWN"
    for label, rx in INTENTS:
        if rx.search(s): return label
    return "OTHER"

def drip_status(direction, status, optout):
    if optout: return "STOPPED"
    if direction == "INBOUND": return "REPLIED"
    if direction == "OUTBOUND":
        if status in ("DELIVERED","SENT"): return "SENT"
        if status in ("FAILED","UNDELIVERED","BOUNCED"): return "FAILED"
    return "UNKNOWN"

rows = C.all()
updated = 0

for r in rows:
    f = r.get("fields", {})
    rid = r["id"]

    msg = f.get("message") or f.get("Message") or f.get("Body") or f.get("Text") or f.get("Message Summary (AI)")
    direction = f.get("direction")
    delivery = f.get("status") or f.get("Last Delivery Status")
    optout = bool(f.get("OptOutFlag"))

    intent = classify(msg)
    drip  = drip_status(direction, delivery, optout)

    patch = {}
    if f.get("intent_detected") != intent:
        patch["intent_detected"] = intent
    if f.get("drip_status") != drip:
        patch["drip_status"] = drip
    if f.get("processed_by") != "rule-engine:v1":
        patch["processed_by"] = "rule-engine:v1"

    if patch:
        if DRY_RUN:
            print(f"[DRY] {rid} <- {patch}")
        else:
            try:
                C.update(rid, patch); time.sleep(0.12)
            except Exception as e:
                print("⚠️", rid, e)
        updated += 1

print(f"Updated rows: {updated} (DRY_RUN={DRY_RUN})")
PY
chmod +x annotate_conversations.py