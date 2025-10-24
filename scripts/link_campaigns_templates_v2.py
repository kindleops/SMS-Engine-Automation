#!/usr/bin/env python3
import time
from datetime import datetime, timedelta, timezone
from sms.tables import get_table

# --- Config ---
DRY_RUN = False
DRIP_INTERVALS = {
    "Interest": timedelta(hours=1),  # quick follow-up
    "Delay": timedelta(hours=24),  # next-day check
    "Negative": timedelta(days=7),  # weekly recheck
    "Opt Out": None,  # stop all
    "Wrong Number": None,
    "Neutral": timedelta(hours=6),  # nurture interval
}

# --- Airtable setup ---
C = get_table("AIRTABLE_API_KEY", "LEADS_CONVOS_BASE", "CONVERSATIONS_TABLE", "Conversations")
rows = C.all()
print(f"Total conversations: {len(rows)}")

updated = 0
intent_counts = {}

# --- Intent and status mappings ---
intent_map = {
    "interest": "Interest",
    "opt_out": "Opt Out",
    "negative": "Negative",
    "delay": "Delay",
    "wrong_number": "Wrong Number",
    "neutral": "Neutral",
}

status_map = {
    "interest": "PROCESSED-YES",
    "opt_out": "PROCESSED-OPTOUT",
    "negative": "PROCESSED-NO",
    "delay": "PROCESSED-LATER",
    "wrong_number": "PROCESSED-WRONG",
    "neutral": "RECEIVED",
}

# --- Keyword libraries ---
keywords = {
    "interest": [
        "yes",
        "yeah",
        "yep",
        "sure",
        "okay",
        "ok",
        "interested",
        "offer",
        "price",
        "cash",
        "maybe",
        "depends",
        "send it",
        "run numbers",
        "how much",
        "what price",
        "would consider",
        "make me an offer",
        "potentially",
        "open to",
        "how soon",
        "let’s talk",
    ],
    "opt_out": [
        "stop",
        "unsubscribe",
        "remove",
        "opt out",
        "don’t text",
        "no more",
        "quit",
        "take me off",
        "leave me alone",
        "wrong person",
        "do not contact",
    ],
    "negative": [
        "no",
        "not interested",
        "never",
        "don’t bother",
        "don’t want",
        "no thanks",
        "already sold",
        "keep off",
        "not selling",
        "no longer own",
    ],
    "delay": [
        "busy",
        "later",
        "not now",
        "next week",
        "follow up",
        "another time",
        "call me later",
        "in a few days",
        "reach out later",
        "not ready",
    ],
    "wrong_number": [
        "wrong number",
        "who is this",
        "don’t know",
        "not the owner",
        "wrong person",
        "don’t own",
        "not me",
        "no idea",
        "mistake",
    ],
}


def classify_intent(msg):
    txt = msg.lower().strip()
    for label, words in keywords.items():
        for kw in words:
            if kw in txt:
                return label
    return "neutral"


def next_drip_time(intent_label):
    """Return next send date based on intent type."""
    delay = DRIP_INTERVALS.get(intent_label)
    if not delay:
        return None
    return (datetime.now(timezone.utc) + delay).isoformat()


# --- Processing ---
for i, r in enumerate(rows, 1):
    f = r.get("fields", {})
    rid = r.get("id")
    msg = (f.get("message", "") or f.get("Message", "") or f.get("Body", "") or f.get("Text", "") or f.get("SMS Body", "") or "").strip()

    if not msg:
        continue

    # --- Intent classification ---
    intent = classify_intent(msg)
    intent_label = intent_map.get(intent, "Neutral")
    status_label = status_map.get(intent, "RECEIVED")

    # --- Build patch ---
    patch = {}
    if f.get("intent_detected") != intent_label:
        patch["intent_detected"] = intent_label
    if f.get("status") != status_label:
        patch["status"] = status_label
    if not f.get("processed_at"):
        patch["processed_at"] = datetime.now(timezone.utc).isoformat()

    # --- Drip Logic ---
    current_drip = f.get("drip_status", "")
    if intent_label in ["Opt Out", "Wrong Number"]:
        patch["drip_status"] = "STOPPED"
        patch["drip_stage"] = "N/A"
        patch["next_send_date"] = None
    elif intent_label in ["Interest", "Delay", "Neutral", "Negative"]:
        if not current_drip or current_drip == "PENDING":
            patch["drip_status"] = "QUEUED"
            patch["drip_stage"] = intent_label
            nxt = next_drip_time(intent_label)
            if nxt:
                patch["next_send_date"] = nxt

    # --- Commit ---
    if patch:
        updated += 1
        intent_counts[intent_label] = intent_counts.get(intent_label, 0) + 1
        if DRY_RUN:
            print(f"[DRY] {rid} <- {patch}")
        else:
            try:
                C.update(rid, patch)
                time.sleep(0.15)
            except Exception as e:
                print(f"⚠️ {rid} {e}")

    if i % 50 == 0:
        print(f"Processed {i}/{len(rows)}")

# --- Summary ---
print("\n=== Summary ===")
print(f"Conversations scanned: {len(rows)}")
print(f"Updated: {updated}")
for k, v in intent_counts.items():
    print(f"  {k}: {v}")
print(f"DRY_RUN={DRY_RUN}")
