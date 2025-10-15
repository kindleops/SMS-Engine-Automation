# sms/workers/intent_worker.py
import os, time
from datetime import datetime, timedelta, timezone
from sms.tables import get_table

DRIP_DELAYS = {
    "Interest": timedelta(hours=1),
    "Delay":    timedelta(hours=24),
    "Negative": timedelta(days=7),
    "Neutral":  timedelta(hours=6),
    "Wrong Number": None,
    "Opt Out": None,
}

intent_map = {
    "interest": "Interest",
    "opt_out":  "Opt Out",
    "negative": "Negative",
    "delay":    "Delay",
    "wrong_number": "Wrong Number",
    "neutral":  "Neutral",
}
status_map = {
    "interest": "PROCESSED-YES",
    "opt_out":  "PROCESSED-OPTOUT",
    "negative": "PROCESSED-NO",
    "delay":    "PROCESSED-LATER",
    "wrong_number": "PROCESSED-WRONG",
    "neutral":  "RECEIVED",
}
keywords = {
    "interest": ["yes","yeah","yep","sure","ok","okay","interested","offer","price","cash","maybe","send it","run numbers","how much","what price","would consider","make me an offer","potentially","open to","let’s talk"],
    "opt_out": ["stop","unsubscribe","remove","opt out","don’t text","no more","quit","take me off","leave me alone","wrong person","do not contact"],
    "negative": ["no","not interested","never","don’t bother","don’t want","no thanks","already sold","not selling","no longer own"],
    "delay": ["busy","later","not now","next week","follow up","another time","call me later","in a few days","reach out later","not ready"],
    "wrong_number": ["wrong number","who is this","not the owner","wrong person","don’t own","not me","no idea","mistake"],
}

def classify(msg: str) -> str:
    t = (msg or "").lower()
    for label, words in keywords.items():
        if any(w in t for w in words):
            return label
    return "neutral"

def iso_now(): return datetime.now(timezone.utc).isoformat()

def next_send(label: str):
    delay = DRIP_DELAYS.get(label)
    return (datetime.now(timezone.utc) + delay).isoformat() if delay else None

def process_conversation(rec: dict) -> dict | None:
    f = rec.get("fields", {})
    body = f.get("message") or f.get("Body") or ""
    if not body: return None

    label_key = classify(body)
    intent_label = intent_map[label_key]
    status_label = status_map[label_key]

    patch = {}
    if f.get("intent_detected") != intent_label:
        patch["intent_detected"] = intent_label
    if f.get("status") != status_label:
        patch["status"] = status_label
    if not f.get("processed_at"):
        patch["processed_at"] = iso_now()

    # drip logic
    if intent_label in ("Opt Out","Wrong Number"):
        patch.update({"drip_status":"STOPPED","drip_stage":"N/A","next_send_date":None})
    else:
        if f.get("drip_status") in (None,"","PENDING","QUEUED"):
            patch["drip_status"] = "QUEUED"
            patch["drip_stage"] = intent_label
            n = next_send(intent_label)
            if n: patch["next_send_date"] = n

    return patch or None

def run_batch():
    C = get_table("AIRTABLE_API_KEY","LEADS_CONVOS_BASE","CONVERSATIONS_TABLE","Conversations")
    rows = C.all()
    updated = 0
    for r in rows:
        patch = process_conversation(r)
        if patch:
            try:
                C.update(r["id"], patch)
                updated += 1
                time.sleep(0.1)
            except Exception as e:
                print("⚠️", r["id"], e)
    print(f"[intent] updated={updated}")

if __name__ == "__main__":
    run_batch()