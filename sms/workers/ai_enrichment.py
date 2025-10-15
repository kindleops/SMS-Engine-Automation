# sms/workers/ai_enrichment.py
import time, statistics, re
from datetime import datetime, timezone
from sms.tables import get_table

def iso_now(): return datetime.now(timezone.utc).isoformat()
def to_int(v, default=0):
    try: return int(float(v))
    except: return default

def score_row(f: dict) -> tuple[int,str,str]:
    """Returns (MotivationScore 0–100, DistressTier, NextAction) using simple rules."""
    score = 0
    # recency / activity
    if f.get("Last Inbound"): score += 15
    if to_int(f.get("Reply Count")) >= 2: score += 10
    # property signals (absentee, equity, year built)
    if (f.get("Absentee Owner") or "").lower() in ("absentee","out of state","true","yes"): score += 10
    score += min(20, max(0, to_int(f.get("Equity Percent"))//5))  # up to +20
    yb = to_int(f.get("Year Built"))
    if yb and yb < 1980: score += 10
    # language signals captured by autoresponder
    intent = f.get("Intent Last Detected") or f.get("intent_detected") or ""
    if intent in ("Interest","Delay"): score += 20
    if intent == "Negative": score -= 10
    if (f.get("Owner Verified") or "").lower() == "yes": score += 10

    # tier & next action
    tier = "COLD"
    if score >= 80: tier = "HOT"
    elif score >= 60: tier = "WARM"

    next_action = "Awaiting Reply"
    if intent == "Interest": next_action = "Ask Price / Condition"
    elif intent == "Delay": next_action = "Schedule Follow-Up"
    elif intent in ("Wrong Number","Opt Out"): next_action = "Stop / Clean Data"

    return max(0, min(100, score)), tier, next_action

def run():
    P = get_table("AIRTABLE_API_KEY","LEADS_CONVOS_BASE","PROSPECTS_TABLE","Prospects")
    rows = P.all()
    updated = 0
    for r in rows:
        f = r.get("fields", {})
        score, tier, action = score_row(f)
        patch = {}
        if f.get("Motivation Score") != score: patch["Motivation Score"] = score
        if f.get("Distress Tier") != tier:     patch["Distress Tier"] = tier
        if f.get("Next Action") != action:     patch["Next Action"] = action
        if patch:
            try:
                P.update(r["id"], patch)
                updated += 1
                time.sleep(0.1)
            except Exception as e:
                print("⚠️", r["id"], e)
    print(f"[ai_enrichment] updated={updated} ts={iso_now()}")

if __name__ == "__main__":
    run()