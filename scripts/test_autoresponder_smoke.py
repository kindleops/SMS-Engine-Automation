#!/usr/bin/env python3
"""
Run a lightweight, isolated smoke test for the autoresponder.
Simulates inbound messages and prints classification + next-stage logic.
No Airtable writes occur.
"""

import os
from sms.autoresponder import Autoresponder, classify_intent

def main():
    ar = Autoresponder()

    samples = [
        ("Hi, yes I still own it", "positive"),
        ("stop texting me", "optout"),
        ("what’s your offer?", "offer_discussion"),
        ("wrong number", "wrong_number"),
        ("call me next week", "delay"),
        ("who is this?", "info_request"),
    ]

    print("\n=== Intent Classification ===")
    for text, _ in samples:
        print(f"{text!r} → {classify_intent(text)}")

    print("\n=== Stage Transition Simulation ===")
    dummy_record = {
        "id": "recFAKE123",
        "fields": {
            "From": "+19045551234",
            "Body": "Hi, yes I still own it",
            "Direction": "INBOUND",
            "Stage": "Initial Outreach",
        },
    }

    # Simulate one inbound message per intent
    for text, _ in samples:
        dummy_record["fields"]["Body"] = text
        result = ar._process_record(dummy_record, is_quiet=False, next_allowed=None)
        print(f"{text!r} processed successfully ✅")

    print("\nSmoke test complete — check console output for any exceptions.")

if __name__ == "__main__":
    main()
