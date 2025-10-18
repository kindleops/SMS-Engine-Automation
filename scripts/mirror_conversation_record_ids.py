#!/usr/bin/env python3
from __future__ import annotations
import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

from sms.tables import get_table as _get_table

DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

BASE_ENV = "LEADS_CONVOS_BASE" if os.getenv("LEADS_CONVOS_BASE") else "LEADS_CONVO_BASE"

def get_tbl(name, env_name):
    api = "AIRTABLE_API_KEY"
    assert os.getenv(api), f"Missing {api}"
    assert os.getenv(env_name), f"Missing {env_name}"
    return _get_table(api, BASE_ENV, env_name, name)

def main():
    conv = get_tbl("Conversations", "CONVERSATIONS_TABLE")

    link_to_text = [
        ("Lead",      "Lead Record ID"),
        ("Template",  "Template Record ID"),
        ("Campaign",  "Campaign Record ID"),
        ("Prospect",  "Prospect Record ID"),
    ]

    updated = 0
    already = 0
    scanned = 0

    rows = conv.all()
    for r in rows:
        scanned += 1
        rid = r["id"]
        f = r.get("fields", {})

        patch = {}

        # conversation's own text Record ID
        if "Conversation Record ID" in f:
            if f.get("Conversation Record ID") != rid:
                patch["Conversation Record ID"] = rid

        # mirror each link -> text field
        for link_field, text_field in link_to_text:
            link_val = f.get(link_field)
            if isinstance(link_val, list) and link_val and isinstance(link_val[0], str) and link_val[0].startswith("rec"):
                if f.get(text_field) != link_val[0]:
                    patch[text_field] = link_val[0]

        if patch:
            if DRY_RUN:
                print(f"[DRY] {rid} <- {patch}")
            else:
                try:
                    conv.update(rid, patch)
                except Exception as e:
                    print(f"⚠️  Update failed for {rid}: {e}")
                    continue
            updated += 1
        else:
            already += 1

    print("\n=== Mirror Summary ===")
    print(f"Scanned: {scanned}")
    print(f"Updated: {updated}")
    print(f"No change: {already}")
    print("✅ Mirror complete.")

if __name__ == "__main__":
    print(f"=== Mirror start (DRY_RUN={str(DRY_RUN).upper()}) ===")
    main()