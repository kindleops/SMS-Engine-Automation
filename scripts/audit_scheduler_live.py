#!/usr/bin/env python3
"""
Read-only audit of a scheduled campaign: how many *would* be queued,
why some wouldn't, and how 'from' numbers would rotate.

Requires normal env (AIRTABLE_API_KEY, LEADS_CONVOS_BASE, CAMPAIGN_CONTROL_BASE, etc.).
This script DOES NOT write anything.

Usage:
  python scripts/audit_scheduler_live.py --campaign recXXXXXXXXXXXX
  # or audit all scheduled:
  python scripts/audit_scheduler_live.py --all
"""
from __future__ import annotations

import argparse
import random
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sms.datastore import CONNECTOR, list_records
from sms.runtime import last_10_digits

# Reuse the exact helpers that production scheduler uses (number pool, message rendering, etc.)
from sms.scheduler import (
    _campaign_market,
    _campaign_start,
    _prospect_best_phone,
    _render_message,
    _fetch_textgrid_number_pool,
    _market_key,
)

# Robust field maps (mirror the scheduler style, but without hard indexing)
from sms.airtable_schema import (
    campaign_field_map,
    drip_field_map,
    template_field_map,
)

# ───────────────────────────────────────────────────────────────
# Field mapping (robust, no KeyError)
# ───────────────────────────────────────────────────────────────
def _flex(map_: Dict[str, str], name: str, default: str) -> str:
    return map_.get(name) or map_.get(name.upper()) or map_.get(name.lower()) or default


CAMPAIGN_FIELDS = campaign_field_map()
DRIP_FIELDS     = drip_field_map()
TEMPLATE_FIELDS = template_field_map()

# Campaign fields
CAMPAIGN_STATUS_FIELD    = _flex(CAMPAIGN_FIELDS, "Status", "Status")
CAMPAIGN_MARKET_FIELD    = _flex(CAMPAIGN_FIELDS, "Market", "Market")
CAMPAIGN_START_FIELD     = _flex(CAMPAIGN_FIELDS, "Start Time", "Start Time")
CAMPAIGN_PROSPECTS_LINK  = _flex(CAMPAIGN_FIELDS, "Prospects", "Prospects")
CAMPAIGN_TEMPLATES_LINK  = _flex(CAMPAIGN_FIELDS, "Templates", "Templates")

# Drip fields (only what we need for dedupe)
DRIP_CAMPAIGN_LINK_FIELD   = _flex(DRIP_FIELDS, "Campaign", "Campaign")
DRIP_SELLER_PHONE_FIELD    = _flex(DRIP_FIELDS, "Seller Phone Number", "Seller Phone Number")

# Template fields (only what we need)
TEMPLATE_MESSAGE_FIELD     = _flex(TEMPLATE_FIELDS, "Message", "Message")

# Airtable-friendly pacing
CHUNK_SLEEP_SEC = 0.12  # gentle on API when batching OR filters


# ───────────────────────────────────────────────────────────────
# Helpers
# ───────────────────────────────────────────────────────────────
def _coerce_id(v: Any) -> Optional[str]:
    if isinstance(v, list) and v:
        return v[0]
    if isinstance(v, str) and v.strip():
        return v
    return None


def _fetch_templates_by_ids(templates_h, ids: List[str]) -> List[Dict[str, Any]]:
    """Read-only: fetch specific templates by RECORD_ID(), in chunks of 100."""
    out: List[Dict[str, Any]] = []
    if not ids:
        return out
    for i in range(0, len(ids), 100):
        chunk = ids[i : i + 100]
        formula = "OR(" + ",".join([f"RECORD_ID()='{tid}'" for tid in chunk]) + ")"
        resp = templates_h.table.api.request(
            "get",
            templates_h.table.url,
            params={"filterByFormula": formula},
        )
        out.extend((resp or {}).get("records", []))
        time.sleep(CHUNK_SLEEP_SEC)
    return out


def _gather_campaigns(campaigns_h, only_id: Optional[str], all_scheduled: bool) -> List[Dict[str, Any]]:
    """Fetch campaigns (read-only). If --all, only those with Status=Scheduled, sorted by Start Time."""
    rows = list_records(campaigns_h, page_size=100)  # Airtable max pageSize is 100
    if only_id:
        return [r for r in rows if r.get("id") == only_id]
    if all_scheduled:
        out = []
        for r in rows:
            f = r.get("fields", {}) or {}
            if str(f.get(CAMPAIGN_STATUS_FIELD, "")).strip().lower() == "scheduled":
                out.append(r)
        # sort by scheduler's start time logic
        return sorted(out, key=lambda r: (_campaign_start(r.get("fields", {}))).timestamp())
    return rows


# ───────────────────────────────────────────────────────────────
# Core audit
# ───────────────────────────────────────────────────────────────
def audit_campaign(camp: Dict[str, Any]) -> Dict[str, Any]:
    """
    Read-only simulation of the scheduler for a single campaign:
    - Loads linked prospects (batched).
    - Loads message templates.
    - Loads number pool for the campaign's market and rotates per prospect.
    - Tracks why any record would be skipped.
    - Respects dedupe against existing Drip Queue (campaign + last10 digits).
    """
    campaigns_h = CONNECTOR.campaigns()
    prospects_h = CONNECTOR.prospects()
    drip_h = CONNECTOR.drip_queue()
    templates_h = CONNECTOR.templates()

    cf = camp.get("fields", {}) or {}
    camp_id = camp.get("id")
    status = str(cf.get(CAMPAIGN_STATUS_FIELD, "")).strip()
    market = _campaign_market(cf) or ""
    start_time = _campaign_start(cf)

    # Existing drip pairs for dedupe (campaign_id + last10)
    existing = list_records(drip_h, page_size=100)
    existing_pairs = {
        (
            _coerce_id((row.get("fields") or {}).get(DRIP_CAMPAIGN_LINK_FIELD)),
            last_10_digits((row.get("fields") or {}).get(DRIP_SELLER_PHONE_FIELD)),
        )
        for row in existing
        if row.get("fields")
    }

    # Linked prospects
    linked = cf.get(CAMPAIGN_PROSPECTS_LINK) or []
    if not linked:
        return {
            "id": camp_id,
            "status": status,
            "market": market,
            "start_time": start_time.isoformat(),
            "processed": 0,
            "queued": 0,
            "skipped": 0,
            "skip_reasons": {"no_prospects": 0},
            "rotation": {},
            "pool_size": 0,
            "has_templates": False,
            "note": "read-only simulation; no writes performed",
        }

    # Templates for this campaign
    template_ids = cf.get(CAMPAIGN_TEMPLATES_LINK) or []
    template_msgs: List[str] = []
    if template_ids:
        trows = _fetch_templates_by_ids(templates_h, template_ids)
        for tr in trows:
            msg = (tr.get("fields", {}) or {}).get(TEMPLATE_MESSAGE_FIELD)
            if isinstance(msg, str) and msg.strip():
                template_msgs.append(msg.strip())
    has_templates = bool(template_msgs)

    # Fetch all linked prospects (batched)
    prospects: List[Dict[str, Any]] = []
    for i in range(0, len(linked), 100):
        chunk = linked[i : i + 100]
        formula = "OR(" + ",".join([f"RECORD_ID()='{rid}'" for rid in chunk]) + ")"
        resp = prospects_h.table.api.request(
            "get",
            prospects_h.table.url,
            params={"filterByFormula": formula},
        )
        prospects.extend((resp or {}).get("records", []))
        time.sleep(CHUNK_SLEEP_SEC)

    # Market pool + rotation simulation
    pool = _fetch_textgrid_number_pool(market)  # exactly how prod picks numbers for a market
    rotation_idx = 0

    def choose_from(pool_: List[str]) -> Optional[str]:
        nonlocal rotation_idx
        if not pool_:
            return None
        choice = pool_[rotation_idx % len(pool_)]
        rotation_idx += 1
        return choice

    processed = queued = 0
    skip = Counter()
    dist = Counter()
    seen_pairs = set(existing_pairs)  # simulate dedupe during this dry-run

    for pr in prospects:
        processed += 1
        pf = pr.get("fields", {}) or {}

        phone = _prospect_best_phone(pf)
        if not phone:
            skip["missing_phone"] += 1
            continue

        digits = last_10_digits(phone)
        if (camp_id, digits) in seen_pairs:
            skip["duplicate_phone"] += 1
            continue

        if not has_templates:
            skip["no_templates"] += 1
            continue

        from_num = choose_from(pool)
        if not from_num:
            skip["missing_textgrid_number"] += 1
            continue

        # Render once to surface bad placeholders (no send, no write)
        try:
            _ = _render_message(random.choice(template_msgs), pf)
        except Exception:
            # If a template would blow up at runtime, count it as a skip
            skip["template_render_error"] += 1
            continue

        # Simulate successful queue
        queued += 1
        dist[from_num] += 1
        seen_pairs.add((camp_id, digits))

    return {
        "id": camp_id,
        "status": status,
        "market": market,
        "start_time": start_time.isoformat(),
        "processed": processed,
        "queued": queued,
        "skipped": processed - queued,
        "skip_reasons": dict(skip),
        "rotation": dict(dist),
        "pool_size": len(pool),
        "has_templates": has_templates,
        "note": "read-only simulation; no writes performed",
    }


# ───────────────────────────────────────────────────────────────
# CLI
# ───────────────────────────────────────────────────────────────
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--campaign", help="Campaign RECORD_ID() to audit")
    ap.add_argument("--all", action="store_true", help="Audit all campaigns with Status=Scheduled")
    args = ap.parse_args()

    campaigns_h = CONNECTOR.campaigns()
    camps = _gather_campaigns(campaigns_h, args.campaign, args.all)

    if not camps:
        print("No matching campaigns found.")
        sys.exit(1)

    summaries = [audit_campaign(c) for c in camps]

    # Pretty print
    tot_q = sum(s["queued"] for s in summaries)
    print(f"\n=== Scheduler Read-Only Audit ({len(summaries)} campaign(s)) ===")
    for s in summaries:
        print(
            f"\n• Campaign {s['id']}  [{s['status']}]  "
            f"market={s['market']}  pool={s['pool_size']}  templates={s['has_templates']}"
        )
        print(f"  start={s['start_time']}")
        print(f"  processed={s['processed']:,}  queued={s['queued']:,}  skipped={s['skipped']:,}")
        if s["skip_reasons"]:
            print("  skip_reasons:", s["skip_reasons"])
        if s["rotation"]:
            top = sorted(s["rotation"].items(), key=lambda kv: kv[1], reverse=True)
            print("  from_number distribution (top):", top[:10])

    print(f"\nTOTAL queued (simulated): {tot_q:,}")
    print("\nDone (no records were created/updated).")


if __name__ == "__main__":
    main()