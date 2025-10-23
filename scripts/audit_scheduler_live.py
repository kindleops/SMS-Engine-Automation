#!/usr/bin/env python3
"""
Read-only audit of a scheduled campaign: how many *would* be queued,
why some wouldn't, and how 'from' numbers would rotate.
Requires your normal env (AIRTABLE_API_KEY, LEADS_CONVOS_BASE, CAMPAIGN_CONTROL_BASE, etc.).
No writes.
Usage:
  python scripts/audit_scheduler_live.py --campaign recXXXXXXXXXXXX
  # or audit all scheduled:
  python scripts/audit_scheduler_live.py --all
"""
from __future__ import annotations
import os, sys, argparse, time, random
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sms.datastore import CONNECTOR, list_records
from sms.runtime import last_10_digits, normalize_phone
from sms.scheduler import (  # re-use exactly what prod uses
    CAMPAIGN_FIELDS, DRIP_FIELDS, PROSPECT_FIELDS, TEMPLATE_FIELDS,
    _campaign_market, _campaign_start, _prospect_best_phone, _render_message,
    _fetch_textgrid_number_pool, _market_key
)

def _coerce_id(v: Any) -> Optional[str]:
    if isinstance(v, list) and v: return v[0]
    if isinstance(v, str) and v.strip(): return v
    return None

def _fetch_templates_by_ids(templates_h, ids: List[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not ids: return out
    # chunk by 100 for filterByFormula OR
    for i in range(0, len(ids), 100):
        chunk = ids[i:i+100]
        formula = "OR(" + ",".join([f"RECORD_ID()='{tid}'" for tid in chunk]) + ")"
        resp = templates_h.table.api.request("get", templates_h.table.url, params={"filterByFormula": formula})
        out.extend((resp or {}).get("records", []))
        time.sleep(0.12)
    return out

def _gather_campaigns(campaigns_h, only_id: Optional[str], all_scheduled: bool) -> List[Dict[str, Any]]:
    rows = list_records(campaigns_h, page_size=100)
    if only_id:
        return [r for r in rows if r.get("id") == only_id]
    if all_scheduled:
        out = []
        for r in rows:
            f = r.get("fields", {}) or {}
            if str(f.get(CAMPAIGN_FIELDS["Status"], "")).strip().lower() == "scheduled":
                out.append(r)
        return sorted(out, key=lambda r: (_campaign_start(r.get("fields", {}))).timestamp())
    return rows

def audit_campaign(camp: Dict[str, Any]) -> Dict[str, Any]:
    campaigns_h = CONNECTOR.campaigns()
    prospects_h = CONNECTOR.prospects()
    drip_h = CONNECTOR.drip_queue()
    templates_h = CONNECTOR.templates()

    cf = camp.get("fields", {}) or {}
    camp_id = camp["id"]
    status = str(cf.get(CAMPAIGN_FIELDS["Status"], "")).strip()
    market = _campaign_market(cf) or ""
    start_time = _campaign_start(cf)

    # existing drip pairs for this campaign (campaign_id + last10 digits)
    existing = list_records(drip_h, page_size=100)
    existing_pairs = {
        (
            (_coerce_id((row.get("fields") or {}).get(DRIP_FIELDS["Campaign"], None))),
            last_10_digits((row.get("fields") or {}).get(DRIP_FIELDS["Seller Phone Number"]))
        )
        for row in existing if row.get("fields")
    }

    linked = cf.get(CAMPAIGN_FIELDS["Prospects"]) or []
    if not linked:
        return {"id": camp_id, "status": status, "market": market, "processed": 0, "queued": 0, "skipped": 0, "skip_reasons": {"no_prospects": 0}}

    # fetch templates used by this campaign
    template_ids = cf.get(CAMPAIGN_FIELDS["Templates"]) or []
    template_msgs: List[str] = []
    if template_ids:
        trows = _fetch_templates_by_ids(templates_h, template_ids)
        for tr in trows:
            msg = (tr.get("fields", {}) or {}).get(TEMPLATE_FIELDS["Message"])
            if isinstance(msg, str) and msg.strip():
                template_msgs.append(msg.strip())
    has_templates = bool(template_msgs)

    # fetch all linked prospects
    prospects: List[Dict[str, Any]] = []
    for i in range(0, len(linked), 100):
        chunk = linked[i:i+100]
        formula = "OR(" + ",".join([f"RECORD_ID()='{rid}'" for rid in chunk]) + ")"
        resp = prospects_h.table.api.request("get", prospects_h.table.url, params={"filterByFormula": formula})
        prospects.extend((resp or {}).get("records", []))
        time.sleep(0.12)

    pool = _fetch_textgrid_number_pool(market)
    mk = _market_key(market)
    rotation_idx = 0
    def choose_from(pool: List[str]) -> Optional[str]:
        nonlocal rotation_idx
        if not pool: return None
        choice = pool[rotation_idx % len(pool)]
        rotation_idx += 1
        return choice

    processed=queued=skipped=0
    skip = Counter()
    dist = Counter()
    seen_pairs = set(existing_pairs)  # copy so we can simulate dedupe

    for idx, pr in enumerate(prospects):
        processed += 1
        pf = pr.get("fields", {}) or {}

        phone = _prospect_best_phone(pf)
        if not phone:
            skip["missing_phone"] += 1; continue
        digits = last_10_digits(phone)
        if (camp_id, digits) in seen_pairs:
            skip["duplicate_phone"] += 1; continue
        if not has_templates:
            skip["no_templates"] += 1; continue

        from_pool = pool[:]  # market pool
        from_num = choose_from(from_pool)
        if not from_num:
            skip["missing_textgrid_number"] += 1; continue

        # simulate render (also shakes out bad placeholders)
        _ = _render_message(random.choice(template_msgs), pf)

        # simulate queue success
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
        "skipped": skipped or (processed - queued),
        "skip_reasons": dict(skip),
        "rotation": dict(dist),
        "pool_size": len(pool),
        "has_templates": has_templates,
        "note": "read-only simulation; no writes performed",
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--campaign", help="Campaign RECORD_ID() to audit")
    ap.add_argument("--all", action="store_true", help="Audit all scheduled campaigns")
    args = ap.parse_args()

    campaigns_h = CONNECTOR.campaigns()
    camps = _gather_campaigns(campaigns_h, args.campaign, args.all)
    if not camps:
        print("No matching campaigns found."); sys.exit(1)

    summaries = []
    for c in camps:
        s = audit_campaign(c)
        summaries.append(s)

    # Pretty print
    tot_q = sum(s["queued"] for s in summaries)
    print(f"\n=== Scheduler Read-Only Audit ({len(summaries)} campaign(s)) ===")
    for s in summaries:
        print(f"\n• Campaign {s['id']}  [{s['status']}]  market={s['market']}  pool={s['pool_size']}  templates={s['has_templates']}")
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