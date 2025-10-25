from __future__ import annotations
import os, random, re, time, hashlib, traceback
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from functools import lru_cache
from typing import Any, Dict, List, Optional

from sms.runtime import get_logger
from sms.datastore import CONNECTOR
from sms.airtable_schema import DripStatus

log = get_logger("campaign_runner")
QUIET_TZ = ZoneInfo("America/Chicago")

STATUS_ICON = {
    "QUEUED": "⏳",
    "Sending…": "🔄",
    "Sent": "✅",
    "Retry": "🔁",
    "Throttled": "🕒",
    "Failed": "❌",
    "DNC": "⛔",
}

# ───────────────────────────── utilities
def _now(): return datetime.now()
def _ct_future_iso_naive(min_s=3, max_s=30):
    dt = datetime.now(QUIET_TZ) + timedelta(seconds=random.randint(min_s, max_s))
    return dt.replace(tzinfo=None).isoformat(timespec="seconds")

def _field_get(fields: dict, *names: str, default=None):
    for n in names:
        if n in fields and fields[n] not in (None, ""):
            return fields[n]
    return default

def _norm_market(s: str | None) -> str | None:
    if not s: return None
    s = " ".join(str(s).strip().split())
    s = s.replace(" ,", ",").replace(",,", ",")
    s = re.sub(r"\s*,\s*", ", ", s)
    return s

HONORIFICS = {"mr", "mrs", "ms", "dr", "miss", "sir", "madam", "mister"}
def _extract_first_name(full_name: Optional[str]) -> str:
    if not full_name: return ""
    s = str(full_name).strip()
    s = re.split(r"[&/,+]", s, maxsplit=1)[0].strip()
    if "," in s:
        parts = [p.strip() for p in s.split(",", 1)]
        if len(parts) == 2: s = parts[1]
    s = s.replace(".", " ").strip()
    tokens = [t for t in s.split() if t and t.lower() not in HONORIFICS]
    return tokens[0] if tokens else ""

# ───────────────────────────── placeholders
def _apply_placeholders(template_body: str, pf: dict, first_name: str) -> str:
    """Robust placeholder fill for {First}, {Property Address}, {Property City} etc."""
    msg = template_body or ""

    # Replace first name tokens
    for key in ["{First}", "{first}", "{FIRST}", "{First Name}", "{First_Name}"]:
        msg = msg.replace(key, first_name or "")

    # Fuzzy lookup helper (handles linked-field variants)
    def find_field(targets: list[str]) -> str:
        for k in pf.keys():
            norm = k.lower().strip()
            for t in targets:
                if t in norm:
                    val = pf.get(k)
                    if val: return str(val)
        return ""

    addr = find_field(["property address", "address"])
    city = find_field(["property city", "city"])
    msg = msg.replace("{Property Address}", addr)
    msg = msg.replace("{Property City}", city)

    # Catch all remaining placeholders
    for ph in set(re.findall(r"\{([^{}]+)\}", msg)):
        normalized = ph.lower().replace("_", " ").strip()
        replacement = ""
        for k, v in pf.items():
            if normalized in k.lower() and v:
                replacement = str(v)
                break
        msg = msg.replace(f"{{{ph}}}", replacement or "")
    msg = re.sub(r"\{[^{}]+\}", "", msg)
    return " ".join(msg.split())

# ───────────────────────────── templates
def _get_template_body(tbl, tid: str) -> Optional[str]:
    try: rec = tbl.get(tid)
    except Exception as e:
        log.warning(f"[template] read failed: {e}")
        return None
    f = (rec or {}).get("fields", {}) or {}
    for key in ("Body", "Message", "Text", "Template", "Content"):
        v = f.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None

# ───────────────────────────── numbers
def _number_from_fields(f: dict) -> Optional[str]:
    return _field_get(f, "TextGrid Phone Number", "Phone Number", "Number", "From Number")
def _is_active_number(f: dict) -> bool:
    active = f.get("Active"); return True if active is None else bool(active)

@lru_cache(maxsize=256)
def _load_numbers_for_market(mv: Optional[str]) -> List[str]:
    mv = _norm_market(mv)
    if not mv: return []
    try:
        tbl = CONNECTOR.numbers().table
        recs = tbl.all(formula=f"{{Market}}='{mv}'", page_size=100)
    except Exception as e:
        log.error(f"[numbers] Query failed for market {mv}: {e}")
        return []
    pool = []
    for r in recs or []:
        f = (r or {}).get("fields", {}) or {}
        if _is_active_number(f):
            num = _number_from_fields(f)
            if num: pool.append(str(num).strip())
    return list(dict.fromkeys(pool))

def _stable_rr_index(pid: str, n: int) -> int:
    if n <= 0: return 0
    h = hashlib.sha1(pid.encode()).hexdigest()
    return int(h[:8], 16) % n
def _pick_textgrid_number_for_campaign(cf: dict, pid: str) -> Optional[str]:
    market = _field_get(cf, "Market", "Market Name", "market")
    pool = _load_numbers_for_market(market)
    if not pool: return None
    return pool[_stable_rr_index(pid, len(pool))]

# ───────────────────────────── Airtable safe create
def _robust_create_drip(tbl, payload: Dict[str, Any], dryrun=False) -> bool:
    if dryrun or os.getenv("TEST_MODE", "false").lower() == "true":
        log.info(f"[dryrun] Would queue → {payload.get('Message')[:100]}")
        return True
    try:
        tbl.create(payload); return True
    except Exception as e:
        msg = str(e)
        if "INVALID_MULTIPLE_CHOICE_OPTIONS" in msg or "Insufficient permissions" in msg:
            bad = payload.pop("Market", None)
            log.warning(f"⚠️ Market select rejected ({bad}); retrying without Market.")
            try: tbl.create(payload); return True
            except Exception as e2:
                log.error(f"Retry failed: {e2}"); return False
        log.error(f"Airtable create failed [Drip Queue]: {e}")
        return False

# ───────────────────────────── campaign filter
def _eligible_campaigns(tbl) -> List[Dict]:
    now_iso = _now().isoformat()
    formula = (
        f"AND("
        f"OR({{Status}}='Active',AND({{Status}}='Scheduled',{{Start Time}}<='{now_iso}')),"
        f"NOT({{Status}}='Paused'),NOT({{Status}}='Completed')"
        f")"
    )
    try: return tbl.all(formula=formula, page_size=100)
    except Exception as e:
        log.error(f"❌ Failed to fetch campaigns: {e}")
        return []

# ───────────────────────────── queueing
def _queue_one_campaign(camp: Dict, limit: int, dryrun=False) -> int:
    cf = camp.get("fields", {}) or {}
    cid = camp.get("id"); cname = cf.get("Name") or "Unnamed Campaign"
    log.info(f"➡️ Queuing campaign: {cname}")

    drip_tbl = CONNECTOR.drip_queue().table
    pros_ids = cf.get("Prospects") or []
    if not pros_ids:
        log.warning(f"⚠️ No linked prospects for {cname}")
        return 0

    pros_tbl = CONNECTOR.prospects().table
    formula = "OR(" + ",".join([f"RECORD_ID()='{pid}'" for pid in pros_ids]) + ")"
    try: prospects = pros_tbl.all(formula=formula, page_size=min(limit, 100))
    except Exception as e:
        log.error(f"Failed to fetch prospects for {cname}: {e}")
        return 0
    if not prospects: return 0

    tmpl_links = cf.get("Templates") or []
    tmpl_tbl = CONNECTOR.templates().table if tmpl_links else None
    tmpl_total, tmpl_idx, queued = len(tmpl_links), 0, 0

    for p in prospects:
        pf = (p or {}).get("fields", {}) or {}
        pid = p.get("id") or ""
        phone = _field_get(pf, "Seller Phone Number", "Phone 1 (from Linked Owner)", "Phone", "Primary Phone", "Mobile")
        if not phone: continue
        market = _norm_market(_field_get(pf, "Market", "market", "Market Name"))
        first = _extract_first_name(_field_get(pf, "First Name", "Owner First Name", "Seller Name", "Owner Name", "Name"))
        tg = _pick_textgrid_number_for_campaign(cf, pid)

        tmpl_id = None; message = None
        if tmpl_tbl and tmpl_total:
            tmpl_id = tmpl_links[tmpl_idx % tmpl_total]; tmpl_idx += 1
            body = _get_template_body(tmpl_tbl, tmpl_id)
            if body: message = _apply_placeholders(body, pf, first)
        if not message:
            message = f"Hi {first}, this is Ryan, a local investor. Are you still the owner? Reply STOP to opt out."

        payload = {
            "Campaign": [cid], "Prospect": [pid],
            "Seller Phone Number": str(phone).strip(),
            "TextGrid Phone Number": tg,
            "Message": message, "Market": market,
            "Status": DripStatus.QUEUED.value,
            "UI": STATUS_ICON["QUEUED"],
            "Next Send Date": _ct_future_iso_naive(3, 30),
            "Template": [tmpl_id] if tmpl_id else None,
        }
        if _robust_create_drip(drip_tbl, payload, dryrun=dryrun):
            queued += 1
        time.sleep(random.uniform(0.05, 0.2))
    log.info(f"✅ Queued {queued} for {cname}")
    return queued

# ───────────────────────────── main
def run_campaigns(limit="ALL", send_after_queue=True, dryrun=False) -> Dict[str, Any]:
    log.info(f"🚀 Campaign Runner — limit={limit}, dryrun={dryrun}")
    camp_tbl = CONNECTOR.campaigns().table
    camps = _eligible_campaigns(camp_tbl)
    if not camps:
        log.info("⚠️ No due/active campaigns found.")
        return {"ok": True, "queued": 0}
    per_limit = 1_000_000 if str(limit).upper()=="ALL" else max(int(limit),1)
    total = 0
    for c in camps:
        try: total += _queue_one_campaign(c, per_limit, dryrun=dryrun)
        except Exception as e:
            log.error(f"Queue fail: {e}")
            log.debug(traceback.format_exc())
    if not dryrun and send_after_queue and total>0:
        try:
            from sms.outbound_batcher import send_batch
            send_batch(limit=500)
        except Exception as e:
            log.error(f"⚠️ Outbound batch send failed: {e}")
    log.info(f"🏁 Done — {total} queued across {len(camps)} campaigns.")
    return {"ok": True, "queued": total, "processed": len(camps)}

if __name__ == "__main__":
    import argparse
    p=argparse.ArgumentParser()
    p.add_argument("--limit",default="ALL")
    p.add_argument("--dryrun",action="store_true")
    a=p.parse_args()
    log.info(f"Running Campaign Runner with limit={a.limit}, dryrun={a.dryrun}")
    res=run_campaigns(limit=a.limit,send_after_queue=not a.dryrun,dryrun=a.dryrun)
    if a.dryrun: log.info("🧪 Dry-run: no Airtable records created or sent.")
    print(res)
