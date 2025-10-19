#!/usr/bin/env python3
import os, re, time
from datetime import datetime
from sms.tables import get_table as _get
from sms.config import TEMPLATE_FIELD_MAP as TEMPLATE_FIELDS

DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"
BASE = "LEADS_CONVOS_BASE" if os.getenv("LEADS_CONVOS_BASE") else "LEADS_CONVO_BASE"

C = _get("AIRTABLE_API_KEY", BASE, "CONVERSATIONS_TABLE", "Conversations")
T = _get("AIRTABLE_API_KEY", BASE, "TEMPLATES_TABLE", "Templates")
K = _get("AIRTABLE_API_KEY", BASE, "CAMPAIGNS_TABLE", "Campaigns")

TEMPLATE_INTERNAL_ID_FIELD = TEMPLATE_FIELDS["INTERNAL_ID"]
TEMPLATE_PRIMARY_FIELD = TEMPLATE_FIELDS["PRIMARY"]
TEMPLATE_MESSAGE_FIELD = TEMPLATE_FIELDS["MESSAGE"]
TEMPLATE_NAME_KEY_FIELD = TEMPLATE_FIELDS.get("NAME_KEY", "Name (Key)")
TEMPLATE_NAME_FIELD = TEMPLATE_FIELDS.get("NAME", "Name")
TEMPLATE_RECORD_ID_FIELD = TEMPLATE_FIELDS.get("RECORD_ID", "Record ID")

def ok_link(v): return isinstance(v, list) and v and isinstance(v[0], str) and v[0].startswith("rec")
def first(*vals):
    for v in vals:
        if v not in (None, "", [], {}): return v
    return None

def norm_text(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def tokenize(s: str) -> set[str]:
    # remove punctuation, keep words/numbers, collapse whitespace
    s = (s or "").lower()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return set(s.split()) if s else set()

def jaccard(a:set[str], b:set[str]) -> float:
    if not a or not b: return 0.0
    inter = len(a & b); union = len(a | b)
    return inter/union if union else 0.0

def parse_dt(x):
    if not x: return None
    s = str(x).strip()
    for fmt in (None, "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            if fmt:
                return datetime.strptime(s, fmt)
            # try ISO
            return datetime.fromisoformat(s.replace("Z","+00:00"))
        except Exception:
            continue
    return None

# ------------------ Index Templates ------------------
print("Building template maps...")
tmpl_by_body = {}
tmpl_tokens = {}         # rec -> token set
tmpl_prefix = {}         # normalized first 80 chars -> rec
tmpl_names = {}          # name -> rec
tmpl_internal = {}       # internal id -> rec

for r in T.all():
    f = r.get("fields", {})
    rec_id = f.get(TEMPLATE_RECORD_ID_FIELD) or r["id"]
    body = first(
        f.get(TEMPLATE_MESSAGE_FIELD),
        f.get("Body"),
        f.get("Text"),
        f.get("Content"),
    )
    name = first(f.get(TEMPLATE_NAME_KEY_FIELD), f.get(TEMPLATE_NAME_FIELD), f.get("Template Name"))
    internal = first(f.get(TEMPLATE_INTERNAL_ID_FIELD), f.get(TEMPLATE_PRIMARY_FIELD))

    if isinstance(body, str) and body.strip():
        nb = norm_text(body)
        tmpl_by_body[nb] = rec_id
        tmpl_tokens[rec_id] = tokenize(body)
        tmpl_prefix[nb[:80]] = rec_id
    if isinstance(name, str) and name.strip():
        tmpl_names[name.strip()] = rec_id
    if isinstance(internal, str) and internal.strip():
        tmpl_internal[internal.strip()] = rec_id

print(f"Templates: by_body={len(tmpl_by_body)}, by_name={len(tmpl_names)}, by_internal={len(tmpl_internal)}")

# ------------------ Index Campaigns ------------------
print("Indexing campaigns...")
camp_meta = {}          # rec -> dict(name, market, start_dt, templates:set(rec))
for r in K.all():
    f = r.get("fields", {})
    cid = f.get("Record ID") or r["id"]
    name = f.get("Campaign Name")
    market = first(f.get("Market"), f.get("market"))
    start = parse_dt(f.get("Start Time"))
    tlinks = f.get("Templates") or []
    tset = set([x for x in tlinks if isinstance(x, str) and x.startswith("rec")])
    camp_meta[cid] = {
        "name": (str(name).strip() if name else None),
        "market": (str(market).strip() if market else None),
        "start": start,
        "templates": tset
    }
print(f"Campaigns: {len(camp_meta)} indexed")

def resolve_template_from_message(msg: str, name_hint: str = None, text_rec_hint: str = None):
    # 0) trust a text rec id if provided
    if isinstance(text_rec_hint, str) and text_rec_hint.startswith("rec"):
        return text_rec_hint

    if not msg:
        # maybe the name hint maps
        if isinstance(name_hint, str) and name_hint.strip():
            return tmpl_names.get(name_hint.strip())
        return None

    nm = norm_text(msg)
    tok = tokenize(msg)

    # 1) exact normalized body
    hit = tmpl_by_body.get(nm)
    if hit: return hit

    # 2) prefix match on normalized body
    hit = tmpl_prefix.get(nm[:80])
    if hit: return hit

    # 3) fuzzy via token jaccard
    best, best_sim = None, 0.0
    for rec_id, tks in tmpl_tokens.items():
        sim = jaccard(tok, tks)
        if sim > best_sim:
            best, best_sim = rec_id, sim
    if best and best_sim >= 0.8:   # tweak threshold as needed
        return best

    # 4) name hint as last resort
    if isinstance(name_hint, str) and name_hint.strip():
        return tmpl_names.get(name_hint.strip())

    return None

def choose_campaign(template_rec: str, convo_market: str, convo_time):
    # candidates that include this template
    cands = []
    for cid, meta in camp_meta.items():
        if template_rec in meta["templates"]:
            cands.append((cid, meta))
    if not cands:
        return None

    # filter by market if provided
    if convo_market:
        c2 = [(cid,m) for cid,m in cands if (m["market"] or "").lower() == convo_market.lower()]
        if c2:
            cands = c2

    # pick the one with latest start <= convo_time, else latest start overall
    def score(meta):
        st = meta["start"]
        if convo_time and st and st <= convo_time:
            # prefer starts before/at convo time; larger is better
            return (1, st.timestamp())
        return (0, st.timestamp() if st else 0.0)

    best = None
    best_key = None
    for cid, meta in cands:
        key = score(meta)
        if (best is None) or (key > best_key):
            best, best_key = cid, key
    return best

rows = C.all()
scan = len(rows)
link_t = link_c = already_t = already_c = 0

for r in rows:
    f = r.get("fields", {})
    rid = r["id"]
    patch = {}

    # ===== TEMPLATE =====
    if ok_link(f.get("Template")) and f.get("Template Record ID") == f["Template"][0]:
        already_t += 1
        tmpl_rec = f["Template"][0]
    else:
        msg = first(f.get("message"), f.get("Message"), f.get("Text"), f.get("Body"))
        name_hint = f.get("Template Name") or f.get("Name (Key)")
        text_rec_hint = f.get("Template Record ID")
        tmpl_rec = resolve_template_from_message(msg, name_hint, text_rec_hint)
        if tmpl_rec:
            patch["Template"] = [tmpl_rec]
            patch["Template Record ID"] = tmpl_rec
            link_t += 1

    # ===== CAMPAIGN =====
    if ok_link(f.get("Campaign")) and f.get("Campaign Record ID") == f["Campaign"][0]:
        already_c += 1
    else:
        cmp_rec = None
        market = first(
            f.get("Market (from Lead Status (from Lead))"),
            f.get("Market (from Prospect)"),
            f.get("Market"),
        )
        ctime = parse_dt(first(f.get("sent_at"), f.get("received_at")))
        if tmpl_rec:
            cmp_rec = choose_campaign(tmpl_rec, market, ctime)
        # fallback: exact name if present
        if not cmp_rec and isinstance(f.get("Campaign Name"), str):
            nm = f["Campaign Name"].strip()
            for cid, meta in camp_meta.items():
                if (meta["name"] or "") == nm:
                    cmp_rec = cid
                    break
        if cmp_rec:
            patch["Campaign"] = [cmp_rec]
            patch["Campaign Record ID"] = cmp_rec
            link_c += 1

    if patch:
        if DRY_RUN:
            print(f"[DRY] {rid} <- {patch}")
        else:
            try:
                C.update(rid, patch); time.sleep(0.12)
            except Exception as e:
                print("⚠️", rid, e)

print("\n=== Summary ===")
print(f"Conversations scanned: {scan}")
print(f"Templates: linked={link_t}, already={already_t}")
print(f"Campaigns:  linked={link_c}, already={already_c}")
print(f"DRY_RUN={DRY_RUN}")
