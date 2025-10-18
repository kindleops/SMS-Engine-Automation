What this solves

End-to-end texting (inbound → AI intent → reply → delivery receipts) without manual touch.

Canonical logging in Airtable for every inbound/outbound.

Deterministic promotion from Prospect → Lead (after true selling/offer interest).

Campaign automation with quiet hours, rate limits, retries, and KPIs.

System overview
TextGrid → /inbound ─┐
                     ├── log Conversation (AT) → link prospect/lead
                     ├── AI intent + stage → autoresponder (if eligible)
                     ├── /outbound (echo) logs sent → Conversation
TextGrid ← /delivery ┘
                         ↳ Numbers counters (delivered/failed/opt-out)
                         ↳ Lead aggregates (last activity, reply count)


Core modules

sms/main.py — FastAPI app, mounts all routers.

sms/inbound_webhook.py — Validates inbound payload, logs to Conversations, promotion triggers.

sms/outbound_webhook.py — Logs a “sent” event for idempotency and analytics.

sms/delivery_webhook.py — Normalizes provider statuses and bumps counters.

sms/dispatcher.py — Queues + sends outbound with jitter & rate limits.

sms/autoresponder.py — AI reply policy + template fallback, quiet-hours aware.

sms/campaign_runner.py — Picks recipients and schedules messages.

sms/number_pools.py — Per-DID quotas & daily resets.

sms/workers/* — long-running helpers (autolinker, enrichment, etc.).

engine_runner.py — CLI entry to run periodic jobs locally.

FastAPI routes (contracts)

All routes accept auth via:

?token=..., 2) x-webhook-token/x-cron-token, or 3) Authorization: Bearer ….

Route	Method	Purpose	Body (min)	Returns
/inbound	POST	Receive TextGrid inbound	{From, To, Body, MessageSid}	`{"status":"ok","conversation_id":..., "linked_to":"lead
/outbound	POST	Confirm an outbound was sent (echo)	{From, To, Body, MessageSid}	{"status":"ok"}
/delivery	POST	Delivery receipt (sent/delivered/failed/undelivered)	{MessageSid, MessageStatus, To, From}	`{"status":"ok","normalized":"delivered
/jobs/autolinker	POST	Backfill links Conversations ↔ Leads/Prospects	{limit?}	{"linked":N}
/jobs/intent-batch	POST	Run AI classifier on recent inbounds	{limit?}	{"classified":N}
/jobs/lead-promoter	POST	Promote qualified prospects → leads	{}	{"promoted":N}
/cron/all	POST	Composite cron tick	{limit?}	Summary object

Error policy

Missing From or Body → HTTPException(422).

STOP-like messages ("STOP","UNSUBSCRIBE","REMOVE","OPT OUT"; case/space insensitive):

log inbound, set Delivery Status = OPT OUT, increment optout counters, and return {"status":"optout"}.

All handlers are idempotent on MessageSid.

Environment

Required (Render secrets or .env):

# Airtable
AIRTABLE_API_KEY=***
LEADS_CONVOS_BASE=appXXXXXXXXXXXX   # or AIRTABLE_LEADS_CONVOS_BASE_ID

# Table names (override if needed)
CONVERSATIONS_TABLE=Conversations
LEADS_TABLE=Leads
CAMPAIGNS_TABLE=Campaigns
TEMPLATES_TABLE=Templates
PROSPECTS_TABLE=Prospects
NUMBERS_TABLE=Numbers

# Webhook / Cron auth
WEBHOOK_TOKEN=***     # used by /inbound,/outbound,/delivery
CRON_TOKEN=***        # used by /jobs/* and /cron/*

# Quiet hours & sending
QUIET_HOURS_ENFORCED=true
QUIET_START_HOUR_LOCAL=21
QUIET_END_HOUR_LOCAL=9
RATE_PER_NUMBER_PER_MIN=20
GLOBAL_RATE_PER_MIN=5000
DAILY_LIMIT=750
JITTER_SECONDS=2

# Worker/runner
WORKER_INTERVAL_SEC=30
SEND_BATCH_LIMIT=500
RETRY_LIMIT=100

# Provider
TEXTGRID_ACCOUNT_SID=***
TEXTGRID_AUTH_TOKEN=***

# Rate limiter (optional)
UPSTASH_REDIS_REST_URL=***
UPSTASH_REDIS_REST_TOKEN=***

# Logging
LOG_LEVEL=debug

Airtable schema (authoritative)

Style: Proper case field names for UI. Code refers to them exactly as listed.

1) Conversations (canonical log of every SMS)

Primary: Conversation ID (Autonumber)

Field	Type	Allowed values / notes
Stage	Single select	STAGE 1 - OWNERSHIP CONFIRMATION · STAGE 2 - INTEREST FEELER · STAGE 3 - PRICE QUALIFICATION · STAGE 4 - PROPERTY CONDITION · STAGE 5 - MOTIVATION / TIMELINE · STAGE 6 - OFFER FOLLOW UP · STAGE 7 - CONTRACT READY · STAGE 8 - CONTRACT SENT · STAGE 9 - CONTRACT FOLLOW UP · OPT OUT · DNC
Processed By	Single select	Campaign Runner · Autoresponder · AI: Phi-3 Mini · AI: Phi-3 Medium · AI: GPT-4o · AI: Mistral 7B · AI: Gemma 2 · Manual / Human
Intent Detected	Single select	Positive · Neutral · Delay · Reject · DNC
Direction	Single select	INBOUND · OUTBOUND
Delivery Status	Single select	QUEUED · SENT · DELIVERED · FAILED · UNDELIVERED · OPT OUT
AI Intent	Single select	intro · who_is_this · how_got_number · interest_detected · ask_price · offer_discussion · motivation_detected · condition_question · not_interested · wrong_number · delay · neutral · other · timeline_question
TextGrid Phone Number	Text	E.164 of our DID
TextGrid ID	Text	Provider message SID (idempotency key)
Template Record ID	Text	Optional context when a template drove the send
Seller Phone Number	Text	Target phone (E.164)
Prospect Record ID	Text	Foreign key during pre-lead stage
Lead Record ID	Text	Foreign key after promotion
Campaign Record ID	Link to Campaigns	Owning campaign
Sent Count	Number	auto
Reply Count	Number	auto
Message Summary (AI)	Long text	short reasoning
Message	Long text	full body
Template	Link to Templates	optional
Prospects	Link to Prospects	
Prospect	Link to Prospects	(legacy alias ok)
Lead Status (from Lead)	Lookup	passive
Campaign	Link to Campaigns	(alias of Campaign Record ID ok)
Response Time (Minutes)	Formula/Number	calc allowed
Record ID	Formula	RECORD_ID()
Received Time	Date	inbound timestamp
Processed Time	Date	AI/autoresponder handled at
Last Sent Time	Date	last outbound
Last Retry Time	Date	last retry
AI Response Trigger	Checkbox/Text (optional)	allows manual AI kick

Link rules

A Conversation must link to either Prospect or Lead (never both).

Linking preference:

If a Lead exists for Seller Phone Number, link to Lead.

Else link/create Prospect; promotion later (see below).

2) Leads (created only after true selling/offer interest)

Primary: Lead ID (Autonumber)

Field	Type	Allowed / notes
Campaigns	Link to Campaigns	one-to-many
Conversations	Link to Conversations	all related messages
Deals	Link to Deals	optional
Delivered Count	Number	agg
Failed Count	Number	agg
Last Activity	Date	max(received/sent)
Last Delivery Status	Single select	QUEUED · SENT · DELIVERED · FAILED · UNDELIVERED · OPT OUT
Last Direction	Single select	OUTBOUND · INBOUND
Last Inbound	Date	
Last Message	Long text	
Last Outbound	Date	
Lead Status	Single select	NEW · CONTACTED · ACTIVE COMMUNICATION · LEAD FOLLOW UP · WARM COMPS? · MAKE OFFER · OFFER FOLLOW UP · UNDER CONTRACT · DISPOSITION STAGE · IN ESCROW · CLOSING SET · DEAD
Phone	Single line text	seller E.164
Prospect	Link to Prospects	original prospect row
Reply Count	Number	
Response Time (Minutes)	Number	
Sent Count	Number	
Market (from Prospect)	Lookup	
Address/City/State/Zip (from Prospect)	Lookup	
Source	Single line text	channel label
Record ID	Formula	RECORD_ID()

Promotion rule (Prospect → Lead)
Create a Lead when any of these happen on the Conversation:

Intent Detected = Positive or AI Intent ∈ {interest_detected, offer_discussion, ask_price}

OR Stage ≥ STAGE 3 - PRICE QUALIFICATION

OR manual command via AI Response Trigger + positive classifier.

On promotion:

Create Lead with Phone, Campaigns (current), link Conversation to Lead (remove Prospect link), increment Lead aggregates, set Lead Status = ACTIVE COMMUNICATION (or next stage if provided).

3) Campaigns

Primary: Campaign ID (Autonumber)

Field	Type	Allowed / notes
Name	Single line text	internal label
Status	Single select	Draft · Scheduled · Running · Paused · Completed
Market	Single select	Los Angeles, CA · Tampa, FL · Charlotte, NC · Miami, FL · Minneapolis, MN · Jacksonville, FL · Houston, TX (extendable)
View/Segment	Single line text	saved filter
Campaign Name	Single line text	public label
Total Sent / Replies / Opt Outs / Offers / Leads / Failed / Deals / Contacts	Number	counters
Last Run Result	Long text	last job summary
Templates	Link to Templates	
Prospects	Link to Prospects	
Notifications	Link	optional
Drip Queue	Link	optional
Deals	Link	optional
Conversations	Link	backref
Associated Leads	Link	backref
Start Time	Date	schedule window open
Last Run At	Date	last engine tick
End Time	Date	schedule window close

Scheduling contract

Campaign Runner sends only when Status=Running and current time ∈ [Start Time, End Time].

Scheduled means queued to switch to Running at Start Time.

Quiet hours block send but queue is allowed.

4) Templates (outbound library; AI still allowed to free-compose)

Primary: Template ID (Autonumber)

Key fields:

Stage (Single select) — align with Conversations.Stage list.

Category (Single select) — Intro · Positive · Negative · Opt-Out · Price Check · Delay · Closing · Wrong Number · Not Owner · Buyer · Misc Request · Internal Reminder · Internal Document · Delay / Retry (extensible).

Name, Internal ID, Message (text).

Performance counters & computed rates (keep formulas if useful; engine treats them read-only).

Policy: Autoresponder can (1) pick a template by Stage/Category, or (2) free-compose using AI with guardrails. Either way, it must log the outbound to Conversations with Direction=OUTBOUND, Processed By=Autoresponder (or the model used), and set Delivery Status=QUEUED initially.

Processing rules (script-level contracts)
Inbound (sms/inbound_webhook.py)

Validate payload: require From, Body; To optional but recommended.

If missing → HTTPException(422).

Normalize numbers to E.164.

Create Conversation row:

Direction=INBOUND, Delivery Status remains SENT/blank for inbound.

Link existing Lead by phone; else link/create Prospect.

Stamp Received Time=now.

Check Opt-out: if Body matches stop terms → mark Conversation Delivery Status=OPT OUT, increment number/lead opt-out counters, return {"status":"optout"}.

Classify intent (AI Intent, Intent Detected) and set Stage progression.

Promotion: if rules satisfied (see Leads section), promote Prospect→Lead.

Invoke Autoresponder (if eligible & within policy); otherwise enqueue for follow-up. Always log the outbound reply as a new Conversation row.

Outbound echo (sms/outbound_webhook.py)

Idempotent log of a send event with Direction=OUTBOUND, Delivery Status=SENT, Processed By=Campaign Runner|Autoresponder|AI:…, Last Sent Time=now.

If provided Template Record ID, link it.

Ensure linked Lead/Prospect exists.

Delivery receipts (sms/delivery_webhook.py)

Accept JSON or form payload; normalize MessageStatus to one of:
queued, sent, delivered, failed, undelivered, optout.

Update the matching Conversation by TextGrid ID and bump Numbers counters:

delivered → Delivered Count+, failed/undelivered → Failed Count+.

Update Lead last delivery fields and totals.

Campaign Runner (sms/campaign_runner.py)

Select eligible rows for Status=Running, time window open, throttle by:

RATE_PER_NUMBER_PER_MIN, GLOBAL_RATE_PER_MIN, DAILY_LIMIT.

Pick message: template by Stage/Category or AI compose.

For each send: create Conversation (QUEUED), dispatch, and rely on /outbound + /delivery to complete lifecycle.

Autoresponder (sms/autoresponder.py)

Policy matrix drives reply choice:

INBOUND + Positive/Question → AI reply (model router below).

Delay/Neutral → short check-in template.

Reject/DNC → no reply; mark status accordingly.

Respect Quiet Hours (QUIET_HOURS_ENFORCED). If blocked, queue with Delivery Status=QUEUED.

Always log outbound to Conversations and increment Lead reply count when appropriate.

Model router (inside autoresponder)

Default order (free/open first where possible):

AI: Phi-3 Mini (fast, cheap triage)

AI: Mistral 7B (clarification / paraphrase)

AI: Gemma 2 (polish)

AI: GPT-4o (hard cases / safety)

Record the selected engine in Processed By.

Promotion & stage progression (deterministic)

First owner verification → STAGE 1.

Any selling interest or price talk → move ≥ STAGE 3, promote to Lead if not already.

After offer details / negotiation → STAGE 6–STAGE 7.

Document transfer → STAGE 8–STAGE 9.

OPT OUT/DNC are terminal; autoresponder disabled.

Running locally
# API
uvicorn sms.main:app --host 0.0.0.0 --port 10000 --reload

# Engine tick (all)
python engine_runner.py --all

# Focused tasks
python engine_runner.py --autolinker --limit 200
python engine_runner.py --intent-batch --limit 200


Health

GET /ping → 200 OK

GET /health → internal checks (numbers cache, Airtable reachability)

Deployment (Render)

render.yaml provision:

Web service: rei-sms-engine @ ./Dockerfile.sms, health /health

Workers: rei-sms-worker, rei-autolinker

Cron jobs call /autoresponder/autoresponder, /run-campaigns, /retry, /reset-quotas, KPI aggregations, /cron/all

Secrets: add all envs from Environment section (no quotes in values).

Observability & KPIs

Numbers table (not detailed here) tracks per-DID: Sent Today/Total, Delivered Today/Total, Failed Today/Total, OptOut Today/Total, Remaining, Reset window.

KPI jobs roll up daily/weekly/monthly aggregates to Performance base.

Log level via LOG_LEVEL, noisy third-party logs suppressed in production.

Testing contracts

Inbound

With valid payload → returns {"status":"ok"}.

STOP payload → {"status":"optout"} and no autoresponder.

Missing From or Body → HTTP 422.

Delivery

Missing To/From should not mutate Numbers; return HTTP 422.

Valid delivered → response includes {"status":"ok"} and increments delivered counter.

Idempotency

Reposting same MessageSid must not duplicate rows.

Troubleshooting
Symptom	Likely cause	Fix
Conversations not linking	Field name mismatch	Verify exact names above; update ENV overrides if you customized
Autoresponder silent	Quiet hours on	Disable or allow queue; check QUIET_* envs
Delivery webhook 200 but no counters	Missing TextGrid ID mapping	Ensure inbound/outbound set TextGrid ID and delivery uses the same SID
Render build OK but routes 401	Token missing	Use ?token=, x-webhook-token, or Bearer token
Appendix — ENV field overrides (optional)

If you change Airtable field names, set the exact mapping:

# Conversations field overrides (examples)
CONV_STAGE_FIELD=Stage
CONV_PROCESSED_BY_FIELD=Processed By
CONV_INTENT_FIELD=Intent Detected
CONV_DIRECTION_FIELD=Direction
CONV_STATUS_FIELD=Delivery Status
CONV_AI_INTENT_FIELD=AI Intent
CONV_TEXTGRID_ID_FIELD=TextGrid ID
CONV_SELLER_PHONE_FIELD=Seller Phone Number
CONV_MESSAGE_FIELD=Message
CONV_LEAD_LINK_FIELD=Lead
CONV_PROSPECT_LINK_FIELD=Prospect
CONV_CAMPAIGN_LINK_FIELD=Campaign


Authoritative schema date: 2025-10-17
If you add/remove single-select options, update this README first—Codex treats it as the contract and will align code/tests to match.

What Codex should do next

Validate all field lookups against this schema; add a thin mapping layer reading ENV overrides.

Make every webhook idempotent on TextGrid ID.

Enforce promotion rules & stage progression exactly as above.

Centralize quiet-hour + rate-limit checks in dispatcher.

Ensure every send (manual/AI/template) creates a Conversations row first (QUEUED) → /outbound → /delivery.

Expand tests to cover: opt-out, missing fields, idempotent delivery, promotion, stage flow, and quiet-hour queueing.