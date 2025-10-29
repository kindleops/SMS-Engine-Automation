import os
import re
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from fastapi import APIRouter, HTTPException, Request, Header, Query
from pyairtable import Table

from sms.number_pools import increment_delivered, increment_failed, increment_opt_out

router = APIRouter()

# === ENV CONFIG ===
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
BASE_ID = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")

# === WEBHOOK AUTHENTICATION ===
WEBHOOK_TOKEN = (
    os.getenv("WEBHOOK_TOKEN")
    or os.getenv("CRON_TOKEN")
    or os.getenv("TEXTGRID_AUTH_TOKEN")
    or os.getenv("INBOUND_WEBHOOK_TOKEN")
)

# === REDIS/UPSTASH CONFIG ===
REDIS_URL = os.getenv("REDIS_URL") or os.getenv("redis_url")
REDIS_TLS = str(os.getenv("REDIS_TLS", "true")).lower() in ("true", "1", "yes")
UPSTASH_REST_URL = os.getenv("UPSTASH_REDIS_REST_URL") or os.getenv("upstash_redis_rest_url")
UPSTASH_REST_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN") or os.getenv("upstash_redis_rest_token")

# Optional Redis / Upstash
try:
    import redis as _redis  # type: ignore
except Exception:
    _redis = None
try:
    import requests  # type: ignore
except Exception:
    requests = None

CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")
LEADS_TABLE = os.getenv("LEADS_TABLE", "Leads")
PROSPECTS_TABLE = os.getenv("PROSPECTS_TABLE", "Prospects")

# === CONFIGURABLE LEAD PHONE FIELD ===
LEAD_PHONE_FIELD = os.getenv("LEAD_PHONE_FIELD", "phone")

# === FIELD MAPPINGS ===
from sms.airtable_schema import conversations_field_map

# Get the proper field mappings for Conversations table
CONV_FIELDS = conversations_field_map()

FROM_FIELD = CONV_FIELDS["FROM"]
TO_FIELD = CONV_FIELDS["TO"]
MSG_FIELD = CONV_FIELDS["BODY"]
STATUS_FIELD = CONV_FIELDS["STATUS"]
DIR_FIELD = CONV_FIELDS["DIRECTION"]
TG_ID_FIELD = CONV_FIELDS["TEXTGRID_ID"]
RECEIVED_AT = CONV_FIELDS["RECEIVED_AT"]
SENT_AT = CONV_FIELDS["SENT_AT"]
PROCESSED_BY = CONV_FIELDS["PROCESSED_BY"]
STAGE_FIELD = CONV_FIELDS["STAGE"]
INTENT_FIELD = CONV_FIELDS["INTENT"]
AI_INTENT_FIELD = CONV_FIELDS["AI_INTENT"]

# Legacy env var support for link fields
LEAD_LINK_FIELD = os.getenv("CONV_LEAD_LINK_FIELD", "lead_id")
PROSPECT_LINK_FIELD = os.getenv("CONV_PROSPECT_LINK_FIELD", "Prospect")

# === AIRTABLE CLIENTS ===
convos = Table(AIRTABLE_API_KEY, BASE_ID, CONVERSATIONS_TABLE) if AIRTABLE_API_KEY else None
leads = Table(AIRTABLE_API_KEY, BASE_ID, LEADS_TABLE) if AIRTABLE_API_KEY else None
prospects = Table(AIRTABLE_API_KEY, BASE_ID, PROSPECTS_TABLE) if AIRTABLE_API_KEY else None

# === AUTHENTICATION ===
def _is_authorized(header_token: Optional[str], query_token: Optional[str]) -> bool:
    """Check if request is authorized via header or query token."""
    if not WEBHOOK_TOKEN:
        return True  # auth disabled
    return (header_token == WEBHOOK_TOKEN) or (query_token == WEBHOOK_TOKEN)


# === ENHANCED IDEMPOTENCY STORE ===
class IdempotencyStore:
    """Redis / Upstash idempotency with local fallback for message deduplication."""
    def __init__(self):
        self.r = None
        self.rest = bool(UPSTASH_REST_URL and UPSTASH_REST_TOKEN and requests)
        if REDIS_URL and _redis:
            try:
                self.r = _redis.from_url(REDIS_URL, ssl=REDIS_TLS, decode_responses=True)
            except Exception:
                traceback.print_exc()
        self._mem = set()
        self._max_mem_size = 10000  # Bounded memory set

    def seen(self, msg_id: Optional[str]) -> bool:
        """Check if message ID has been seen before, mark as seen if not."""
        if not msg_id:
            return False
        
        key = f"inbound:msg:{msg_id}"
        
        # Redis TCP
        if self.r:
            try:
                # Use SET with NX (not exists) and EX (expiry in seconds, 24 hours)
                ok = self.r.set(key, "1", nx=True, ex=24 * 60 * 60)
                return not bool(ok)  # True if key already existed
            except Exception:
                traceback.print_exc()
        
        # Upstash REST
        if self.rest:
            try:
                resp = requests.post(
                    UPSTASH_REST_URL,
                    headers={"Authorization": f"Bearer {UPSTASH_REST_TOKEN}"},
                    json=["SET", key, "1", "EX", "86400", "NX"],  # Fixed: direct array format
                    timeout=5,
                )
                data = resp.json() if resp.ok else {}
                return data.get("result") != "OK"  # True if key already existed
            except Exception:
                traceback.print_exc()
        
        # Local fallback with bounded memory
        if key in self._mem:
            return True
        
        # Prevent memory from growing unbounded
        if len(self._mem) >= self._max_mem_size:
            # Remove oldest 20% of entries (simple FIFO approximation)
            to_remove = list(self._mem)[:self._max_mem_size // 5]
            for old_key in to_remove:
                self._mem.discard(old_key)
        
        self._mem.add(key)
        return False

IDEM = IdempotencyStore()


# === BODY PARSING ===
async def _parse_body(request: Request) -> Dict[str, Any]:
    """Parse request body supporting both JSON and form data."""
    content_type = request.headers.get("content-type", "").lower()
    
    try:
        if "application/json" in content_type:
            # Try JSON first
            body = await request.json()
            return dict(body) if isinstance(body, dict) else {}
        elif "application/x-www-form-urlencoded" in content_type:
            # Form data
            form = await request.form()
            return {k: (v if isinstance(v, str) else str(v)) for k, v in dict(form).items()}
        else:
            # Auto-detect: try JSON first, fall back to form
            try:
                body = await request.json()
                return dict(body) if isinstance(body, dict) else {}
            except Exception:
                form = await request.form()
                return {k: (v if isinstance(v, str) else str(v)) for k, v in dict(form).items()}
    except Exception as e:
        print(f"⚠️ Failed to parse request body: {e}")
        raise HTTPException(status_code=422, detail="Invalid payload")

# === HELPERS ===
PHONE_CANDIDATES = [
    "phone",
    "Phone",
    "Mobile",
    "Cell",
    "Phone Number",
    "Primary Phone",
    "Phone 1",
    "Phone 2",
    "Phone 3",
    "Owner Phone",
    "Owner Phone 1",
    "Owner Phone 2",
    "Phone 1 (from Linked Owner)",
    "Phone 2 (from Linked Owner)",
    "Phone 3 (from Linked Owner)",
]

STAGE_SEQUENCE = [
    "STAGE 1 - OWNERSHIP CONFIRMATION",
    "STAGE 2 - INTEREST FEELER",
    "STAGE 3 - PRICE QUALIFICATION",
    "STAGE 4 - PROPERTY CONDITION",
    "STAGE 5 - MOTIVATION / TIMELINE",
    "STAGE 6 - OFFER FOLLOW UP",
    "STAGE 7 - CONTRACT READY",
    "STAGE 8 - CONTRACT SENT",
    "STAGE 9 - CONTRACT FOLLOW UP",
]

PROMOTION_INTENTS = {"positive"}
PROMOTION_AI_INTENTS = {"interest_detected", "offer_discussion", "ask_price"}

POSITIVE_KEYWORDS = {
    "yes",
    "interested",
    "offer",
    "ready",
    "let's talk",
    "lets talk",
    "sure",
    "sounds good",
}

PRICE_KEYWORDS = {"price", "ask", "number", "how much", "offer"}
CONTRACT_KEYWORDS = {"contract", "paperwork", "agreement"}
TIMELINE_KEYWORDS = {"timeline", "move", "closing", "close"}

STOP_WORDS = {"stop", "unsubscribe", "remove", "opt out", "quit"}

# === PROSPECT FIELD MAPPING ===
PROSPECT_FIELD_MAP = {
    "SELLER_ASKING_PRICE": "Seller Asking Price",
    "CONDITION_NOTES": "Condition Notes",
    "TIMELINE_MOTIVATION": "Timeline / Motivation",
    "LAST_INBOUND": "Last Inbound",
    "LAST_OUTBOUND": "Last Outbound", 
    "LAST_ACTIVITY": "Last Activity",
    "OWNERSHIP_CONFIRMED_DATE": "Ownership Confirmation Date",
    "LEAD_PROMOTION_DATE": "Lead Promotion Date",
    "PHONE_1_VERIFIED": "Phone 1 Ownership Verified",
    "PHONE_2_VERIFIED": "Phone 2 Ownership Verified", 
    "INTENT_LAST_DETECTED": "Intent Last Detected",
    "LAST_DIRECTION": "Last Direction",
    "ACTIVE_PHONE_SLOT": "Active Phone Slot",
    "LAST_TRIED_SLOT": "Last Tried Slot",
    "TEXTGRID_PHONE": "TextGrid Phone Number",
    "LAST_MESSAGE": "Last Message",
    "REPLY_COUNT": "Reply Count",
    "OPT_OUT": "Opt Out",
    "SEND_COUNT": "Send Count",
    "STAGE": "Stage",
    "STATUS": "Status",
}

# Enhanced price regex for comprehensive price extraction
PRICE_REGEX = re.compile(
    r'\$\s*(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)|(\d{1,4})\s*k(?:\s|$|[^\w])|(\d{1,4})k',
    re.IGNORECASE
)

# Enhanced condition keywords
ENHANCED_COND_WORDS = [
    "repair", "fix", "renovation", "remodel", "update", "condition", "shape",
    "needs work", "fixer upper", "handyman special", "as-is", "move-in ready",
    "turnkey", "cosmetic", "structural", "foundation", "electrical", "plumbing",
    "hvac", "roof", "flooring", "kitchen", "bathroom", "paint", "carpet",
    "appliances", "windows", "siding", "landscaping", "pool", "deck", "garage",
    "renovated", "updated", "new", "old", "vintage", "restored", "tenant"
]


def iso_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _generate_message_summary(body: str, intent: str, ai_intent: str) -> dict:
    """Generate a structured AI summary of the message matching Airtable format"""
    if not body:
        return {
            "state": "error",
            "errorType": "emptyDependency", 
            "value": None,
            "isStale": False
        }
    
    # Create a concise summary
    summary_parts = []
    
    # Add intent classification
    if intent and intent != "Neutral":
        summary_parts.append(f"Intent: {intent}")
    
    if ai_intent and ai_intent != "neutral":
        summary_parts.append(f"AI: {ai_intent.replace('_', ' ').title()}")
    
    # Add content indicators
    if any(word in body.lower() for word in ["yes", "interested", "ready"]):
        summary_parts.append("Positive response")
    elif any(word in body.lower() for word in ["stop", "unsubscribe", "remove"]):
        summary_parts.append("Opt-out request")
    elif "?" in body:
        summary_parts.append("Question asked")
    
    summary_text = "; ".join(summary_parts) if summary_parts else "Standard message"
    
    return {
        "state": "complete",
        "errorType": None,
        "value": summary_text,
        "isStale": False
    }


def _get_response_time_value(is_inbound: bool = True) -> int:
    """Get appropriate response time value for conversation record"""
    # For inbound messages, response time is 0 since we're receiving not responding
    return 0


def _determine_ai_trigger(intent: str, ai_intent: str, stage: str) -> str:
    """Determine appropriate AI response trigger based on conversation context"""
    if intent.lower() == "positive":
        if "price" in ai_intent or "offer" in ai_intent:
            return "PRICE_DISCUSSION"
        elif "interest" in ai_intent:
            return "INTEREST_DETECTED" 
        else:
            return "POSITIVE_RESPONSE"
    elif intent.lower() == "delay":
        return "FOLLOW_UP_NEEDED"
    elif any(word in intent.lower() for word in ["dnc", "stop", "opt"]):
        return "OPT_OUT"
    else:
        return "STANDARD_RESPONSE"


def _generate_conversation_id(phone: str, timestamp: str) -> int:
    """Generate a unique conversation ID based on phone and timestamp"""
    import hashlib
    
    # Create a hash from phone and timestamp
    hash_input = f"{phone}_{timestamp}".encode()
    hash_digest = hashlib.md5(hash_input).hexdigest()
    
    # Convert first 8 characters to integer
    return int(hash_digest[:8], 16) % 100000  # Keep it under 100k for readability


def _digits(value: Any) -> str:
    return "".join(re.findall(r"\d+", value or "")) if isinstance(value, str) else ""


def _last10(value: Any) -> Optional[str]:
    digits = _digits(value)
    return digits[-10:] if len(digits) >= 10 else None


def normalize_e164(phone: str, field: str = "phone") -> str:
    """Normalize phone number to E.164 format (+1XXXXXXXXXX)."""
    if not phone:
        return ""
    
    # Extract digits only
    digits = _digits(phone)
    
    # If no digits, return original
    if not digits:
        return phone
    
    # If already has country code (11+ digits), assume it's correct
    if len(digits) >= 11:
        return f"+{digits}"
    
    # If 10 digits, assume US number and add +1
    if len(digits) == 10:
        return f"+1{digits}"
    
    # Otherwise return with + prefix
    return f"+{digits}"


def _first_existing_fields(tbl, candidates):
    try:
        probe = tbl.all(max_records=1) or []
        keys = list((probe[0] or {}).get("fields", {}).keys()) if probe else []
    except Exception:
        keys = []
    return [c for c in candidates if c in keys]


def _find_by_phone_last10(tbl, phone):
    """Return first record whose phone-like field matches last10 digits."""
    if not tbl or not phone:
        return None
    want = _last10(phone)
    if not want:
        return None
    fields = _first_existing_fields(tbl, PHONE_CANDIDATES)
    try:
        for r in tbl.all():
            f = r.get("fields", {})
            for col in fields:
                if _last10(f.get(col)) == want:
                    return r
    except Exception:
        traceback.print_exc()
    return None


def _lookup_existing_lead(phone_number: str) -> Tuple[Optional[str], Optional[str]]:
    if not phone_number or not leads:
        return None, None
    try:
        existing = _find_by_phone_last10(leads, phone_number)
        if existing:
            return existing["id"], existing["fields"].get("Property ID")
    except Exception:
        traceback.print_exc()
    return None, None


def _lookup_prospect_property(phone_number: str) -> Optional[str]:
    if not phone_number or not prospects:
        return None
    try:
        prospect = _find_by_phone_last10(prospects, phone_number)
        if prospect:
            return prospect.get("fields", {}).get("Property ID")
    except Exception:
        traceback.print_exc()
    return None


def _lookup_prospect_info(phone_number: str) -> tuple[Optional[str], Optional[str]]:
    """Return (prospect_id, property_id) for the given phone number."""
    if not phone_number or not prospects:
        return None, None
    try:
        prospect = _find_by_phone_last10(prospects, phone_number)
        if prospect:
            prospect_id = prospect.get("id")
            property_id = prospect.get("fields", {}).get("Property ID")
            return prospect_id, property_id
    except Exception:
        traceback.print_exc()
    return None, None


# === PROMOTE PROSPECT → LEAD ===
def promote_prospect_to_lead(phone_number: str, source: str = "Inbound"):
    if not phone_number or not leads:
        return None, None
    try:
        existing = _find_by_phone_last10(leads, phone_number)
        if existing:
            return existing["id"], existing["fields"].get("Property ID")

        fields: Dict[str, Any] = {}
        property_id = None
        prospect = _find_by_phone_last10(prospects, phone_number)
        if prospect:
            p_fields = prospect["fields"]
            for p_col, l_col in {
                "phone": LEAD_PHONE_FIELD,
                "Property ID": "Property ID",
                "Owner Name": "Owner Name",
                "Address": "Address",
                "Market": "Market",
                "Sync Source": "Synced From",
                "List": "Source List",
                "Property Type": "Property Type",
            }.items():
                if p_col in p_fields:
                    fields[l_col] = p_fields[p_col]
            property_id = p_fields.get("Property ID")

        new_lead = leads.create({
            **fields,
            LEAD_PHONE_FIELD: phone_number,
            "Lead Status": "New",
            "Source": source,
            "Reply Count": 0,
            "Last Inbound": iso_timestamp(),
        })
        
        # Update prospect with lead promotion date
        if prospects and prospect:
            try:
                prospects.update(prospect["id"], {
                    PROSPECT_FIELD_MAP["LEAD_PROMOTION_DATE"]: iso_timestamp(),
                    PROSPECT_FIELD_MAP["STATUS"]: "Promoted to Lead"
                })
                print(f"✅ Updated prospect {prospect['id']} with lead promotion date")
            except Exception as e:
                print(f"⚠️ Failed to update prospect with lead promotion: {e}")
        
        print(f"✨ Promoted {phone_number} → Lead")
        return new_lead["id"], property_id

    except Exception as e:
        print(f"⚠️ Prospect promotion failed for {phone_number}: {e}")
        return None, None


def _normalize_stage(stage: Optional[str]) -> str:
    if not stage:
        return STAGE_SEQUENCE[0]
    stage_upper = str(stage).strip().upper()
    for defined in STAGE_SEQUENCE:
        if stage_upper.startswith(defined):
            return defined
    match = re.search(r"(\d)", stage_upper)
    if match:
        idx = int(match.group(1)) - 1
        if 0 <= idx < len(STAGE_SEQUENCE):
            return STAGE_SEQUENCE[idx]
    return STAGE_SEQUENCE[0]


def _stage_rank(stage: Optional[str]) -> int:
    normalized = _normalize_stage(stage)
    try:
        return STAGE_SEQUENCE.index(normalized) + 1
    except ValueError:
        return 1


def _classify_message(body: str, overrides: Optional[Dict[str, str]] = None) -> Tuple[str, str, str]:
    """Return (stage, intent_detected, ai_intent)."""
    overrides = overrides or {}
    intent_override = overrides.get("intent")
    ai_intent_override = overrides.get("ai_intent")
    stage_override = overrides.get("stage")

    if intent_override or ai_intent_override or stage_override:
        stage = _normalize_stage(stage_override)
        intent = intent_override or ("Positive" if _stage_rank(stage) >= 3 else "Neutral")
        ai_intent = ai_intent_override or ("interest_detected" if intent.lower() == "positive" else "neutral")
        return stage, intent, ai_intent

    text = (body or "").strip().lower()

    stage = STAGE_SEQUENCE[0]
    intent = "Neutral"
    ai_intent = "neutral"

    if any(token in text for token in POSITIVE_KEYWORDS):
        intent = "Positive"
        ai_intent = "interest_detected"
        stage = STAGE_SEQUENCE[2]
    elif any(token in text for token in PRICE_KEYWORDS):
        intent = "Positive"
        ai_intent = "ask_price"
        stage = STAGE_SEQUENCE[2]
    elif any(token in text for token in CONTRACT_KEYWORDS):
        intent = "Positive"
        ai_intent = "offer_discussion"
        stage = STAGE_SEQUENCE[6]
    elif any(token in text for token in TIMELINE_KEYWORDS):
        intent = "Delay"
        ai_intent = "timeline_question"
        stage = STAGE_SEQUENCE[4]

    return stage, intent, ai_intent


def _should_promote(intent: str, ai_intent: str, stage: str) -> bool:
    if intent.lower() in PROMOTION_INTENTS:
        return True
    if ai_intent in PROMOTION_AI_INTENTS:
        return True
    return _stage_rank(stage) >= 3


def _is_opt_out(body: str) -> bool:
    body_lower = (body or "").lower()
    return any(token in body_lower for token in STOP_WORDS)


# === COMPREHENSIVE PROSPECT DATA EXTRACTION ===
def _extract_price_from_message(body: str) -> Optional[str]:
    """Extract price information from message text with enhanced pattern matching"""
    if not body:
        return None
    
    text = body.lower()
    
    # Enhanced price patterns including more variations
    # Pattern 1: Standard price formats ($250,000 or $250000)
    standard_pattern = r'\$\s*(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)'
    
    # Pattern 2: 'k' notation (250k, 250K)
    k_pattern = r'(\d{1,4})\s*k(?:\s|$|[^\w])'
    
    # Pattern 3: Around/about patterns (around 250k, about $250,000)
    around_pattern = r'(?:around|about|approximately|roughly)\s*[\$]?\s*(\d{1,3}(?:,\d{3})*|\d{1,4}k)'
    
    # Try standard pattern first
    standard_matches = re.findall(standard_pattern, text)
    if standard_matches:
        price = standard_matches[0].replace(',', '').strip()
        # Validate reasonable price range (25k to 10M)
        try:
            price_val = float(price)
            if 25000 <= price_val <= 10000000:
                return price
        except ValueError:
            pass
    
    # Try 'k' notation
    k_matches = re.findall(k_pattern, text)
    if k_matches:
        try:
            k_value = float(k_matches[0])
            if 25 <= k_value <= 10000:  # 25k to 10Mk
                return str(int(k_value * 1000))
        except ValueError:
            pass
    
    # Try around/about patterns
    around_matches = re.findall(around_pattern, text)
    if around_matches:
        price_text = around_matches[0].replace('$', '').replace(',', '').strip()
        if price_text.endswith('k'):
            try:
                k_value = float(price_text[:-1])
                if 25 <= k_value <= 10000:
                    return str(int(k_value * 1000))
            except ValueError:
                pass
        else:
            try:
                price_val = float(price_text)
                if 25000 <= price_val <= 10000000:
                    return price_text
            except ValueError:
                pass
    
    # Fallback to original PRICE_REGEX
    price_matches = PRICE_REGEX.findall(text)
    if price_matches:
        for match in price_matches:
            if match[0]:  # Full price format
                return match[0].replace(',', '').strip()
            elif match[1] or match[2]:  # 'k' format
                k_value = (match[1] or match[2]).replace('k', '').strip()
                try:
                    return str(int(float(k_value)) * 1000)
                except ValueError:
                    continue
    
    return None


def _extract_condition_info(body: str) -> Optional[str]:
    """Extract condition information from message text with enhanced analysis"""
    if not body:
        return None
    
    text = body.lower()
    condition_indicators = []
    
    # Check for condition-related keywords with enhanced context
    for word in ENHANCED_COND_WORDS:
        if word in text:
            # Extract surrounding context (up to 15 words around the keyword)
            words = body.split()
            for i, w in enumerate(words):
                if word in w.lower():
                    start = max(0, i - 7)
                    end = min(len(words), i + 8)
                    context = ' '.join(words[start:end])
                    condition_indicators.append(context.strip())
                    break
    
    # Look for specific condition patterns
    # Pattern for "needs X" statements
    needs_pattern = r'needs?\s+(?:a\s+)?(?:new\s+)?(\w+(?:\s+\w+){0,2})'
    needs_matches = re.findall(needs_pattern, text)
    for match in needs_matches:
        condition_indicators.append(f"needs {match}")
    
    # Pattern for "X is/are Y" statements about condition
    condition_statement_pattern = r'(roof|foundation|kitchen|bathroom|flooring|hvac|plumbing|electrical|windows)\s+(?:is|are)\s+(\w+(?:\s+\w+){0,2})'
    condition_matches = re.findall(condition_statement_pattern, text)
    for item, condition in condition_matches:
        condition_indicators.append(f"{item} is {condition}")
    
    # Remove duplicates while preserving order
    unique_indicators = []
    for indicator in condition_indicators:
        if indicator not in unique_indicators:
            unique_indicators.append(indicator)
    
    return '; '.join(unique_indicators[:3]) if unique_indicators else None  # Limit to top 3 most relevant


def _extract_timeline_motivation(body: str) -> Optional[str]:
    """Extract timeline and motivation information from message text with enhanced patterns"""
    if not body:
        return None
    
    text = body.lower()
    
    # Enhanced timeline and motivation keywords
    timeline_words = {
        "urgent", "asap", "soon", "immediately", "quickly", "fast", "rush",
        "month", "months", "week", "weeks", "year", "years", "day", "days",
        "deadline", "date", "timeline", "schedule", "time frame",
        "move", "moving", "relocate", "relocating", "relocation",
        "divorce", "divorcing", "separated", "separation",
        "financial", "finances", "money", "cash", "debt", "bills", "mortgage",
        "foreclosure", "foreclosing", "behind", "payments",
        "inheritance", "inherited", "estate", "probate",
        "job", "work", "employment", "transfer", "promotion",
        "health", "medical", "illness", "sick", "hospital",
        "family", "children", "kids", "school", "education",
        "retirement", "retiring", "downsize", "downsizing",
        "upgrade", "upgrading", "bigger", "smaller", "expand"
    }
    
    timeline_indicators = []
    
    # Check for timeline/motivation keywords with enhanced context
    for word in timeline_words:
        if word in text:
            words = body.split()
            for i, w in enumerate(words):
                if word in w.lower():
                    start = max(0, i - 6)
                    end = min(len(words), i + 7)
                    context = ' '.join(words[start:end])
                    timeline_indicators.append(context.strip())
                    break
    
    # Look for specific timeline patterns
    # Pattern for "need to sell by/before X"
    deadline_pattern = r'(?:need|have|must)\s+to\s+sell\s+(?:by|before|within)\s+(\w+(?:\s+\w+){0,3})'
    deadline_matches = re.findall(deadline_pattern, text)
    for match in deadline_matches:
        timeline_indicators.append(f"deadline: {match}")
    
    # Pattern for "because of X" motivation
    motivation_pattern = r'because\s+(?:of\s+)?(\w+(?:\s+\w+){0,4})'
    motivation_matches = re.findall(motivation_pattern, text)
    for match in motivation_matches:
        timeline_indicators.append(f"motivation: {match}")
    
    # Pattern for "due to X" motivation
    due_to_pattern = r'due\s+to\s+(\w+(?:\s+\w+){0,4})'
    due_to_matches = re.findall(due_to_pattern, text)
    for match in due_to_matches:
        timeline_indicators.append(f"due to: {match}")
    
    # Pattern for time expressions (in X months, within X weeks)
    time_expression_pattern = r'(?:in|within|by)\s+(\d+\s+(?:day|week|month|year)s?)'
    time_matches = re.findall(time_expression_pattern, text)
    for match in time_matches:
        timeline_indicators.append(f"timeframe: {match}")
    
    # Remove duplicates while preserving order
    unique_indicators = []
    for indicator in timeline_indicators:
        if indicator not in unique_indicators:
            unique_indicators.append(indicator)
    
    return '; '.join(unique_indicators[:3]) if unique_indicators else None  # Limit to top 3 most relevant


def _determine_active_phone_slot(prospect_record: Optional[Dict[str, Any]], used_phone: str) -> str:
    """Determine which phone slot (1 or 2) is active based on the phone used"""
    if not prospect_record:
        return "1"  # Default to slot 1
    
    fields = prospect_record.get("fields", {}) or {}
    phone1 = fields.get("Phone 1 (from Linked Owner)") or fields.get("Phone 1")
    phone2 = fields.get("Phone 2 (from Linked Owner)") or fields.get("Phone 2") 
    
    digits_used = _last10(used_phone)
    
    if phone2 and _last10(phone2) == digits_used:
        return "2"
    return "1"


def _assess_urgency_level(message: str, intent: str) -> int:
    """Assess urgency level from 1-5 based on message content and intent"""
    text = message.lower()
    urgency = 1
    
    # Intent-based urgency
    if intent.lower() == "positive":
        urgency += 1
    
    # Keyword-based urgency
    high_urgency_words = ['urgent', 'asap', 'quickly', 'soon', 'deadline', 'foreclosure', 'emergency']
    medium_urgency_words = ['need to sell', 'moving', 'relocating', 'divorce', 'financial']
    
    if any(word in text for word in high_urgency_words):
        urgency += 2
    elif any(word in text for word in medium_urgency_words):
        urgency += 1
    
    return min(urgency, 5)


def _calculate_lead_quality_score(
    intent: str, 
    ai_intent: str, 
    price: Optional[str], 
    condition: Optional[str], 
    timeline: Optional[str], 
    urgency: int
) -> int:
    """Calculate overall lead quality score from 1-100"""
    score = 20  # baseline
    
    # Intent scoring
    if intent.lower() == "positive":
        score += 25
    if ai_intent in ["interest_detected", "ask_price", "offer_discussion"]:
        score += 20
    
    # Data completeness scoring
    if price:
        score += 15
    if condition:
        score += 10
    if timeline:
        score += 10
    
    # Urgency scoring
    score += urgency * 4
    
    return min(score, 100)


# === COMPREHENSIVE PROSPECT UPDATES ===
def update_prospect_comprehensive(
    phone_number: str,
    body: str,
    intent: str,
    ai_intent: str,
    stage: str,
    direction: str = "IN",
    to_number: Optional[str] = None
) -> None:
    """Comprehensive prospect update with all required fields for inbound messages"""
    if not prospects or not phone_number:
        return
    
    try:
        # Find the prospect record
        prospect_record = _find_by_phone_last10(prospects, phone_number)
        if not prospect_record:
            print(f"⚠️ No prospect found for phone {phone_number}")
            return
        
        prospect_id = prospect_record["id"]
        prospect_fields = prospect_record.get("fields", {}) or {}
        now_iso = iso_timestamp()
        
        # Build comprehensive update payload
        update_payload = {}
        
        # Extract conversation data with enhanced analysis
        extracted_price = _extract_price_from_message(body)
        condition_info = _extract_condition_info(body)
        timeline_motivation = _extract_timeline_motivation(body)
        urgency_level = _assess_urgency_level(body, intent)
        
        # Seller Asking Price (if found in conversation)
        if extracted_price and intent.lower() == "positive":
            update_payload[PROSPECT_FIELD_MAP["SELLER_ASKING_PRICE"]] = extracted_price
        
        # Condition Notes (accumulate from conversations with enhanced analysis)
        if condition_info:
            existing_conditions = prospect_fields.get(PROSPECT_FIELD_MAP["CONDITION_NOTES"], "")
            if existing_conditions:
                # Avoid duplicating similar condition information
                if condition_info.lower() not in existing_conditions.lower():
                    update_payload[PROSPECT_FIELD_MAP["CONDITION_NOTES"]] = f"{existing_conditions}; {condition_info}"
            else:
                update_payload[PROSPECT_FIELD_MAP["CONDITION_NOTES"]] = condition_info
        
        # Timeline / Motivation (accumulate from conversations with priority)
        if timeline_motivation:
            existing_timeline = prospect_fields.get(PROSPECT_FIELD_MAP["TIMELINE_MOTIVATION"], "")
            if existing_timeline:
                # Prioritize more urgent or specific timeline information
                if urgency_level > _assess_urgency_level(existing_timeline, "neutral"):
                    update_payload[PROSPECT_FIELD_MAP["TIMELINE_MOTIVATION"]] = f"{timeline_motivation}; {existing_timeline}"
                elif timeline_motivation.lower() not in existing_timeline.lower():
                    update_payload[PROSPECT_FIELD_MAP["TIMELINE_MOTIVATION"]] = f"{existing_timeline}; {timeline_motivation}"
            else:
                update_payload[PROSPECT_FIELD_MAP["TIMELINE_MOTIVATION"]] = timeline_motivation
        
        # Activity timestamps with enhanced tracking
        if direction.upper() in ("IN", "INBOUND"):
            update_payload[PROSPECT_FIELD_MAP["LAST_INBOUND"]] = now_iso
            # Increment reply count
            current_replies = prospect_fields.get(PROSPECT_FIELD_MAP["REPLY_COUNT"], 0) or 0
            update_payload[PROSPECT_FIELD_MAP["REPLY_COUNT"]] = current_replies + 1
        else:
            update_payload[PROSPECT_FIELD_MAP["LAST_OUTBOUND"]] = now_iso
            # Increment send count
            current_sends = prospect_fields.get(PROSPECT_FIELD_MAP["SEND_COUNT"], 0) or 0
            update_payload[PROSPECT_FIELD_MAP["SEND_COUNT"]] = current_sends + 1
        
        update_payload[PROSPECT_FIELD_MAP["LAST_ACTIVITY"]] = now_iso
        
        # Ownership confirmation tracking with enhanced verification
        if intent.lower() == "positive" and stage.upper().startswith("STAGE 1"):
            update_payload[PROSPECT_FIELD_MAP["OWNERSHIP_CONFIRMED_DATE"]] = now_iso
            # Mark the active phone as verified
            active_slot = _determine_active_phone_slot(prospect_record, phone_number)
            if active_slot == "1":
                update_payload[PROSPECT_FIELD_MAP["PHONE_1_VERIFIED"]] = True
                update_payload["Phone 1 Verification Date"] = now_iso
            else:
                update_payload[PROSPECT_FIELD_MAP["PHONE_2_VERIFIED"]] = True
                update_payload["Phone 2 Verification Date"] = now_iso
            update_payload[PROSPECT_FIELD_MAP["ACTIVE_PHONE_SLOT"]] = active_slot
        
        # Intent tracking with confidence scoring
        update_payload[PROSPECT_FIELD_MAP["INTENT_LAST_DETECTED"]] = ai_intent
        
        # Direction tracking
        update_payload[PROSPECT_FIELD_MAP["LAST_DIRECTION"]] = direction
        
        # Phone slot tracking with enhanced verification
        active_slot = _determine_active_phone_slot(prospect_record, phone_number)
        update_payload[PROSPECT_FIELD_MAP["LAST_TRIED_SLOT"]] = active_slot
        
        # TextGrid phone number tracking
        if to_number:
            update_payload[PROSPECT_FIELD_MAP["TEXTGRID_PHONE"]] = to_number
        
        # Last message with conversation context
        update_payload[PROSPECT_FIELD_MAP["LAST_MESSAGE"]] = body[:500] if body else ""
        
        # Opt out tracking with reason
        if _is_opt_out(body):
            update_payload[PROSPECT_FIELD_MAP["OPT_OUT"]] = True
            update_payload["Opt Out Date"] = now_iso
        
        # Enhanced stage mapping (convert conversation stages to prospect stages)
        prospect_stage_map = {
            "STAGE 1 - OWNERSHIP CONFIRMATION": "Stage #1 – Ownership Check",
            "STAGE 2 - INTEREST FEELER": "Stage #2 – Offer Interest", 
            "STAGE 3 - PRICE QUALIFICATION": "Stage #3 – Price/Condition",
            "STAGE 4 - PROPERTY CONDITION": "Stage #3 – Price/Condition",
            "STAGE 5 - MOTIVATION / TIMELINE": "Stage #4 – Timeline/Motivation",
            "STAGE 6 - OFFER FOLLOW UP": "Stage #5 – Offer Follow-up",
            "STAGE 7 - CONTRACT READY": "Stage #6 – Contract Ready",
            "STAGE 8 - CONTRACT SENT": "Stage #7 – Contract Sent",
            "STAGE 9 - CONTRACT FOLLOW UP": "Stage #8 – Contract Follow-up",
            "OPT OUT": "Opt-Out"
        }
        if stage in prospect_stage_map:
            update_payload[PROSPECT_FIELD_MAP["STAGE"]] = prospect_stage_map[stage]
        
        # Enhanced status mapping based on intent, stage, and conversation quality
        status_map = {
            "positive": "Interested" if urgency_level >= 3 else "Replied",
            "neutral": "Replied",
            "delay": "Follow-up Required",
            "dnc": "Opt-Out"
        }
        
        if _is_opt_out(body):
            update_payload[PROSPECT_FIELD_MAP["STATUS"]] = "Opt-Out"
        elif intent.lower() in status_map:
            if intent.lower() == "positive":
                # More nuanced status based on conversation quality and stage
                if extracted_price or "offer" in body.lower():
                    update_payload[PROSPECT_FIELD_MAP["STATUS"]] = "Hot Lead"
                elif stage.upper().startswith("STAGE 1") and urgency_level >= 3:
                    update_payload[PROSPECT_FIELD_MAP["STATUS"]] = "Owner Verified"
                else:
                    update_payload[PROSPECT_FIELD_MAP["STATUS"]] = status_map[intent.lower()]
            else:
                update_payload[PROSPECT_FIELD_MAP["STATUS"]] = status_map[intent.lower()]
        elif direction.upper() in ("IN", "INBOUND"):
            update_payload[PROSPECT_FIELD_MAP["STATUS"]] = "Replied"
        
        # Lead quality scoring
        lead_quality_score = _calculate_lead_quality_score(
            intent, ai_intent, extracted_price, condition_info, timeline_motivation, urgency_level
        )
        update_payload["Lead Quality Score"] = lead_quality_score
        
        # Update conversation count and progression tracking
        total_conversations = prospect_fields.get("Total Conversations", 0) or 0
        update_payload["Total Conversations"] = total_conversations + 1
        
        # Track engagement quality
        engagement_score = 5  # baseline
        if len(body) > 100:
            engagement_score += 2
        elif len(body) > 50:
            engagement_score += 1
        if intent.lower() == "positive":
            engagement_score += 3
        if '?' in body:
            engagement_score += 1
        update_payload["Engagement Score"] = min(engagement_score, 10)
        
        # Enhanced conversation tracking
        conversation_summary = f"{ai_intent} intent detected"
        if extracted_price:
            conversation_summary += f" (mentioned price)"
        if condition_info:
            conversation_summary += f" (discussed condition)"
        
        existing_summary = prospect_fields.get("Conversation Summary", "")
        if existing_summary:
            update_payload["Conversation Summary"] = f"{existing_summary}\n{now_iso}: {conversation_summary}"
        else:
            update_payload["Conversation Summary"] = f"{now_iso}: {conversation_summary}"
        
        # Track stage progression history
        current_stage_field = prospect_fields.get(PROSPECT_FIELD_MAP["STAGE"])
        new_stage_field = update_payload.get(PROSPECT_FIELD_MAP["STAGE"])
        if current_stage_field != new_stage_field and new_stage_field:
            stage_history = prospect_fields.get("Stage History", "") or ""
            stage_entry = f"{now_iso}: {current_stage_field} → {new_stage_field}"
            if stage_history:
                update_payload["Stage History"] = f"{stage_history}\n{stage_entry}"
            else:
                update_payload["Stage History"] = stage_entry
        
        # Apply the update
        prospects.update(prospect_id, update_payload)
        print(f"✅ Updated prospect {prospect_id} with comprehensive data: {len(update_payload)} fields")
        
    except Exception as exc:
        print(f"⚠️ Failed to update prospect comprehensively for {phone_number}: {exc}")
        traceback.print_exc()


# === ACTIVITY UPDATES ===
def update_lead_activity(lead_id: str, body: str, direction: str, reply_increment: bool = False):
    if not leads or not lead_id:
        return
    try:
        lead = leads.get(lead_id)
        reply_count = lead["fields"].get("Reply Count", 0)
        send_count = lead["fields"].get("Send Count", 0) or 0
        
        updates = {
            "Last Activity": iso_timestamp(),
            "Last Direction": direction,
            "Last Message": (body or "")[:500],
        }
        
        if reply_increment:
            updates["Reply Count"] = reply_count + 1
            updates["Last Inbound"] = iso_timestamp()
            
            # Enhanced lead scoring based on reply quality
            lead_quality_score = 50  # baseline
            if len(body) > 100:
                lead_quality_score += 20
            elif len(body) > 50:
                lead_quality_score += 10
                
            # Check for positive engagement indicators
            positive_indicators = ["yes", "interested", "offer", "price", "when", "how much"]
            if any(indicator in body.lower() for indicator in positive_indicators):
                lead_quality_score += 30
                
            # Skip Lead Quality Score field - not in table schema
            # updates["Lead Quality Score"] = min(lead_quality_score, 100)
            
        if direction == "OUT":
            updates["Last Outbound"] = iso_timestamp()
            updates["Send Count"] = send_count + 1
            
        # Track conversation progression
        total_messages = (reply_count + send_count + (1 if reply_increment else 0))
        updates["Total Messages"] = total_messages
        
        # Update lead stage based on reply quality and engagement
        if reply_increment and any(word in body.lower() for word in ["yes", "interested", "offer"]):
            current_stage = lead["fields"].get("Lead Stage", "New")
            if current_stage == "New":
                updates["Lead Stage"] = "Engaged"
            elif current_stage == "Engaged" and "price" in body.lower():
                updates["Lead Stage"] = "Qualified"
                
        leads.update(lead_id, updates)
        print(f"✅ Updated lead {lead_id} activity comprehensively")
        
    except Exception as e:
        print(f"⚠️ Failed to update lead activity: {e}")


def log_conversation(payload: dict):
    print(f"🔍 Attempting to log conversation: {payload.get('Seller Phone Number', 'unknown phone')}")
    
    if not convos:
        print(f"⚠️ Conversations table not initialized. AIRTABLE_API_KEY: {'SET' if AIRTABLE_API_KEY else 'NOT SET'}, BASE_ID: {'SET' if BASE_ID else 'NOT SET'}")
        return
    
    # Filter payload to only include fields that exist in the table
    # Based on our complete schema check, these are the available fields:
    valid_fields = {
        # "AI Response Trigger",  # Skipping - computed field
        # "Conversation ID",  # Let Airtable auto-generate for now
        "Delivery Status", 
        "Direction",
        "Intent Detected",
        "Lead Record ID",
        "Message",
        # "Message Summary (AI)",  # Skipping - complex field format
        "Processed By",
        "Processed Time",
        "Prospect",
        "Prospect Record ID", 
        "Prospects copy",
        "Received Time",
        "Record ID",
        # "Response Time (Minutes)",  # Skipping - computed field
        "Seller Phone Number",
        "TextGrid ID",
        "TextGrid Phone Number"
    }
    
    # Create a filtered payload with only valid fields
    filtered_payload = {}
    for key, value in payload.items():
        if key in valid_fields:
            filtered_payload[key] = value
        else:
            print(f"⚠️ Skipping field '{key}' - not in Airtable schema")
    
    try:
        print(f"📝 Creating conversation record with {len(filtered_payload)} valid fields: {list(filtered_payload.keys())}")
        result = convos.create(filtered_payload)
        print(f"✅ Successfully logged conversation to Airtable: {result.get('id', 'unknown ID')}")
    except Exception as e:
        print(f"⚠️ Failed to log to Conversations: {e}")
        print(f"🔍 Filtered payload keys: {list(filtered_payload.keys())}")
        traceback.print_exc()


# === TESTABLE HANDLER (used by CI) ===
def handle_inbound(payload: dict):
    """Non-async inbound handler used by tests."""
    print(f"🔄 Processing inbound message from {payload.get('From', 'unknown')}: {payload.get('Body', '')[:50]}...")
    
    from_number = payload.get("From")
    to_number = payload.get("To")
    body = payload.get("Body")
    msg_id = payload.get("MessageSid") or payload.get("TextGridId")

    if not from_number or not body:
        print(f"❌ Missing required fields - From: {bool(from_number)}, Body: {bool(body)}")
        raise HTTPException(status_code=422, detail="Missing From or Body")

    if _is_opt_out(body):
        return process_optout(payload)

    # TEMPORARILY DISABLED: Skip idempotency check due to Redis hanging issues
    print("⚠️ EMERGENCY MODE: Skipping idempotency check")
    
    # TODO: Re-enable once Redis/Upstash hanging issue resolved:
    # Enhanced idempotency check
    # if msg_id and IDEM.seen(msg_id):
    #     return {"status": "duplicate", "msg_id": msg_id}

    overrides: Dict[str, str] = {}
    for key in ("Intent", "Intent Detected", "intent"):
        if payload.get(key):
            overrides["intent"] = str(payload[key])
            break
    for key in ("AI Intent", "AiIntent", "ai_intent"):
        if payload.get(key):
            overrides["ai_intent"] = str(payload[key])
            break
    for key in ("Stage", "stage"):
        if payload.get(key):
            overrides["stage"] = str(payload[key])
            break

    stage, intent, ai_intent = _classify_message(body, overrides)

    stage, intent, ai_intent = _classify_message(body, overrides)

    lead_id, property_id = _lookup_existing_lead(from_number)
    promoted = False
    if not lead_id and _should_promote(intent, ai_intent, stage):
        lead_id, property_id = promote_prospect_to_lead(from_number)
        promoted = bool(lead_id)
    elif lead_id:
        promoted = _should_promote(intent, ai_intent, stage)

    # Lookup prospect information for linking
    prospect_id, prospect_property_id = _lookup_prospect_info(from_number)
    if not property_id and prospect_property_id:
        property_id = prospect_property_id

    # Create comprehensive conversation record with all available fields
    now_timestamp = iso_timestamp()
    
    record = {
        # Core message data (always populated)
        FROM_FIELD: from_number,  # "Seller Phone Number"
        TO_FIELD: to_number,  # "TextGrid Phone Number" 
        MSG_FIELD: body,  # "Message"
        DIR_FIELD: "INBOUND",  # "Direction"
        TG_ID_FIELD: msg_id,  # "TextGrid ID"
        RECEIVED_AT: now_timestamp,  # "Received Time"
        
        # Processing metadata
        "Delivery Status": "DELIVERED",  # Mark as delivered since we received it
        "Processed Time": now_timestamp,
        "Processed By": "Campaign Runner",  # Use existing allowed value
        "Intent Detected": intent,
    }

    # Add linking fields if we have the data
    if lead_id:
        record["Lead Record ID"] = lead_id
    
    if prospect_id:
        record["Prospect Record ID"] = prospect_id
        record["Prospect"] = [prospect_id]  # Linked field format

    # Let Airtable auto-generate Conversation ID (computed field)

    print(f"📊 About to log conversation record: {record}")
    log_conversation(record)
    if lead_id:
        update_lead_activity(lead_id, body, "IN", reply_increment=True)

    # Comprehensive prospect update for ALL inbound messages
    update_prospect_comprehensive(
        phone_number=from_number,
        body=body,
        intent=intent,
        ai_intent=ai_intent,
        stage=stage,
        direction="IN",
        to_number=to_number
    )

    return {"status": "ok", "stage": stage, "intent": intent, "promoted": promoted}


# === TESTABLE OPTOUT HANDLER ===
def process_optout(payload: dict):
    """Handles STOP/unsubscribe messages for tests + webhook."""
    from_number = payload.get("From")
    raw_body = payload.get("Body")
    msg_id = payload.get("MessageSid") or payload.get("TextGridId")
    body = "" if raw_body is None else str(raw_body)

    if not from_number or not body:
        raise HTTPException(status_code=422, detail="Missing From or Body")

    if not _is_opt_out(body):
        return {"status": "ignored"}

    # TEMPORARILY DISABLED: Skip idempotency check due to Redis hanging issues
    print("⚠️ EMERGENCY MODE: Skipping idempotency check in optout")
    
    # TODO: Re-enable once Redis/Upstash hanging issue resolved:
    # Enhanced idempotency check
    # if msg_id and IDEM.seen(msg_id):
    #     return {"status": "duplicate", "msg_id": msg_id}

    print(f"🚫 [TEST] Opt-out from {from_number}")
    # TEMPORARILY DISABLED: Skip number pool updates due to potential Airtable hanging
    print("⚠️ EMERGENCY MODE: Skipping increment_opt_out due to potential Airtable delays")
    # TODO: Re-enable once stable:
    # increment_opt_out(from_number)

    lead_id, property_id = _lookup_existing_lead(from_number)
    prospect_id, prospect_property_id = _lookup_prospect_info(from_number)
    if not property_id and prospect_property_id:
        property_id = prospect_property_id

    # Create comprehensive opt-out conversation record
    now_timestamp = iso_timestamp()
    
    record = {
        # Core message data
        FROM_FIELD: from_number,  # "Seller Phone Number"
        MSG_FIELD: body,  # "Message"
        DIR_FIELD: "INBOUND",  # "Direction"
        TG_ID_FIELD: msg_id,  # "TextGrid ID"
        RECEIVED_AT: now_timestamp,  # "Received Time"
        
        # Processing metadata for opt-out
        "Delivery Status": "DELIVERED",
        "Processed Time": now_timestamp,
        "Processed By": "Campaign Runner",  # Use existing allowed value
        "Intent Detected": "DNC",
    }

    # Add linking fields if we have the data
    if lead_id:
        record["Lead Record ID"] = lead_id
    
    if prospect_id:
        record["Prospect Record ID"] = prospect_id
        record["Prospect"] = [prospect_id]  # Linked field format

    log_conversation(record)
    if lead_id:
        update_lead_activity(lead_id, body, "IN")

    # Comprehensive prospect update for opt-out
    update_prospect_comprehensive(
        phone_number=from_number,
        body=body,
        intent="DNC",
        ai_intent="not_interested",
        stage="OPT OUT",
        direction="IN",
        to_number=None
    )

    return {"status": "optout"}


# === TESTABLE STATUS HANDLER ===
def process_status(payload: dict):
    """Testable delivery status handler used by CI and webhook."""
    msg_id = payload.get("MessageSid")
    status = (payload.get("MessageStatus") or "").lower()
    to = payload.get("To")
    from_num = payload.get("From")

    print(f"📡 [TEST] Delivery receipt for {to} [{status}]")

    if not to or not from_num:
        raise HTTPException(status_code=422, detail="Missing To or From")

    if status == "delivered":
        increment_delivered(from_num)
    elif status in ("failed", "undelivered"):
        increment_failed(from_num)

    return {"ok": True, "status": status or "unknown"}


# === FASTAPI ROUTES ===
@router.post("/inbound")
async def inbound_handler(
    request: Request,
    x_webhook_token: Optional[str] = Header(None, convert_underscores=False),
    token: Optional[str] = Query(None),
):
    """Handle inbound SMS messages with authentication and flexible body parsing."""
    if not _is_authorized(x_webhook_token, token):
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    try:
        data = await _parse_body(request)
        return handle_inbound(data)
    except HTTPException:
        raise
    except Exception as e:
        print("❌ Inbound webhook error:")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/optout")
async def optout_handler(
    request: Request,
    x_webhook_token: Optional[str] = Header(None, convert_underscores=False),
    token: Optional[str] = Query(None),
):
    """Handle opt-out SMS messages with authentication and flexible body parsing."""
    if not _is_authorized(x_webhook_token, token):
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    try:
        data = await _parse_body(request)
        return process_optout(data)
    except HTTPException:
        raise
    except Exception as e:
        print("❌ Opt-out webhook error:")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/status")
async def status_handler(
    request: Request,
    x_webhook_token: Optional[str] = Header(None, convert_underscores=False),
    token: Optional[str] = Query(None),
):
    """Handle delivery status messages with authentication and flexible body parsing."""
    if not _is_authorized(x_webhook_token, token):
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    try:
        data = await _parse_body(request)
        return process_status(data)
    except HTTPException:
        raise
    except Exception as e:
        print("❌ Status webhook error:")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
