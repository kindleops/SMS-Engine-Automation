# sms/ai/responder.py
from __future__ import annotations

import os
import re
import json
import time
from datetime import datetime
from typing import Any, Dict, Optional

# Optional dependency — never crash if missing
try:
    from openai import OpenAI  # OpenAI Python SDK v1.x
except Exception:
    OpenAI = None  # type: ignore


# ───────────────────────────────────────────────────────────
# Config (env override friendly)
# ───────────────────────────────────────────────────────────
OPENAI_API_KEY: Optional[str] = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL: str = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
OPENAI_TEMPERATURE: float = float(os.getenv("OPENAI_TEMPERATURE", "0.6"))
OPENAI_TIMEOUT: float = float(os.getenv("OPENAI_TIMEOUT", "12"))  # seconds
OPENAI_MAX_RETRIES: int = int(os.getenv("OPENAI_MAX_RETRIES", "2"))
OPENAI_MAX_TOKENS: int = int(os.getenv("OPENAI_MAX_TOKENS", "180"))

TEST_MODE: bool = os.getenv("AI_TEST_MODE", "false").lower() in ("1", "true", "yes")


# ───────────────────────────────────────────────────────────
# Helpers
# ───────────────────────────────────────────────────────────
def _log(msg: str) -> None:
    print(f"[AIResponder] {msg}")

def _safe_name(context: Dict[str, Any]) -> Optional[str]:
    for k in ("Owner Name", "Seller Name", "Contact Name", "Name", "name", "owner_name"):
        v = context.get(k)
        if isinstance(v, str) and v.strip():
            # return first word only for SMS vibe
            first = v.strip().split()[0]
            return re.sub(r"[^A-Za-z\-'.]", "", first) or None
    return None

def _shorten(s: str, limit: int) -> str:
    s = (s or "").strip()
    if len(s) <= limit:
        return s
    return s[:limit - 1].rstrip() + "…"

def _reduce_context(ctx: Dict[str, Any], max_chars: int = 1200) -> str:
    """
    Keep only useful, small fields; truncate aggressively to avoid huge prompts.
    """
    keep_keys = [
        "phone", "last_message", "Address", "Property Address", "City", "State", "Zip",
        "Market", "Owner Name", "Seller Name", "Beds", "Baths", "Sqft", "ARV", "Notes"
    ]
    slim: Dict[str, Any] = {}
    for k in keep_keys:
        if k in ctx and ctx[k] not in (None, ""):
            v = ctx[k]
            if isinstance(v, str):
                slim[k] = _shorten(v, 240)
            else:
                slim[k] = v
    j = json.dumps(slim, ensure_ascii=False)
    if len(j) > max_chars:
        j = j[:max_chars - 1] + "…"
    return j

def _fallback_reply(ctx: Dict[str, Any]) -> str:
    who = _safe_name(ctx)
    greet = f"Hey {who}," if who else "Hey there,"
    return _shorten(
        f"{greet} thanks for confirming! Do you have a ballpark price you’d be happy with if we cover closing costs and buy as-is?",
        240,
    )

def _postprocess_sms(text: str) -> str:
    """
    Normalize to SMS: no links, keep it short, friendly tone, single or two sentences max.
    """
    if not isinstance(text, str):
        text = str(text or "")
    # remove URLs (deliverability)
    text = re.sub(r"https?://\S+|www\.\S+", "", text)
    # collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    # enforce soft length limit
    text = _shorten(text, 240)
    # keep to max 2 sentences by splitting on punctuation
    parts = re.split(r"(?<=[.!?])\s+", text)
    text = " ".join(parts[:2]).strip()
    # polite end with question mark if no punctuation
    if not re.search(r"[.!?]$", text):
        if len(text) < 238 and not text.endswith("?"):
            text += "?"
    return text


# ───────────────────────────────────────────────────────────
# Client
# ───────────────────────────────────────────────────────────
def _client() -> Optional[Any]:
    if not OPENAI_API_KEY or not OpenAI:
        return None
    try:
        return OpenAI(api_key=OPENAI_API_KEY, timeout=OPENAI_TIMEOUT)
    except Exception as e:
        _log(f"Client init failed: {e}")
        return None


# ───────────────────────────────────────────────────────────
# Public API
# ───────────────────────────────────────────────────────────
class AIResponder:
    @staticmethod
    def reply(context: Dict[str, Any]) -> str:
        """
        Generate an SMS-length, natural reply for a seller who confirmed ownership & interest.
        Returns a single string. Never raises; falls back to a templated message on errors.
        """
        # Defensive defaults
        ctx = context or {}
        if TEST_MODE:
            return _fallback_reply(ctx)

        cli = _client()
        if cli is None:
            _log("OpenAI unavailable or missing API key; using fallback reply.")
            return _fallback_reply(ctx)

        # Prompt (system + user) — keep concise for latency/cost
        slim_ctx = _reduce_context(ctx)
        system_msg = (
            "You are a concise, friendly real-estate acquisitions SMS assistant. "
            "The seller has confirmed they own the property and is open to an offer. "
            "Write a SHORT, natural SMS (max ~240 chars), no links, no emojis, no formalities. "
            "Tone: warm, human, not salesy. End with ONE specific question."
        )
        user_msg = (
            f"Context (JSON): {slim_ctx}\n"
            "Goals:\n"
            "1) Thank them for confirming.\n"
            "2) Ask for their ballpark price or flexibility.\n"
            "3) Mention we buy as-is & cover closing costs (briefly).\n"
            "4) Keep to 1–2 short sentences max."
        )

        # Call with retries
        last_err: Optional[str] = None
        for attempt in range(1, OPENAI_MAX_RETRIES + 2):  # e.g., 1 original + retries
            try:
                resp = cli.chat.completions.create(
                    model=OPENAI_MODEL,
                    messages=[
                        {"role": "system", "content": system_msg},
                        {"role": "user", "content": user_msg},
                    ],
                    temperature=OPENAI_TEMPERATURE,
                    max_tokens=OPENAI_MAX_TOKENS,
                )
                content = (
                    (resp.choices[0].message.content if resp and resp.choices else None)
                    or ""
                ).strip()
                if not content:
                    raise RuntimeError("Empty completion content")
                return _postprocess_sms(content)
            except Exception as e:
                last_err = str(e)
                _log(f"Completion attempt {attempt} failed: {last_err}")
                if attempt <= OPENAI_MAX_RETRIES:
                    # Exponential backoff with mild jitter
                    time.sleep(1.2 ** attempt)
                else:
                    break

        # Fallback on final failure
        _log(f"Falling back after errors: {last_err}")
        return _fallback_reply(ctx)