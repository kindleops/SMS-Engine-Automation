import random

# --- Ownership Outreach Templates (include STOP line) ---
ownership_templates = [
    "Hi {First}, this is Ryan with Everline. Quick check — are you the owner of {Address}? Reply STOP to opt out.",
    "Hey {First} — Ryan here with Everline. Can you confirm if you still own {Address}? Reply STOP to opt out.",
    "{First}, this is Ryan reaching out from Everline. Do you still own {Address}? If not, let me know. Reply STOP to opt out.",
    "Hi {First}, this is Ryan from Everline. Just wanted to confirm, do you still own {Address}? Reply STOP to opt out.",
]

# --- Follow-Up Templates (clean, no STOP line) ---
followup_yes = [
    "Thanks! Are you open to a cash offer if the numbers make sense?",
    "Appreciate it — would you consider a cash offer if the price was right?",
]

followup_no = [
    "All good, thanks for confirming. If anything changes, text me here anytime.",
]

followup_wrong = [
    "Thanks for letting me know — I’ll remove this number from our list.",
]

# --- Helper: Extract first name from full string ---
def _get_first_name(full_name: str) -> str:
    if not full_name:
        return "there"
    return full_name.strip().split(" ")[0]  # take first word only

# --- Helper: Safe Formatter ---
def _format_safe(template: str, fields: dict) -> str:
    return template.format(
        First=_get_first_name(fields.get("Phone 1 Name (Primary)") or fields.get("First")),
        Address=fields.get("Property Address") or fields.get("Address", "your property")
    )

# --- Registry ---
TEMPLATES = {
    "intro": lambda fields: _format_safe(random.choice(ownership_templates), fields),
    "followup_yes": lambda fields: random.choice(followup_yes),
    "followup_no": lambda fields: random.choice(followup_no),
    "followup_wrong": lambda fields: random.choice(followup_wrong),
}

# --- Exported API ---
def get_template(name: str, fields: dict | None = None) -> str:
    """
    Fetch a message body by template key.
    Falls back to 'intro' if unknown key is requested.
    """
    fields = fields or {}
    generator = TEMPLATES.get(name, TEMPLATES["intro"])
    return generator(fields)