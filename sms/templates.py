import random

# --- Ownership Outreach Templates ---
ownership_templates = [
    "Hi {First}, this is Ryan with Everline. Quick check — are you the owner of {Address}? Reply STOP to opt out.",
    "Hey {First} — Ryan here with Everline. Can you confirm if you still own {Address}? Reply STOP to opt out.",
    "{First}, this is Ryan reaching out from Everline. Do you still own {Address}? If not, let me know. Reply STOP to opt out.",
    "Hi {First}, this is Ryan from Everline. Just wanted to confirm, do you still own {Address}? Reply STOP to opt out.",
]

# --- Follow-Up Templates ---
followup_yes = [
    "Thanks! Are you open to a cash offer if the numbers make sense? Reply STOP to opt out.",
    "Appreciate it — would you consider a cash offer if the price was right? Reply STOP to opt out.",
]

followup_no = [
    "All good, thanks for confirming. If anything changes, text me here anytime. Reply STOP to opt out.",
]

followup_wrong = [
    "Thanks for letting me know — I’ll remove this number from our list. Reply STOP to opt out.",
]

# --- Helper: Safe Formatter ---
def _format_safe(template: str, fields: dict) -> str:
    """Format with fallbacks if First/Address missing."""
    return template.format(
        First=fields.get("First", "there"),
        Address=fields.get("Address", "your property")
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