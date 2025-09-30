# sms/templates.py
import random

# -------------------------------
# Outreach Templates
# -------------------------------

# 1️⃣ Ownership Outreach (include STOP for compliance)
ownership_templates = [
    "Hi {First}, this is Ryan with Everline. Quick check — are you the owner of {Address}? Reply STOP to opt out.",
    "Hey {First} — Ryan here with Everline. Can you confirm if you still own {Address}? Reply STOP to opt out.",
    "{First}, this is Ryan reaching out from Everline. Do you still own {Address}? If not, let me know. Reply STOP to opt out.",
    "Hi {First}, this is Ryan from Everline. Just wanted to confirm, do you still own {Address}? Reply STOP to opt out.",
]

# 2️⃣ Offer Interest (no STOP line → reduce friction)
followup_yes = [
    "Thanks! Are you open to a cash offer if the numbers make sense?",
    "Appreciate it — would you consider a cash offer if the price was right?",
]

# 3️⃣ Opt-out paths
followup_no = [
    "All good, thanks for confirming. If anything changes, text me here anytime.",
]

followup_wrong = [
    "Thanks for letting me know — I’ll remove this number from our list.",
]

# 4️⃣ Price Inquiry (sets up AI takeover)
price_inquiry = [
    "Got it — do you have a ballpark number in mind you’d want for the property?",
    "Thanks for considering an offer. Do you already have a price you’d be happy with?",
    "Understood. Is there a number you had in mind that would make sense for you?",
]

# -------------------------------
# Helpers
# -------------------------------

def _get_first_name(full_name: str | None) -> str:
    """Extract first name from full name string safely."""
    if not full_name:
        return "there"
    return full_name.strip().split(" ")[0]


def _format_safe(template: str, fields: dict) -> str:
    """Safely format a template with prospect/lead fields."""
    return template.format(
        First=_get_first_name(
            fields.get("Phone 1 Name (Primary)") or fields.get("First")
        ),
        Address=fields.get("Property Address")
        or fields.get("Address")
        or "your property",
    )

# -------------------------------
# Template Registry
# -------------------------------
TEMPLATES = {
    "intro": lambda fields: _format_safe(random.choice(ownership_templates), fields),
    "followup_yes": lambda fields: random.choice(followup_yes),
    "followup_no": lambda fields: random.choice(followup_no),
    "followup_wrong": lambda fields: random.choice(followup_wrong),
    "price_inquiry": lambda fields: random.choice(price_inquiry),
}

# -------------------------------
# Public API
# -------------------------------
def get_template(name: str, fields: dict | None = None) -> str:
    """
    Fetch a message body by template key.
    Falls back to 'intro' if unknown key is requested.

    Args:
        name (str): template key, e.g. "intro", "followup_yes", "price_inquiry".
        fields (dict): optional data for personalization.

    Returns:
        str: personalized message text.
    """
    fields = fields or {}
    generator = TEMPLATES.get(name, TEMPLATES["intro"])
    return generator(fields)


# -------------------------------
# Demo Runner (for testing locally)
# -------------------------------
if __name__ == "__main__":
    sample_fields = {
        "First": "John",
        "Address": "123 Main St",
    }

    print("🔹 Template Demo Runner 🔹")
    for key in TEMPLATES.keys():
        print(f"\n[{key.upper()}]")
        for _ in range(2):  # show a couple of random picks
            print("-", get_template(key, sample_fields))
