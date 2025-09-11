import random

# Multiple options → rotates for natural variation
ownership_templates = [
    "Hi {First}, this is Ryan with Everline. Quick check—are you the owner of {Address}? Reply STOP to opt out.",
    "Hey {First}—Ryan here with Everline. Can you confirm if you still own {Address}? Reply STOP to opt out.",
    "{First}, this is Ryan reaching out from Everline. Do you still own {Address}? If not, let me know. Reply STOP to opt out.",
    "Hi {First}, this is Ryan from Everline. Just wanted to confirm, do you still own {Address}? Reply STOP to opt out.",
]

# Follow-up responses
followup_yes = [
    "Thanks! Are you open to a cash offer if the numbers make sense?",
    "Appreciate it — would you consider a cash offer if the price was right?",
]
followup_no = [
    "All good, thanks for confirming. If anything changes, text me here anytime.",
]
followup_wrong = [
    "Thanks for letting me know—I’ll remove this number from our list.",
]

# Central template registry
TEMPLATES = {
    "intro": lambda fields: random.choice(ownership_templates).format(**fields),
    "followup_yes": lambda fields: random.choice(followup_yes),
    "followup_no": lambda fields: random.choice(followup_no),
    "followup_wrong": lambda fields: random.choice(followup_wrong),
}