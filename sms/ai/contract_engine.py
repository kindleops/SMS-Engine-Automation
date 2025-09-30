import os
import requests

DOCUSIGN_API = os.getenv("DOCUSIGN_API")
DOCUSIGN_TOKEN = os.getenv("DOCUSIGN_TOKEN")

def send_contract(seller_name, seller_email, address, offer_price):
    """
    Creates and sends an e-sign contract automatically.
    """
    payload = {
        "templateId": os.getenv("PURCHASE_AGREEMENT_TEMPLATE"),
        "emailSubject": f"Cash Offer for {address}",
        "recipient": {
            "name": seller_name,
            "email": seller_email
        },
        "mergeFields": {
            "PropertyAddress": address,
            "OfferPrice": f"${offer_price:,.0f}"
        }
    }
    headers = {"Authorization": f"Bearer {DOCUSIGN_TOKEN}"}
    r = requests.post(f"{DOCUSIGN_API}/envelopes", json=payload, headers=headers)
    r.raise_for_status()
    return r.json()