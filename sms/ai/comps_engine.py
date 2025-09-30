from sms.ai import dealmachine_client, zillow_client

def run_comps(address, city, state, zip_code):
    results = {}

    # Step 1: Try DealMachine
    try:
        dm_comps = dealmachine_client.get_comps(address, city, state, zip_code)
        results["dealmachine"] = dm_comps
    except Exception as e:
        results["dealmachine_error"] = str(e)
        dm_comps = []

    # Step 2: Try Zillow (fallback)
    try:
        zillow_data = zillow_client.get_zestimate(address, city, state, zip_code)
        results["zillow"] = zillow_data
    except Exception as e:
        results["zillow_error"] = str(e)
        zillow_data = {}

    # Step 3: AI reconciliation (basic example now)
    arv = None
    if dm_comps:
        avg_dm = sum(c["sold_price"] for c in dm_comps if c.get("sold_price")) / len(dm_comps)
        arv = avg_dm
    if zillow_data and zillow_data.get("zestimate"):
        if arv:
            # If both sources exist, average them
            arv = (arv + zillow_data["zestimate"]) / 2
        else:
            arv = zillow_data["zestimate"]

    results["arv"] = arv
    return results