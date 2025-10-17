import os
import sys

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest

from sms.datastore import reset_state


@pytest.fixture(autouse=True)
def _reset_datastore():
    for key in [
        "AIRTABLE_API_KEY",
        "LEADS_CONVOS_BASE",
        "AIRTABLE_LEADS_CONVOS_BASE_ID",
        "CAMPAIGN_CONTROL_BASE",
        "AIRTABLE_CAMPAIGN_CONTROL_BASE_ID",
    ]:
        os.environ.pop(key, None)
    os.environ["SMS_FORCE_IN_MEMORY"] = "1"
    reset_state()
