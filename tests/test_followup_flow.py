import pytest

from sms import followup_flow


class StubTable:
    def __init__(self, rows=None):
        self._rows = rows or []
        self.created = []
        self.updated = []
        self.get_calls = []
        self.all_calls = []

    def all(self, **kwargs):
        self.all_calls.append(kwargs)
        return list(self._rows)

    def get(self, rec_id):
        self.get_calls.append(rec_id)
        for row in self._rows:
            if row.get("id") == rec_id:
                return row
        return {}

    def create(self, payload):
        self.created.append(payload)
        return payload

    def update(self, rec_id, payload):
        self.updated.append((rec_id, payload))
        return payload


@pytest.fixture(autouse=True)
def clear_template_cache():
    followup_flow._TEMPLATE_CACHE.clear()
    yield
    followup_flow._TEMPLATE_CACHE.clear()


def test_schedule_personalizes_and_normalizes_phone(monkeypatch):
    drip = StubTable()
    lead_fields = {
        "Owner Name": "Alex Agent",
        "Property Address": "123 Lane",
        "Phone": "(555) 111-2222",
        "drip_stage": "NURTURE_30",
        "Next Followup Date": "2023-01-01",
        "Last Followup": "2023-01-01T00:00:00",
    }
    leads = StubTable([
        {"id": "lead123", "fields": lead_fields},
    ])
    templates = StubTable([
        {"id": "tmpl1", "fields": {"Internal ID": "followup_30", "Message": "Hi {First} re {Address}"}},
    ])

    def fake_table(name):
        mapping = {
            followup_flow.DRIP_TABLE_NAME: drip,
            followup_flow.LEADS_TABLE_NAME: leads,
            followup_flow.TEMPLATES_TABLE_NAME: templates,
        }
        return mapping.get(name)

    monkeypatch.setattr(followup_flow, "_table", fake_table)

    result = followup_flow.schedule_from_response(
        phone="555-222-3333",
        intent="neutral",
        lead_id="lead123",
        market="Austin",
        property_id="prop-1",
    )

    assert result["ok"]
    assert drip.created, "Drip queue should receive a new row"
    payload = drip.created[0]
    assert payload["phone"] == "5552223333"
    assert "Alex" in payload["message_preview"]
    assert "123 Lane" in payload["message_preview"]
    assert leads.updated, "Lead record should be patched"


def test_run_followups_reuses_snapshot(monkeypatch):
    today = followup_flow.utcnow().date().isoformat()
    leads = StubTable([
        {
            "id": "lead-one",
            "fields": {
                "Next Followup Date": today,
                "Phone": "555-111-2222",
            },
        },
        {
            "id": "lead-two",
            "fields": {
                "Next Followup Date": today,
                "Phone": "555-333-4444",
            },
        },
    ])
    drip = StubTable([
        {
            "id": "existing",
            "fields": {
                "phone": "5551112222",
                "status": "QUEUED",
                "next_send_date": f"{today}T09:00:00",
            },
        }
    ])

    def fake_table(name):
        mapping = {
            followup_flow.DRIP_TABLE_NAME: drip,
            followup_flow.LEADS_TABLE_NAME: leads,
        }
        return mapping.get(name)

    monkeypatch.setattr(followup_flow, "_table", fake_table)
    created_payloads = []
    monkeypatch.setattr(followup_flow, "_safe_create", lambda tbl, payload: created_payloads.append(payload))

    seen_rows = []
    original = followup_flow._already_queued_today

    def fake_already(drip_tbl, phone, rows=None):
        seen_rows.append(rows)
        return original(drip_tbl, phone, rows)

    monkeypatch.setattr(followup_flow, "_already_queued_today", fake_already)

    result = followup_flow.run_followups()

    assert result["queued_from_leads"] == 1
    assert len(created_payloads) == 1
    assert all(rows is not None for rows in seen_rows)

