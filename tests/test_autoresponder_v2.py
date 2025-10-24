# tests/test_autoresponder_v2.py
import builtins
import types
import sms.autoresponder as ar


class FakeTable:
    def __init__(self, name, rows=None):
        self.name = name
        self._rows = rows or []
        self.created = []
        self.updated = []

    def all(self, view=None, max_records=None):
        return list(self._rows)

    def create(self, payload):
        rid = f"rec_{len(self.created) + 1}"
        self.created.append(payload)
        return {"id": rid, "fields": payload}

    def update(self, rec_id, payload):
        self.updated.append((rec_id, payload))
        return {"id": rec_id, "fields": payload}


def fake_convo(from_num, body):
    return {
        "id": f"conv_{from_num[-4:]}",
        "fields": {"Direction": "IN", "From": from_num, "To": "+18885551234", "Body": body},
    }


def make_env():
    """Attach fake tables so autoresponder logic runs without Airtable."""
    fake_conv = FakeTable(
        "Conversations",
        [
            fake_convo("+15551230001", "Who is this?"),
            fake_convo("+15551230002", "Yes I own it"),
            fake_convo("+15551230003", "Stop texting me"),
            fake_convo("+15551230004", "Depends on price"),
            fake_convo("+15551230005", "How did you get my number"),
            fake_convo("+15551230006", "maybe later next week"),
            fake_convo("+15551230007", "cash offer?"),
            fake_convo("+15551230008", "wrong number"),
        ],
    )
    fake_leads = FakeTable("Leads")
    fake_props = FakeTable("Prospects")
    fake_temps = FakeTable(
        "Templates",
        [
            {"id": "temp_yes", "fields": {"Internal ID": "followup_yes", "Message": "Awesome {First}, we’ll reach out!"}},
            {"id": "temp_intro", "fields": {"Internal ID": "intro", "Message": "Hey {First}, do you still own {Address}?"}},
            {"id": "temp_neutral", "fields": {"Internal ID": "neutral", "Message": "Got it, thanks {First}."}},
        ],
    )
    fake_drip = FakeTable("Drip Queue")

    # monkeypatch getters
    ar.conversations = lambda: fake_conv
    ar.leads_tbl = lambda: fake_leads
    ar.prospects_tbl = lambda: fake_props
    ar.templates_tbl = lambda: fake_temps
    ar.drip_tbl = lambda: fake_drip
    return fake_conv, fake_leads, fake_props, fake_temps, fake_drip


def test_run_autoresponder_basic(monkeypatch):
    fake_conv, fake_leads, fake_props, fake_temps, fake_drip = make_env()

    # monkeypatch send() to simulate instant delivery
    class DummyMP:
        @staticmethod
        def send(**kwargs):
            print(f"SMS → {kwargs['phone']}: {kwargs['body']}")
            return {"status": "sent"}

    ar.MessageProcessor = DummyMP

    result = ar.run_autoresponder(limit=10)
    print("\nResult:", result)

    # Ensure we processed something
    assert result["processed"] > 0
    # Validate that updates occurred
    assert any(u for _, u in fake_conv.updated)
    # Ensure at least one template got queued or sent
    assert fake_drip.created or result["breakdown"]

    # Pretty print summary
    print("\n=== Conversations Updated ===")
    for rec_id, payload in fake_conv.updated:
        print(rec_id, payload)
    print("\n=== Drip Created ===")
    for payload in fake_drip.created:
        print(payload)
