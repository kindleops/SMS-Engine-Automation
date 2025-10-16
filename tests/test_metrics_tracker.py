from sms import metrics_tracker as mt


def test_is_optout_matches_variants():
    assert mt._is_optout("STOP")
    assert mt._is_optout("Please unsubscribe me")
    assert mt._is_optout("I'd like to opt out now")
    assert not mt._is_optout("Hello there")


def _rec(status: str | None = None, message: str | None = None):
    fields = {}
    if status is not None:
        fields[mt.CONV_STATUS_FIELD] = status
    if message is not None:
        fields[mt.CONV_MESSAGE_FIELD] = message
    return {"fields": fields}


def test_compute_convo_metrics_counts_correctly():
    sent = [
        _rec(status="DELIVERED"),
        _rec(status="FAILED"),
    ]
    inbound = [
        _rec(message="stop"),
        _rec(message="Sounds good"),
    ]

    metrics = mt._compute_convo_metrics(sent, inbound)

    assert metrics == {
        "sent": 2,
        "delivered": 1,
        "failed": 1,
        "responses": 2,
        "optouts": 1,
        "delivery_rate": 50.0,
        "optout_rate": 50.0,
    }


class DummyTable:
    def __init__(self, *, match_formula: str | None = None, existing_id: str | None = None):
        self.match_formula = match_formula
        self.existing_id = existing_id
        self.schema = {
            "Campaign": "",
            "Metric": "",
            "Value": 0.0,
            "Date": "",
            "Timestamp": "",
        }
        self.created = []
        self.updated = []
        self.update_calls = []

    def all(self, **kwargs):
        formula = kwargs.get("formula")
        if formula:
            if self.match_formula and formula == self.match_formula and self.existing_id:
                return [
                    {
                        "id": self.existing_id,
                        "fields": self.schema | {"Campaign": "Camp", "Metric": "TOTAL_SENT", "Date": "2024-01-01"},
                    }
                ]
            return []
        return [{"fields": self.schema}]

    def get(self, rec_id):
        if rec_id == self.existing_id:
            return {"id": rec_id, "fields": self.schema}
        return None

    def create(self, payload):
        self.created.append(payload)
        return {"id": "recNEW", "fields": payload}

    def update(self, rec_id, payload):
        self.update_calls.append((rec_id, payload))
        self.updated.append(payload)
        return {"id": rec_id, "fields": payload}


def test_upsert_metric_creates_when_missing():
    table = DummyTable()
    mt._upsert_metric(
        table,
        campaign="Camp",
        metric="TOTAL_SENT",
        value=5,
        day="2024-01-01",
        timestamp="2024-01-01T00:00:00Z",
    )

    assert table.created == [
        {
            "Campaign": "Camp",
            "Metric": "TOTAL_SENT",
            "Value": 5.0,
            "Date": "2024-01-01",
            "Timestamp": "2024-01-01T00:00:00Z",
        }
    ]
    assert table.update_calls == []


def test_upsert_metric_updates_when_existing():
    expected_formula = "AND("
    expected_formula += ", ".join(
        [
            mt._formula_equals("Campaign", "Camp"),
            mt._formula_equals("Metric", "TOTAL_SENT"),
            mt._formula_equals("Date", "2024-01-01"),
        ]
    )
    expected_formula += ")"

    table = DummyTable(match_formula=expected_formula, existing_id="rec123")

    mt._upsert_metric(
        table,
        campaign="Camp",
        metric="TOTAL_SENT",
        value=7,
        day="2024-01-01",
        timestamp="2024-01-01T10:00:00Z",
    )

    assert table.created == []
    assert table.update_calls == [
        (
            "rec123",
            {
                "Campaign": "Camp",
                "Metric": "TOTAL_SENT",
                "Value": 7.0,
                "Date": "2024-01-01",
                "Timestamp": "2024-01-01T10:00:00Z",
            },
        )
    ]
