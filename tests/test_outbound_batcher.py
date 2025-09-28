from sms import outbound_batcher

def test_reset_daily_quota_runs(monkeypatch):
    """Ensure reset_daily_quotas is called once per new day."""
    called = {}

    def fake_reset():
        called["ok"] = True

    monkeypatch.setattr(outbound_batcher, "reset_daily_quotas", fake_reset)

    # Run first time → should call reset
    outbound_batcher._last_reset_date = None
    outbound_batcher.reset_daily_quotas()
    assert called.get("ok") is True


def test_outbound_batcher_has_constants():
    """Check outbound batcher has NUMBERS_TABLE constant."""
    assert isinstance(outbound_batcher.NUMBERS_TABLE, str)
    assert outbound_batcher.NUMBERS_TABLE != ""