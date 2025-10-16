import sms.campaign_runner as cr


class DummyTable:
    def __init__(self, name: str):
        self.name = name

    def all(self, *_, **__):
        return []


def test_get_campaigns_table_recovers_after_cache_reset(monkeypatch):
    monkeypatch.setattr(cr, "AIRTABLE_KEY", "key")
    monkeypatch.setattr(cr, "LEADS_CONVOS_BASE", "leads")
    monkeypatch.setattr(cr, "CAMPAIGN_CONTROL_BASE", "ctrl")
    cr.reset_table_caches()

    called = []

    def fake_make(api_key, base_id, table_name):
        if not (api_key and base_id and table_name):
            return None
        called.append((api_key, base_id, table_name))
        return DummyTable(f"{base_id}:{table_name}")

    monkeypatch.setattr(cr, "_make_table", fake_make)
    monkeypatch.setattr(cr, "_probe_table", lambda base, *_: False)
    monkeypatch.setattr(cr, "_choose_campaigns_base", lambda: None)

    assert cr.get_campaigns_table() is None
    assert called == []

    monkeypatch.setattr(cr, "_probe_table", lambda base, *_: base == "ctrl")
    monkeypatch.setattr(cr, "_choose_campaigns_base", lambda: "ctrl")

    tbl = cr.get_campaigns_table()
    assert isinstance(tbl, DummyTable)
    assert tbl.name == "ctrl:Campaigns"
    assert called == [("key", "ctrl", "Campaigns")]

    called.clear()
    monkeypatch.setattr(cr, "_choose_campaigns_base", lambda: "leads")
    cr.reset_table_caches()

    tbl2 = cr.get_campaigns_table()
    assert isinstance(tbl2, DummyTable)
    assert tbl2.name == "leads:Campaigns"
    assert called == [("key", "leads", "Campaigns")]

    tbl3 = cr.get_campaigns_table()
    assert tbl3 is tbl2
    assert called == [("key", "leads", "Campaigns")]
