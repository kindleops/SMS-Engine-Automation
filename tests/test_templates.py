# tests/test_templates.py
import pytest
from sms import templates


@pytest.mark.parametrize("key", list(templates.TEMPLATES.keys()))
def test_templates_return_nonempty_string(key):
    """Every template key should return a non-empty string."""
    fields = {"First": "Jane", "Address": "456 Oak St"}
    msg = templates.get_template(key, fields)
    assert isinstance(msg, str)
    assert len(msg.strip()) > 0


def test_intro_contains_first_and_address():
    """Intro template should insert First and Address placeholders."""
    fields = {"First": "Alex", "Address": "789 Pine St"}
    msg = templates.get_template("intro", fields)
    assert "Alex" in msg
    assert "789 Pine St" in msg


def test_price_inquiry_mentions_price_terms():
    """Price inquiry templates should reference pricing in some way."""
    fields = {"First": "Sam", "Address": "101 Maple Ave"}
    msg = templates.get_template("price_inquiry", fields).lower()
    assert any(term in msg for term in ["price", "number", "ballpark", "happy"])


def test_unknown_key_defaults_to_intro():
    """Unknown template keys should fall back to intro."""
    fields = {"First": "Taylor", "Address": "202 Birch Rd"}
    msg = templates.get_template("not_a_real_key", fields)
    assert "Taylor" in msg or "202 Birch Rd" in msg


def test_stop_opt_out_only_in_intro():
    """STOP opt-out line should only appear in intro templates."""
    fields = {"First": "Jordan", "Address": "303 Elm St"}

    intro_msg = templates.get_template("intro", fields)
    assert "STOP" in intro_msg.upper()

    # all other templates must not include STOP
    for key in templates.TEMPLATES:
        if key != "intro":
            msg = templates.get_template(key, fields)
            assert "STOP" not in msg.upper()
