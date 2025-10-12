# tests/test_template_compliance.py
import pytest
from sms import templates


@pytest.mark.parametrize("key", list(templates.TEMPLATES.keys()))
def test_template_sms_length(key):
    """Ensure each template stays within 160 chars (1 SMS segment)."""
    fields = {"First": "Christopher", "Address": "1234 Long Street Name"}
    msg = templates.get_template(key, fields)
    assert len(msg) <= 160, f"Template '{key}' too long: {len(msg)} chars"


@pytest.mark.parametrize("key", list(templates.TEMPLATES.keys()))
def test_template_no_line_breaks(key):
    """Ensure templates do not contain unexpected line breaks."""
    fields = {"First": "Chris", "Address": "789 Maple Ave"}
    msg = templates.get_template(key, fields)
    assert "\n" not in msg, f"Template '{key}' contains line breaks"
