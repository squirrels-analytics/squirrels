import pytest

from squirrels import _utils as u


@pytest.mark.parametrize('input_str,expected', [
    ("", []),
    ("[]", []),
    ("1", ["1"]),
    ('["1"]', ["1"]),
    ("1,2,3", ["1", "2", "3"]),
    ('["1", "2", "3"]', ["1", "2", "3"])
])
def test_load_json_or_comma_delimited_str(input_str, expected):
    assert u.load_json_or_comma_delimited_str(input_str) == expected
