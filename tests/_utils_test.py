import pytest, polars as pl

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
    assert u.load_json_or_comma_delimited_str_as_list(input_str) == expected


def test_run_sql_on_dataframes():
    df_dict = { "input_df": pl.LazyFrame({"a": [1, 2, 3], "b": [4, 5, 6]}) }
    expected = pl.DataFrame({"total": [5, 7, 9]})
    result = u.run_sql_on_dataframes("SELECT a+b AS total FROM input_df", df_dict)
    assert result.equals(expected)


def test_to_bool_truthy_values():
    truthy_inputs = [True, "1", "true", "TrUe", "t", "yes", "YeS", "y", "on", "  on  ", 1]
    for val in truthy_inputs:
        assert u.to_bool(val) is True


def test_to_bool_falsey_values():
    falsey_inputs = [False, None, 0, "0", "false", "FaLsE", "f", "no", "n", "off", "", " random "]
    for val in falsey_inputs:
        assert u.to_bool(val) is False


def test_user_has_elevated_privileges_matrix():
    cases = [
        ("admin", "admin", True),
        ("admin", "member", True),
        ("admin", "guest", True),
        ("member", "admin", False),
        ("member", "member", True),
        ("member", "guest", True),
        ("guest", "admin", False),
        ("guest", "member", False),
        ("guest", "guest", True),
        ("AdMiN", "MeMbEr", True),  # case-insensitive input
    ]
    for user_level, required_level, expected in cases:
        assert u.user_has_elevated_privileges(user_level, required_level) is expected
