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
