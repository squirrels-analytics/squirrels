import pytest, pandas as pd

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


@pytest.mark.parametrize("do_use_duckdb", [True, False])
def test_run_sql_on_dataframes(do_use_duckdb: bool):
    df_dict = { "input_df": pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}) }
    expected = pd.DataFrame({"total": [5, 7, 9]})
    result = u.run_sql_on_dataframes("SELECT a+b AS total FROM input_df", df_dict, do_use_duckdb=do_use_duckdb)
    assert result.equals(expected)
