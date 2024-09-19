import pytest, pandas as pd

from squirrels import _utils as _u


@pytest.mark.parametrize('input_str,expected', [
    ("", []),
    ("[]", []),
    ("1", ["1"]),
    ('["1"]', ["1"]),
    ("1,2,3", ["1", "2", "3"]),
    ('["1", "2", "3"]', ["1", "2", "3"])
])
def test_load_json_or_comma_delimited_str(input_str, expected):
    assert _u.load_json_or_comma_delimited_str_as_list(input_str) == expected


@pytest.mark.parametrize("do_use_duckdb", [True, False])
def test_run_sql_on_dataframes(do_use_duckdb: bool):
    df_dict = { "input_df": pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}) }
    expected = pd.DataFrame({"total": [5, 7, 9]})
    result = _u.run_sql_on_dataframes("SELECT a+b AS total FROM input_df", df_dict, do_use_duckdb)
    assert result.equals(expected)


@pytest.mark.parametrize('in_dimensions,out_dimensions', [
    (None, ["B"]),
    (["A", "B"], ["A", "B"])
])
def test_df_to_json(in_dimensions: list[str] | None, out_dimensions: list[str]):
    df = pd.DataFrame({'A': [1.0, 2.0], 'B': ['a', 'b'], 'C': [1, 2]})
    actual = _u.df_to_json0(df, in_dimensions)
    expected = {
        "schema": {
            "fields": [
                {"name": "A", "type": "number"},
                {"name": "B", "type": "string"},
                {"name": "C", "type": "integer"}
            ],
            "dimensions": out_dimensions
        },
        "data": [
            {"A": 1.0, "B": "a", "C": 1},
            {"A": 2.0, "B": "b", "C": 2}
        ]
    }
    assert actual == expected
