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
    df_dict = { "input_df": pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}) }
    expected = pl.DataFrame({"total": [5, 7, 9]})
    result = u.run_sql_on_dataframes("SELECT a+b AS total FROM input_df", df_dict)
    assert result.equals(expected)


@pytest.mark.parametrize('in_dimensions,out_dimensions', [
    (None, ["B"]),
    (["A", "B"], ["A", "B"])
])
def test_df_to_json(in_dimensions: list[str] | None, out_dimensions: list[str]):
    df = pl.DataFrame({'A': [1.0, 2.0], 'B': ['a', 'b'], 'C': [1, 2]})
    actual = u.df_to_json0(df, in_dimensions)
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
