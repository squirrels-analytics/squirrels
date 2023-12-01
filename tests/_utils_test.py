from typing import List, Optional
import pytest, pandas as pd

from squirrels import _utils as u


@pytest.mark.parametrize('in_dimensions,out_dimensions', [
    (None, ["B"]),
    (["A", "B"], ["A", "B"])
])
def test_df_to_json(in_dimensions: Optional[List[str]], out_dimensions: List[str]):
    df = pd.DataFrame({'A': [1.0, 2.0], 'B': ['a', 'b'], 'C': [1, 2]})
    result = u.df_to_json0(df, in_dimensions)
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
    assert result == expected


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
