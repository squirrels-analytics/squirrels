from typing import List, Optional
import pytest, pandas as pd

from squirrels import _api_server as asv


@pytest.mark.parametrize('in_dimensions,out_dimensions', [
    (None, ["B"]),
    (["A", "B"], ["A", "B"])
])
def test_df_to_json(in_dimensions: Optional[List[str]], out_dimensions: List[str]):
    df = pd.DataFrame({'A': [1.0, 2.0], 'B': ['a', 'b'], 'C': [1, 2]})
    result = asv.df_to_json(df, in_dimensions)
    expected = {
        "response_version": 0,
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
