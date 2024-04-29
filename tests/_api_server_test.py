from typing import Optional
import pytest, pandas as pd

from squirrels import _api_server


@pytest.mark.parametrize('in_dimensions,out_dimensions', [
    (None, ["B"]),
    (["A", "B"], ["A", "B"])
])
def test_df_to_json(in_dimensions: Optional[list[str]], out_dimensions: list[str]):
    df = pd.DataFrame({'A': [1.0, 2.0], 'B': ['a', 'b'], 'C': [1, 2]})
    result = _api_server.df_to_api_response0(df, in_dimensions)
    expected = {
        "data_schema": {
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
    actual = result.model_dump()
    assert actual == expected
