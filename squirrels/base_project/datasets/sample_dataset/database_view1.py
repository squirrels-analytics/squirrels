import pandas as pd
import squirrels as sq
from typing import Dict, Callable, Any


def main(prms: Callable[[str], sq.Parameter], ctx: Callable[[str], Any], proj: Callable[[str], str]) -> pd.DataFrame:
    df = pd.DataFrame({'dim1': ['a', 'b', 'c', 'd', 'e', 'f'], 'metric1': [1, 2, 3, 4, 5, 6], 'metric2': [2, 4, 5, 1, 7, 3]})
    limit_parameter: sq.NumberParameter = prms('limit')
    limit: str = limit_parameter.get_selected_value()
    return df.query(f'metric1 <= {limit}')
