from typing import Dict, Any
import pandas as pd
import squirrels as sq


def main(prms: sq.ParameterSet, ctx: Dict[str, Any], proj: Dict[str, str]) -> pd.DataFrame:
    df = pd.DataFrame({'dim1': ['a', 'b', 'c', 'd', 'e', 'f'], 'metric1': [1, 2, 3, 4, 5, 6], 'metric2': [2, 4, 5, 1, 7, 3]})
    limit_parameter: sq.NumberParameter = prms('number_example')
    limit: str = limit_parameter.get_selected_value() 
    # limit: str = ctx('limit') # use this instead if context.py is defined
    return df.query(f'metric1 <= {limit}')
