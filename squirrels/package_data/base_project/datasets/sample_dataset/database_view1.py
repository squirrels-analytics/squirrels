from typing import Dict, Union, Any
import sqlalchemy as sa
import pandas as pd

import squirrels as sq


def main(connection_pool: Union[sa.Engine, sa.Pool], connection_set: sq.ConnectionSet,
         prms: sq.ParameterSet, ctx: Dict[str, Any], proj: Dict[str, str], *args, **kwargs) -> pd.DataFrame:
    # conn = connection_pool.connect() # use this to create the corresponding DBAPI2 connection
    
    df = pd.DataFrame({
        'dim1': ['a', 'b', 'c', 'd', 'e', 'f'], 
        'metric1': [1, 2, 3, 4, 5, 6], 
        'metric2': [2, 4, 5, 1, 7, 3]
    })
    limit_parameter: sq.NumberParameter = prms['upper_bound']
    limit = limit_parameter.get_selected_value() 
    # limit: str = ctx['limit'] # use this instead if context.py is defined

    return df.query(f'metric1 <= {limit}')
