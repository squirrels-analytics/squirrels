from typing import Dict, Any
import pandas as pd

import squirrels as sr


def main(connection_set: sr.ConnectionSet, 
         prms: Dict[str, sr.Parameter], ctx: Dict[str, Any], args: Dict[str, Any], 
         *p_args, **kwargs) -> pd.DataFrame:
    # pool = connection_set.get_connection_pool("default")
    # conn = pool.connect() # use this to get a DBAPI connection from a Pool or sqlalchemy connection from an Engine
    # conn = pool.raw_connection() # use this to get a DBAPI connection from an Engine
    
    df = pd.DataFrame({
        'dim1': ['a', 'b', 'c', 'd', 'e', 'f'], 
        'metric1': [1, 2, 3, 4, 5, 6], 
        'metric2': [2, 4, 5, 1, 7, 3]
    })
    limit_parameter: sr.NumberParameter = prms['upper_bound']
    limit = limit_parameter.get_selected_value() 
    # limit: str = ctx['limit'] # use this instead if context.py is defined

    return df.query(f'metric1 <= {limit}')
