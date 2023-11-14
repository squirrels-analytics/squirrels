from typing import Dict, Any, Optional, Union
from sqlalchemy import Engine, Pool
import pandas as pd
import squirrels as sr


def main(
    connection_pool: Union[Engine, Pool], connection_set: sr.ConnectionSet, user: Optional[sr.UserBase], 
    prms: Dict[str, sr.Parameter], ctx: Dict[str, Any], args: Dict[str, Any], *p_args, **kwargs
) -> pd.DataFrame:
    
    query = f"""
        SELECT {ctx["group_by_cols"]}
            , sum(-Amount) as Total_Amount
        FROM transactions
        WHERE Category IN ({ctx["categories"]})
            AND Subcategory IN ({ctx["subcategories"]})
            AND "Date" >= {ctx["start_date"]}
            AND "Date" <= {ctx["end_date"]}
            AND -Amount >= {ctx["min_amount"]}
            AND -Amount <= {ctx["max_amount"]}
        GROUP BY {ctx["group_by_cols"]}
    """
    
    # # Use 'connection_set' to query from any connection key
    # connection_pool = connection_set.get_connection_pool("default")

    conn = connection_pool.raw_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    return df
