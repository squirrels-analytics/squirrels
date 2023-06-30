from typing import Dict, Any
import pandas as pd
import squirrels as sr


def main(database_views: Dict[str, pd.DataFrame], 
         prms: Dict[str, sr.Parameter], ctx: Dict[str, Any], args: Dict[str, Any], 
         *p_args, **kwargs) -> pd.DataFrame:
    df = database_views['database_view1']
    dim_cols = [x.strip() for x in ctx["group_by_cols"].split(",")]
    return df.sort_values(dim_cols)
