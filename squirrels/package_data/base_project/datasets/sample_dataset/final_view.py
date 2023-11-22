from typing import Dict, Any, Optional
import pandas as pd
import squirrels as sr


def main(
    database_views: Dict[str, pd.DataFrame], user: Optional[sr.UserBase], prms: Dict[str, sr.Parameter], 
    ctx: Dict[str, Any], args: Dict[str, Any], *p_args, **kwargs
) -> pd.DataFrame:
    
    group_by_cols: str = ctx["group_by_cols"]
    df = database_views['database_view1']
    dim_cols = [x.strip() for x in group_by_cols.split(",")]
    return df.sort_values(dim_cols)
