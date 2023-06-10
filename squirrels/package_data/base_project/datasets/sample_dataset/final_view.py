from typing import Dict, Any
import pandas as pd
import squirrels as sr


def main(database_views: Dict[str, pd.DataFrame], 
         prms: Dict[str, sr.Parameter], ctx: Dict[str, Any], proj: Dict[str, Any], 
         *p_args, **kwargs) -> pd.DataFrame:
    df = database_views['database_view1']
    return df
