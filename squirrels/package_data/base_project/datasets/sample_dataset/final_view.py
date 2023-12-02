from typing import Dict, Any, Optional
import pandas as pd, squirrels as sr


def main(
    database_views: Dict[str, pd.DataFrame], user: Optional[sr.UserBase], prms: Dict[str, sr.Parameter], 
    ctx: Dict[str, Any], args: Dict[str, Any], **kwargs
) -> pd.DataFrame:
    
    if len(ctx) > 0: ## leveraging context.py
        group_by_cols: str = ctx["group_by_cols_list"]
    else: ## vs. not leveraging context.py
        group_by_cols: str = prms["group_by"].get_selected("columns")
    
    df = database_views['database_view1']
    return df.sort_values(group_by_cols, ascending=False)
