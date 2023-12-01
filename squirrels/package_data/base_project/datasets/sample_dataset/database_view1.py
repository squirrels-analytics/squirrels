from typing import Dict, Any, Optional, Union
from sqlalchemy import Engine, Pool
from textwrap import dedent
import pandas as pd, squirrels as sr


def main(
    connection_pool: Union[Engine, Pool], user: Optional[sr.UserBase], prms: Dict[str, sr.Parameter], 
    ctx: Dict[str, Any], args: Dict[str, Any], **kwargs
) -> pd.DataFrame:
    
    if len(ctx) > 0: ## leveraging context.py
        category_clause = f'AND Category IN ({ctx["categories"]})\n' if ctx["has_categories"] else ''
        subcategory_clause = f'AND Subcategory IN ({ctx["subcategories"]})\n' if ctx["has_subcategories"] else ''
        query = dedent(f"""
            SELECT {ctx["group_by_cols"]}
                , sum(-Amount) as Total_Amount
            FROM transactions
            WHERE 1=1
                {category_clause}{subcategory_clause}AND "Date" >= {ctx["start_date"]}
                AND "Date" <= {ctx["end_date"]}
                AND -Amount >= {ctx["min_amount"]}
                AND -Amount <= {ctx["max_amount"]}
            GROUP BY {ctx["group_by_cols"]}
        """).strip()
    
    else: ## vs. not leveraging context.py
        category_clause = f'AND Category IN ({ prms["category"].get_selected_labels_quoted_joined() })\n' \
            if prms["category"].has_non_empty_selection() else ''
        
        subcategory_clause = f'AND Subcategory IN ({ prms["subcategory"].get_selected_labels_quoted_joined() })\n' \
            if prms["subcategory"].has_non_empty_selection() else ''
        
        query = dedent(f"""
            SELECT { ",".join(prms["group_by"].get_selected("columns")) }
                , sum(-Amount) as Total_Amount
            FROM transactions
            WHERE 1=1
                {category_clause}{subcategory_clause}AND "Date" >= { prms["start_date"].get_selected_date_quoted() }
                AND "Date" <= { prms["end_date"].get_selected_date_quoted() }
                AND -Amount >= { prms["min_filter"].get_selected_value() }
                AND -Amount <= { prms["max_filter"].get_selected_value() }
            GROUP BY { ",".join(prms["group_by"].get_selected("columns")) }
        """).strip()
    
    ## Use 'get_connection_pool' to get a different connection pool
    # connection_pool = sr.get_connection_pool("default")

    return sr.run_sql_query(query, connection_pool)
