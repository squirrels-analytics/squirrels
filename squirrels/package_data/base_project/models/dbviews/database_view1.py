from textwrap import dedent
from squirrels import ModelArgs
import pandas as pd


def main(sqrl: ModelArgs) -> pd.DataFrame:
    """
    Create a database view model in Python by sending an external query to a database or API, and return a 
    pandas DataFrame of the result in this function. Since the result is loaded into server memory, be mindful of 
    the size of the results coming from the external query.
    """

    ## If working with sqlalchemy ORMs, use 'sqrl.connections' to get a sqlalchemy engine
    # from typing import Union
    # engine1 = sqrl.connections[sqrl.connection_name] ## using the pre-assigned key
    # engine2 = sqrl.connections["my_connection_name"] ## or use any defined key
    
    ## Example with building and running a sql query
    masked_id = "id" if getattr(sqrl.user, "role", "") == "manager" else "'***'"
    desc_cond = "description LIKE :desc_pattern" if "desc_pattern" in sqrl.placeholders else "true"
    category_cond = f"category IN ({sqrl.ctx['categories']})" if sqrl.ctx["has_categories"] else "true"
    subcategory_cond = f"subcategory IN ({sqrl.ctx['subcategories']})" if sqrl.ctx["has_subcategories"] else "true"
    query = dedent(f"""
        WITH
        transactions_with_masked_id AS (
            SELECT *, 
                {masked_id} as masked_id
            FROM transactions
        )
        SELECT {sqrl.ctx["select_dim_cols"]}
            , sum(-amount) as total_amount
        FROM transactions_with_masked_id
        WHERE date >= :start_date
            AND date <= :end_date
            AND -amount >= :min_amount
            AND -amount <= :max_amount
            AND {desc_cond}
            AND {category_cond}
            AND {subcategory_cond}
        GROUP BY {sqrl.ctx["group_by_cols"]}
    """).strip()

    return sqrl.run_external_sql(query) 

    ## A 'connection_name' argument is available to use a different connection key
    # return sqrl.run_external_sql(query, connection_name="different_key")
