from textwrap import dedent
import pandas as pd, squirrels as sr


def main(sqrl: sr.DbviewModelArgs) -> pd.DataFrame:
    """
    Create a database view model in Python by sending an external query to a database or API, and return a 
    pandas dataframe of the result in this function. Since the result is loaded into server memory, be mindful of 
    the size of the results coming from the external query.
    """

    """ If working with sqlalchemy ORMs, use 'sqrl.connections' to get a connection pool / engine """
    # from typing import Union
    # from sqlalchemy import Engine, Pool
    # engine1: Union[Engine, Pool] = sqrl.connections[sqrl.connection_name] ## using the pre-assigned key
    # engine2: Union[Engine, Pool] = sqrl.connections["my_connection_name"] ## or use any defined key
    
    """ Example with building and running a sql query """
    category_clause = f'AND Category IN ({sqrl.ctx["categories"]})\n' if sqrl.ctx["has_categories"] else ''
    subcategory_clause = f'AND Subcategory IN ({sqrl.ctx["subcategories"]})\n' if sqrl.ctx["has_subcategories"] else ''
    query = dedent(f"""
        SELECT {sqrl.ctx["group_by_cols"]}
            , sum(-Amount) as Total_Amount
        FROM transactions
        WHERE 1=1
            {category_clause}{subcategory_clause}AND "Date" >= {sqrl.ctx["start_date"]}
            AND "Date" <= {sqrl.ctx["end_date"]}
            AND -Amount >= {sqrl.ctx["min_amount"]}
            AND -Amount <= {sqrl.ctx["max_amount"]}
        GROUP BY {sqrl.ctx["group_by_cols"]}
    """).strip()

    return sqrl.run_external_sql(query) 

    """ A 'connection_name' argument is available to use a different connection key """
    # return sqrl.run_external_sql(query, connection_name="different_key")
