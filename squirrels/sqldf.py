from typing import Dict
import sqlite3, pandas as pd


def sqldf(query: str, df_by_db_views: Dict[str, pd.DataFrame], **kwargs) -> pd.DataFrame:
    """
    Uses a dictionary of dataframes to execute a SQL query in an in-memory sqlite database

    Parameters:
        query: The SQL query to run using sqlite
        df_by_db_views: A dictionary of table names to their pandas Dataframe
    
    Returns:
        The result as a pandas Dataframe from running the query
    """
    conn = sqlite3.connect(":memory:")
    try:
        for db_view, df in df_by_db_views.items():
            df.to_sql(db_view, conn, index=False)
        return pd.read_sql(query, conn)
    finally:
        conn.close()
