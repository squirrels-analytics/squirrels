from typing import Union, Callable, Any
from dataclasses import dataclass
from sqlalchemy import Engine, Pool
import pandas as pd, sqlite3

from .init_time_args import ParametersArgs
from ..user_base import User
from ..parameters import Parameter
from .._connection_set import ConnectionSetIO


@dataclass
class ContextArgs(ParametersArgs):
    user: User
    prms: dict[str, Parameter]
    args: dict[str, Any]


@dataclass
class DbviewModelArgs(ContextArgs):
    ctx: dict[str, Any]
    connection_name: str
    connections: dict[str, Union[Engine, Pool]]

    def run_external_sql(self, sql: str, *, connection_name: str = None, **kwargs) -> pd.DataFrame:
        connection_name = self.connection_name if connection_name is None else connection_name
        return ConnectionSetIO.obj.run_sql_query_from_conn_name(sql, connection_name)


@dataclass
class FederateModelArgs(ContextArgs):
    ctx: dict[str, Any]
    ref: Callable[[str], pd.DataFrame]
    dependencies: set[str]

    def run_sql_on_dataframes(self, query: str, *, dataframes: dict[str, pd.DataFrame] = None, **kwargs) -> pd.DataFrame:
        """
        Uses a dictionary of dataframes to execute a SQL query in an in-memory sqlite database

        Parameters:
            query: The SQL query to run using sqlite
            dataframes: A dictionary of table names to their pandas Dataframe
        
        Returns:
            The result as a pandas Dataframe from running the query
        """
        if dataframes is None:
            dataframes = {x: self.ref(x) for x in self.dependencies}

        conn = sqlite3.connect(":memory:")
        try:
            for name, df in dataframes.items():
                df.to_sql(name, conn, index=False)
            return pd.read_sql(query, conn)
        finally:
            conn.close()
