from typing import Callable, Any
from dataclasses import dataclass
from sqlalchemy import Engine
import pandas as pd, sqlite3

from .init_time_args import ConnectionsArgs, ParametersArgs
from ..user_base import User
from ..parameters import Parameter
from .._connection_set import ConnectionSetIO
from .. import _utils as u


@dataclass
class AuthArgs(ConnectionsArgs):
    connections: dict[str, Engine]
    username: str
    password: str


@dataclass
class ContextArgs(ParametersArgs):
    user: User
    prms: dict[str, Parameter]
    traits: dict[str, Any]


@dataclass
class ModelDepsArgs(ContextArgs):
    ctx: dict[str, Any]


@dataclass
class ModelArgs(ModelDepsArgs):
    connection_name: str
    connections: dict[str, Engine]
    _ref: Callable[[str], pd.DataFrame]
    dependencies: set[str]

    def __post_init__(self):
        self.ref = self._ref
    
    def ref(self, model: str) -> pd.DataFrame:
        """
        Returns the result (as pandas DataFrame) of a dependent model (predefined in "dependencies" function)

        Note: This is different behaviour than the "ref" function for SQL models, which figures out the dependent models for you, 
        and returns a string for the table/view name in SQLite instead of a pandas DataFrame.

        Parameters:
            model: The model name
        
        Returns:
            A pandas DataFrame
        """

    def run_external_sql(self, sql: str, *, connection_name: str = None, **kwargs) -> pd.DataFrame:
        """
        Runs a SQL query against an external database, with option to specify the connection name

        Parameters:
            sql: The SQL query
            connection_name: The connection name for the database. If None, uses the one configured for the model
        
        Returns:
            The query result as a pandas DataFrame
        """
        connection_name = self.connection_name if connection_name is None else connection_name
        return ConnectionSetIO.obj.run_sql_query_from_conn_name(sql, connection_name)

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

        use_duckdb = u.use_duckdb()
        if use_duckdb:
            import duckdb
            conn = duckdb.connect()
        else:
            conn = sqlite3.connect(":memory:")
        
        try:
            for name, df in dataframes.items():
                if use_duckdb:
                    conn.execute(f"CREATE TABLE {name} AS FROM df")
                else:
                    df.to_sql(name, conn, index=False)
            
            return conn.execute(query).df() if use_duckdb else pd.read_sql(query, conn)
        finally:
            conn.close()
