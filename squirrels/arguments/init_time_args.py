from typing import Any, Iterable, Callable
from dataclasses import dataclass
import polars as pl

from .. import _utils as u

@dataclass
class ConnectionsArgs:
    project_path: str
    _proj_vars: dict[str, Any]
    _env_vars: dict[str, str]

    @property
    def proj_vars(self) -> dict[str, Any]:
        return self._proj_vars.copy()
    
    @property
    def env_vars(self) -> dict[str, str]:
        return self._env_vars.copy()


@dataclass
class ParametersArgs(ConnectionsArgs):
    pass


@dataclass
class _WithConnectionDictArgs(ConnectionsArgs):
    _connections: dict[str, Any]

    @property
    def connections(self) -> dict[str, Any]:
        """
        A dictionary of connection keys to SQLAlchemy Engines for database connections. 
        
        Can also be used to store other in-memory objects in advance such as ML models.
        """
        return self._connections.copy()


class BuildModelArgs(_WithConnectionDictArgs):

    def __init__(
        self, conn_args: ConnectionsArgs, _connections: dict[str, Any], 
        dependencies: Iterable[str], 
        ref: Callable[[str], pl.LazyFrame], 
        run_external_sql: Callable[[str, str], pl.DataFrame]
    ):
        super().__init__(conn_args.project_path, conn_args.proj_vars, conn_args.env_vars, _connections)
        self._dependencies = dependencies
        self._ref = ref
        self._run_external_sql = run_external_sql

    @property
    def dependencies(self) -> set[str]:
        """
        The set of dependent data model names
        """
        return set(self._dependencies)
    
    def ref(self, model: str) -> pl.LazyFrame:
        """
        Returns the result (as polars DataFrame) of a dependent model (predefined in "dependencies" function)

        Note: This is different behaviour than the "ref" function for SQL models, which figures out the dependent models for you, 
        and returns a string for the table/view name instead of a polars DataFrame.

        Arguments:
            model: The model name
        
        Returns:
            A polars DataFrame
        """
        return self._ref(model)

    def run_external_sql(self, connection_name: str, sql_query: str, **kwargs) -> pl.DataFrame:
        """
        Runs a SQL query against an external database, with option to specify the connection name. Placeholder values are provided automatically

        Arguments:
            sql_query: The SQL query. Can be parameterized with placeholders
            connection_name: The connection name for the database. If None, uses the one configured for the model
        
        Returns:
            The query result as a polars DataFrame
        """
        return self._run_external_sql(sql_query, connection_name)

    def run_sql_on_dataframes(self, sql_query: str, *, dataframes: dict[str, pl.LazyFrame] | None = None, **kwargs) -> pl.DataFrame:
        """
        Uses a dictionary of dataframes to execute a SQL query in an embedded in-memory database (sqlite or duckdb based on setting)

        Arguments:
            sql_query: The SQL query to run
            dataframes: A dictionary of table names to their polars LazyFrame. If None, uses results of dependent models
        
        Returns:
            The result as a polars LazyFrame from running the query
        """
        if dataframes is None:
            dataframes = {x: self.ref(x) for x in self._dependencies}

        return u.run_sql_on_dataframes(sql_query, dataframes)
