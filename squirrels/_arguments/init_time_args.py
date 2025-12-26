from typing import Any, Iterable, Callable
from dataclasses import dataclass
import polars as pl

from .. import _utils as u


@dataclass
class ConnectionsArgs:
    project_path: str
    proj_vars: dict[str, Any]
    env_vars: dict[str, str]

    def __post_init__(self) -> None:
        self.proj_vars = self.proj_vars.copy()
        self.env_vars = self.env_vars.copy()


@dataclass
class AuthProviderArgs(ConnectionsArgs):
    pass


@dataclass
class ParametersArgs(ConnectionsArgs):
    pass


@dataclass
class BuildModelArgs(ConnectionsArgs):
    connections: dict[str, Any]
    dependencies: Iterable[str]
    _ref_func: Callable[[str], pl.LazyFrame]
    _run_external_sql_func: Callable[[str, str], pl.DataFrame]

    def __post_init__(self) -> None:
        super().__post_init__()
        self.connections = self.connections.copy()
        self.dependencies = set(self.dependencies)

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
        return self._ref_func(model)

    def run_external_sql(self, connection_name: str, sql_query: str, **kwargs) -> pl.DataFrame:
        """
        Runs a SQL query against an external database, with option to specify the connection name. Placeholder values are provided automatically

        Arguments:
            sql_query: The SQL query. Can be parameterized with placeholders
            connection_name: The connection name for the database. If None, uses the one configured for the model
        
        Returns:
            The query result as a polars DataFrame
        """
        return self._run_external_sql_func(sql_query, connection_name)

    def run_sql_on_dataframes(self, sql_query: str, *, dataframes: dict[str, pl.LazyFrame] | None = None, **kwargs) -> pl.DataFrame:
        """
        Uses a dictionary of dataframes to execute a SQL query in an embedded in-memory DuckDB database

        Arguments:
            sql_query: The SQL query to run (DuckDB dialect)
            dataframes: A dictionary of table names to their polars LazyFrame. If None, uses results of dependent models
        
        Returns:
            The result as a polars DataFrame from running the query
        """
        if dataframes is None:
            dataframes = {x: self.ref(x) for x in self.dependencies}

        return u.run_sql_on_dataframes(sql_query, dataframes)
