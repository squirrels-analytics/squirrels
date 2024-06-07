from typing import Union, Callable, Any
from dataclasses import dataclass, field
from sqlalchemy import Engine
import pandas as pd

from .init_time_args import ConnectionsArgs, ParametersArgs
from ..user_base import User
from ..parameters import Parameter, _TextValue
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
    _placeholders: dict[str, Any] = field(init=False, default_factory=dict)

    def set_placeholder(self, placeholder: str, value: Union[_TextValue, Any]) -> None:
        if isinstance(value, _TextValue):
            value = value._value_do_not_touch
        self._placeholders[placeholder] = value
    
    def prms_contain(self, param_name: str) -> bool:
        return (param_name in self.prms and self.prms[param_name].is_enabled())


@dataclass
class ModelDepsArgs(ContextArgs):
    ctx: dict[str, Any]


@dataclass
class ModelArgs(ModelDepsArgs):
    connection_name: str
    connections: dict[str, Engine]
    placeholders: dict[str, Any]
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

    def run_external_sql(
        self, sql_query: str, *, connection_name: str = None, **kwargs
    ) -> pd.DataFrame:
        """
        Runs a SQL query against an external database, with option to specify the connection name

        Parameters:
            sql_query: The SQL query
            connection_name: The connection name for the database. If None, uses the one configured for the model
        
        Returns:
            The query result as a pandas DataFrame
        """
        connection_name = self.connection_name if connection_name is None else connection_name
        return ConnectionSetIO.obj.run_sql_query_from_conn_name(sql_query, connection_name, self.placeholders)

    def run_sql_on_dataframes(self, sql_query: str, *, dataframes: dict[str, pd.DataFrame] = None, **kwargs) -> pd.DataFrame:
        """
        Uses a dictionary of dataframes to execute a SQL query in an embedded in-memory database (sqlite or duckdb based on setting)

        Parameters:
            sql_query: The SQL query to run
            dataframes: A dictionary of table names to their pandas Dataframe
        
        Returns:
            The result as a pandas Dataframe from running the query
        """
        if dataframes is None:
            dataframes = {x: self.ref(x) for x in self.dependencies}

        return u.run_sql_on_dataframes(sql_query, dataframes)
