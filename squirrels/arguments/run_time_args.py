from typing import Union, Callable, Optional, Any
from dataclasses import dataclass
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
    _placeholders: dict[str, Any]

    def set_placeholder(self, placeholder: str, value: Union[_TextValue, Any]) -> None:
        """
        Method to set a placeholder value.

        Parameters:
            placeholder: A string for the name of the placeholder
            value: The value of the placeholder. Can be of any type
        """
        if isinstance(value, _TextValue):
            value = value._value_do_not_touch
        self._placeholders[placeholder] = value
    
    def param_exists(self, param_name: str) -> bool:
        """
        Method to check whether a given parameter exists and is enabled (i.e., not hidden based on other parameter selections) for the current 
        dataset at runtime.

        Parameters:
            param_name: A string for the name of the parameter
        
        Returns:
            A boolean for whether the parameter exists
        """
        return (param_name in self.prms and self.prms[param_name].is_enabled())


@dataclass
class ModelDepsArgs(ContextArgs):
    ctx: dict[str, Any]


@dataclass
class ModelArgs(ModelDepsArgs):
    connection_name: str
    _connections: dict[str, Engine]
    _dependencies: set[str]
    _ref: Callable[[str], pd.DataFrame]

    @property
    def connections(self) -> dict[str, Engine]:
        return self._connections.copy()

    @property
    def dependencies(self) -> set[str]:
        return self._dependencies.copy()
    
    def is_placeholder(self, placeholder: str) -> bool:
        """
        Checks whether a name is a valid placeholder

        Parameters:
            placeholder: A string for the name of the placeholder
        
        Returns:
            A boolean for whether name exists
        """
        return placeholder in self._placeholders
    
    def get_placeholder_value(self, placeholder: str) -> Optional[Any]:
        """
        Gets the value of a placeholder.

        USE WITH CAUTION. Do not use the return value directly in a SQL query since that could be prone to SQL injection

        Parameters:
            placeholder: A string for the name of the placeholder
        
        Returns:
            An type for the value of the placeholder
        """
        return self._placeholders.get(placeholder)
    
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
        return self._ref(model)

    def run_external_sql(self, sql_query: str, *, connection_name: str = None, **kwargs) -> pd.DataFrame:
        """
        Runs a SQL query against an external database, with option to specify the connection name. Placeholder values are provided automatically

        Parameters:
            sql_query: The SQL query. Can be parameterized with placeholders
            connection_name: The connection name for the database. If None, uses the one configured for the model
        
        Returns:
            The query result as a pandas DataFrame
        """
        connection_name = self.connection_name if connection_name is None else connection_name
        return ConnectionSetIO.obj.run_sql_query_from_conn_name(sql_query, connection_name, self._placeholders)

    def run_sql_on_dataframes(self, sql_query: str, *, dataframes: dict[str, pd.DataFrame] = None, **kwargs) -> pd.DataFrame:
        """
        Uses a dictionary of dataframes to execute a SQL query in an embedded in-memory database (sqlite or duckdb based on setting)

        Parameters:
            sql_query: The SQL query to run
            dataframes: A dictionary of table names to their pandas Dataframe. If None, uses results of dependent models
        
        Returns:
            The result as a pandas Dataframe from running the query
        """
        if dataframes is None:
            dataframes = {x: self.ref(x) for x in self._dependencies}

        return u.run_sql_on_dataframes(sql_query, dataframes)
