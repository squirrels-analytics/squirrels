from typing import Union, Callable, Optional, Any
from dataclasses import dataclass
from sqlalchemy import Engine
import pandas as pd

from .init_time_args import ConnectionsArgs, ParametersArgs
from ..user_base import User
from ..parameters import Parameter, TextValue
from .._connection_set import ConnectionSetIO
from .. import _utils as u


@dataclass
class AuthArgs(ConnectionsArgs):
    _connections: dict[str, Engine]
    username: str
    password: str

    @property
    def connections(self) -> Engine:
        """
        A dictionary of connection keys to SQLAlchemy Engines for database connections. 
        
        Can also be used to store other in-memory objects in advance such as ML models.
        """
        return self._connections.copy()


@dataclass
class DashboardArgs(ParametersArgs):
    _dataset: Callable[[str, dict], pd.DataFrame]

    def dataset(self, name: str, fixed_parameters: dict) -> pd.DataFrame:
        """
        Get dataset as DataFrame given dataset name.

        The parameters used for the dataset include the parameter selections coming from the REST API and the fixed_parameters argument. The fixed_parameters takes precedence.

        Arguments:
            name: A string for the dataset name
            fixed_parameters: 
        
        Returns:
            A DataFrame for the result of the dataset
        """
        return self._dataset(name, fixed_parameters)


@dataclass
class ContextArgs(ParametersArgs):
    user: Optional[User]
    _prms: dict[str, Parameter]
    _traits: dict[str, Any]
    _placeholders: dict[str, Any]

    @property
    def prms(self) -> dict[str, Parameter]:
        """
        A dictionary of parameter names to parameter
        """
        return self._prms.copy()
    
    @property
    def traits(self) -> dict[str, Any]:
        """
        A dictionary of dataset trait name to value
        """
        return self._traits.copy()

    @property
    def placeholders(self) -> dict[str, Any]:
        """
        A dictionary of placeholder name to placeholder value
        """
        return self._placeholders.copy()

    def set_placeholder(self, placeholder: str, value: Union[TextValue, Any]) -> str:
        """
        Method to set a placeholder value.

        Arguments:
            placeholder: A string for the name of the placeholder
            value: The value of the placeholder. Can be of any type
        """
        if isinstance(value, TextValue):
            value = value._value_do_not_touch
        self._placeholders[placeholder] = value
        return ""
    
    def param_exists(self, param_name: str) -> bool:
        """
        Method to check whether a given parameter exists and is enabled (i.e., not hidden based on other parameter selections) for the current 
        dataset at runtime.

        Arguments:
            param_name: A string for the name of the parameter
        
        Returns:
            A boolean for whether the parameter exists
        """
        return (param_name in self.prms and self.prms[param_name].is_enabled())


@dataclass
class ModelDepsArgs(ContextArgs):
    _ctx: dict[str, Any]

    @property
    def ctx(self) -> dict[str, Any]:
        """
        Dictionary of context variables
        """
        return self._ctx.copy()


@dataclass
class ModelArgs(ModelDepsArgs):
    connection_name: str
    _connections: dict[str, Engine]
    _dependencies: set[str]
    _ref: Callable[[str], pd.DataFrame]

    @property
    def connections(self) -> dict[str, Engine]:
        """
        A dictionary of connection keys to SQLAlchemy Engines for database connections. 
        
        Can also be used to store other in-memory objects in advance such as ML models.
        """
        return self._connections.copy()

    @property
    def dependencies(self) -> set[str]:
        """
        The set of dependent data model names
        """
        return self._dependencies.copy()
    
    def is_placeholder(self, placeholder: str) -> bool:
        """
        Checks whether a name is a valid placeholder

        Arguments:
            placeholder: A string for the name of the placeholder
        
        Returns:
            A boolean for whether name exists
        """
        return placeholder in self._placeholders
    
    def get_placeholder_value(self, placeholder: str) -> Optional[Any]:
        """
        Gets the value of a placeholder.

        USE WITH CAUTION. Do not use the return value directly in a SQL query since that could be prone to SQL injection

        Arguments:
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

        Arguments:
            model: The model name
        
        Returns:
            A pandas DataFrame
        """
        return self._ref(model)

    def run_external_sql(self, sql_query: str, *, connection_name: str = None, **kwargs) -> pd.DataFrame:
        """
        Runs a SQL query against an external database, with option to specify the connection name. Placeholder values are provided automatically

        Arguments:
            sql_query: The SQL query. Can be parameterized with placeholders
            connection_name: The connection name for the database. If None, uses the one configured for the model
        
        Returns:
            The query result as a pandas DataFrame
        """
        connection_name = self.connection_name if connection_name is None else connection_name
        return ConnectionSetIO.obj.run_sql_query_from_conn_name(sql_query, connection_name, self._placeholders)

    def run_sql_on_dataframes(self, sql_query: str, *, dataframes: Optional[dict[str, pd.DataFrame]] = None, **kwargs) -> pd.DataFrame:
        """
        Uses a dictionary of dataframes to execute a SQL query in an embedded in-memory database (sqlite or duckdb based on setting)

        Arguments:
            sql_query: The SQL query to run
            dataframes: A dictionary of table names to their pandas Dataframe. If None, uses results of dependent models
        
        Returns:
            The result as a pandas Dataframe from running the query
        """
        if dataframes is None:
            dataframes = {x: self.ref(x) for x in self._dependencies}

        return u.run_sql_on_dataframes(sql_query, dataframes)
