__version__ = '0.2.0'

from typing import Union, Tuple
from sqlalchemy import Engine, Pool
from pandas import DataFrame

from .parameter_options import SelectParameterOption, DateParameterOption, DateRangeParameterOption, NumberParameterOption, NumRangeParameterOption
from .parameters import Parameter, SingleSelectParameter, MultiSelectParameter, DateParameter, DateRangeParameter, NumberParameter, NumRangeParameter
from .data_sources import SingleSelectDataSource, MultiSelectDataSource, DateDataSource, DateRangeDataSource, NumberDataSource, NumRangeDataSource
from .sqldf import sqldf
from .user_base import UserBase, WrongPassword


def get_env_var(key: str, **kwargs) -> str:
    """
    Gets the environment variable set in .squirrelscfg.yaml

    Parameters:
        key (str): The environment variable key
    
    Returns:
        A string
    """
    from ._environcfg import EnvironConfigIO
    return EnvironConfigIO.obj.get_env_var(key)


def get_credential(key: str, **kwargs) -> Tuple[str, str]:
    """
    Gets the username and password for credentials set in .squirrelscfg.yaml

    Parameters:
        key (str): The credential key
    
    Returns:
        Tuple of two strings
    """
    from ._environcfg import EnvironConfigIO
    return EnvironConfigIO.obj.get_credential(key)


def get_connection_pool(conn_name: str, **kwargs) -> Union[Engine, Pool]:
    """
    Gets to sqlalchemy Pool or Engine from the database connection name

    Parameters:
        conn_name: Name of Pool or Engine
    
    Returns:
        A sqlalchemy Pool or Engine
    """
    from ._connection_set import ConnectionSetIO
    return ConnectionSetIO.obj.get_connection_pool(conn_name)


def run_sql_query(query: str, connection_pool: Union[Engine, Pool], **kwargs) -> DataFrame:
    """
    Runs a SQL query on a database connection name, and returns the results as pandas DataFrame

    Parameters:
        query: The SQL query to run
        connection_pool: A sqlalchemy Pool or Engine
    
    Returns:
        A pandas DataFrame
    """
    from ._connection_set import ConnectionSetIO
    return ConnectionSetIO.obj.run_sql_query(query, connection_pool)
