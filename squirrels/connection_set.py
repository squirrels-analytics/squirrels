from typing import Dict, Union
from importlib.machinery import SourceFileLoader
from sqlalchemy import Engine, Pool
import sqlite3

from squirrels import _constants as c, _manifest as mf
from squirrels._timed_imports import pandas as pd
from squirrels._utils import ConfigurationError

ConnectionPool = Union[Engine, Pool]


class ConnectionSet:
    def __init__(self, conn_pools: Dict[str, ConnectionPool]) -> None:
        """
        Constructor for ConnectionSet, a wrapper class around a collection of Connection Pools or Sqlalchemy Engines

        Parameters:
            conn_pools: A dictionary of connection pool name to the corresponding Pool or Engine from sqlalchemy
        """
        self._conn_pools = conn_pools
    
    def get_connection_pool(self, conn_name: str = "default") -> ConnectionPool:
        """
        Gets to sqlalchemy Pool or Engine from the database connection name

        Parameters:
            conn_name: Name of Pool or Engine. If not provided, defaults to "default"
        
        Returns:
            A sqlalchemy Pool or Engine
        """
        try:
            connection_pool = self._conn_pools[conn_name]
        except KeyError as e:
            raise ConfigurationError(f'Connection name "{conn_name}" was not configured') from e
        return connection_pool
    
    def __getitem__(self, conn_name: str) -> ConnectionPool:
        """
        Same as get_connection_pool
        """
        return self.get_connection_pool(conn_name)
    
    def get_dataframe_from_query(self, conn_name: str, query: str) -> pd.DataFrame:
        """
        Runs a SQL query on a database connection name, and returns the results as pandas DataFrame

        Parameters:
            conn_name: Name of Pool or Engine
            query: The SQL query to run
        
        Returns:
            A pandas DataFrame
        """
        connector = self.get_connection_pool(conn_name)
        if isinstance(connector, Pool):
            conn = connector.connect()
        elif isinstance(connector, Engine):
            conn = connector.raw_connection()
        else:
            raise TypeError(f'Type for connection name "{conn_name}" not supported')
        
        try:
            cur = conn.cursor()
            cur.execute(query)
            df = pd.DataFrame(data=cur.fetchall(), columns=[x[0] for x in cur.description])
        finally:
            conn.close()

        return df

    def _dispose(self) -> None:
        """
        Disposes of all the connection pools in this ConnectionSet
        """
        for pool in self._conn_pools.values():
            pool.dispose()


def _from_file(manifest: mf.Manifest) -> ConnectionSet:
    """
    Takes the DB Connections from both the squirrels.yaml and connections.py files and merges them
    into a single ConnectionSet

    Parameters:
        manifest: The object of Manifest class, the interface for the squirrels.yaml file
    
    Returns:
        A ConnectionSet with the DB connections from both squirrels.yaml and connections.py
    """
    connections = manifest.get_db_connections()
    try:
        module = SourceFileLoader(c.CONNECTIONS_FILE, c.CONNECTIONS_FILE).load_module()
    except FileNotFoundError:
        module = None
    
    if module is not None:
        proj_vars = manifest.get_proj_vars()
        try:
            conn_from_py_file = module.main(proj_vars)
        except Exception as e:
            raise ConfigurationError(f'Error in the {c.CONNECTIONS_FILE} file') from e
    else:
        conn_from_py_file = {}
    return ConnectionSet({**connections, **conn_from_py_file})


def sqldf(query: str, df_by_db_views: Dict[str, pd.DataFrame]) -> pd.DataFrame:
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
