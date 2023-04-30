from typing import Dict, Union
from importlib.machinery import SourceFileLoader
from sqlalchemy import Engine, Pool
import sqlite3

from squirrels import manifest as mf, constants as c
from squirrels.timed_imports import pandas as pd
from squirrels.utils import ConfigurationError

ConnectionPool = Union[Engine, Pool]


class ConnectionSet:
    def __init__(self, conn_pools: Dict[str, ConnectionPool]) -> None:
        self._conn_pools = conn_pools
    
    def get_connection_pool(self, conn_name: str) -> ConnectionPool:
        try:
            connection_pool = self._conn_pools[conn_name]
        except KeyError as e:
            raise ConfigurationError(f'Connection name "{conn_name}" was not configured') from e
        return connection_pool
    
    def get_dataframe_from_query(self, conn_name: str, query: str) -> pd.DataFrame:
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
            data = cur.fetchall()
            columns = [x[0] for x in cur.description]
            df = pd.DataFrame(data, columns=columns)
        finally:
            conn.close()

        return df

    def dispose(self) -> None:
        for pool in self._conn_pools.values():
            pool.dispose()


def from_file(manifest: mf.Manifest) -> ConnectionSet:
    module = SourceFileLoader(c.CONNECTIONS_FILE, c.CONNECTIONS_FILE).load_module()
    proj_vars = manifest.get_proj_vars()
    try:
        return module.main(proj_vars)
    except Exception as e:
        raise ConfigurationError(f'Error in the {c.CONNECTIONS_FILE} file') from e


def sqldf(query: str, df_by_db_views: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    conn = sqlite3.connect(":memory:")
    try:
        for db_view, df in df_by_db_views.items():
            df.to_sql(db_view, conn, index=False)
        return pd.read_sql(query, conn)
    finally:
        conn.close()
