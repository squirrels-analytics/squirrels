from typing import Dict
from importlib.machinery import SourceFileLoader
from sqlalchemy.pool import QueuePool, PoolProxiedConnection as DBAPIConnection
import sqlite3

from squirrels import manifest as mf, constants as c
from squirrels.timed_imports import pandas as pd
from squirrels.utils import ConfigurationError


class ConnectionSet:
    def __init__(self, conn_pools: Dict[str, QueuePool]) -> None:
        self.conn_pools = conn_pools
    
    def get_connection_pool(self, conn_name: str) -> QueuePool:
        try:
            connection_pool = self.conn_pools[conn_name]
        except KeyError as e:
            raise ConfigurationError(f'Connection name "{conn_name}" was not configured') from e
        return connection_pool
    
    def get_dataframe_from_query(self, conn_name: str, query: str) -> pd.DataFrame:
        conn = self.get_connection_pool(conn_name).connect()
        try:
            cur = conn.cursor()
            cur.execute(query)
            data = cur.fetchall()
            columns = [x[0] for x in cur.description]
            df = pd.DataFrame(data, columns=columns)
        finally:
            conn.close()

        return df

    def close(self):
        for pool in self.conn_pools.values():
            pool.dispose()


def from_file(manifest: mf.Manifest) -> ConnectionSet:
    module = SourceFileLoader(c.CONNECTIONS_FILE, c.CONNECTIONS_FILE).load_module()
    return module.main(manifest.get_proj_vars())


def sqldf(query: str, df_by_db_views: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    conn = sqlite3.connect(":memory:")
    try:
        for db_view, df in df_by_db_views.items():
            df.to_sql(db_view, conn, index=False)
        return pd.read_sql(query, conn)
    finally:
        conn.close()
