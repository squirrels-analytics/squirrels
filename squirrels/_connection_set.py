from typing import Dict, Union
from dataclasses import dataclass
from sqlalchemy import Engine, Pool, create_engine
import pandas as pd

from . import _utils as u, _constants as c
from ._environcfg import EnvironConfigIO
from ._manifest import ManifestIO
from ._timer import timer, time


@dataclass
class ConnectionSet:
    """
    A wrapper class around a collection of Connection Pools or Sqlalchemy Engines

    Attributes:
        conn_pools: A dictionary of connection pool name to the corresponding Pool or Engine from sqlalchemy
    """
    _conn_pools: Dict[str, Union[Engine, Pool]]
    
    def get_connection_pool(self, conn_name: str) -> Union[Engine, Pool]:
        try:
            connection_pool = self._conn_pools[conn_name]
        except KeyError as e:
            raise u.ConfigurationError(f'Connection name "{conn_name}" was not configured') from e
        return connection_pool
    
    def run_sql_query(self, query: str, connection_pool: Union[Engine, Pool]):
        if isinstance(connection_pool, Pool):
            conn = connection_pool.connect()
        elif isinstance(connection_pool, Engine):
            conn = connection_pool.raw_connection()
        else:
            raise TypeError(f'Type of connection_pool not supported')
        
        try:
            cur = conn.cursor()
            cur.execute(query)
            df = pd.DataFrame(data=cur.fetchall(), columns=[x[0] for x in cur.description])
        finally:
            conn.close()

        return df
    
    def run_sql_query_from_conn_name(self, query: str, conn_name: str) -> pd.DataFrame:
        connector = self.get_connection_pool(conn_name)
        return self.run_sql_query(query, connector)

    def _dispose(self) -> None:
        """
        Disposes of all the connection pools in this ConnectionSet
        """
        for pool in self._conn_pools.values():
            pool.dispose()


class ConnectionSetIO:
    obj: ConnectionSet

    @classmethod
    def LoadFromFile(cls):
        """
        Takes the DB Connections from both the squirrels.yaml and connections.py files and merges them
        into a single ConnectionSet
        
        Returns:
            A ConnectionSet with the DB connections from both squirrels.yaml and connections.py
        """
        start = time.time()
        connection_configs = ManifestIO.obj.get_db_connections()
        connections = {}
        for key, config in connection_configs.items():
            cred_key = config.get(c.DB_CREDENTIALS_KEY)
            username, password = EnvironConfigIO.obj.get_credential(cred_key)
            if c.URL_KEY not in config or config[c.URL_KEY] is None:
                raise u.ConfigurationError(f"The db_connection '{key}' is missing attribute '{c.URL_KEY}'")
            url = config[c.URL_KEY].format(username=username, password=password)
            connections[key] = create_engine(url)
        
        proj_vars = ManifestIO.obj.get_proj_vars()
        u.run_pyconfig_main(c.CONNECTIONS_FILE, {"connections": connections, "proj": proj_vars})
        cls.obj = ConnectionSet(connections)
        timer.add_activity_time("creating sqlalchemy engines or pools", start)

    @classmethod
    def Dispose(cls):
        cls.obj._dispose()
