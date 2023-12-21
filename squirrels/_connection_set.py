from typing import Union
from dataclasses import dataclass
from sqlalchemy import Engine, Pool, create_engine
import pandas as pd

from . import _utils as u, _constants as c, _py_module as pm
from .arguments.init_time_args import ConnectionsArgs
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
    _conn_pools: dict[str, Union[Engine, Pool]]

    def get_connections_as_dict(self):
        return self._conn_pools
    
    def get_connection_pool(self, conn_name: str) -> Union[Engine, Pool]:
        try:
            connection_pool = self._conn_pools[conn_name]
        except KeyError as e:
            raise u.ConfigurationError(f'Connection name "{conn_name}" was not configured') from e
        return connection_pool
    
    def _run_sql_query(self, query: str, connection_pool: Union[Engine, Pool]):
        if isinstance(connection_pool, Pool):
            conn = connection_pool.connect()
        elif isinstance(connection_pool, Engine):
            conn = connection_pool.raw_connection()
        else:
            raise TypeError(f'Type of connection_pool not supported')
        
        try:
            cur = conn.cursor()
            try:
                cur.execute(query)
            except Exception as e:
                raise u.ConfigurationError(e)
            df = pd.DataFrame(data=cur.fetchall(), columns=[x[0] for x in cur.description])
        finally:
            conn.close()

        return df
    
    def run_sql_query_from_conn_name(self, query: str, conn_name: str) -> pd.DataFrame:
        connector = self.get_connection_pool(conn_name)
        return self._run_sql_query(query, connector)

    def _dispose(self) -> None:
        """
        Disposes of all the connection pools in this ConnectionSet
        """
        for pool in self._conn_pools.values():
            pool.dispose()


class ConnectionSetIO:
    args: ConnectionsArgs
    obj: ConnectionSet

    @classmethod
    def LoadFromFile(cls):
        """
        Takes the DB Connections from both the squirrels.yml and connections.py files and merges them
        into a single ConnectionSet
        
        Returns:
            A ConnectionSet with the DB connections from both squirrels.yml and connections.py
        """
        start = time.time()
        connections = {}
        for config in ManifestIO.obj.db_connections:
            connections[config.connection_name] = create_engine(config.url)
        
        proj_vars = ManifestIO.obj.project_variables
        env_vars = EnvironConfigIO.obj.get_all_env_vars()
        get_credential = EnvironConfigIO.obj.get_credential
        cls.args = ConnectionsArgs(proj_vars, env_vars, get_credential)
        pm.run_pyconfig_main(c.CONNECTIONS_FILE, {"connections": connections, "sqrl": cls.args})
        cls.obj = ConnectionSet(connections)
        timer.add_activity_time("creating sqlalchemy engines or pools", start)

    @classmethod
    def Dispose(cls):
        cls.obj._dispose()
