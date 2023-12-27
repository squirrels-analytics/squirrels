from dataclasses import dataclass
from sqlalchemy import Engine, create_engine
import pandas as pd

from . import _utils as u, _constants as c, _py_module as pm
from .arguments.init_time_args import ConnectionsArgs
from ._environcfg import EnvironConfigIO
from ._manifest import ManifestIO
from ._timer import timer, time


@dataclass
class ConnectionSet:
    """
    A wrapper class around a collection of sqlalchemy engines

    Attributes:
        _engines: A dictionary of connection name to the corresponding sqlalchemy engine
    """
    _engines: dict[str, Engine]

    def get_engines_as_dict(self):
        return self._engines.copy()
    
    def _get_engine(self, conn_name: str) -> Engine:
        try:
            connection_pool = self._engines[conn_name]
        except KeyError as e:
            raise u.ConfigurationError(f'Connection name "{conn_name}" was not configured') from e
        return connection_pool
    
    def run_sql_query_from_conn_name(self, query: str, conn_name: str) -> pd.DataFrame:
        engine = self._get_engine(conn_name)
        df = pd.read_sql(query, engine)
        return df

    def _dispose(self) -> None:
        """
        Disposes of all the engines in this ConnectionSet
        """
        for pool in self._engines.values():
            pool.dispose()


class ConnectionSetIO:
    args: ConnectionsArgs
    obj: ConnectionSet

    @classmethod
    def LoadFromFile(cls):
        """
        Takes the DB connection engines from both the squirrels.yml and connections.py files and merges them
        into a single ConnectionSet
        
        Returns:
            A ConnectionSet with the DB connections from both squirrels.yml and connections.py
        """
        start = time.time()
        engines: dict[str, Engine] = {}
        cls.obj = ConnectionSet(engines)
        try:
            for config in ManifestIO.obj.connections.values():
                engines[config.name] = create_engine(config.url)
            
            proj_vars = ManifestIO.obj.project_variables
            env_vars = EnvironConfigIO.obj.get_all_env_vars()
            get_credential = EnvironConfigIO.obj.get_credential
            cls.args = ConnectionsArgs(proj_vars, env_vars, get_credential)
            pm.run_pyconfig_main(c.CONNECTIONS_FILE, {"connections": engines, "sqrl": cls.args})
        except Exception as e:
            cls.Dispose()
            raise e
        
        timer.add_activity_time("creating sqlalchemy engines", start)

    @classmethod
    def Dispose(cls):
        cls.obj._dispose()
