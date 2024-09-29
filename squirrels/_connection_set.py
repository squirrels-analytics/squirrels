from dataclasses import dataclass
from sqlalchemy import Engine, create_engine
import time, pandas as pd

from . import _utils as u, _constants as c, _py_module as pm
from .arguments.init_time_args import ConnectionsArgs
from ._environcfg import EnvironConfig
from ._manifest import ManifestConfig


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
    
    def run_sql_query_from_conn_name(self, query: str, conn_name: str, placeholders: dict = {}) -> pd.DataFrame:
        engine = self._get_engine(conn_name)
        try:
            df = pd.read_sql(query, engine, params=placeholders)
            return df
        except Exception as e:
            raise RuntimeError(e) from e

    def dispose(self) -> None:
        """
        Disposes of all the engines in this ConnectionSet
        """
        for pool in self._engines.values():
            if isinstance(pool, Engine):
                pool.dispose()


class ConnectionSetIO:

    @classmethod
    def load_conn_py_args(cls, logger: u.Logger, env_cfg: EnvironConfig, manifest_cfg: ManifestConfig) -> ConnectionsArgs:
        start = time.time()
        
        proj_vars = manifest_cfg.project_variables.model_dump()
        env_vars = env_cfg.get_all_env_vars()
        conn_args = ConnectionsArgs(proj_vars, env_vars, env_cfg.get_credential)
        
        logger.log_activity_time("setting up arguments for connections.py", start)
        return conn_args

    @classmethod
    def load_from_file(cls, logger: u.Logger, base_path: str, manifest_cfg: ManifestConfig, conn_args: ConnectionsArgs) -> ConnectionSet:
        """
        Takes the DB connection engines from both the squirrels.yml and connections.py files and merges them
        into a single ConnectionSet
        
        Returns:
            A ConnectionSet with the DB connections from both squirrels.yml and connections.py
        """
        start = time.time()
        engines: dict[str, Engine] = {}
        
        for config in manifest_cfg.connections.values():
            engines[config.name] = create_engine(config.url)

        pm.run_pyconfig_main(base_path, c.CONNECTIONS_FILE, {"connections": engines, "sqrl": conn_args})
        conn_set = ConnectionSet(engines)

        logger.log_activity_time("creating sqlalchemy engines", start)
        return conn_set
