from typing import Any
from dataclasses import dataclass
from sqlalchemy import Engine, create_engine
import time, polars as pl

from . import _utils as u, _constants as c, _py_module as pm
from .arguments.init_time_args import ConnectionsArgs
from ._environcfg import EnvironConfig
from ._manifest import ManifestConfig, ConnectionProperties, ConnectionType


@dataclass
class ConnectionSet:
    """
    A wrapper class around a collection of sqlalchemy engines

    Attributes:
        _engines: A dictionary of connection name to the corresponding sqlalchemy engine
    """
    _connections: dict[str, Any]

    def get_connections_as_dict(self):
        return self._connections.copy()
    
    def _get_connection(self, conn_name: str) -> Engine:
        try:
            connection = self._connections[conn_name]
        except KeyError as e:
            raise u.ConfigurationError(f'Connection name "{conn_name}" was not configured') from e
        return connection
    
    def run_sql_query_from_conn_name(self, query: str, conn_name: str, placeholders: dict = {}) -> pl.DataFrame:
        conn = self._get_connection(conn_name)
        is_conn_arrow_based = isinstance(conn, ConnectionProperties) and (conn.type == ConnectionType.CONNECTORX or conn.type == ConnectionType.ADBC)
        if is_conn_arrow_based and len(placeholders) > 0:
            raise u.ConfigurationError(f"Connection '{conn_name}' is a ConnectorX or ADBC connection, which does not support placeholders")
        
        try:
            if is_conn_arrow_based:
                df = pl.read_database_uri(query, conn.uri, engine=conn.type.value)
            else:
                df = pl.read_database(query, conn, execute_options={"parameters": placeholders})
            return df
        except Exception as e:
            raise RuntimeError(e) from e

    def dispose(self) -> None:
        """
        Disposes / closes all the connections in this ConnectionSet
        """
        for conn in self._connections.values():
            if isinstance(conn, Engine):
                conn.dispose()
            if hasattr(conn, 'close') and callable(conn.close):
                conn.close()


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
        connections: dict[str, ConnectionProperties | Any] = {}
        
        for config in manifest_cfg.connections.values():
            connections[config.name] = ConnectionProperties(type=config.type, uri=config.uri)

        pm.run_pyconfig_main(base_path, c.CONNECTIONS_FILE, {"connections": connections, "sqrl": conn_args})

        finalized_connections = {}
        for conn_name, conn_props in connections.items():
            if isinstance(conn_props, ConnectionProperties) and conn_props.type == ConnectionType.SQLALCHEMY:
                finalized_connections[conn_name] = create_engine(conn_props.uri)
            else:
                finalized_connections[conn_name] = conn_props

        conn_set = ConnectionSet(finalized_connections)

        logger.log_activity_time("creating sqlalchemy engines", start)
        return conn_set
