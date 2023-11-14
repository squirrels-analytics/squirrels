from sqlalchemy import create_engine

from . import _utils as u, _constants as c
from .connection_set import ConnectionSet, sqldf
from ._environcfg import EnvironConfigIO
from ._manifest import ManifestIO


class ConnectionSetIO:
    obj: ConnectionSet

    @classmethod
    def LoadFromFile(cls):
        """
        Takes the DB Connections from both the squirrels.yaml and connections.py files and merges them
        into a single ConnectionSet

        Parameters:
            manifest: The object of Manifest class, the interface for the squirrels.yaml file
        
        Returns:
            A ConnectionSet with the DB connections from both squirrels.yaml and connections.py
        """
        connection_configs = ManifestIO.obj.get_db_connections()
        connections = {}
        for key, config in connection_configs.items():
            cred_key = config.get(c.DB_CREDENTIALS_KEY)
            username, password = EnvironConfigIO.obj.get_credential(cred_key)
            url = config[c.URL_KEY].replace("${username}", username).replace("${password}", password)
            connections[key] = create_engine(url)
        
        proj_vars = ManifestIO.obj.get_proj_vars()
        conn_from_py_file = u.run_module_main(c.CONNECTIONS_FILE, {"proj_vars": proj_vars})
        if conn_from_py_file is None:
            conn_from_py_file = {}
        cls.obj = ConnectionSet({**connections, **conn_from_py_file})

    @classmethod
    def Dispose(cls):
        cls.obj._dispose()
