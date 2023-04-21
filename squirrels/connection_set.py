from typing import Dict, Callable
from sqlite3.dbapi2 import Connection
from importlib.machinery import SourceFileLoader

from squirrels import manifest as mf, constants as c
from squirrels.timed_imports import pandas as pd
from squirrels.utils import ConfigurationError


class ConnectionSet:
    def __init__(self, conn_factories: Dict[str, Callable[[], Connection]]) -> None:
        self.conn_factories = conn_factories
        self.saved_conns: Dict[str, Connection] = {}
    
    def get_dataframe_from_query(self, conn_name: str, query: str) -> pd.DataFrame:
        if conn_name not in self.saved_conns:
            try:
                conn_factory = self.conn_factories[conn_name]
            except KeyError as e:
                raise ConfigurationError(f'Connection name "{conn_name}" was not configured') from e
            self.saved_conns[conn_name] = conn_factory()
        
        conn: Connection = self.saved_conns[conn_name]
        cur = conn.execute(query)
        df = pd.DataFrame(cur.fetchall())
        df.columns = [x[0] for x in cur.description]
        cur.close()

        return df

    def close(self):
        for conn in self.saved_conns.values():
            conn.close()


def from_file(manifest: mf.Manifest) -> ConnectionSet:
    module = SourceFileLoader(c.CONNECTIONS_FILE, c.CONNECTIONS_FILE).load_module()
    return ConnectionSet(module.main(manifest.get_proj_vars()))
