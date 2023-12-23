from typing import Union
from sqlalchemy import create_engine, Engine, Pool
import squirrels as sr


def main(connections: dict[str, Union[Engine, Pool]], sqrl: sr.ConnectionsArgs) -> None:
    """
    Define connections by adding sqlalchemy engines or connection pools to the dictionary "connections".
    Note that all connections in a pool should be shareable across threads. No writes should be occuring on them.
    """
    
    """ Example of getting the username and password """
    # username, password = sqrl.get_credential('my_key')

    """ SQLAlchemy URL for a connection pool / engine """
    conn_str = 'sqlite:///./database/expenses.db' 

    """ Can also leverage the environcfg.yml file for connection details """
    # conn_str_raw: str = sqrl.env_vars["sqlite_conn_str"]
    # conn_str = conn_str_raw.format(username=username, password=password) 
    
    """ Assigning names to connection pool / engine """
    connections["default"] = create_engine(conn_str)

    """ Example of using QueuePool to use a custom db connector without a SQLAlchemy URL """
    # from sqlalchemy import QueuePool
    # connection_creator = lambda: sqlite3.connect("./database/expenses.db", check_same_thread=False)
    # connections["default"] = QueuePool(connection_creator)
