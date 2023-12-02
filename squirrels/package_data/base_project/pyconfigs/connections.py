from typing import Dict, Union, Any
from sqlalchemy import create_engine, Engine, Pool, QueuePool
import squirrels as sr


## Note: all connections in a pool should be shareable across threads. No writes will occur on them
def main(connections: Dict[str, Union[Engine, Pool]], proj: Dict[str, Any], **kwargs) -> None:
    
    ## Example of getting the username and password
    # username, password = sr.get_credential('my_key')

    ## SQLAlchemy URL for a connection pool / engine
    conn_str = 'sqlite:///./database/expenses.db' 

    ## Can also do this by leveraging the environcfg.yaml file
    # conn_str = sr.get_env_var("sqlite_conn_str").format(username=username, password=password) 
    
    ## Assigning names to connection pool / engine
    connections["default"] = create_engine(conn_str)

    ## Example of using QueuePool to use a custom db connector without a SQLAlchemy URL
    # connection_creator = lambda: sqlite3.connect("./database/expenses.db", check_same_thread=False)
    # connections["default"] = QueuePool(connection_creator)
