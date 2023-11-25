from typing import Dict, Union, Any
from sqlalchemy import create_engine, Engine, Pool, QueuePool
import squirrels as sr


# Note: all connections must be shareable across multiple thread. No writes will occur on them
def main(proj: Dict[str, Any], *p_args, **kwargs) -> Dict[str, Union[Engine, Pool]]:

    # # Example of getting the username and password
    # username, password = sr.get_credential('my_key')

    # Create a connection pool / engine
    pool = create_engine('sqlite:///./database/expenses.db')

    # # Example of using QueuePool instead for a custom db connector:
    # connection_creator = lambda: sqlite3.connect('./database/expenses.db', check_same_thread=False)
    # pool = QueuePool(connection_creator)
    
    return {'default': pool}
