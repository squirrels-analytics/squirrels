from typing import Dict, Union, Any
from sqlalchemy import create_engine, Engine, Pool, QueuePool

from squirrels import get_credential


# Note: all connections must be shareable across multiple thread. No writes will occur on them
def main(proj: Dict[str, Any], *p_args, **kwargs) -> Dict[str, Union[Engine, Pool]]:

    ## Example of getting the username and password set with "$ squirrels set-credential [key]"
    # cred = get_credential('my_key') # then use cred.username and cred.password to access the username and password

    # Create a connection pool / engine
    pool = create_engine('sqlite:///./database/sample_database.db')

    ## Example of using QueuePool instead for a custom db connector:
    # connection_creator = lambda: sqlite3.connect('./database/sample_database.db', check_same_thread=False)
    # pool = QueuePool(connection_creator)
    
    return {'default': pool}
