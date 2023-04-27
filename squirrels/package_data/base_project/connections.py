from typing import Dict
from sqlalchemy import create_engine, QueuePool

from squirrels import ConnectionSet, get_credential


# Note: all connections must be shareable across multiple thread. No writes will occur on them
def main(proj: Dict[str, str], *args, **kwargs) -> ConnectionSet:

    # ## Example of getting the username and password set with "$ squirrels set-credential [key]"
    # cred = get_credential('my_key')
    # user, pw = cred.username, cred.password

    # Create a connection pool / engine
    pool = create_engine('sqlite:///./database/sample_database.db')

    # ## Example of using QueuePool instead with a custom db connector:
    # connection_creator = lambda: sqlite3.connect('./database/sample_database.db', check_same_thread=False)
    # pool = QueuePool(connection_creator)
    
    return ConnectionSet({'my_db': pool})
