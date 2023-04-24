from typing import Dict
from functools import partial
import sqlite3

from squirrels import ConnectionSet, QueuePool

# All connections must be shareable across multiple thread. No writes will occur on them
def main(proj: Dict[str, str], *args, **kwargs) -> ConnectionSet:
    # cred = sq.get_credential('my_key')
    # user, pw = cred.username, cred.password
    
    sample_db_connector = partial(sqlite3.connect, './database/sample_database.db', check_same_thread=False)
    return ConnectionSet({
        'my_db': QueuePool(sample_db_connector)
    })
