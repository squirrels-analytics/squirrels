from typing import Dict, Callable
from functools import partial
from sqlite3.dbapi2 import Connection
import sqlite3
import squirrels as sq


def main(proj: Dict[str, str]) -> Dict[str, Callable[[], Connection]]:
    cred = sq.get_credential('my_key')
    user, pw = cred.username, cred.password
    
    return {
        'my_db': partial(sqlite3.connect, './database/seattle_weather.db')
    }
