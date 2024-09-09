from sqlalchemy import create_engine, Engine
from squirrels import ConnectionsArgs


def main(connections: dict[str, Engine], sqrl: ConnectionsArgs) -> None:
    """
    Define sqlalchemy engines by adding them to the "connections" dictionary
    """
    
    ## SQLAlchemy URL for a connection engine
    conn_str = 'sqlite:///./assets/expenses.db' 

    ## Can also leverage environment variables and credentials in the env.yml file for connection details
    # conn_str_raw: str = sqrl.env_vars["sqlite_conn_str"]
    # username, password = sqrl.get_credential('my_key')
    # conn_str = conn_str_raw.format(username=username, password=password) 
    
    ## Assigning names to connection engines
    connections["default"] = create_engine(conn_str)
