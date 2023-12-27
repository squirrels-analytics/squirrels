from sqlalchemy import create_engine, Engine
import squirrels as sr


def main(connections: dict[str, Engine], sqrl: sr.ConnectionsArgs) -> None:
    """
    Define sqlalchemy engines by adding them to the "connections" dictionary
    """
    
    """ Example of getting the username and password """
    # username, password = sqrl.get_credential('my_key')
    
    """ SQLAlchemy URL for a connection engine """
    conn_str = 'sqlite:///database/expenses.db' 

    """ Can also leverage environment variables in the environcfg.yml file for connection details """
    # conn_str_raw: str = sqrl.env_vars["sqlite_conn_str"]
    # conn_str = conn_str_raw.format(username=username, password=password) 
    
    """ Assigning names to connection engines """
    connections["default"] = create_engine(conn_str)
