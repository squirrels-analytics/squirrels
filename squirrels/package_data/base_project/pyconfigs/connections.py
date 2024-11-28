from typing import Any
from squirrels import ConnectionsArgs, ConnectionProperties


def main(connections: dict[str, ConnectionProperties | Any], sqrl: ConnectionsArgs) -> None:
    """
    Define sqlalchemy engines by adding them to the "connections" dictionary
    """
    ## SQLAlchemy URL for a connection engine
    conn_str: str = sqrl.env_vars["sqlite_uri"]

    ## Assigning names to connection engines
    connections["default"] = ConnectionProperties(type="sqlalchemy", uri=conn_str)
