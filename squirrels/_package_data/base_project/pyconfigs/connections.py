from typing import Any
from squirrels import arguments as args, connections as cn


def main(connections: dict[str, cn.ConnectionProperties | Any], sqrl: args.ConnectionsArgs) -> None:
    """
    Define sqlalchemy engines by adding them to the "connections" dictionary
    """
    ## SQLAlchemy URL for a connection engine
    conn_str: str = sqrl.env_vars["SQLITE_URI"].format(project_path=sqrl.project_path)

    ## Assigning names to connection engines
    connections["default"] = cn.ConnectionProperties(
        label="SQLite Expenses Database", 
        type=cn.ConnectionTypeEnum.SQLALCHEMY, 
        uri=conn_str
    )
    