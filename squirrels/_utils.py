from typing import Sequence, Optional, Union, TypeVar, Callable
from pathlib import Path
import json, sqlite3, jinja2 as j2, pandas as pd

from . import _constants as c

FilePath = Union[str, Path]


## Custom Exceptions

class InvalidInputError(Exception):
    """
    Use this exception when the error is due to providing invalid inputs to the REST API
    """
    pass

class ConfigurationError(Exception):
    """
    Use this exception when the server error is due to errors in the squirrels project instead of the squirrels framework/library
    """
    pass

class FileExecutionError(ConfigurationError):
    def __init__(self, message: str, error: Exception, *args) -> None:
        new_message = message + f"\n... Produced error message `{error}` (scroll up for more details on handled exception)"
        super().__init__(new_message, *args)


## Utility functions/variables

def join_paths(*paths: FilePath) -> Path:
    """
    Joins paths together.

    Parameters:
        paths (str | pathlib.Path): The paths to join.

    Returns:
        (pathlib.Path) The joined path.
    """
    return Path(*paths)


_j2_env = j2.Environment(loader=j2.FileSystemLoader('.'))

def render_string(raw_str: str, **kwargs: dict) -> str:
    """
    Given a template string, render it with the given keyword arguments

    Parameters:
        raw_str: The template string
        kwargs: The keyword arguments

    Returns:
        The rendered string
    """
    template = _j2_env.from_string(raw_str)
    return template.render(kwargs)


T = TypeVar('T')
def __process_file_handler(file_handler: Callable[[FilePath], T], filepath: FilePath, is_required: bool) -> Optional[T]:
    try:
        return file_handler(filepath)
    except FileNotFoundError as e:
        if is_required:
            raise ConfigurationError(f"Required file not found: '{str(filepath)}'") from e


def read_file(filepath: FilePath, *, is_required: bool = True) -> Optional[str]:
    """
    Reads a file and return its content if required

    Parameters:
        filepath (str | pathlib.Path): The path to the file to read
        is_required: If true, throw error if file doesn't exist

    Returns:
        Content of the file, or None if doesn't exist and not required
    """
    def file_handler(filepath: FilePath):
        with open(filepath, 'r') as f:
            return f.read()
    return __process_file_handler(file_handler, filepath, is_required)


def normalize_name(name: str) -> str:
    """
    Normalizes names to the convention of the squirrels manifest file.

    Parameters:
        name: The name to normalize.

    Returns:
        The normalized name.
    """
    return name.replace('-', '_')


def normalize_name_for_api(name: str) -> str:
    """
    Normalizes names to the REST API convention.

    Parameters:
        name: The name to normalize.

    Returns:
        The normalized name.
    """
    return name.replace('_', '-')


def load_json_or_comma_delimited_str_as_list(input_str: Union[str, Sequence]) -> Sequence[str]:
    """
    Given a string, load it as a list either by json string or comma delimited value

    Parameters:
        input_str: The input string
    
    Returns:
        The list representation of the input string
    """
    if not isinstance(input_str, str):
        return (input_str)
    
    output = None
    try:
        output = json.loads(input_str)
    except json.decoder.JSONDecodeError:
        pass
    
    if isinstance(output, list):
        return output
    elif input_str == "":
        return []
    else:
        return [x.strip() for x in input_str.split(",")]


X, Y = TypeVar('X'), TypeVar('Y')
def process_if_not_none(input_val: Optional[X], processor: Callable[[X], Y]) -> Optional[Y]:
    """
    Given a input value and a function that processes the value, return the output of the function unless input is None

    Parameters:
        input_val: The input value
        processor: The function that processes the input value
    
    Returns:
        The output type of "processor" or None if input value if None
    """
    if input_val is None:
        return None
    return processor(input_val)


def use_duckdb() -> bool:
    """
    Determines whether to use DuckDB instead of SQLite for embedded database

    Returns:
        A boolean
    """
    from ._manifest import ManifestIO
    return (ManifestIO.obj.settings.get(c.IN_MEMORY_DB_SETTING, c.SQLITE) == c.DUCKDB)


def run_sql_on_dataframes(sql_query: str, dataframes: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Runs a SQL query against a collection of dataframes

    Parameters:
        sql_query: The SQL query to run
        dataframes: A dictionary of table names to their pandas Dataframe
    
    Returns:
        The result as a pandas Dataframe from running the query
    """
    do_use_duckdb = use_duckdb()
    if do_use_duckdb:
        import duckdb
        conn = duckdb.connect()
    else:
        conn = sqlite3.connect(":memory:")
    
    try:
        for name, df in dataframes.items():
            if do_use_duckdb:
                conn.execute(f"CREATE TABLE {name} AS FROM df")
            else:
                df.to_sql(name, conn, index=False)
        
        return conn.execute(sql_query).df() if do_use_duckdb else pd.read_sql(sql_query, conn)
    finally:
        conn.close()
