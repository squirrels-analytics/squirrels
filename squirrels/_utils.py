from typing import Sequence, Optional, Union, TypeVar, Callable, Any, Iterable
from datetime import datetime
from pathlib import Path
from functools import lru_cache
from pydantic import BaseModel
import os, time, logging, json, duckdb, polars as pl, yaml
import jinja2 as j2, jinja2.nodes as j2_nodes
import sqlglot, sqlglot.expressions, asyncio

from . import _constants as c
from ._exceptions import ConfigurationError

FilePath = Union[str, Path]

# Polars
polars_dtypes_to_sqrl_dtypes: dict[type[pl.DataType], list[str]] = {
    pl.String: ["string", "varchar", "char", "text"],
    pl.Int8: ["tinyint", "int1"],
    pl.Int16: ["smallint", "short", "int2"],
    pl.Int32: ["integer", "int", "int4"],
    pl.Int64: ["bigint", "long", "int8"],
    pl.Float32: ["float", "float4", "real"],
    pl.Float64: ["double", "float8"],
    pl.Boolean: ["boolean", "bool", "logical"],
    pl.Date: ["date"],
    pl.Time: ["time"],
    pl.Datetime: ["timestamp", "datetime"],
    pl.Duration: ["interval"],
    pl.Binary: ["blob", "binary", "varbinary"]
}

sqrl_dtypes_to_polars_dtypes: dict[str, type[pl.DataType]] = {sqrl_type: k for k, v in polars_dtypes_to_sqrl_dtypes.items() for sqrl_type in v}


## Other utility classes

class Logger(logging.Logger):
    def log_activity_time(self, activity: str, start_timestamp: float, *, request_id: str | None = None) -> None:
        end_timestamp = time.time()
        time_taken = round((end_timestamp-start_timestamp) * 10**3, 3)
        data = { "activity": activity, "start_timestamp": start_timestamp, "end_timestamp": end_timestamp, "time_taken_ms": time_taken }
        info = { "request_id": request_id } if request_id else {}
        self.info(f'Time taken for "{activity}": {time_taken}ms', extra={"data": data, "info": info})


class EnvironmentWithMacros(j2.Environment):
    def __init__(self, logger: logging.Logger, loader: j2.FileSystemLoader, *args, **kwargs):
        super().__init__(*args, loader=loader, **kwargs)
        self._logger = logger
        self._macros = self._load_macro_templates(logger)

    def _load_macro_templates(self, logger: logging.Logger) -> str:
        macros_dirs = self._get_macro_folders_from_packages()
        macro_templates = []
        for macros_dir in macros_dirs:
            for root, _, files in os.walk(macros_dir):
                files: list[str]
                for filename in files:
                    if any(filename.endswith(x) for x in [".sql", ".j2", ".jinja", ".jinja2"]):
                        filepath = Path(root, filename)
                        logger.info(f"Loaded macros from: {filepath}")
                        with open(filepath, 'r') as f:
                            content = f.read()
                        macro_templates.append(content)
        return '\n'.join(macro_templates)
    
    def _get_macro_folders_from_packages(self) -> list[Path]:
        assert isinstance(self.loader, j2.FileSystemLoader)
        packages_folder = Path(self.loader.searchpath[0], c.PACKAGES_FOLDER)
        
        subdirectories = []
        if os.path.exists(packages_folder):
            for item in os.listdir(packages_folder):
                item_path = Path(packages_folder, item)
                if os.path.isdir(item_path):
                    subdirectories.append(Path(item_path, c.MACROS_FOLDER))
        
        subdirectories.append(Path(self.loader.searchpath[0], c.MACROS_FOLDER))
        return subdirectories

    def _parse(self, source: str, name: str | None, filename: str | None) -> j2_nodes.Template:
        source = self._macros + source
        return super()._parse(source, name, filename)


## Utility functions/variables

def log_activity_time(logger: logging.Logger, activity: str, start_timestamp: float, *, request_id: str | None = None) -> None:
    end_timestamp = time.time()
    time_taken = round((end_timestamp-start_timestamp) * 10**3, 3)
    data = { "activity": activity, "start_timestamp": start_timestamp, "end_timestamp": end_timestamp, "time_taken_ms": time_taken }
    info = { "request_id": request_id } if request_id else {}
    logger.debug(f'Time taken for "{activity}": {time_taken}ms', extra={"data": data, "info": info})


def render_string(raw_str: str, *, base_path: str = ".", **kwargs) -> str:
    """
    Given a template string, render it with the given keyword arguments

    Arguments:
        raw_str: The template string
        kwargs: The keyword arguments

    Returns:
        The rendered string
    """
    j2_env = j2.Environment(loader=j2.FileSystemLoader(base_path))
    template = j2_env.from_string(raw_str)
    return template.render(kwargs)


def read_file(filepath: FilePath) -> str:
    """
    Reads a file and return its content if required

    Arguments:
        filepath (str | pathlib.Path): The path to the file to read

    Returns:
        Content of the file, or None if doesn't exist and not required
    """
    try:
        with open(filepath, 'r') as f:
            return f.read()
    except FileNotFoundError as e:
        raise ConfigurationError(f"Required file not found: '{str(filepath)}'") from e


def normalize_name(name: str) -> str:
    """
    Normalizes names to the convention of the squirrels manifest file.

    Arguments:
        name: The name to normalize.

    Returns:
        The normalized name.
    """
    return name.replace('-', '_')


def normalize_name_for_api(name: str) -> str:
    """
    Normalizes names to the REST API convention.

    Arguments:
        name: The name to normalize.

    Returns:
        The normalized name.
    """
    return name.replace('_', '-')


def load_json_or_comma_delimited_str_as_list(input_str: Union[str, Sequence]) -> Sequence[str]:
    """
    Given a string, load it as a list either by json string or comma delimited value

    Arguments:
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


X = TypeVar('X'); Y = TypeVar('Y')
def process_if_not_none(input_val: Optional[X], processor: Callable[[X], Y]) -> Optional[Y]:
    """
    Given a input value and a function that processes the value, return the output of the function unless input is None

    Arguments:
        input_val: The input value
        processor: The function that processes the input value
    
    Returns:
        The output type of "processor" or None if input value if None
    """
    if input_val is None:
        return None
    return processor(input_val)


@lru_cache(maxsize=1)
def _read_duckdb_init_sql() -> tuple[str, Path | None]:
    """
    Reads and caches the duckdb init file content.
    Returns None if file doesn't exist or is empty.
    """
    try:
        init_contents = []
        global_init_path = Path(os.path.expanduser('~'), c.GLOBAL_ENV_FOLDER, c.DUCKDB_INIT_FILE)
        if global_init_path.exists():
            with open(global_init_path, 'r') as f:
                init_contents.append(f.read())
        
        if Path(c.DUCKDB_INIT_FILE).exists():
            with open(c.DUCKDB_INIT_FILE, 'r') as f:
                init_contents.append(f.read())
        
        init_sql = "\n".join(init_contents).strip()
        target_init_path = None
        if init_sql:
            target_init_path = Path(c.TARGET_FOLDER, c.DUCKDB_INIT_FILE)
            target_init_path.parent.mkdir(parents=True, exist_ok=True)
            target_init_path.write_text(init_sql)
        
        return init_sql, target_init_path
    except Exception as e:
        raise ConfigurationError(f"Failed to read {c.DUCKDB_INIT_FILE}: {str(e)}") from e

def create_duckdb_connection(filepath: str | Path = ":memory:", *, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """
    Creates a DuckDB connection and initializes it with statements from duckdb init file

    Arguments:
        filepath: Path to the DuckDB database file. Defaults to in-memory database.
        read_only: Whether to open the database in read-only mode. Defaults to False.
    
    Returns:
        A DuckDB connection (which must be closed after use)
    """
    conn = duckdb.connect(filepath, read_only=read_only)
    
    try:
        init_sql, _ = _read_duckdb_init_sql()
        if init_sql:
            conn.execute(init_sql)
    except Exception as e:
        conn.close()
        raise ConfigurationError(f"Failed to execute {c.DUCKDB_INIT_FILE}: {str(e)}") from e
    
    return conn


def run_sql_on_dataframes(sql_query: str, dataframes: dict[str, pl.LazyFrame]) -> pl.DataFrame:
    """
    Runs a SQL query against a collection of dataframes

    Arguments:
        sql_query: The SQL query to run
        dataframes: A dictionary of table names to their polars LazyFrame
    
    Returns:
        The result as a polars Dataframe from running the query
    """
    duckdb_conn = create_duckdb_connection()
    
    try:
        for name, df in dataframes.items():
            duckdb_conn.register(name, df)
        
        result_df = duckdb_conn.sql(sql_query).pl()
    finally:
        duckdb_conn.close()
    
    return result_df


def load_yaml_config(filepath: FilePath) -> dict:
    """
    Loads a YAML config file

    Arguments:
        filepath: The path to the YAML file
    
    Returns:
        A dictionary representation of the YAML file
    """
    try:
        with open(filepath, 'r') as f:
            return yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ConfigurationError(f"Failed to parse yaml file: {filepath}") from e


def run_duckdb_stmt(
    logger: Logger, duckdb_conn: duckdb.DuckDBPyConnection, stmt: str, *, params: dict[str, Any] | None = None, redacted_values: list[str] = []
) -> duckdb.DuckDBPyConnection:
    """
    Runs a statement on a DuckDB connection

    Arguments:
        logger: The logger to use
        duckdb_conn: The DuckDB connection
        stmt: The statement to run
        params: The parameters to use
        redacted_values: The values to redact
    """
    redacted_stmt = stmt
    for value in redacted_values:
        redacted_stmt = redacted_stmt.replace(value, "[REDACTED]")
    
    logger.info(f"Running statement: {redacted_stmt}", extra={"data": {"params": params}})
    try:
        return duckdb_conn.execute(stmt, params)
    except duckdb.ParserException as e:
        logger.error(f"Failed to run statement: {redacted_stmt}", exc_info=e)
        raise e


def get_current_time() -> str:
    """
    Returns the current time in the format HH:MM:SS.ms
    """
    return datetime.now().strftime('%H:%M:%S.%f')[:-3]


def parse_dependent_tables(sql_query: str, all_table_names: Iterable[str]) -> tuple[set[str], sqlglot.Expression]:
    """
    Parses the dependent tables from a SQL query

    Arguments:
        sql_query: The SQL query to parse
        all_table_names: The list of all table names
    
    Returns:
        The set of dependent tables
    """
    # Parse the SQL query and extract all table references
    parsed = sqlglot.parse_one(sql_query)
    dependencies = set()
    
    # Collect all table references from the parsed SQL
    for table in parsed.find_all(sqlglot.expressions.Table):
        if table.name in set(all_table_names):
            dependencies.add(table.name)
    
    return dependencies, parsed


async def asyncio_gather(coroutines: list):
    tasks = [asyncio.create_task(coro) for coro in coroutines]
    
    try:
        return await asyncio.gather(*tasks)
    except BaseException:
        # Cancel all tasks
        for task in tasks:
            if not task.done():
                task.cancel()
        # Wait for tasks to be cancelled
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
