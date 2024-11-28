from typing import Sequence, Optional, Union, TypeVar, Callable
from pathlib import Path
from pandas.api import types as pd_types
import os, time, logging, json, duckdb, polars as pl, yaml
import jinja2 as j2, jinja2.nodes as j2_nodes

from . import _constants as c

FilePath = Union[str, Path]

# Polars
type_to_polars_dtype = {
    "str": pl.String,
    "string": pl.String,
    "int": pl.Int64,
    "integer": pl.Int64,
    "int8": pl.Int8,
    "int16": pl.Int16,
    "int32": pl.Int32,
    "int64": pl.Int64,
    "float": pl.Float64,
    "float32": pl.Float32,
    "float64": pl.Float64,
    "bool": pl.Boolean,
    "boolean": pl.Boolean,
    "date": pl.Date,
    "time": pl.Time,
    "datetime": pl.Datetime,
    "timestamp": pl.Datetime,
}


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

class FileExecutionError(Exception):
    def __init__(self, message: str, error: Exception, *args) -> None:
        t = "  "
        new_message = f"\n" + message + f"\n{t}Produced error message:\n{t}{t}{error} (see above for more details on handled exception)"
        super().__init__(new_message, *args)
        self.error = error


## Other utility classes

class Logger(logging.Logger):
    def log_activity_time(self, activity: str, start_timestamp: float, *, request_id: str | None = None) -> None:
        end_timestamp = time.time()
        time_taken = round((end_timestamp-start_timestamp) * 10**3, 3)
        data = { "activity": activity, "start_timestamp": start_timestamp, "end_timestamp": end_timestamp, "time_taken_ms": time_taken }
        info = { "request_id": request_id } if request_id else {}
        self.debug(f'Time taken for "{activity}": {time_taken}ms', extra={"data": data, "info": info})


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
        is_required: If true, throw error if file doesn't exist

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


X, Y = TypeVar('X'), TypeVar('Y')
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


def run_sql_on_dataframes(sql_query: str, dataframes: dict[str, pl.LazyFrame]) -> pl.DataFrame:
    """
    Runs a SQL query against a collection of dataframes

    Arguments:
        sql_query: The SQL query to run
        dataframes: A dictionary of table names to their pandas Dataframe
    
    Returns:
        The result as a pandas Dataframe from running the query
    """
    duckdb_conn = duckdb.connect()
    
    try:
        for name, df in dataframes.items():
            duckdb_conn.register(name, df)
        
        result_df = duckdb_conn.sql(sql_query).pl()
    finally:
        duckdb_conn.close()
    
    return result_df


def df_to_json0(df: pl.DataFrame, dimensions: list[str] | None = None) -> dict:
    """
    Convert a pandas DataFrame to the response format that the dataset result API of Squirrels outputs.

    Arguments:
        df: The dataframe to convert into an API response
        dimensions: The list of declared dimensions. If None, all non-numeric columns are assumed as dimensions

    Returns:
        The response of a Squirrels dataset result API
    """
    df_pandas = df.to_pandas()
    in_df_json = json.loads(df_pandas.to_json(orient='table', index=False))
    out_fields = []
    non_numeric_fields = []
    for in_column in in_df_json["schema"]["fields"]:
        col_name: str = in_column["name"]
        out_column = { "name": col_name, "type": in_column["type"] }
        out_fields.append(out_column)
        
        if not pd_types.is_numeric_dtype(df_pandas[col_name].dtype):
            non_numeric_fields.append(col_name)
    
    out_dimensions = non_numeric_fields if dimensions is None else dimensions
    dataset_json = {
        "schema": { "fields": out_fields, "dimensions": out_dimensions },
        "data": in_df_json["data"]
    }
    return dataset_json


def load_yaml_config(filepath: FilePath) -> dict:
    try:
        with open(filepath, 'r') as f:
            return yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ConfigurationError(f"Failed to parse yaml file: {filepath}") from e
