from typing import Sequence, Optional, Union, TypeVar, Callable, Any
from pathlib import Path
from pandas.api import types as pd_types
import os, json, sqlite3, jinja2 as j2, pandas as pd

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

class FileExecutionError(Exception):
    def __init__(self, message: str, error: Exception, *args) -> None:
        t = "  "
        new_message = f"\n" + message + f"\n{t}Produced error message:\n{t}{t}{error} (see above for more details on handled exception)"
        super().__init__(new_message, *args)
        self.error = error


## Other utility classes

class MacroLoader(j2.FileSystemLoader):
    def __init__(self, searchpath, macros_dirs, *, encoding='utf-8', followlinks=False):
        super().__init__(searchpath, encoding, followlinks)
        self.macros_dirs = macros_dirs
        self.macro_templates = self._load_macro_templates()

    def _load_macro_templates(self):
        macro_templates = []
        for macros_dir in self.macros_dirs:
            for root, _, files in os.walk(macros_dir):
                files: list[str]
                for filename in files:
                    if filename.endswith('.sql'):
                        filepath = Path(root, filename)
                        print(f"Loaded macros from: {filepath}")
                        with open(filepath, 'r', encoding=self.encoding) as f:
                            content = f.read()
                        macro_templates.append(content)
        return macro_templates

    def get_source(self, environment, template):
        try:
            source, filename, uptodate = super().get_source(environment, template)
            for macro_template in self.macro_templates:
                source = macro_template + '\n' + source
            return source, filename, uptodate
        except j2.TemplateNotFound:
            raise j2.TemplateNotFound(template)


## Utility functions/variables
    
def get_macro_folders_from_packages() -> list[Path]:
    packages_folder = c.PACKAGES_FOLDER
    if not os.path.exists(packages_folder):
        return []
    
    subdirectories = []
    for item in os.listdir(packages_folder):
        item_path = Path(packages_folder, item)
        if os.path.isdir(item_path):
            subdirectories.append(Path(item_path, c.MACROS_FOLDER))
    
    return subdirectories


def render_string(base_path: str, raw_str: str, **kwargs) -> str:
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


def run_sql_on_dataframes(sql_query: str, dataframes: dict[str, pd.DataFrame], do_use_duckdb: bool) -> pd.DataFrame:
    """
    Runs a SQL query against a collection of dataframes

    Arguments:
        sql_query: The SQL query to run
        dataframes: A dictionary of table names to their pandas Dataframe
    
    Returns:
        The result as a pandas Dataframe from running the query
    """
    if do_use_duckdb:
        import duckdb
        duckdb_conn = duckdb.connect()
    else:
        conn = sqlite3.connect(":memory:")
    
    try:
        for name, df in dataframes.items():
            if do_use_duckdb:
                duckdb_conn.execute(f"CREATE TABLE {name} AS FROM df")
            else:
                df.to_sql(name, conn, index=False)
        
        return duckdb_conn.execute(sql_query).df() if do_use_duckdb else pd.read_sql(sql_query, conn)
    finally:
        duckdb_conn.close() if do_use_duckdb else conn.close()


def df_to_json0(df: pd.DataFrame, dimensions: list[str] | None = None) -> dict:
    """
    Convert a pandas DataFrame to the response format that the dataset result API of Squirrels outputs.

    Arguments:
        df: The dataframe to convert into an API response
        dimensions: The list of declared dimensions. If None, all non-numeric columns are assumed as dimensions

    Returns:
        The response of a Squirrels dataset result API
    """
    in_df_json = json.loads(df.to_json(orient='table', index=False))
    out_fields = []
    non_numeric_fields = []
    for in_column in in_df_json["schema"]["fields"]:
        col_name: str = in_column["name"]
        out_column = { "name": col_name, "type": in_column["type"] }
        out_fields.append(out_column)
        
        if not pd_types.is_numeric_dtype(df[col_name].dtype):
            non_numeric_fields.append(col_name)
    
    out_dimensions = non_numeric_fields if dimensions is None else dimensions
    dataset_json = {
        "schema": { "fields": out_fields, "dimensions": out_dimensions },
        "data": in_df_json["data"]
    }
    return dataset_json
