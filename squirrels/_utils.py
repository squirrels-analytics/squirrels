from typing import List, Dict, Optional, Union, Any, TypeVar, Callable
from types import ModuleType
from pathlib import Path
from importlib.machinery import SourceFileLoader
from pandas.api import types as pd_types
import json, jinja2 as j2, pandas as pd

from . import _constants as c

FilePath = Union[str, Path]


# Custom Exceptions
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


# Utility functions/variables
def join_paths(*paths: FilePath) -> Path:
    """
    Joins paths together.

    Parameters:
        paths: The paths to join.

    Returns:
        The joined path.
    """
    return Path(*paths)


_j2_env = j2.Environment(loader=j2.FileSystemLoader('.'))

def render_string(raw_str: str, kwargs: Dict) -> str:
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
        filepath: The path to the file to read
        is_required: If true, throw error if file doesn't exist

    Returns:
        Content of the file, or None if doesn't exist and not required
    """
    def file_handler(filepath: FilePath):
        with open(filepath, 'r') as f:
            return f.read()
    return __process_file_handler(file_handler, filepath, is_required)


def import_file_as_module(filepath: FilePath) -> ModuleType:
    """
    Imports a python file as a module.

    Parameters:
        filepath: The path to the file to import.

    Returns:
        The imported module.
    """
    filepath = str(filepath)
    return SourceFileLoader(filepath, filepath).load_module()


def get_py_main(filepath: FilePath, *, is_required: bool = False) -> Optional[Callable]:
    """
    Given the full path to a python file, get its main function
    
    Parameters:
        filepath: The path to the python file with main function
        is_required: If true, throw error if file doesn't exist
    
    Returns:
        The main function of the python file
    """
    try:
        module = import_file_as_module(filepath)
    except FileNotFoundError as e:
        if is_required:
            raise ConfigurationError(f"Required file not found: '{str(filepath)}'") from e
        return
    
    try:
        return module.main
    except AttributeError as e:
        raise ConfigurationError(f"Python file missing main function: '{str(filepath)}'") from e


def run_pyconfig_main(filename: str, kwargs: Dict[str, Any] = {}) -> None:
    """
    Given a python file in the 'pyconfigs' folder, run its main function
    
    Parameters:
        filename: The name of the file to run main function
        kwargs: Dictionary of the main function arguments
    """
    filepath = join_paths(c.PYCONFIG_FOLDER, filename)
    main_function = get_py_main(filepath)
    if main_function:
        try:
            main_function(**kwargs)
        except Exception as e:
            raise ConfigurationError(f'Error in the python file: "{filepath}"\n  See above for more details') from e


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


def df_to_json0(df: pd.DataFrame, dimensions: List[str] = None) -> Dict[str, Any]:
    """
    Convert a pandas DataFrame to the same JSON format that the dataset result API of Squirrels outputs.

    Parameters:
        df: The dataframe to convert into JSON
        dimensions: The list of declared dimensions. If None, all non-numeric columns are assumed as dimensions

    Returns:
        The JSON response of a Squirrels dataset result API
    """
    in_df_json = json.loads(df.to_json(orient='table', index=False))
    out_fields = []
    non_numeric_fields = []
    for in_column in in_df_json["schema"]["fields"]:
        col_name: str = in_column["name"]
        out_column = {"name": col_name, "type": in_column["type"]}
        out_fields.append(out_column)
        
        if not pd_types.is_numeric_dtype(df[col_name].dtype):
            non_numeric_fields.append(col_name)
    
    out_dimensions = non_numeric_fields if dimensions is None else dimensions
    out_schema = {"fields": out_fields, "dimensions": out_dimensions}
    return {"schema": out_schema, "data": in_df_json["data"]}


def load_json_or_comma_delimited_str_as_list(input_str: str) -> List[str]:
    """
    Given a string, load it as a list either by json string or comma delimited value

    Parameters:
        input_str: The input string
    
    Returns:
        The list representation of the input string
    """
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
