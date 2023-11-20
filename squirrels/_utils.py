from typing import List, Dict, Optional, Union, Any, TypeVar, Callable
from types import ModuleType
from pathlib import Path
from importlib.machinery import SourceFileLoader
import json

from ._timed_imports import jinja2 as j2, pandas as pd, pd_types

FilePath = Union[str, Path]


# Custom Exceptions
class InvalidInputError(Exception):
    pass

class ConfigurationError(Exception):
    pass


# Utility functions/variables
_j2_env = j2.Environment(loader=j2.FileSystemLoader('.'))

def render_string(raw_str: str, kwargs: Dict):
    template = _j2_env.from_string(raw_str)
    return template.render(kwargs)


def import_file_as_module(filepath: Optional[FilePath]) -> Optional[ModuleType]:
    """
    Imports a python file as a module.

    Parameters:
        filepath: The path to the file to import.

    Returns:
        The imported module.
    """
    filepath = str(filepath) if filepath is not None else None
    return SourceFileLoader(filepath, filepath).load_module() if filepath is not None else None


def run_module_main(filepath: Optional[FilePath], kwargs: Dict[str, Any]) -> Optional[ModuleType]:
    try:
        module = import_file_as_module(filepath)
    except FileNotFoundError:
        module = None
    
    if module is not None:
        try:
            return module.main(**kwargs)
        except Exception as e:
            raise ConfigurationError(f'Error in the {filepath} file') from e


def join_paths(*paths: FilePath) -> Path:
    """
    Joins paths together.

    Parameters:
        paths: The paths to join.

    Returns:
        The joined path.
    """
    return Path(*paths)


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


def df_to_json(df: pd.DataFrame, dimensions: List[str] = None) -> Dict[str, Any]:
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
    return {"response_version": 0, "schema": out_schema, "data": in_df_json["data"]}


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
    else:
        return [] if input_str == "" else input_str.split(",")


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
