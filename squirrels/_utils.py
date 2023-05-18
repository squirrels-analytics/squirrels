from typing import List, Optional, Union, Any
from types import ModuleType
from pathlib import Path
from importlib.machinery import SourceFileLoader
import json

from squirrels._timed_imports import jinja2 as j2, pandas as pd

FilePath = Union[str, Path]


# Custom Exceptions
class InvalidInputError(Exception):
    pass

class ConfigurationError(Exception):
    pass

class AbstractMethodCallError(NotImplementedError):
    def __init__(self, cls, method, more_message = ""):
        message = f"Abstract method {method}() not implemented in {cls.__name__}."
        super().__init__(message + more_message)


# Utility functions/variables
j2_env = j2.Environment(loader=j2.FileSystemLoader('.'))


def import_file_as_module(filepath: Optional[FilePath]) -> ModuleType:
    """
    Imports a python file as a module.

    Parameters:
        filepath: The path to the file to import.

    Returns:
        The imported module.
    """
    filepath = str(filepath) if filepath is not None else None
    return SourceFileLoader(filepath, filepath).load_module() if filepath is not None else None


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


def get_row_value(row: pd.Series, value: str) -> Any:
    """
    Gets the value of a row from a pandas Series.

    Parameters:
        row: The row to get the value from.
        value: The name of the column to get the value from.

    Returns:
        The value of the column.

    Raises:
        ConfigurationError: If the column does not exist.
    """
    try:
        result = row[value]
    except KeyError as e:
        raise ConfigurationError(f'Column name "{value}" does not exist') from e
    return result


def load_json_or_comma_delimited_str(input_str: str) -> List[str]:
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
