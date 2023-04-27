from typing import Dict, List, Optional, Union, Any
from types import ModuleType
from pathlib import Path
from importlib.machinery import SourceFileLoader

from squirrels.timed_imports import jinja2 as j2, pandas as pd

FilePath = Union[str, Path]


# Custom Exceptions
class InvalidInputError(Exception):
    pass

class ConfigurationError(Exception):
    pass


# Utility functions/variables
j2_env = j2.Environment(loader=j2.FileSystemLoader('.'))

def import_file_as_module(filepath: Optional[FilePath]) -> ModuleType:
    filepath = str(filepath) if filepath is not None else None
    return SourceFileLoader(filepath, filepath).load_module() if filepath is not None else None

def join_paths(*paths: FilePath) -> Path:
    return Path(*paths)

def normalize_name(name: str) -> str:
    return name.replace('-', '_')

def normalize_name_for_api(name: str) -> str:
    return name.replace('_', '-')

def get_row_value(row: pd.Series, value: str) -> Any:
    try:
        result = row[value]
    except KeyError as e:
        raise ConfigurationError(f'Column name "{value}" does not exist') from e
    return result
