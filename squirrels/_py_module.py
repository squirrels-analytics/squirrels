from typing import Type, Optional, Any
import importlib.util

from . import _constants as c, _utils as u
from ._exceptions import ConfigurationError, FileExecutionError


class PyModule:
    def __init__(self, filepath: u.FilePath, *, default_class: Optional[Type] = None, is_required: bool = False) -> None:
        """
        Constructor for PyModule, an abstract module for a file that may or may not exist
        
        Arguments:
            filepath (str | pathlib.Path): The file path to the python module
            is_required: If true, throw an error if the file path doesn't exist
        """
        self.filepath = str(filepath)
        try:
            spec = importlib.util.spec_from_file_location(self.filepath, self.filepath)
            assert spec is not None and spec.loader is not None
            self.module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(self.module)
        except FileNotFoundError as e:
            if is_required:
                raise ConfigurationError(f"Required file not found: '{self.filepath}'") from e
            self.module = default_class
    
    def get_func_or_class(self, attr_name: str, *, default_attr: Any = None, is_required: bool = True) -> Any:
        """
        Get an attribute of the module. Usually a python function or class.

        Arguments:
            attr_name: The attribute name
            default_attr: The default function or class to use if the attribute cannot be found
            is_required: If true, throw an error if the attribute cannot be found, unless default_attr is not None
        
        Returns:
            The attribute of the module
        """
        func_or_class = getattr(self.module, attr_name, default_attr)
        if func_or_class is None and is_required:
            raise ConfigurationError(f"Module '{self.filepath}' missing required attribute '{attr_name}'")
        return func_or_class


def run_pyconfig_main(base_path: str, filename: str, kwargs: dict[str, Any] = {}) -> Any | None:
    """
    Given a python file in the 'pyconfigs' folder, run its main function
    
    Arguments:
        base_path: The base path of the project
        filename: The name of the file to run main function
        kwargs: Dictionary of the main function arguments
    """
    filepath = u.Path(base_path, c.PYCONFIGS_FOLDER, filename)
    module = PyModule(filepath)
    main_function = module.get_func_or_class(c.MAIN_FUNC, is_required=False)
    if main_function:
        try:
            return main_function(**kwargs)
        except Exception as e:
            raise FileExecutionError(f'Failed to run python file "{filepath}"', e) from e
