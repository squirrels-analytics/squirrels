from typing import Tuple
from .parameter_options import SelectParameterOption, DateParameterOption, DateRangeParameterOption, NumberParameterOption, NumRangeParameterOption
from .parameters import Parameter, SingleSelectParameter, MultiSelectParameter, DateParameter, DateRangeParameter, NumberParameter, NumRangeParameter
from .data_sources import SingleSelectDataSource, MultiSelectDataSource, DateDataSource, DateRangeDataSource, NumberDataSource, NumRangeDataSource
from .connection_set import ConnectionSet, sqldf
from .user_base import UserBase, WrongPassword


def get_env_var(key: str) -> str:
    """
    Gets the environment variable set in .squirrelscfg.yaml

    Parameters:
        key (str): The environment variable key
    
    Returns:
        A string
    """
    from ._environcfg import EnvironConfigIO
    return EnvironConfigIO.obj.get_env_var(key)


def get_credential(key: str) -> Tuple[str, str]:
    """
    Gets the username and password for credentials set in .squirrelscfg.yaml

    Parameters:
        key (str): The credential key
    
    Returns:
        Tuple of two strings
    """
    from ._environcfg import EnvironConfigIO
    return EnvironConfigIO.obj.get_credential(key)
