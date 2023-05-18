from .parameter_options import SelectParameterOption, DateParameterOption, NumberParameterOption, NumRangeParameterOption
from .parameters import Parameter, SingleSelectParameter, MultiSelectParameter, DateParameter, NumberParameter, NumRangeParameter, DataSourceParameter
from .data_sources import SelectionDataSource, DateDataSource, NumberDataSource, NumRangeDataSource
from .parameter_set import ParameterSet
from .connection_set import ConnectionSet


def get_credential(key: str):
    """
    Gets the username and password that was set through "$squirrels set-credential [key]"

    Parameters:
        key (str): The credential key
    
    Returns:
        Credential: Object with attributes "username" and "password"
    """
    from ._credentials_manager import squirrels_config_io
    return squirrels_config_io.get_credential(key)
