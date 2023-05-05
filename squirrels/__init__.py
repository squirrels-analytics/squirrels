from .param_configs.parameter_options import SelectParameterOption, DateParameterOption, NumberParameterOption, NumRangeParameterOption
from .param_configs.parameters import WidgetType, Parameter, SingleSelectParameter, MultiSelectParameter, DateParameter, NumberParameter, NumRangeParameter
from .param_configs.data_sources import SelectionDataSource, DateDataSource, NumberDataSource, NumRangeDataSource, DataSourceParameter
from .param_configs.parameter_set import ParameterSet
from .connection_set import ConnectionSet
from .version import __version__, major_version, minor_version, patch_version

def get_credential(key: str):
    """
    Gets the username and password that was set through "$squirrels set-credential [key]"

    Parameters:
        key (str): The credential key
    
    Returns:
        Credential: Object with attributes "username" and "password"
    """
    from .credentials_manager import squirrels_config_io
    return squirrels_config_io.get_credential(key)
