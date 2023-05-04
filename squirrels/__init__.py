from .param_configs.parameter_options import SelectParameterOption, DateParameterOption, NumberParameterOption, NumRangeParameterOption
from .param_configs.parameters import WidgetType, Parameter, SingleSelectParameter, MultiSelectParameter, DateParameter, NumberParameter, NumRangeParameter
from .param_configs.data_sources import SelectionDataSource, DateDataSource, NumberDataSource, RangeDataSource, DataSourceParameter
from .param_configs.parameter_set import ParameterSet
from .credentials_manager import Credential
from .connection_set import ConnectionSet
from .version import __version__, major_version, minor_version, patch_version

def get_credential(key: str) -> Credential:
    from .credentials_manager import squirrels_config_io
    return squirrels_config_io.get_credential(key)
