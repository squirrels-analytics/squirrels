from squirrels.param_configs.parameter_options import SelectParameterOption, DateParameterOption, NumberParameterOption, RangeParameterOption
from squirrels.param_configs.parameters import WidgetType, Parameter, SingleSelectParameter, MultiSelectParameter, DateParameter, NumberParameter, RangeParameter
from squirrels.param_configs.data_sources import SelectionDataSource, DateDataSource, NumberDataSource, RangeDataSource, DataSourceParameter
from squirrels.param_configs.parameter_set import ParameterSet
from squirrels.credentials_manager import Credential
from squirrels.connection_set import DBAPIConnection, QueuePool, ConnectionSet
from squirrels.version import __version__, major_version, minor_version, patch_version

def get_credential(key: str) -> Credential:
    from squirrels.credentials_manager import squirrels_config_io
    return squirrels_config_io.get_credential(key)
