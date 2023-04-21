from squirrels.configs.parameter_options import SelectParameterOption, DateParameterOption, NumberParameterOption, RangeParameterOption
from squirrels.configs.parameters import WidgetType, Parameter, SingleSelectParameter, MultiSelectParameter, DateParameter, NumberParameter, RangeParameter
from squirrels.configs.data_sources import SelectionDataSource, DateDataSource, NumberDataSource, RangeDataSource, DataSourceParameter
from squirrels.configs.parameter_set import ParameterSet
from squirrels._version import __version__, major_version, minor_version, patch_version
from squirrels.credentials_manager import Credential

def get_credential(key: str) -> Credential:
    from squirrels.credentials_manager import squirrels_config_io
    return squirrels_config_io.get_credential(key)
