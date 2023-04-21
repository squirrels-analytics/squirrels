from squirrels.parameter_configs import ParameterOption, WidgetType, Parameter, ParameterSet
from squirrels.parameter_configs import SingleSelectParameter, MultiSelectParameter, DateParameter, NumberParameter, RangeParameter, DataSourceParameter
from squirrels.parameter_configs import OptionsDataSource, NumberDataSource, RangeDataSource, DateDataSource
import os

version_file = os.path.join(os.path.dirname(__file__), 'version.txt')
with open(version_file, 'r') as f:
    __version__ = f.read()

major_version, minor_version, patch_version = __version__.split('.')
