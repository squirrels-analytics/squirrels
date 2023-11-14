import pytest

from squirrels import _parameter_configs as pc, parameters as p, parameter_options as po


@pytest.fixture(scope="package")
def multi_select_config_grandparent() -> pc.MultiSelectParameterConfig:
    param_options = (
        po.SelectParameterOption('gp0', 'Option 1'), po.SelectParameterOption('gp1', 'Option 2'),
        po.SelectParameterOption('gp2', 'Option 3'), po.SelectParameterOption('gp3', 'Option 4')
    )
    return pc.MultiSelectParameterConfig("multi_select_grandparent", "Multi Select Grandparent", param_options)


