import pytest

from squirrels import _parameter_configs as pc, _parameter_options as po, _parameters as p
from squirrels._schemas.auth_models import CustomUserFields, GuestUser

class TestCustomUserFields(CustomUserFields):
    organization: str = ""

def create_test_user(organization: str = ""):
    custom_fields = TestCustomUserFields(organization=organization)
    return GuestUser(username="test", custom_fields=custom_fields)


@pytest.fixture(scope="module")
def ms_config_basic() -> pc.MultiSelectParameterConfig:
    param_options = (
        po.SelectParameterOption('ms0', 'Multi Option 1', is_default=False, user_groups=["org1", "org2"]), 
        po.SelectParameterOption('ms1', 'Multi Option 2', is_default=False, user_groups=["org1", "org3"]),
        po.SelectParameterOption('ms2', 'Multi Option 3', is_default=False, user_groups=["org2", "org3"]), 
        po.SelectParameterOption('ms3', 'Multi Option 4', is_default=True, user_groups=["org1"])
    )
    return pc.MultiSelectParameterConfig("multi_select_basic", "Multi Select Basic", param_options, user_attribute="custom_fields.organization")


@pytest.fixture(scope="module")
def ms_param_basic(ms_config_basic: pc.MultiSelectParameterConfig) -> p.MultiSelectParameter:
    selectable_options = [ms_config_basic.all_options[x] for x in [0,1,3]]
    return p.MultiSelectParameter(ms_config_basic, selectable_options, ['ms3'])


@pytest.fixture(scope="module")
def ss_config_with_ms_parent() -> pc.SingleSelectParameterConfig:
    param_options = (
        po.SelectParameterOption('ss0', 'Single Option 1', is_default=False, parent_option_ids=['ms0', 'ms1']), 
        po.SelectParameterOption('ss1', 'Single Option 2', is_default=False, parent_option_ids=['ms1', 'ms2']),
        po.SelectParameterOption('ss2', 'Single Option 3', is_default=True, parent_option_ids=['ms2', 'ms3']),
        po.SelectParameterOption('ss3', 'Single Option 4', is_default=True, parent_option_ids=['ms3', 'ms0'])
    )
    return pc.SingleSelectParameterConfig(
        "single_select_with_ms_parent", "Single With Parent 1", param_options, parent_name="multi_select_basic"
    )


@pytest.fixture(scope="module")
def ss_param_with_ms_parent(ss_config_with_ms_parent: pc.SingleSelectParameterConfig) -> p.SingleSelectParameter:
    selectable_options = [ss_config_with_ms_parent.all_options[x] for x in [2,3]]
    return p.SingleSelectParameter(ss_config_with_ms_parent, selectable_options, 'ss2')


@pytest.fixture(scope="module")
def ms_config_with_ms_parent() -> pc.MultiSelectParameterConfig:
    param_options = (
        po.SelectParameterOption('ms00', 'Multi Option 1', parent_option_ids=['ms0']),
        po.SelectParameterOption('ms01', 'Multi Option 2', parent_option_ids=['ms0', 'ms1']),
        po.SelectParameterOption('ms02', 'Multi Option 3', parent_option_ids=['ms2', 'ms3'])
    )
    return pc.MultiSelectParameterConfig("multi_select_with_ms_parent", "Multi With Parent 1", param_options,
                                         parent_name="multi_select_basic")


@pytest.fixture(scope="module")
def ss_config_with_ss_parent() -> pc.SingleSelectParameterConfig:
    param_options = (
        po.SelectParameterOption('ss00', 'Single Option 1', parent_option_ids=['ss0']),
        po.SelectParameterOption('ss01', 'Single Option 2', parent_option_ids=['ss1']),
        po.SelectParameterOption('ss02', 'Single Option 3', parent_option_ids=['ss0', 'ss1'])
    )
    return pc.SingleSelectParameterConfig("single_select_with_ss_parent", "Single With Parent 2", param_options,
                                          parent_name="single_select_with_ms_parent")


@pytest.fixture(scope="module")
def ms_config_with_ss_parent() -> pc.MultiSelectParameterConfig:
    param_options = (
        po.SelectParameterOption('ms00', 'Multi Option 1', parent_option_ids=['ss0']),
        po.SelectParameterOption('ms01', 'Multi Option 2', parent_option_ids=['ss0', 'ss1']),
        po.SelectParameterOption('ms02', 'Multi Option 3', parent_option_ids=['ss1'])
    )
    return pc.MultiSelectParameterConfig("multi_select_with_ss_parent", "Multi With Parent 2", param_options,
                                         parent_name="single_select_with_ms_parent")


@pytest.fixture(scope="module")
def date_config_with_parent() -> pc.DateParameterConfig:
    param_options = [
        po.DateParameterOption('2023-01-01', user_groups=['org0'], parent_option_ids=['ss2']),
        po.DateParameterOption('2023-02-01', user_groups=['org1', 'org3'], parent_option_ids=['ss1', 'ss2']),
        po.DateParameterOption('2023-03-01', user_groups=['org1'], parent_option_ids=['ss3'])
    ]
    return pc.DateParameterConfig(
        "date_param_with_parent", "Date With Parent", param_options, 
        user_attribute="custom_fields.organization", parent_name="single_select_with_ms_parent"
    )


@pytest.fixture(scope="module")
def date_range_config() -> pc.DateRangeParameterConfig:
    param_options = [po.DateRangeParameterOption("01-01-2023", "12-31-2023", date_format="%m-%d-%Y")]
    return pc.DateRangeParameterConfig("date_range_param", "Date Range Param", param_options)


@pytest.fixture(scope="module")
def num_config_with_parent() -> pc.NumberParameterConfig:
    param_options = [
        po.NumberParameterOption(0, 5, default_value=3, parent_option_ids="ss2"),
        po.NumberParameterOption(0, 10, parent_option_ids="ss3")
    ]
    return pc.NumberParameterConfig("num_param", "Number Param", param_options, parent_name="single_select_with_ms_parent")


@pytest.fixture(scope="module")
def num_range_config() -> pc.NumberRangeParameterConfig:
    param_options = [
        po.NumberRangeParameterOption(0, 5, user_groups="org0"),
        po.NumberRangeParameterOption("10.5", "15.3", increment="0.4", user_groups="org1")
    ]
    return pc.NumberRangeParameterConfig("num_range_param", "Number Range Param", param_options, user_attribute="custom_fields.organization")
