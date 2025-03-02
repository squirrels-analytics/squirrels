from collections import OrderedDict
import pytest, polars as pl

from squirrels import _parameter_sets as ps, parameters as p, _parameter_configs as _pc, parameter_options as _po, data_sources as d
from squirrels import _utils as u, _auth as a

from tests.parameter_configs_tests.conftest import User

@pytest.fixture(scope="module")
def user() -> User:
    return User(username="user1", organization="org3")


def add_param(data: dict[str, p.Parameter], param: p.Parameter):
    data[param._config.name] = param


@pytest.fixture(scope="module")
def parameter_set0(ss_config_with_ms_parent: _pc.SingleSelectParameterConfig) -> ps.ParameterSet:
    data = OrderedDict()
    
    ss_options = ss_config_with_ms_parent.all_options
    add_param(data, p.SingleSelectParameter(ss_config_with_ms_parent, ss_options, "ss1"))
    
    return ps.ParameterSet(data)


@pytest.fixture(scope="module")
def parameter_set1(ms_config_basic: _pc.MultiSelectParameterConfig, ss_config_with_ms_parent: _pc.SingleSelectParameterConfig) -> ps.ParameterSet:
    data = OrderedDict()
    
    ss_options = [ss_config_with_ms_parent.all_options[idx] for idx in [0,1,2]]
    add_param(data, p.SingleSelectParameter(ss_config_with_ms_parent, ss_options, "ss1"))
    
    ms_config_copy = ms_config_basic.copy()
    ms_config_copy._add_child_mutate(ss_config_with_ms_parent)

    ms_options = [ms_config_copy.all_options[idx] for idx in [1,2]]
    add_param(data, p.MultiSelectParameter(ms_config_copy, ms_options, []))
    
    return ps.ParameterSet(data)


@pytest.fixture(scope="module")
def parameter_set2(
    ms_config_basic: _pc.MultiSelectParameterConfig, ss_config_with_ms_parent: _pc.SingleSelectParameterConfig, 
    date_config_with_parent: _pc.DateParameterConfig
) -> ps.ParameterSet:
    data = OrderedDict()

    ss_config = ss_config_with_ms_parent.copy()
    ss_config._add_child_mutate(date_config_with_parent)
    ms_config = ms_config_basic.copy()
    ms_config._add_child_mutate(ss_config)

    curr_option = date_config_with_parent.all_options[1]
    add_param(data, p.DateParameter(date_config_with_parent, curr_option, "2023-10-01"))

    ss_options = [ss_config.all_options[idx] for idx in [0,1,2]]
    add_param(data, p.SingleSelectParameter(ss_config, ss_options, "ss1"))

    ms_options = [ms_config.all_options[idx] for idx in [1,2]]
    add_param(data, p.MultiSelectParameter(ms_config, ms_options, []))
    
    return ps.ParameterSet(data)


@pytest.fixture(scope="module")
def param_configs_set1(
    ms_config_basic: _pc.MultiSelectParameterConfig, ss_config_with_ms_parent: _pc.SingleSelectParameterConfig
) -> ps.ParameterConfigsSet:
    config_set = ps.ParameterConfigsSet()
    config_set.add(ss_config_with_ms_parent.copy())
    config_set.add(ms_config_basic.copy())
    config_set._post_process_params({})
    return config_set


@pytest.fixture(scope="module")
def param_configs_set2(
    ms_config_basic: _pc.MultiSelectParameterConfig, ss_config_with_ms_parent: _pc.SingleSelectParameterConfig, 
    date_config_with_parent: _pc.DateParameterConfig
) -> ps.ParameterConfigsSet:
    config_set = ps.ParameterConfigsSet()
    config_set.add(ss_config_with_ms_parent.copy())

    datasource = d.SelectDataSource(
        "ms_table", "my_id", "my_label", is_default_col="my_default_flag", user_group_col="my_user_group"
    )
    ms_ds_param = _pc.DataSourceParameterConfig(
        _pc.MultiSelectParameterConfig, ms_config_basic.name, ms_config_basic.label, datasource, 
        user_attribute=ms_config_basic.user_attribute
    )
    config_set.add(ms_ds_param)

    data_datasource = d.DateDataSource(
        "date_table", "my_default", id_col="my_id", user_group_col="my_user_group", parent_id_col="my_parent_id"
    )
    date_ds_param = _pc.DataSourceParameterConfig(
        _pc.DateParameterConfig, date_config_with_parent.name, date_config_with_parent.label, data_datasource, 
        user_attribute=date_config_with_parent.user_attribute, parent_name=ss_config_with_ms_parent.name
    )
    config_set.add(date_ds_param)

    df_dict = {}

    def make_ms_option(x: _po.SelectParameterOption, user_group: str):
        return {"my_id": x._identifier, "my_label": x._label, "my_default_flag": int(x._is_default), "my_user_group": user_group}
    
    ms_data = [make_ms_option(x, user_group) for x in ms_config_basic.all_options for user_group in x._user_groups]
    df_dict[ms_ds_param.name] = pl.DataFrame(ms_data)

    def make_date_option(x: _po.DateParameterOption, user_group: str, parent_id: str):
        default_date = x._default_date.strftime("%Y-%m-%d")
        return {"my_id": 'id'+default_date, "my_default": default_date, "my_user_group": user_group, "my_parent_id": parent_id}
    
    date_data = [
        make_date_option(x, user_group, parent_id)
        for x in date_config_with_parent.all_options for user_group in x._user_groups for parent_id in x._parent_option_ids
    ]
    df_dict[date_ds_param.name] = pl.DataFrame(date_data)
    
    config_set._post_process_params(df_dict)
    return config_set


def test_parameter_set_to_json_dict(parameter_set1: ps.ParameterSet):
    expected_params = []

    ss_param_json = {
        "widget_type": "single_select",
        "name": "single_select_with_ms_parent",
        "label": "Single With Parent 1",
        "description": "",
        "options": [
            {"id": "ss0", "label": "Single Option 1"},
            {"id": "ss1", "label": "Single Option 2"},
            {"id": "ss2", "label": "Single Option 3"}
        ],
        "trigger_refresh": False,
        "selected_id": "ss1"
    }
    expected_params.append(ss_param_json)

    ms_param_json = {
        "widget_type": "multi_select",
        "name": "multi_select_basic",
        "label": "Multi Select Basic",
        "description": "",
        "options": [
            { "id":"ms1", "label":"Multi Option 2"},
            { "id":"ms2", "label":"Multi Option 3" }
        ],
        "trigger_refresh": True,
        "show_select_all": True,
        "order_matters": False,
        "selected_ids": []
    }
    expected_params.append(ms_param_json)

    expected = {
        "parameters": expected_params
    }
    assert parameter_set1.to_api_response_model0().model_dump() == expected


def test_invalid_non_select_parent():
    configs_set = ps.ParameterConfigsSet()
    configs_set.add(_pc.DateParameterConfig("parent_date", "My Date", (_po.DateParameterOption("2023-01-01"),)))
    configs_set.add(_pc.SingleSelectParameterConfig("child_ss", "My Single Select", (), parent_name="parent_date"))
    with pytest.raises(u.ConfigurationError):
        configs_set._post_process_params({})


def test_invalid_ms_parent_on_non_select_child():
    configs_set = ps.ParameterConfigsSet()
    configs_set.add(_pc.MultiSelectParameterConfig("parent_ms", "My Multi Select", ()))
    configs_set.add(_pc.DateParameterConfig("child_date", "My Date", (_po.DateParameterOption("2023-01-01"),), parent_name="parent_ms"))
    with pytest.raises(u.ConfigurationError):
        configs_set._post_process_params({})


def test_invalid_overlapping_parent_options():
    configs_set = ps.ParameterConfigsSet()
    select_options = [_po.SelectParameterOption("ss0", "Option 0")]
    configs_set.add(_pc.SingleSelectParameterConfig("parent_ss", "My Single Select", select_options))
    number_options = [
        _po.NumberParameterOption(0, 10, parent_option_ids="ss0"),
        _po.NumberParameterOption(0, 20, parent_option_ids="ss0")
    ]
    configs_set.add(_pc.NumberParameterConfig("child_number", "My Number", number_options, parent_name="parent_ss"))
    with pytest.raises(u.ConfigurationError):
        configs_set._post_process_params({})


def test_invalid_overlapping_parent_options_within_user_group():
    configs_set = ps.ParameterConfigsSet()
    select_options = [_po.SelectParameterOption("ss0", "Option 0")]
    configs_set.add(_pc.SingleSelectParameterConfig("parent_ss", "My Single Select", select_options))
    number_options = [
        _po.NumberParameterOption(0, 10, parent_option_ids="ss0", user_groups="org1"),
        _po.NumberParameterOption(0, 20, parent_option_ids="ss0", user_groups="org2"),
        _po.NumberParameterOption(0, 30, parent_option_ids="ss1", user_groups="org2")
    ]
    configs_set.add(_pc.NumberParameterConfig("child_number", "My Number", number_options, parent_name="parent_ss"))
    try:
        configs_set._post_process_params({})
    except u.ConfigurationError:
        pytest.fail("Duplicate parent_option_ids under different user_groups should not error")
    
    number_options.append(_po.NumberParameterOption(0, 40, parent_option_ids="ss1", user_groups="org2"))
    configs_set.add(_pc.NumberParameterConfig("child_number", "My Number", number_options, parent_name="parent_ss"))
    with pytest.raises(u.ConfigurationError):
        configs_set._post_process_params({})


def test_apply_selections1(
    user: a.BaseUser, param_configs_set1: ps.ParameterConfigsSet, parameter_set0: ps.ParameterSet, parameter_set1: ps.ParameterSet
):
    selections = {"single_select_with_ms_parent": "ss1"}

    actual = param_configs_set1.apply_selections(None, selections, user)
    assert actual == parameter_set1

    actual = param_configs_set1.apply_selections(None, selections, user, parent_param="single_select_with_ms_parent")
    assert actual == parameter_set0


def test_apply_selections2(
    user: a.BaseUser, param_configs_set2: ps.ParameterConfigsSet, parameter_set2: ps.ParameterSet
):
    dataset_parms = ["date_param_with_parent", "single_select_with_ms_parent", "multi_select_basic"]
    selections = {
        "single_select_with_ms_parent": "ss1", "date_param_with_parent": "2023-10-01"
    }
    actual_param_set = param_configs_set2.apply_selections(dataset_parms, selections, user)
    assert actual_param_set._parameters_dict == parameter_set2._parameters_dict
