from copy import copy
from datetime import date
from decimal import Decimal
import pytest, pandas as pd

from squirrels import _parameter_configs as pc, parameters as p, parameter_options as po, data_sources as ds
from squirrels import _connection_set as cs, _seeds as s, _utils as u
from tests.parameter_configs_tests._user_class import User


@pytest.fixture(scope="module")
def user() -> User:
    return User.Create("user1", organization="org1")


class TestMultiSelectParameterConfig:
    def test_with_selection1(
        self, user: User, ms_param_basic: p.MultiSelectParameter, ms_config_basic: pc.MultiSelectParameterConfig
    ):
        param = ms_config_basic.with_selection(None, user=user, parent_param=None)
        assert param == ms_param_basic

    def test_with_selection2(
        self, user: User, ms_param_basic: p.MultiSelectParameter, ms_config_basic: pc.MultiSelectParameterConfig
    ):
        expected = copy(ms_param_basic)
        expected._selected_ids = ("ms0", "ms1")
        param = ms_config_basic.with_selection('["ms0","ms1"]', user=user, parent_param=None)
        assert param == expected

    def test_with_selection3(
        self, user: User, ms_param_basic: p.MultiSelectParameter, ms_config_with_ms_parent: pc.MultiSelectParameterConfig
    ):
        selectable_options = [ms_config_with_ms_parent.all_options[2]]
        expected = p.MultiSelectParameter(ms_config_with_ms_parent, selectable_options, [])
        param = ms_config_with_ms_parent.with_selection(None, user=user, parent_param=ms_param_basic)
        assert param == expected
    
    def test_with_selection4(
        self, user: User, ss_param_with_ms_parent: p.SingleSelectParameter, ms_config_with_ss_parent: pc.MultiSelectParameterConfig
    ):
        expected = p.MultiSelectParameter(ms_config_with_ss_parent, tuple(), tuple())
        param = ms_config_with_ss_parent.with_selection(None, user=user, parent_param=ss_param_with_ms_parent)
        assert param == expected

    def test_invalid_with_selection(self, user: User, ms_config_basic: pc.MultiSelectParameterConfig):
        with pytest.raises(u.InvalidInputError):
            ms_config_basic.with_selection('["ms0","ms2"]', user=user, parent_param=None)


class TestSingleSelectParameterConfig:
    def test_with_selection1(
        self, user: User, ms_param_basic: p.MultiSelectParameter, ss_param_with_ms_parent: p.SingleSelectParameter,
        ss_config_with_ms_parent: pc.SingleSelectParameterConfig
    ):
        param = ss_config_with_ms_parent.with_selection(None, user, ms_param_basic)
        assert param == ss_param_with_ms_parent

    def test_with_selection2(
        self, user: User, ms_param_basic: p.MultiSelectParameter, ss_param_with_ms_parent: p.SingleSelectParameter,
        ss_config_with_ms_parent: pc.SingleSelectParameterConfig
    ):
        expected = copy(ss_param_with_ms_parent)
        expected._selected_id = "ss3"
        param = ss_config_with_ms_parent.with_selection('ss3', user, ms_param_basic)
        assert param == expected
    
    def test_with_selection3(
        self, user: User, ss_param_with_ms_parent: p.SingleSelectParameter, ss_config_with_ss_parent: pc.SingleSelectParameterConfig
    ):
        expected = p.SingleSelectParameter(ss_config_with_ss_parent, tuple(), None)
        param = ss_config_with_ss_parent.with_selection(None, user, ss_param_with_ms_parent)
        assert param == expected
    
    def test_invalid_with_selection(
        self, user: User, ms_param_basic: p.MultiSelectParameter, ss_config_with_ms_parent: pc.SingleSelectParameterConfig
    ):
        with pytest.raises(u.InvalidInputError):
            ss_config_with_ms_parent.with_selection('ss1', user, ms_param_basic)


class TestDateParameterConfig:
    def test_with_selection1(
        self, user: User, ss_param_with_ms_parent: p.SingleSelectParameter, date_config_with_parent: pc.DateParameterConfig
    ):
        curr_option = date_config_with_parent.all_options[1]
        expected = p.DateParameter(date_config_with_parent, curr_option, date(2023, 2, 1))
        param = date_config_with_parent.with_selection(None, user, ss_param_with_ms_parent)
        assert param == expected

    def test_with_selection2(
        self, user: User, ss_param_with_ms_parent: p.SingleSelectParameter, date_config_with_parent: pc.DateParameterConfig
    ):
        curr_option = date_config_with_parent.all_options[1]
        expected = p.DateParameter(date_config_with_parent, curr_option, date(2023, 5, 1))
        param = date_config_with_parent.with_selection("2023-05-01", user, ss_param_with_ms_parent)
        assert param == expected
    
    def test_with_selection3(self, user: User):
        all_options = [po.DateParameterOption("2022-01-01"), po.DateParameterOption("2022-02-01")]
        date_config = pc.DateParameterConfig("date_param", "Date Parameter", all_options)
        expected = p.DateParameter(date_config, all_options[0], date(2022, 1, 1)) # only first option used
        param = date_config.with_selection(None, user, None)
        assert param == expected
    
    def test_invalid_with_selection(self, user: User, date_config_with_parent: pc.DateParameterConfig):
        with pytest.raises(u.InvalidInputError):
            date_config_with_parent.with_selection("01-01-2023", user, None)
        with pytest.raises(u.ConfigurationError):
            date_config_with_parent.with_selection("2023-01-01", None, None)


class TestDateRangeParameterConfig:
    def test_with_selection1(self, user: User, date_range_config: pc.DateRangeParameterConfig):
        curr_option = date_range_config.all_options[0]
        expected = p.DateRangeParameter(date_range_config, curr_option, date(2023,1,1), date(2023,12,31))
        param = date_range_config.with_selection(None, user, None)
        assert param == expected
        
    def test_with_selection2(self, date_range_config: pc.DateRangeParameterConfig):
        curr_option = date_range_config.all_options[0]
        expected = p.DateRangeParameter(date_range_config, curr_option, date(2023,2,1), date(2023,10,31))
        param = date_range_config.with_selection("2023-02-01,2023-10-31", None, None)
        assert param == expected

    def test_invalid_with_selection(self, date_range_config: pc.DateRangeParameterConfig):
        with pytest.raises(u.InvalidInputError):
            date_range_config.with_selection("2023-02-01,2023-10-31,2023-11-30", None, None)
        with pytest.raises(u.InvalidInputError):
            date_range_config.with_selection("2023-02-01", None, None)


class TestNumberParameterConfig:
    def test_with_selection1(self, ss_param_with_ms_parent: p.SingleSelectParameter, num_config_with_parent: pc.NumberParameterConfig):
        curr_option = num_config_with_parent.all_options[0]
        expected = p.NumberParameter(num_config_with_parent, curr_option, Decimal("2"))
        param = num_config_with_parent.with_selection("2", None, ss_param_with_ms_parent)
        assert expected == param
    
    def test_invalid_with_selection(self, ss_param_with_ms_parent: p.SingleSelectParameter, num_config_with_parent: pc.NumberParameterConfig):
        with pytest.raises(u.InvalidInputError):
            num_config_with_parent.with_selection("2.0.0", None, ss_param_with_ms_parent) # not a number
        with pytest.raises(u.InvalidInputError):
            num_config_with_parent.with_selection("8", None, ss_param_with_ms_parent) # out of range
        with pytest.raises(u.InvalidInputError):
            num_config_with_parent.with_selection("2.3", None, ss_param_with_ms_parent) # not in increment


class TestNumberRangeParameterConfig:
    def test_with_selection1(self, user: User, num_range_config: pc.NumberRangeParameterConfig):
        curr_option = num_range_config.all_options[1]
        expected = p.NumberRangeParameter(num_range_config, curr_option, Decimal("10.9"), Decimal("12.1"))
        param = num_range_config.with_selection("10.9, 12.1", user, None)
        assert param == expected

    def test_invalid_with_selection(self, user: User, num_range_config: pc.NumberRangeParameterConfig):
        with pytest.raises(u.InvalidInputError):
            num_range_config.with_selection("10.9", user, None) # only one number
        with pytest.raises(u.InvalidInputError):
            num_range_config.with_selection("10.9,12.1,12.5", user, None) # three numbers
        with pytest.raises(u.InvalidInputError):
            num_range_config.with_selection("10.9.0,12.1", user, None) # wrong number format


class TestDataSourceParameterConfig:
    def test_get_dataframe(self):
        data_source = ds.SelectDataSource("SELECT DISTINCT col_id, col_val FROM seed_test", "col_id", "col_val", from_seeds=True)
        ds_config = pc.DataSourceParameterConfig(pc.SingleSelectParameterConfig, "ds_test", "", data_source)

        input_df = pd.DataFrame({
            "col_id": [1, 1, 2, 2, 3],
            "col_val": ["a", "a", "b", "b", "c"]
        })
        seeds = s.Seeds({"seed_test": input_df})
        output_df = ds_config.get_dataframe("default", cs.ConnectionSet({}), seeds)

        assert output_df.equals(pd.DataFrame({
            "col_id": [1, 2, 3],
            "col_val": ["a", "b", "c"]
        }))
