from typing import List
import pytest

from squirrels import parameter_options as po, _utils as u


@pytest.fixture(scope="module")
def select_option_basic() -> po.SelectParameterOption:
    return po.SelectParameterOption('o1', 'One')


@pytest.fixture(scope="module")
def select_option_user_groups() -> po.SelectParameterOption:
    return po.SelectParameterOption('a', 'A', user_groups=["org1", "org2"])


@pytest.fixture(scope="module")
def select_option_parents1() -> po.SelectParameterOption:
    return po.SelectParameterOption('a', 'A', parent_option_ids=["par1", "par2"])


@pytest.fixture(scope="module")
def select_option_parents2() -> po.SelectParameterOption:
    return po.SelectParameterOption('a', 'A', parent_option_ids=["par3", "par4"])


@pytest.mark.parametrize('select_option_name,expected', [
    ('select_option_basic', [False, False]),
    ('select_option_user_groups', [False, True]),
    ('select_option_parents1', [True, False]),
    ('select_option_parents2', [False, False])
])
def test_is_valid(select_option_name: str, expected: List[bool], request: pytest.FixtureRequest):
    param_opt: po.SelectParameterOption = request.getfixturevalue(select_option_name)
    assert param_opt._is_valid(None, None)
    assert param_opt._is_valid(None, []) == False
    assert param_opt._is_valid('nonexistent', None) == False
    assert param_opt._is_valid(None, ['par1', 'par10']) == expected[0]
    assert param_opt._is_valid('org1', None) == expected[1]


def test_get_custom_field(select_option_basic: po.SelectParameterOption):
    assert select_option_basic.get_custom_field('id') == 'o1'
    assert select_option_basic.get_custom_field('label') == 'One'
    assert select_option_basic.get_custom_field('order', default_field='id') == 'o1'
    assert select_option_basic.get_custom_field('order', default='test') == 'test'
    assert select_option_basic.get_custom_field('order', default_field='iden', default='test') == 'test'
    assert select_option_basic.get_custom_field('order', default_field='id', default='test') == 'o1'
    with pytest.raises(u.ConfigurationError):
        select_option_basic.get_custom_field('order')
    with pytest.raises(u.ConfigurationError):
        select_option_basic.get_custom_field('order', default_field='iden')
    
    param_opt = po.SelectParameterOption('op1', 'Op 1', custom_fields={'a':'1', 'b':'0'}, b='2', c='3')
    assert param_opt.get_custom_field('a') == '1'
    assert param_opt.get_custom_field('b') == '0'
    assert param_opt.get_custom_field('c') == '3'


def test_invalid_date_parameter_options():
    with pytest.raises(u.ConfigurationError):
        po.DateParameterOption('20200101')
    with pytest.raises(u.ConfigurationError):
        po.DateRangeParameterOption('2023-01-01', '2022-01-01')
    
def test_invalid_number_parameter_options():
    with pytest.raises(u.ConfigurationError):
        po.NumberParameterOption(10, 0)
    with pytest.raises(u.ConfigurationError):
        po.NumberParameterOption(0, 10, increment=3)
    with pytest.raises(u.ConfigurationError):
        po.NumberParameterOption(0, 2, default_value=5)
    with pytest.raises(u.ConfigurationError):
        po.NumberParameterOption(2, 4, default_value=0)
    with pytest.raises(u.ConfigurationError):
        po.NumberParameterOption(0, 4, increment=3)
    with pytest.raises(u.ConfigurationError):
        po.NumberParameterOption(0, 4, increment=2, default_value=3)

def test_invalid_numrange_parameter_options():
    with pytest.raises(u.ConfigurationError):
        po.NumRangeParameterOption(2, 8, default_lower_value=0)
    with pytest.raises(u.ConfigurationError):
        po.NumRangeParameterOption(2, 8, default_lower_value=10)
    with pytest.raises(u.ConfigurationError):
        po.NumRangeParameterOption(2, 8, default_upper_value=0)
    with pytest.raises(u.ConfigurationError):
        po.NumRangeParameterOption(2, 8, default_upper_value=10)
    with pytest.raises(u.ConfigurationError):
        po.NumRangeParameterOption(2, 8, default_lower_value=6, default_upper_value=4)
    with pytest.raises(u.ConfigurationError):
        po.NumRangeParameterOption(2, 8, default_lower_value=6, increment=3)
    with pytest.raises(u.ConfigurationError):
        po.NumRangeParameterOption(2, 8, default_upper_value=6, increment=3)
