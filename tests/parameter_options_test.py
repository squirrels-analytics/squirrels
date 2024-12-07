import pytest

from squirrels import parameter_options as _po, _utils as u


@pytest.fixture(scope="module")
def select_option_basic() -> _po.SelectParameterOption:
    return _po.SelectParameterOption('o1', 'One')


@pytest.fixture(scope="module")
def select_option_user_groups() -> _po.SelectParameterOption:
    return _po.SelectParameterOption('a', 'A', user_groups=["org1", "org2"])


@pytest.fixture(scope="module")
def select_option_parents1() -> _po.SelectParameterOption:
    return _po.SelectParameterOption('a', 'A', parent_option_ids=["par1", "par2"])


@pytest.fixture(scope="module")
def select_option_parents2() -> _po.SelectParameterOption:
    return _po.SelectParameterOption('a', 'A', parent_option_ids=["par3", "par4"])


@pytest.mark.parametrize('select_option_name,expected', [
    ('select_option_basic', [False, False]),
    ('select_option_user_groups', [False, True]),
    ('select_option_parents1', [True, False]),
    ('select_option_parents2', [False, False])
])
def test_is_valid(select_option_name: str, expected: list[bool], request: pytest.FixtureRequest):
    param_opt: _po.SelectParameterOption = request.getfixturevalue(select_option_name)
    assert param_opt._is_valid(None, None)
    assert param_opt._is_valid(None, []) == False
    assert param_opt._is_valid('nonexistent', None) == False
    assert param_opt._is_valid(None, ['par1', 'par10']) == expected[0]
    assert param_opt._is_valid('org1', None) == expected[1]


def test_get_custom_field(select_option_basic: _po.SelectParameterOption):
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
    
    param_opt = _po.SelectParameterOption('op1', 'Op 1', custom_fields={'a':'1', 'b':'0'}, b='2', c='3')
    assert param_opt.get_custom_field('a') == '1'
    assert param_opt.get_custom_field('b') == '0'
    assert param_opt.get_custom_field('c') == '3'


def test_invalid_date_parameter_options():
    with pytest.raises(u.ConfigurationError):
        _po.DateParameterOption('20200101')
    with pytest.raises(u.ConfigurationError):
        _po.DateParameterOption('2021-02-29')
    with pytest.raises(u.ConfigurationError):
        _po.DateRangeParameterOption('2023-01-01', '2022-01-01')
    
    with pytest.raises(u.ConfigurationError):
        _po.DateParameterOption('2021-01-01', min_date='2022-01-01')
    with pytest.raises(u.ConfigurationError):
        _po.DateParameterOption('2023-01-01', max_date='2022-01-01')
    with pytest.raises(u.ConfigurationError):
        _po.DateRangeParameterOption('2022-01-01', '2024-01-01', min_date='2023-01-01')
    with pytest.raises(u.ConfigurationError):
        _po.DateRangeParameterOption('2022-01-01', '2024-01-01', max_date='2023-01-01')


def test_valid_date_parameter_options():
    try:
        _po.DateParameterOption('2020-01-01')
        _po.DateParameterOption('01012020', date_format='%m%d%Y')
    except Exception:
        pytest.fail("Unexpected exception")


def test_invalid_number_parameter_options():
    with pytest.raises(u.ConfigurationError):
        _po.NumberParameterOption(10, 0)
    with pytest.raises(u.ConfigurationError):
        _po.NumberParameterOption(0, 10, increment=3)
    with pytest.raises(u.ConfigurationError):
        _po.NumberParameterOption(0, 2, default_value=5)
    with pytest.raises(u.ConfigurationError):
        _po.NumberParameterOption(2, 4, default_value=0)
    with pytest.raises(u.ConfigurationError):
        _po.NumberParameterOption(0, 4, increment=3)
    with pytest.raises(u.ConfigurationError):
        _po.NumberParameterOption(0, 4, increment=2, default_value=3)


def test_valid_number_parameter_option():
    try:
        _po.NumberParameterOption(0, 4, default_value=3)
        _po.NumberParameterOption("0.8", "3.6", increment="0.4", default_value=2)
    except Exception:
        pytest.fail("Unexpected exception")


def test_invalid_numrange_parameter_options():
    with pytest.raises(u.ConfigurationError):
        _po.NumberRangeParameterOption(2, 8, default_lower_value=0)
    with pytest.raises(u.ConfigurationError):
        _po.NumberRangeParameterOption(2, 8, default_lower_value=10)
    with pytest.raises(u.ConfigurationError):
        _po.NumberRangeParameterOption(2, 8, default_upper_value=0)
    with pytest.raises(u.ConfigurationError):
        _po.NumberRangeParameterOption(2, 8, default_upper_value=10)
    with pytest.raises(u.ConfigurationError):
        _po.NumberRangeParameterOption(2, 8, default_lower_value=6, default_upper_value=4)
    with pytest.raises(u.ConfigurationError):
        _po.NumberRangeParameterOption(2, 8, default_lower_value=6, increment=3)
    with pytest.raises(u.ConfigurationError):
        _po.NumberRangeParameterOption(2, 8, default_upper_value=6, increment=3)
