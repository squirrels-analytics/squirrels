from typing import Optional
import pytest, polars as pl

from squirrels import data_sources as d, _parameter_configs as _pc, parameter_options as _po, _utils as u


class TestSelectDataSource:
    def create_data_source(self, *, parent_id_col: Optional[str] = None, user_group_col: Optional[str] = None) -> d.SelectDataSource:
        return d.SelectDataSource(
            'table', 'test_id', 'test_options', custom_cols={"test_field": "test_col"},
            is_default_col='test_is_default', parent_id_col=parent_id_col, user_group_col=user_group_col
        )
    
    @pytest.fixture(scope="class")
    def data_source(self) -> d.SelectDataSource:
        return self.create_data_source()
    
    @pytest.fixture(scope="class")
    def data_source_with_parent(self) -> d.SelectDataSource:
        return self.create_data_source(parent_id_col='test_parent_id')
    
    @pytest.fixture(scope="class")
    def data_source_with_user(self) -> d.SelectDataSource:
        return self.create_data_source(user_group_col='test_user_group')

    def test_get_query(self):
        data_source1 = d.SelectDataSource('table_name', 'my_ids', 'my_options')
        assert data_source1._get_query() == 'SELECT * FROM table_name'

        data_source2 = d.SelectDataSource('select', 'my_ids', 'my_options')
        assert data_source2._get_query() == 'SELECT * FROM select'
        
        data_source3 = d.SelectDataSource('SELECT * FROM table_name', 'my_ids', 'my_options')
        assert data_source3._get_query() == 'SELECT * FROM table_name'

        data_source4 = d.SelectDataSource('select * from table_name', 'my_ids', 'my_options')
        assert data_source4._get_query() == 'select * from table_name'

    def test_convert(self, data_source: d.SelectDataSource, data_source_with_parent: d.SelectDataSource, data_source_with_user: d.SelectDataSource):
        data = {
            'test_id': ['0', '1', '2'],
            'test_options': ['zero', 'one', 'two'],
            'test_col': ['zerox', 'wonder', 'tutor'],
            'test_is_default': [0, 1, 1]
        }
        df = pl.DataFrame(data).sort(by='test_id')

        ds_param = _pc.DataSourceParameterConfig(
            _pc.MultiSelectParameterConfig, 'test_param', 'Test Parameter', data_source, extra_args={"show_select_all": False}
        )
        param1: _pc.MultiSelectParameterConfig = ds_param.convert(df)
        param_options = (
            _po.SelectParameterOption('0', 'zero', test_field='zerox'),
            _po.SelectParameterOption('1', 'one', test_field='wonder', is_default=True),
            _po.SelectParameterOption('2', 'two', test_field='tutor', is_default=True)
        )
        expected = _pc.MultiSelectParameterConfig('test_param', 'Test Parameter', param_options, show_select_all=False)
        assert param1 == expected

        data['test_parent_id'] = ['gp1', 'gp1', 'gp2']
        df = pl.DataFrame(data).sort(by='test_id')

        ds_param = _pc.DataSourceParameterConfig(
            _pc.SingleSelectParameterConfig, 'test_param', 'Test Parameter', data_source_with_parent, parent_name='multi_select_grandparent'
        )
        param2: _pc.SingleSelectParameterConfig = ds_param.convert(df)
        param_options = (
            _po.SelectParameterOption('0', 'zero', test_field='zerox', parent_option_ids=['gp1']),
            _po.SelectParameterOption('1', 'one', test_field='wonder', is_default=True, parent_option_ids=['gp1']),
            _po.SelectParameterOption('2', 'two', test_field='tutor', is_default=True, parent_option_ids=['gp2'])
        )
        expected = _pc.SingleSelectParameterConfig('test_param', 'Test Parameter', param_options, parent_name='multi_select_grandparent')
        assert param2 == expected

        data['test_user_group'] = ['org1', 'org2', 'org2']
        df = pl.DataFrame(data).sort(by='test_id')

        ds_param = _pc.DataSourceParameterConfig(
            _pc.SingleSelectParameterConfig, 'test_param', 'Test Parameter', data_source_with_user, user_attribute='organization'
        )
        param3: _pc.SingleSelectParameterConfig = ds_param.convert(df)
        param_options = (
            _po.SelectParameterOption('0', 'zero', test_field='zerox', user_groups=['org1']),
            _po.SelectParameterOption('1', 'one', test_field='wonder', is_default=True, user_groups=['org2']),
            _po.SelectParameterOption('2', 'two', test_field='tutor', is_default=True, user_groups=['org2'])
        )
        expected = _pc.SingleSelectParameterConfig('test_param', 'Test Parameter', param_options, user_attribute='organization')
        assert param3 == expected
    
    def test_invalid_column_names(self, data_source: d.SelectDataSource):
        ds_param = _pc.DataSourceParameterConfig(_pc.SingleSelectParameterConfig, 'test_param', 'Test Parameter', data_source)
        df = pl.DataFrame({
            'invalid_name': ['0', '1', '2'],
            'test_options': ['zero', 'one', 'two'],
            'test_is_default': [0, 1, 1]
        })
        with pytest.raises(u.ConfigurationError):
            ds_param.convert(df)
        
        df = pl.DataFrame({
            'test_id': ['0', '1', '2'],
            'test_options': ['zero', 'one', 'two'],
            'invalid_name': [0, 1, 1]
        })
        with pytest.raises(u.ConfigurationError):
            ds_param.convert(df)


class TestDateDataSource:
    @pytest.fixture(scope="class")
    def data_source(self) -> d.DateDataSource:
        return d.DateDataSource('table', 'test_date')

    def test_convert(self, data_source: d.DateDataSource):
        df = pl.DataFrame({
            'test_date': ['2022-01-01', '2022-02-01']
        }).sort(by='test_date')

        ds_param = _pc.DataSourceParameterConfig(_pc.DateParameterConfig, 'name', 'Label', data_source)
        param: _pc.DateParameterConfig = ds_param.convert(df)
        param_options = [_po.DateParameterOption('2022-01-01'), _po.DateParameterOption('2022-02-01')]
        expected = _pc.DateParameterConfig('name', 'Label', param_options)
        assert param == expected


class TestDateRangeDataSource:
    @pytest.fixture(scope="class")
    def data_source(self) -> d.DateRangeDataSource:
        return d.DateRangeDataSource('table', 'test_start_date', 'test_end_date')

    def test_convert(self, data_source: d.DateDataSource):
        df = pl.DataFrame({
            'test_start_date': ['2023-01-01', '2023-02-01'],
            'test_end_date': ['2023-04-01', '2023-03-01'],
        }).sort(by='test_start_date')

        ds_param = _pc.DataSourceParameterConfig(_pc.DateRangeParameterConfig, 'name', 'Label', data_source)
        param: _pc.DateRangeParameterConfig = ds_param.convert(df)
        param_options = [
            _po.DateRangeParameterOption('2023-01-01', '2023-04-01'), 
            _po.DateRangeParameterOption('2023-02-01', '2023-03-01')
        ]
        expected = _pc.DateRangeParameterConfig('name', 'Label', param_options)
        assert param == expected


class TestNumberDataSource:
    @pytest.fixture(scope="class")
    def data_source(self) -> d.NumberDataSource:
        return d.NumberDataSource('table', 'test_min', 'test_max', default_value_col='test_default')

    def test_convert(self, data_source: d.NumberDataSource):
        df = pl.DataFrame([{ 'test_min': 0, 'test_max': 10, 'test_default': 2 }])
        ds_param = _pc.DataSourceParameterConfig(_pc.NumberParameterConfig, 'name', 'Label', data_source)
        param: _pc.NumberParameterConfig = ds_param.convert(df)
        param_options = [_po.NumberParameterOption(0, 10, default_value=2)]
        expected = _pc.NumberParameterConfig('name', 'Label', param_options)
        assert param == expected
    

class TestNumRangeDataSource:
    @pytest.fixture(scope="class")
    def data_source(self) -> d.NumberRangeDataSource:
        return d.NumberRangeDataSource(
            'table', 'test_min', 'test_max', increment_col='test_increment', 
            default_lower_value_col='test_default_lower', default_upper_value_col='test_default_upper',
            id_col='test_id', parent_id_col='test_parent_id', user_group_col='test_user_group'
        )

    def test_convert(self, data_source: d.NumberRangeDataSource):
        df = pl.DataFrame([
            { 
                'test_id': 0, 'test_min': 0, 'test_max': 10, 'test_increment': 2, 'test_default_lower': 4, 'test_default_upper': 8,
                'test_parent_id': 5, 'test_user_group': 'org1'
            },
            { 
                'test_id': 0, 'test_min': 0, 'test_max': 100, 'test_increment': 5, 'test_default_lower': 15, 'test_default_upper': 85,
                'test_parent_id': 6, 'test_user_group': 'org2'
            }
        ])

        ds_param = _pc.DataSourceParameterConfig(_pc.NumberRangeParameterConfig, 'name', 'Label', data_source)
        param: _pc.NumberRangeParameterConfig = ds_param.convert(df)
        param_options = [
            _po.NumberRangeParameterOption(
                0, 10, increment=2, default_lower_value=4, default_upper_value=8, 
                parent_option_ids=['5', '6'], user_groups=['org1', 'org2']
            )
        ]
        expected = _pc.NumberRangeParameterConfig('name', 'Label', param_options)
        assert param == expected


class TestTextDataSource:
    @pytest.fixture(scope="class")
    def data_source(self) -> d.TextDataSource:
        return d.TextDataSource('table', default_text_col='test_default')

    def test_convert(self, data_source: d.TextDataSource):
        df = pl.DataFrame([{ 'test_default': "Hello World" }])
        ds_param = _pc.DataSourceParameterConfig(_pc.TextParameterConfig, 'name', 'Label', data_source)
        param: _pc.TextParameterConfig = ds_param.convert(df)
        param_options = [_po.TextParameterOption(default_text="Hello World")]
        expected = _pc.TextParameterConfig('name', 'Label', param_options)
        assert param == expected
    