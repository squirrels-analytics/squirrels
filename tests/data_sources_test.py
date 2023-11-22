from typing import Optional
import pytest, pandas as pd

from squirrels import data_sources as d, _parameter_configs as pc, parameters as p, parameter_options as po, _utils as u


class TestSingleSelectDataSource:
    def create_data_source(self, *, parent_id_col: Optional[str] = None, user_group_col: Optional[str] = None) -> d.SingleSelectDataSource:
        return d.SingleSelectDataSource('table', 'test_id', 'test_options', custom_cols={"test_field": "test_col"},
                                        is_default_col='test_is_default', parent_id_col=parent_id_col, user_group_col=user_group_col)
    
    @pytest.fixture(scope="class")
    def data_source(self) -> d.SingleSelectDataSource:
        return self.create_data_source()
    
    @pytest.fixture(scope="class")
    def data_source_with_parent(self) -> d.SingleSelectDataSource:
        return self.create_data_source(parent_id_col='test_parent_id')
    
    @pytest.fixture(scope="class")
    def data_source_with_user(self) -> d.SingleSelectDataSource:
        return self.create_data_source(user_group_col='test_user_group')

    def test_get_query(self):
        data_source1 = d.SingleSelectDataSource('table_name', 'my_ids', 'my_options')
        assert data_source1._get_query() == 'SELECT * FROM table_name'

        data_source2 = d.SingleSelectDataSource('select', 'my_ids', 'my_options')
        assert data_source2._get_query() == 'SELECT * FROM select'
        
        data_source3 = d.SingleSelectDataSource('SELECT * FROM table_name', 'my_ids', 'my_options')
        assert data_source3._get_query() == 'SELECT * FROM table_name'

        data_source4 = d.SingleSelectDataSource('select * from table_name', 'my_ids', 'my_options')
        assert data_source4._get_query() == 'select * from table_name'

    def test_convert(self, data_source: d.SingleSelectDataSource, data_source_with_parent: d.SingleSelectDataSource,
                     data_source_with_user: d.SingleSelectDataSource):
        df = pd.DataFrame({
            'test_id': ['0', '1', '2'],
            'test_options': ['zero', 'one', 'two'],
            'test_col': ['zerox', 'wonder', 'tutor'],
            'test_is_default': [0, 1, 1]
        })

        ds_param = pc.DataSourceParameterConfig(p.SingleSelectParameter, 'test_param', 'Test Parameter', data_source)
        param: pc.SingleSelectParameterConfig = ds_param.convert(df)
        param_options = (
            po.SelectParameterOption('0', 'zero', test_field='zerox'),
            po.SelectParameterOption('1', 'one', test_field='wonder', is_default=True),
            po.SelectParameterOption('2', 'two', test_field='tutor', is_default=True)
        )
        expected = pc.SingleSelectParameterConfig('test_param', 'Test Parameter', param_options)
        assert param == expected

        ds_param = pc.DataSourceParameterConfig(p.SingleSelectParameter, 'test_param', 'Test Parameter', data_source_with_parent,
                                                parent_name='multi_select_grandparent')
        df['test_parent_id'] = ['gp1', 'gp1', 'gp2']
        param: pc.SingleSelectParameterConfig = ds_param.convert(df)
        param_options = (
            po.SelectParameterOption('0', 'zero', test_field='zerox', parent_option_ids=['gp1']),
            po.SelectParameterOption('1', 'one', test_field='wonder', is_default=True, parent_option_ids=['gp1']),
            po.SelectParameterOption('2', 'two', test_field='tutor', is_default=True, parent_option_ids=['gp2'])
        )
        expected = pc.SingleSelectParameterConfig('test_param', 'Test Parameter', param_options, parent_name='multi_select_grandparent')
        assert param == expected

        ds_param = pc.DataSourceParameterConfig(p.SingleSelectParameter, 'test_param', 'Test Parameter', data_source_with_user,
                                                user_attribute='organization')
        df['test_user_group'] = ['org1', 'org2', 'org2']
        param: pc.SingleSelectParameterConfig = ds_param.convert(df)
        param_options = (
            po.SelectParameterOption('0', 'zero', test_field='zerox', user_groups=['org1']),
            po.SelectParameterOption('1', 'one', test_field='wonder', is_default=True, user_groups=['org2']),
            po.SelectParameterOption('2', 'two', test_field='tutor', is_default=True, user_groups=['org2'])
        )
        expected = pc.SingleSelectParameterConfig('test_param', 'Test Parameter', param_options, user_attribute='organization')
        assert param == expected
    
    def test_invalid_column_names(self, data_source: d.SingleSelectDataSource):
        ds_param = pc.DataSourceParameterConfig(p.SingleSelectParameter, 'test_param', 'Test Parameter', data_source)
        df = pd.DataFrame({
            'invalid_name': ['0', '1', '2'],
            'test_options': ['zero', 'one', 'two'],
            'test_is_default': [0, 1, 1]
        })
        with pytest.raises(u.ConfigurationError):
            ds_param.convert(df)
        
        df = pd.DataFrame({
            'test_id': ['0', '1', '2'],
            'test_options': ['zero', 'one', 'two'],
            'invalid_name': [0, 1, 1]
        })
        with pytest.raises(u.ConfigurationError):
            ds_param.convert(df)


class TestMultiSelectDataSource:
    @pytest.fixture(scope="class")
    def data_source(self) -> d.MultiSelectDataSource:
        return d.MultiSelectDataSource("table_name", "test_id", "test_options", include_all=False, order_matters=True,
                                       parent_id_col="test_parent_id")
    
    def test_convert(self, data_source: d.MultiSelectDataSource):
        df = pd.DataFrame({
            'test_id': ['0', '1', '1', '2'],
            'test_options': ['zero', 'one', 'one', 'two'],
            'test_parent_id': ['0', '0', '1', '1']
        })

        ds_param = pc.DataSourceParameterConfig(p.MultiSelectParameter, 'name', 'Label', data_source)
        param: pc.MultiSelectParameterConfig = ds_param.convert(df)
        param_options = [
            po.SelectParameterOption('0', 'zero', parent_option_ids=['0']),
            po.SelectParameterOption('1', 'one', parent_option_ids=['0', '1']),
            po.SelectParameterOption('2', 'two', parent_option_ids=['1'])
        ]
        expected = pc.MultiSelectParameterConfig('name', 'Label', param_options, include_all=False, order_matters=True)
        assert param == expected


class TestDateDataSource:
    @pytest.fixture(scope="class")
    def data_source(self) -> d.DateDataSource:
        return d.DateDataSource('table', 'test_id', 'test_date')

    def test_convert(self, data_source: d.DateDataSource):
        df = pd.DataFrame({
            'test_id': ['0', '1'], 
            'test_date': ['2022-01-01', '2022-02-01']
        })

        ds_param = pc.DataSourceParameterConfig(p.DateParameter, 'name', 'Label', data_source)
        param: pc.DateParameterConfig = ds_param.convert(df)
        param_options = [po.DateParameterOption('2022-01-01'), po.DateParameterOption('2022-02-01')]
        expected = pc.DateParameterConfig('name', 'Label', param_options)
        assert param == expected


class TestDateRangeDataSource:
    @pytest.fixture(scope="class")
    def data_source(self) -> d.DateRangeDataSource:
        return d.DateRangeDataSource('table', 'test_id', 'test_start_date', 'test_end_date')

    def test_convert(self, data_source: d.DateDataSource):
        df = pd.DataFrame({
            'test_id': ['0', '1'],
            'test_start_date': ['2023-01-01', '2023-02-01'],
            'test_end_date': ['2023-04-01', '2023-03-01'],
        })

        ds_param = pc.DataSourceParameterConfig(p.DateRangeParameter, 'name', 'Label', data_source)
        param: pc.DateRangeParameterConfig = ds_param.convert(df)
        param_options = [
            po.DateRangeParameterOption('2023-01-01', '2023-04-01'), 
            po.DateRangeParameterOption('2023-02-01', '2023-03-01')
        ]
        expected = pc.DateRangeParameterConfig('name', 'Label', param_options)
        assert param == expected


class TestNumberDataSource:
    @pytest.fixture(scope="class")
    def data_source(self) -> d.NumberDataSource:
        return d.NumberDataSource('table', 'test_id', 'test_min', 'test_max', default_value_col='test_default')

    def test_convert(self, data_source: d.NumberDataSource):
        df = pd.DataFrame([{ 'test_id': 0, 'test_min': 0, 'test_max': 10, 'test_default': 2 }])
        ds_param = pc.DataSourceParameterConfig(p.NumberParameter, 'name', 'Label', data_source)
        param: pc.NumberParameterConfig = ds_param.convert(df)
        param_options = [po.NumberParameterOption(0, 10, default_value=2)]
        expected = pc.NumberParameterConfig('name', 'Label', param_options)
        assert param == expected
    

class TestNumRangeDataSource:
    @pytest.fixture(scope="class")
    def data_source(self) -> d.NumRangeDataSource:
        return d.NumRangeDataSource('table', 'test_id', 'test_min', 'test_max', increment_col='test_increment', 
                                    default_lower_value_col='test_default_lower', default_upper_value_col='test_default_upper',
                                    parent_id_col='test_parent_id', user_group_col='test_user_group')

    def test_convert(self, data_source: d.NumRangeDataSource):
        df = pd.DataFrame([
            { 
                'test_id': 0, 'test_min': 0, 'test_max': 10, 'test_increment': 2, 'test_default_lower': 4, 'test_default_upper': 8,
                'test_parent_id': 5, 'test_user_group': 'org1'
            },
            { 
                'test_id': 0, 'test_min': 0, 'test_max': 100, 'test_increment': 5, 'test_default_lower': 15, 'test_default_upper': 85,
                'test_parent_id': 6, 'test_user_group': 'org2'
            }
        ])

        ds_param = pc.DataSourceParameterConfig(p.NumRangeParameter, 'name', 'Label', data_source)
        param: pc.NumRangeParameterConfig = ds_param.convert(df)
        param_options = [
            po.NumRangeParameterOption(0, 10, increment=2, default_lower_value=4, default_upper_value=8, 
                                       parent_option_ids=['5', '6'], user_groups=['org1', 'org2'])
        ]
        expected = pc.NumRangeParameterConfig('name', 'Label', param_options)
        assert param == expected
