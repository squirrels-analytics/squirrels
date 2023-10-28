from typing import Optional
import pytest, pandas as pd

from squirrels import data_sources as d
from squirrels.parameters import DataSourceParameter
from squirrels._utils import ConfigurationError
from tests.parent_parameters import TestParentParameters
import squirrels as sr


class TestDataSource:
    data_source1 = d.DateDataSource('table_name', 'my_dates')
    data_source2 = d.SelectionDataSource('select', 'my_ids', 'my_options')
    data_source3 = d.NumberDataSource("SELECT * FROM table_name WHERE col = 'value'", 'my_min_values', 'my_max_values')
    data_source4 = d.NumRangeDataSource("select * from table_name where col = 'value'", 'my_min_values', 'my_max_values')
    
    def test_get_query(self):
        assert self.data_source1.get_query() == 'SELECT * FROM table_name'
        assert self.data_source2.get_query() == 'SELECT * FROM select'
        assert self.data_source3.get_query() == "SELECT * FROM table_name WHERE col = 'value'"
        assert self.data_source4.get_query() == "select * from table_name where col = 'value'"


class TestSelectionDataSource(TestParentParameters):
    def create_data_source(self, parent_id_col: Optional[str]):
        return d.SelectionDataSource('table', 'test_id', 'test_options', custom_cols={"alliteration": "test_alliteration"},
                                     is_default_col='test_is_default', parent_id_col=parent_id_col)
    
    @pytest.fixture
    def select_data_source(self) -> d.SelectionDataSource:
        return self.create_data_source(None)
    
    @pytest.fixture
    def select_data_source_with_parent(self) -> d.SelectionDataSource:
        return self.create_data_source('parent_id')

    def test_convert(self, multi_select_parent: sr.MultiSelectParameter, select_data_source: d.SelectionDataSource, 
                     select_data_source_with_parent: d.SelectionDataSource):
        ds_param = DataSourceParameter(sr.SingleSelectParameter, 'test_param', 'Test Parameter', select_data_source)
        df = pd.DataFrame({
            'test_id': ['0', '1', '2'],
            'test_options': ['zero', 'one', 'two'],
            'test_alliteration': ['zerox', 'wonder', 'tutor'],
            'test_is_default': [0, 1, 1]
        })
        param: sr.SingleSelectParameter = select_data_source.convert(ds_param, df)
        param_to_dict = param.to_json_dict()
        expected = {
            'widget_type': 'SingleSelectParameter',
            'name': 'test_param',
            'label': 'Test Parameter',
            'options': [
                {'id': '0', 'label': 'zero'},
                {'id': '1', 'label': 'one'},
                {'id': '2', 'label': 'two'}
            ],
            'selected_id': '1',
            'trigger_refresh': False
        }
        assert param_to_dict == expected

        custom_field_list = [x.custom_fields["alliteration"] for x in param.options]
        assert custom_field_list == ['zerox', 'wonder', 'tutor']

        ds_param = DataSourceParameter(sr.SingleSelectParameter, 'test_param', 'Test Parameter', select_data_source_with_parent,
                                       parent=multi_select_parent)
        df['parent_id'] = ['p1', 'p1', 'p2']
        converted_param = select_data_source_with_parent.convert(ds_param, df)
        new_parent = multi_select_parent.with_selection('p0')
        assert converted_param.to_json_dict() == expected

        new_child = new_parent.get_all_dependent_params()['test_param']
        new_expected = dict(expected)
        new_expected.update({'options': [], 'selected_id': None})
        assert new_child.to_json_dict() == new_expected
    
    def test_invalid_column(self, select_data_source: d.SelectionDataSource):
        ds_param = DataSourceParameter(sr.SingleSelectParameter, 'test_param', 'Test Parameter', select_data_source)
        df = pd.DataFrame({
            'invalid_name': ['0', '1', '2'],
            'test_options': ['zero', 'one', 'two'],
            'test_is_default': [0, 1, 1]
        })
        with pytest.raises(ConfigurationError):
            select_data_source.convert(ds_param, df)
        
        df = pd.DataFrame({
            'test_id': ['0', '1', '2'],
            'invalid_name': ['zero', 'one', 'two'],
            'test_is_default': [0, 1, 1]
        })
        with pytest.raises(ConfigurationError):
            select_data_source.convert(ds_param, df)


class TestDateDataSource(TestParentParameters):
    def create_data_source(self, parent_id_col: Optional[str]):
        return d.DateDataSource('table', 'date', parent_id_col=parent_id_col)
    
    @pytest.fixture
    def date_data_source(self) -> d.DateDataSource:
        return self.create_data_source(None)
    
    @pytest.fixture
    def date_data_source_with_parent(self) -> d.DateDataSource:
        return self.create_data_source('parent_id')

    def test_convert(self, single_select_parent: sr.SingleSelectParameter, date_data_source: d.DateDataSource, 
                     date_data_source_with_parent: d.DateDataSource):
        ds_param = DataSourceParameter(sr.DateParameter, 'test_param', 'Test Parameter', date_data_source)
        df = pd.DataFrame({'date': ['2022-01-01']})
        param_to_dict = date_data_source.convert(ds_param, df).to_json_dict()
        expected = {
            'widget_type': 'DateParameter',
            'name': 'test_param',
            'label': 'Test Parameter',
            'selected_date': '2022-01-01'
        }
        assert param_to_dict == expected

        ds_param = DataSourceParameter(sr.DateParameter, 'test_param', 'Test Parameter', date_data_source_with_parent,
                                       parent=single_select_parent)
        df = pd.DataFrame({
            'date': ['2020-01-01', '2021-01-01', '2022-01-01', '2023-01-01'],
            'parent_id': ['p0', 'p1', 'p2', 'p3']
        })
        date_data_source_with_parent.convert(ds_param, df)
        new_parent = single_select_parent.with_selection('p2')
        new_child = new_parent.get_all_dependent_params()['test_param']
        expected['selected_date'] = '2022-01-01'
        assert new_child.to_json_dict() == expected


class TestNumberDataSource(TestParentParameters):
    def create_data_source(self, parent_id_col: Optional[str]):
        return d.NumberDataSource('table', 'min_val', 'max_val', default_value_col='default_val', 
                                parent_id_col=parent_id_col) 
    
    @pytest.fixture
    def num_data_source(self) -> d.NumberDataSource:
        return self.create_data_source(None)
    
    @pytest.fixture
    def num_data_source_with_parent(self) -> d.NumberDataSource:
        return self.create_data_source('parent_id')

    def test_convert(self, single_select_parent: sr.SingleSelectParameter, num_data_source: d.NumberDataSource, 
                     num_data_source_with_parent: d.NumberDataSource):
        ds_param = DataSourceParameter(sr.NumberParameter, 'test_param', 'Test Parameter', num_data_source)
        df = pd.DataFrame([{ 'min_val': 0, 'max_val': 10, 'default_val': 2 }])
        param_to_dict = num_data_source.convert(ds_param, df).to_json_dict()
        expected = {
            'widget_type': 'NumberParameter',
            'name': 'test_param',
            'label': 'Test Parameter',
            'min_value': '0',
            'max_value': '10',
            'increment': '1',
            'selected_value': '2'
        }
        assert param_to_dict == expected

        ds_param = DataSourceParameter(sr.NumberParameter, 'test_param', 'Test Parameter', num_data_source_with_parent,
                                       parent=single_select_parent)
        df = pd.DataFrame({
            'min_val': [0, 0, 4, 0],
            'max_val': [10, 10, 9, 10],
            'default_val': [2, 2, 7, 2],
            'parent_id': ['p0', 'p1', 'p2', 'p3']
        })
        num_data_source_with_parent.convert(ds_param, df)
        new_parent = single_select_parent.with_selection('p2')
        new_child = new_parent.get_all_dependent_params()['test_param']
        expected.update({
            'min_value': '4',
            'max_value': '9',
            'selected_value': '7'
        })
        assert new_child.to_json_dict() == expected
    

class TestNumRangeDataSource(TestParentParameters):
    def create_data_source(self, parent_id_col: Optional[str]):
        return d.NumRangeDataSource('table', 'min_val', 'max_val', 'increment', 'default_lower', 
                                    'default_upper', parent_id_col=parent_id_col)
    
    @pytest.fixture
    def range_data_source(self) -> d.NumRangeDataSource:
        return self.create_data_source(None)
    
    @pytest.fixture
    def range_data_source_with_parent(self) -> d.NumRangeDataSource:
        return self.create_data_source('parent_id')

    def test_convert(self, single_select_parent: sr.SingleSelectParameter, range_data_source: d.NumRangeDataSource, 
                     range_data_source_with_parent: d.NumRangeDataSource):
        ds_param = DataSourceParameter(sr.NumRangeParameter, 'test_param', 'Test Parameter', range_data_source)
        df = pd.DataFrame([{ 'min_val': 0, 'max_val': 10, 'increment': 2, 'default_lower': 4, 'default_upper': 8 }])
        param_to_dict = range_data_source.convert(ds_param, df).to_json_dict()
        expected = {
            'widget_type': 'NumRangeParameter',
            'name': 'test_param',
            'label': 'Test Parameter',
            'min_value': '0',
            'max_value': '10',
            'increment': '2',
            'selected_lower_value': '4',
            'selected_upper_value': '8'
        }
        assert param_to_dict == expected

        ds_param = DataSourceParameter(sr.NumRangeParameter, 'test_param', 'Test Parameter', range_data_source_with_parent,
                                       parent=single_select_parent)
        df = pd.DataFrame({
            'min_val': [0, 0, 3, 0],
            'max_val': [10, 10, 9, 10],
            'increment': [2, 2, 3, 2],
            'default_lower': [4, 4, 6, 4],
            'default_upper': [8, 8, 9, 8],
            'parent_id': ['p0', 'p1', 'p2', 'p3']
        })
        range_data_source_with_parent.convert(ds_param, df)
        new_parent = single_select_parent.with_selection('p2')
        new_child = new_parent.get_all_dependent_params()['test_param']
        expected.update({
            'min_value': '3',
            'max_value': '9',
            'increment': '3',
            'selected_lower_value': '6',
            'selected_upper_value': '9'
        })
        assert new_child.to_json_dict() == expected
