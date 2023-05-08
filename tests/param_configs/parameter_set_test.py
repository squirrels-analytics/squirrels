from typing import Dict
import pytest, pandas as pd

from squirrels.param_configs.parameter_set import ParameterSet
from tests.param_configs.parent_parameters import TestParentParameters
import squirrels as sq


class TestParameterSet(TestParentParameters):
    select_data_source = sq.SelectionDataSource('table', 'id_val', 'option', is_default_col='is_default',
                                                parent_id_col='parent_id')
    date_data_source = sq.DateDataSource('table', 'date', parent_id_col='parent_id')

    @pytest.fixture
    def parameter_set(self, multi_select_parent: sq.MultiSelectParameter, ds_param_parent: sq.DataSourceParameter) -> ParameterSet:
        child_param1 = sq.DataSourceParameter(sq.WidgetType.SingleSelect, 'child1', 'Test1 Parameter', self.select_data_source,
                                              parent=multi_select_parent)
        child_param2 = sq.DataSourceParameter(sq.WidgetType.DateField, 'child2', 'Test2 Parameter', self.date_data_source,
                                              parent=ds_param_parent)
        return ParameterSet((multi_select_parent, child_param1, ds_param_parent, child_param2))
    
    @pytest.fixture
    def df_dict(self, data_source_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        output = {}
        output['ds_parent'] = data_source_df
        output['child1'] = pd.DataFrame({
            'id_val': ['c0', 'c1', 'c2', 'c3', 'c4'],
            'option': ['o0', 'o1', 'o2', 'o3', 'o4'],
            'is_default': [0, 1, 0, 1, 1],
            'parent_id': ['p1', 'p1', 'p2', 'p2', 'p3']
        })
        output['child2'] = pd.DataFrame({
            'date': ['2020-03-15', '2022-12-25', '2021-06-14'],
            'parent_id': ['d0', 'd1', 'd2']
        })
        return output
    
    def test_get_datasources(self, parameter_set: ParameterSet):
        result = parameter_set.get_datasources()
        assert result == {
            'child1': self.select_data_source,
            'ds_parent': self.parent_data_source,
            'child2': self.date_data_source
        }
    
    def test_convert_datasource_params_and_merge(self, parameter_set: ParameterSet, df_dict: Dict[str, pd.DataFrame],
                                                 expected_ds_json: Dict):
        parameter_set.convert_datasource_params(df_dict)
        child1_expected = {
            'widget_type': 'SingleSelect',
            'name': 'child1',
            'label': 'Test1 Parameter',
            'options': [
                {'id': 'c0', 'label': 'o0'},
                {'id': 'c1', 'label': 'o1'},
                {'id': 'c2', 'label': 'o2'},
                {'id': 'c3', 'label': 'o3'},
                {'id': 'c4', 'label': 'o4'}
            ],
            'trigger_refresh': False,
            'selected_id': 'c1'
        }
        child2_expected = {
            'widget_type': 'DateField',
            'name': 'child2',
            'label': 'Test2 Parameter',
            'selected_date': '2021-06-14'
        }
        expected_parent1 = self.get_expected_parent_json(sq.WidgetType.MultiSelect)
        expected_parent1['selected_ids'] = []

        actual = parameter_set.to_dict()['parameters']
        assert actual[0] == expected_parent1
        assert actual[1] == child1_expected
        assert actual[2] == expected_ds_json
        assert actual[3] == child2_expected
        
        assert isinstance(parameter_set['child1'], sq.SingleSelectParameter)
        assert isinstance(parameter_set['child2'], sq.DateParameter)
        assert isinstance(parameter_set['ds_parent'], sq.SingleSelectParameter)

        assert parameter_set['child1'].parent is parameter_set['ms_parent']
        assert parameter_set['child2'].parent is parameter_set['ds_parent']

        new_param = parameter_set['ms_parent'].with_selection('p0')
        new_param_set = parameter_set.merge(new_param.get_all_dependent_params())

        expected_parent1['selected_ids'] = ['p0']
        child1_expected['options'] = []
        child1_expected['selected_id'] = None

        actual = new_param_set.to_dict()['parameters']
        assert actual[0] == expected_parent1
        assert actual[1] == child1_expected
    