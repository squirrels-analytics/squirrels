from typing import Sequence, Dict
import pytest, pandas as pd

import squirrels as sq


class TestParentParameters:
    expected_base = {
        'label': 'Parent Param',
        'options': [
            {'id': 'p0', 'label': 'Option 1'},
            {'id': 'p1', 'label': 'Option 2'},
            {'id': 'p2', 'label': 'Option 3'},
            {'id': 'p3', 'label': 'Option 4'}
        ],
        'trigger_refresh': True
    }
    
    def get_expected_parent_json(self, widget_type: sq.WidgetType):
        expected = dict(self.expected_base)
        if widget_type == sq.WidgetType.SingleSelect:
            expected['name'] = 'ss_parent'
            expected['widget_type'] = widget_type.name
        elif widget_type == sq.WidgetType.MultiSelect:
            expected['name'] = 'ms_parent'
            expected['widget_type'] = widget_type.name
            expected.update({
                'include_all': True,
                'order_matters': False
            })
        return expected

    @pytest.fixture
    def parent_options(self) -> Sequence[sq.SelectParameterOption]:
        return (
            sq.SelectParameterOption('p0', 'Option 1'), sq.SelectParameterOption('p1', 'Option 2'),
            sq.SelectParameterOption('p2', 'Option 3'), sq.SelectParameterOption('p3', 'Option 4')
        )

    @pytest.fixture
    def single_select_parent(self, parent_options: Sequence[sq.SelectParameterOption]) -> sq.SingleSelectParameter:
        return sq.SingleSelectParameter('ss_parent', 'Parent Param', parent_options)

    @pytest.fixture
    def multi_select_parent(self, parent_options: Sequence[sq.SelectParameterOption]) -> sq.MultiSelectParameter:
        return sq.MultiSelectParameter('ms_parent', 'Parent Param', parent_options)

    parent_data_source = sq.SelectionDataSource('table', 'id_val', 'options', order_by_col='order_by',
                                                is_default_col='is_default')
    @pytest.fixture
    def ds_param_parent(self) -> sq.DataSourceParameter:
        return sq.DataSourceParameter(sq.WidgetType.SingleSelect, 'ds_parent', 'Parent Param', self.parent_data_source)
    
    @pytest.fixture
    def data_source_df(self) -> pd.DataFrame:
        return pd.DataFrame({
            'id_val': ['d0', 'd1', 'd2'],
            'options': ['first', 'third', 'second'],
            'order_by': [5, 10, 8],
            'is_default': [0, 1, 1]
        })
    
    @pytest.fixture
    def expected_ds_json(self) -> Dict:
        return {
            'widget_type': 'SingleSelect',
            'name': 'ds_parent',
            'label': 'Parent Param',
            'options': [
                {'id': 'd0', 'label': 'first'},
                {'id': 'd2', 'label': 'second'},
                {'id': 'd1', 'label': 'third'}
            ],
            'selected_id': 'd2',
            'trigger_refresh': True
        }
    