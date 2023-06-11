from typing import Sequence
import pytest

from squirrels import parameters as p
from squirrels._utils import InvalidInputError, ConfigurationError
from tests.parent_parameters import TestParentParameters
import squirrels as sr


class TestSingleSelectParameter(TestParentParameters):
    
    class ParameterData:
        def __init__(self, single_select_parent: p.SingleSelectParameter, expected_parent_json) -> None:
            self.parent_param = single_select_parent

            child_options = (
                sr.SelectParameterOption('c0', 'Child option 1', parent_option_id='p0'),
                # Note: 'parent_option_id' takes precedence over 'parent_option_ids'
                sr.SelectParameterOption('c1', 'Child option 2', parent_option_id='p0', parent_option_ids={'p0','p1'}),
                sr.SelectParameterOption('c2', 'Child option 3', parent_option_ids={'p0','p1'}),
                sr.SelectParameterOption('c3', 'Child option 4', parent_option_id='p1', is_default=True),
            )
            self.child_param = p.SingleSelectParameter('child_param', 'Child Param', child_options, parent=self.parent_param)

            class SingleSelectParam2(p.SingleSelectParameter):
                pass

            grandchild_options = (
                sr.SelectParameterOption('g0', 'Grandchild option 1', parent_option_id='c3'),
                sr.SelectParameterOption('g1', 'Grandchild option 2', parent_option_id='c3')
            )
            self.grandchild_param = SingleSelectParam2('grandchild_param', 'Grandchild Param', grandchild_options, parent=self.child_param)
        
            self.expected_parent_json = expected_parent_json

            self.partial_child_dict = {
                'widget_type': 'SingleSelectParameter',
                'name': 'child_param',
                'label': 'Child Param',
                'options': [],
                'trigger_refresh': True
            }

            self.partial_grandchild_dict = {
                'widget_type': 'SingleSelectParameter',
                'name': 'grandchild_param',
                'label': 'Grandchild Param',
                'options': [],
                'trigger_refresh': False
            }

    @pytest.fixture
    def data(self, single_select_parent: p.SingleSelectParameter) -> ParameterData:
        expected_parent_json = self.get_expected_parent_json(p.SingleSelectParameter)
        return self.ParameterData(single_select_parent, expected_parent_json)
    
    def test_refresh_with_selection(self, data: ParameterData):
        expected1 = data.expected_parent_json
        expected2 = data.partial_child_dict
        expected3 = data.partial_grandchild_dict

        new_parent_param = data.parent_param.with_selection('p1')

        expected1['selected_id'] = 'p0'
        assert data.parent_param.to_json_dict() == expected1

        expected1['selected_id'] = 'p1'
        assert new_parent_param.to_json_dict() == expected1

        expected2['options'] = [
            {'id': 'c0', 'label': 'Child option 1'},
            {'id': 'c1', 'label': 'Child option 2'},
            {'id': 'c2', 'label': 'Child option 3'}
        ]
        expected2['selected_id'] = 'c0'
        assert data.child_param.to_json_dict() == expected2
        
        new_child_param = new_parent_param.children[0]
        expected2['options'] = [
            {'id': 'c2', 'label': 'Child option 3'},
            {'id': 'c3', 'label': 'Child option 4'}
        ]
        expected2['selected_id'] = 'c3'
        assert new_child_param.to_json_dict() == expected2

        expected3['selected_id'] = None
        assert data.grandchild_param.to_json_dict() == expected3

        new_grandchild_param = new_child_param.children[0]
        expected3['options'] = [
            {'id': 'g0', 'label': 'Grandchild option 1'},
            {'id': 'g1', 'label': 'Grandchild option 2'}
        ]
        expected3['selected_id'] = 'g0'
        assert new_grandchild_param.to_json_dict() == expected3
    
    def test_invalid_selection(self, data: ParameterData):
        with pytest.raises(InvalidInputError):
            data.parent_param.with_selection('')
        with pytest.raises(InvalidInputError):
            data.parent_param.with_selection('x1')
    
    def test_get_selected(self, data: ParameterData):
        assert data.parent_param.get_selected_id() == 'p0'
        assert data.parent_param.get_selected_id_quoted() == "'p0'"
        assert data.parent_param.get_selected_label() == 'Option 1'
        assert data.parent_param.get_selected_label_quoted() == "'Option 1'"


class TestMultiSelectParameter(TestParentParameters):
    
    class ParameterData:
        def __init__(self, multi_select_parent: p.MultiSelectParameter, expected_parent_json) -> None:
            self.parent_param = multi_select_parent

            child_options = (
                sr.SelectParameterOption('c0', 'Child option 1', parent_option_id='p0'),
                sr.SelectParameterOption('c1', 'Child option 2', parent_option_id='p0'),
                sr.SelectParameterOption('c2', 'Child option 3', parent_option_id='p1'),
                sr.SelectParameterOption('c3', 'Child option 4', parent_option_id='p1', is_default=True),
                sr.SelectParameterOption('c4', 'Child option 5', parent_option_id='p2'),
            )
            self.child_param = p.SingleSelectParameter('child_param', 'Child Param', child_options, parent=self.parent_param)

            grandchild_options = (
                sr.SelectParameterOption('g0', 'Grandchild option 1', parent_option_id='c0'),
                sr.SelectParameterOption('g1', 'Grandchild option 2', parent_option_id='c0', is_default=True),
                sr.SelectParameterOption('g2', 'Grandchild option 3', parent_option_id='c0', is_default=True),
                sr.SelectParameterOption('g3', 'Grandchild option 4', parent_option_id='c3')
            )
            self.grandchild_param = p.MultiSelectParameter('grandchild_param', 'Grandchild Param', grandchild_options, parent=self.child_param)
        
            self.expected_parent_json = expected_parent_json

            self.partial_child_dict = {
                'widget_type': 'SingleSelectParameter',
                'name': 'child_param',
                'label': 'Child Param',
                'options': [],
                'trigger_refresh': True
            }

            self.partial_grandchild_dict = {
                'widget_type': 'MultiSelectParameter',
                'name': 'grandchild_param',
                'label': 'Grandchild Param',
                'options': [],
                'trigger_refresh': False,
                'include_all': True,
                'order_matters': False
            }

    @pytest.fixture
    def data(self, multi_select_parent: p.MultiSelectParameter) -> ParameterData:
        expected_parent_json = self.get_expected_parent_json(p.MultiSelectParameter)
        return self.ParameterData(multi_select_parent, expected_parent_json)
    
    def test_refresh_with_selection(self, data: ParameterData):
        expected1 = data.expected_parent_json
        expected2 = data.partial_child_dict
        expected3 = data.partial_grandchild_dict

        new_parent_param = data.parent_param.with_selection('["p0","p2"]')

        expected1['selected_ids'] = []
        assert data.parent_param.to_json_dict() == expected1

        expected1['selected_ids'] = ['p0','p2']
        assert new_parent_param.to_json_dict() == expected1

        expected2['options'] = [
            {'id': 'c0', 'label': 'Child option 1'},
            {'id': 'c1', 'label': 'Child option 2'},
            {'id': 'c2', 'label': 'Child option 3'},
            {'id': 'c3', 'label': 'Child option 4'},
            {'id': 'c4', 'label': 'Child option 5'}
        ]
        expected2['selected_id'] = 'c3'
        assert data.child_param.to_json_dict() == expected2
        
        new_child_param = new_parent_param.children[0]
        expected2['options'] = [x for x in expected2['options'] if x['id'] in ['c0', 'c1', 'c4']]
        expected2['selected_id'] = 'c0'
        assert new_child_param.to_json_dict() == expected2

        expected3['options'] = [
            {'id': 'g3', 'label': 'Grandchild option 4'}
        ]
        expected3['selected_ids'] = []
        assert data.grandchild_param.to_json_dict() == expected3

        new_grandchild_param = new_child_param.children[0]
        expected3['options'] = [
            {'id': 'g0', 'label': 'Grandchild option 1'},
            {'id': 'g1', 'label': 'Grandchild option 2'},
            {'id': 'g2', 'label': 'Grandchild option 3'}
        ]
        expected3['selected_ids'] = ['g1','g2']
        assert new_grandchild_param.to_json_dict() == expected3
    
    def test_invalid_selection(self, data: ParameterData):
        with pytest.raises(InvalidInputError):
            data.parent_param.with_selection('x1')
        with pytest.raises(InvalidInputError):
            data.parent_param.with_selection('p0,x1')

    def test_get_selected(self):
        options = (
            sr.SelectParameterOption('p0', 'Option 1', is_default=True), 
            sr.SelectParameterOption('p1', 'Option 2'), 
            sr.SelectParameterOption('p2', 'Option 3', is_default=True)
        )
        parameter = p.MultiSelectParameter('parent', 'Parent Param', options)

        assert parameter.get_selected_ids_as_list() == ('p0', 'p2')
        assert parameter.get_selected_ids_joined() == "p0, p2"
        assert parameter.get_selected_ids_quoted_as_list() == ("'p0'", "'p2'")
        assert parameter.get_selected_ids_quoted_joined() == "'p0', 'p2'"

        assert parameter.get_selected_labels_as_list() == ('Option 1', 'Option 3')
        assert parameter.get_selected_labels_joined() == "Option 1, Option 3"
        assert parameter.get_selected_labels_quoted_as_list() == ("'Option 1'", "'Option 3'")
        assert parameter.get_selected_labels_quoted_joined() == "'Option 1', 'Option 3'"

        assert parameter.has_non_empty_selection() == True

        new_param = parameter.with_selection('')
        assert new_param.get_selected_ids_as_list() == ('p0', 'p1', 'p2')

        assert new_param.has_non_empty_selection() == False


class TestDateParameter(TestParentParameters):
    date_parameter = p.DateParameter('date', 'Date Value', '2020-01-01')

    def test_with_selection(self):
        new_date_param = self.date_parameter.with_selection('2023-01-01')

        expected = {
            'widget_type': 'DateParameter',
            'name': 'date',
            'label': 'Date Value',
            'selected_date': '2020-01-01'
        }
        assert self.date_parameter.to_json_dict() == expected

        expected['selected_date'] = '2023-01-01'
        assert new_date_param.to_json_dict() == expected

    def test_cascadable(self, single_select_parent: p.SingleSelectParameter):
        child_options = (
            sr.DateParameterOption('2020-01-01', parent_option_ids={'p0', 'p1'}),
            sr.DateParameterOption('2022-01-01', parent_option_id='p2'),
            sr.DateParameterOption('2023-01-01', parent_option_id='p3')
        )
        child_param = p.DateParameter.WithParent('child', 'Child Param', child_options, single_select_parent)

        new_parent = single_select_parent.with_selection('p2')

        expected = {
            'widget_type': 'DateParameter',
            'name': 'child',
            'label': 'Child Param',
            'selected_date': '2020-01-01'
        }
        assert child_param.to_json_dict() == expected

        expected['selected_date'] = '2022-01-01'
        new_child = new_parent.get_all_dependent_params()['child']
        assert new_child.to_json_dict() == expected

    def test_invalid_selection(self):
        with pytest.raises(InvalidInputError):
            self.date_parameter.with_selection('')
        with pytest.raises(InvalidInputError):
            self.date_parameter.with_selection('not_a_date')
        with pytest.raises(InvalidInputError):
            self.date_parameter.with_selection('01/01/2020') # wrong format
        
    def test_invalid_configuration(self, parent_options: Sequence[sr.SelectParameterOption], single_select_parent: p.SingleSelectParameter):
        with pytest.raises(ConfigurationError):
            multi_select_param = p.MultiSelectParameter('', '', parent_options)
            child_options = (
                sr.DateParameterOption('2020-01-01', parent_option_ids={'p0', 'p1'}),
                sr.DateParameterOption('2022-01-01', parent_option_ids={'p2', 'p3'})
            )
            p.DateParameter.WithParent('child', 'Child Param', child_options, multi_select_param)
        with pytest.raises(ConfigurationError):
            child_options = (
                sr.DateParameterOption('2020-01-01', parent_option_ids={'p0', 'p1'}),
                sr.DateParameterOption('2022-01-01', parent_option_id='p2')
            )
            p.DateParameter.WithParent('child', 'Child Param', child_options, single_select_parent)
        with pytest.raises(ConfigurationError):
            child_options = (
                sr.DateParameterOption('2020-01-01', parent_option_ids={'p0', 'p1'}),
                sr.DateParameterOption('2022-01-01', parent_option_ids={'p1', 'p2'})
            )
            p.DateParameter.WithParent('child', 'Child Param', child_options, single_select_parent)
        
    def test_get_selected(self):
        assert self.date_parameter.get_selected_date() == '2020-01-01'
        assert self.date_parameter.get_selected_date_quoted() == "'2020-01-01'"


class TestNumberParameter(TestParentParameters):
    number_parameter = p.NumberParameter('number', 'Number Value', min_value=0, max_value=10, increment=2, default_value=6)

    def test_with_selection(self):
        new_num_param = self.number_parameter.with_selection('4')

        expected = {
            'widget_type': 'NumberParameter',
            'name': 'number',
            'label': 'Number Value',
            'min_value': '0',
            'max_value': '10',
            'increment': '2',
            'selected_value': '6'
        }
        assert self.number_parameter.to_json_dict() == expected

        expected['selected_value'] = '4'
        assert new_num_param.to_json_dict() == expected

    def test_cascadable(self, single_select_parent: p.SingleSelectParameter):
        child_options = (
            sr.NumberParameterOption(0, 3, 1, 1, parent_option_ids={'p0', 'p1'}),
            sr.NumberParameterOption(0, 4, 2, 2, parent_option_ids={'p2', 'p3'})
        )
        child_param = p.NumberParameter.WithParent('child', 'Child Param', child_options, single_select_parent)

        new_parent = single_select_parent.with_selection('p2')

        expected = {
            'widget_type': 'NumberParameter',
            'name': 'child',
            'label': 'Child Param',
            'min_value': '0',
            'max_value': '3',
            'increment': '1',
            'selected_value': '1'
        }
        assert child_param.to_json_dict() == expected

        expected.update({
            'max_value': '4',
            'increment': '2',
            'selected_value': '2'
        })
        new_child = new_parent.get_all_dependent_params()['child']
        assert new_child.to_json_dict() == expected
    
    def test_invalid_selection(self):
        with pytest.raises(InvalidInputError):
            self.number_parameter.with_selection('')
        with pytest.raises(InvalidInputError):
            self.number_parameter.with_selection('not a number')
        with pytest.raises(InvalidInputError):
            self.number_parameter.with_selection('12')
        with pytest.raises(InvalidInputError):
            self.number_parameter.with_selection('5')

    def test_invalid_configuration(self, single_select_parent: p.SingleSelectParameter):
        with pytest.raises(ConfigurationError):
            p.NumberParameter('', '', min_value='a1', max_value=4, increment=2, default_value=1)
        with pytest.raises(ConfigurationError):
            p.NumberParameter('', '', min_value=3, max_value=1, increment=1, default_value=1)
        with pytest.raises(ConfigurationError):
            p.NumberParameter('', '', min_value=1, max_value=4, increment=2, default_value=1)
        with pytest.raises(ConfigurationError):
            child_options = (
                sr.NumberParameterOption(0, 3, 1, 1, parent_option_ids={'p0', 'p1'}),
                sr.NumberParameterOption(0, 4, 2, 2, parent_option_ids={'p1', 'p2'})
            )
            p.NumberParameter.WithParent('child', 'Child Param', child_options, single_select_parent)
    
    def test_get_selected(self):
        assert self.number_parameter.get_selected_value() == "6"


class TestNumRangeParameter(TestParentParameters):
    range_parameter = p.NumRangeParameter('range', 'Range Value', min_value=0, max_value=10, increment=2, default_lower_value=4, default_upper_value=6)

    def test_with_selection(self):
        new_num_param = self.range_parameter.with_selection('2,8')

        expected = {
            'widget_type': 'NumRangeParameter',
            'name': 'range',
            'label': 'Range Value',
            'min_value': '0',
            'max_value': '10',
            'increment': '2',
            'selected_lower_value': '4',
            'selected_upper_value': '6'
        }
        assert self.range_parameter.to_json_dict() == expected

        expected['selected_lower_value'] = '2'
        expected['selected_upper_value'] = '8'
        assert new_num_param.to_json_dict() == expected

    def test_cascadable(self, single_select_parent: p.SingleSelectParameter):
        child_options = (
            sr.NumRangeParameterOption(0, 3, 1, 1, 2, parent_option_ids={'p0', 'p1'}),
            sr.NumRangeParameterOption(0, 4, 2, 0, 4, parent_option_ids={'p2', 'p3'})
        )
        child_param = p.NumRangeParameter.WithParent('child', 'Child Param', child_options, single_select_parent)

        new_parent = single_select_parent.with_selection('p2')

        expected = {
            'widget_type': 'NumRangeParameter',
            'name': 'child',
            'label': 'Child Param',
            'min_value': '0',
            'max_value': '3',
            'increment': '1',
            'selected_lower_value': '1',
            'selected_upper_value': '2'
        }
        assert child_param.to_json_dict() == expected

        expected.update({
            'max_value': '4',
            'increment': '2',
            'selected_lower_value': '0',
            'selected_upper_value': '4'
        })
        new_child = new_parent.get_all_dependent_params()['child']
        assert new_child.to_json_dict() == expected

    def test_invalid_selection(self):
        with pytest.raises(InvalidInputError):
            self.range_parameter.with_selection('')
        with pytest.raises(InvalidInputError):
            self.range_parameter.with_selection('2')
        with pytest.raises(InvalidInputError):
            self.range_parameter.with_selection('2,4,6')
        with pytest.raises(InvalidInputError):
            self.range_parameter.with_selection('-2,6')
        with pytest.raises(InvalidInputError):
            self.range_parameter.with_selection('2,7')
        with pytest.raises(InvalidInputError):
            self.range_parameter.with_selection('not_number,2')

    def test_invalid_configuration(self, single_select_parent: p.SingleSelectParameter):
        with pytest.raises(ConfigurationError):
            child_options = (
                sr.NumRangeParameterOption(0, 3, 1, 1, 2, parent_option_ids={'p0', 'p1'}),
                sr.NumRangeParameterOption(0, 4, 2, 0, 4, parent_option_ids={'p2'})
            )
            p.NumRangeParameter.WithParent('child', 'Child Param', child_options, single_select_parent)

    def test_get_selected(self):
        assert self.range_parameter.get_selected_lower_value() == '4'
        assert self.range_parameter.get_selected_upper_value() == '6'
