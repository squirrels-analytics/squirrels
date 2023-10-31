from __future__ import annotations
from typing import Type, Sequence, Dict, List, Any, Iterator, Optional, Union
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from abc import ABCMeta, abstractmethod
import copy

from squirrels import parameter_options as po, _utils as u
from squirrels.data_sources import DataSource
from squirrels._parameter_set import ParameterSetBase
from squirrels._timed_imports import pandas as pd


class _ParameterBase(metaclass=ABCMeta):
    @abstractmethod
    def __init__(self, widget_type: str, name: str, label: str, all_options: Sequence[po.ParameterOption],
                 is_hidden: bool, parent: Optional[SelectionParameter]):
        self.widget_type = widget_type
        self.name = name
        self.label = label
        self.all_options = all_options
        self.is_hidden = is_hidden
        self.parent = parent

    def to_json_dict(self) -> Dict:
        """
        Helper method to convert the derived Parameter class into a JSON dictionary
        """
        return {
            'widget_type': self.widget_type,
            'name': self.name,
            'label': self.label
        }


class Parameter(_ParameterBase):
    """
    Abstract class for all parameter classes (except DataSourceParameters)
    """
    @abstractmethod
    def __init__(self, widget_type: str, name: str, label: str, all_options: Sequence[po.ParameterOption],
                 is_hidden: bool, parent: Optional[SelectionParameter]):
        super().__init__(widget_type, name, label, all_options, is_hidden, parent)

    def WithParent(all_options: Sequence[po.ParameterOption], parent: SingleSelectParameter, new_param: Parameter):
        """
        Helper class method to assign a SingleSelectParameter as the parent for another parameter

        Parameters:
            all_options: The list of options with one of "parent_option_id" or "parent_option_ids" attribute set.
            parent: The parent parameter. All option ids of the parent must exist at least once in "parent_option_ids" of all_options
            new_param: The child parameter to modify. Usually not a selection parameter
        """
        new_param._set_parent_and_options(parent, all_options)
        new_param.parent._add_child_mutate(new_param)
        return new_param.refresh(parent)

    def refresh(self, parent: Optional[SelectionParameter] = None) -> Parameter:
        """
        Refreshes the selectable options (or change of default value) based on the selection of the parent parameter

        Parameters:
            parent: The parent parameter subscribed to for updates
        
        Returns:
            A copy of self for the new selectable options based on current selection of parent
        """
        param_copy = copy.copy(self)
        if parent is not None:
            param_copy.parent = parent
        param_copy._refresh_mutate()
        return param_copy

    @abstractmethod
    def with_selection(self, _: str) -> Parameter:
        """
        Abstract method for applying the selection to the parameter
        """
        pass
    
    def get_all_dependent_params(self) -> ParameterSetBase:
        """
        Gets the collection of descendent parameters with changes applied based on the selection of this parameter

        Returns:
            A collection of descendent parameters as a ParameterSetBase
        """
        dependent_params = ParameterSetBase()
        self._accum_all_dependent_params(dependent_params)
        return dependent_params
    
    @abstractmethod
    def _set_default_as_selection_mutate(self) -> None:
        pass
    
    def _refresh_mutate(self) -> None:
        if self.parent is not None and hasattr(self, 'curr_option'):
            self.curr_option = next(self._get_valid_options_iterator())
        self._set_default_as_selection_mutate()
    
    def _get_valid_options_iterator(self) -> Iterator[po.ParameterOption]:
        selected_parent_option_ids = self.parent._get_selected_ids_as_list()
        return (x for x in self.all_options if x.is_valid(selected_parent_option_ids))
    
    def _raise_invalid_input_error(self, selection: str, more_details: str = '', e: Exception = None) -> None:
        raise u.InvalidInputError(f'Selected value "{selection}" is not valid for parameter "{self.name}". ' + more_details) from e
    
    def _verify_parent_is_single_select(self) -> None:
        if not isinstance(self.parent, SingleSelectParameter):
            raise u.ConfigurationError(f'For "{self.name}", it''s not a selection parameter, so its parent must be a SingleSelectParameter')
        
    def _verify_parent_options_have_one_child_each(self) -> None:
        accum_set = set()
        for option in self.all_options:
            if not accum_set.isdisjoint(option.parent_option_ids):
                raise u.ConfigurationError(f'For "{self.name}", it''s not a selection parameter, so no two options can share the same parent option')
            accum_set = accum_set.union(option.parent_option_ids)
        if len(accum_set) != len(self.parent.options):
            raise u.ConfigurationError(f'For "{self.name}", all parent option ids must exist across all options')
    
    def _set_parent_and_options(self, parent: SingleSelectParameter, all_options: Sequence[po.ParameterOption]) -> None:
        self.parent = parent
        self.all_options = all_options
        self._verify_parent_is_single_select()
        self._verify_parent_options_have_one_child_each()
    
    def _accum_all_dependent_params(self, param_set: ParameterSetBase) -> None:
        param_set.add_parameter(self)
        
    def _enquote(self, value: str) -> str:
        return "'" + value.replace("'", "''") + "'" 


class SelectionParameter(Parameter):
    """
    Abstract class for select parameter classes (single-select, multi-select, etc)
    """
    def __init__(self, widget_type: str, name: str, label: str, all_options: Sequence[po.SelectParameterOption], 
                 is_hidden: bool, parent: Optional[SelectionParameter]):
        super().__init__(widget_type, name, label, all_options, is_hidden, parent)
        self.trigger_refresh: bool = False
        self.options: Sequence[po.SelectParameterOption] = tuple(self.all_options)
        self.children: List[SelectionParameter] = list()
        if self.parent is not None:
            self.parent._add_child_mutate(self)
        self._refresh_mutate()
    
    def _add_child_mutate(self, child: Parameter) -> None:
        self.children.append(child)
        self.trigger_refresh = True
    
    def _refresh_mutate(self) -> None:
        if self.parent is not None:
            self.options = tuple(self._get_valid_options_iterator())
        self._set_default_as_selection_mutate()
        self.children = [child.refresh(self) for child in self.children]

    @abstractmethod
    def _get_selected_ids_as_list(self) -> Sequence[str]:
        pass

    def _get_default_iterator(self) -> Iterator[po.ParameterOption]:
        return (x.identifier for x in self.options if x.is_default)
    
    def _validate_selected_id_in_options(self, selected_id: str) -> str:
        if selected_id in (x.identifier for x in self.options):
            return selected_id
        else:
            self._raise_invalid_input_error(selected_id)
    
    def _accum_all_dependent_params(self, param_set: ParameterSetBase) -> None:
        super()._accum_all_dependent_params(param_set)
        for child in self.children:
            child._accum_all_dependent_params(param_set)

    def to_json_dict(self):
        """
        Helper method to convert the derived selection parameter class into a JSON object
        """
        output = super().to_json_dict()
        output['options'] = [x.to_dict() for x in self.options]
        output['trigger_refresh'] = self.trigger_refresh
        return output


@dataclass
class SingleSelectParameter(SelectionParameter):
    """
    Class to define attributes for single-select parameter widgets.

    Attributes:
        name: The name of the parameter
        label: The display label for the parameter
        all_options: A sequence of SelectParameterOption which defines the attribute for each dropdown option
        is_hidden: Whether the parameter is hidden in the parameters API response. Default is False.
        parent: The parent parameter that may cascade the options for this parameter. Default is no parent
        selected_id: The ID of the selected option
    """
    name: str
    label: str
    all_options: Sequence[po.ParameterOption]
    is_hidden: bool
    parent: Optional[SelectionParameter]
    selected_id: str

    def __init__(self, name: str, label: str, all_options: Sequence[po.SelectParameterOption], *, 
                 is_hidden: bool = False, parent: Optional[SelectionParameter] = None) -> None:
        """
        Constructor for SingleSelectParameter class

        Parameters:
            ...see Attributes of SingleSelectParameter except "selected_id"
        """
        super().__init__("SingleSelectParameter", name, label, all_options, is_hidden, parent)
    
    def with_selection(self, selection: str) -> SingleSelectParameter:
        """
        Applies the selected value to this widget parameter

        Parameters:
            selection: The selected value as an ID of one of the dropdown options
        
        Returns:
            A new copy of SingleSelectParameter with the selection applied
        """
        param_copy = copy.copy(self)
        param_copy.selected_id = self._validate_selected_id_in_options(selection)
        param_copy.children = [child.refresh(param_copy) for child in param_copy.children]
        return param_copy
    
    def get_selected(self, field: Optional[str] = None, *, default_field: Optional[str] = None,
                     default: Any = None) -> Union[po.SelectParameterOption, str]:
        """
        Gets the selected single-select option or selected custom field

        Parameters:
            field: If field is not None, the method gets this field from the "custom_fields" attribute of the selected option. 
                Otherwise, returns the class object of the selected option
            default_field: If field does not exist for a parameter option and default_field is not None, the default_field is used 
                as the "field" instead. Does nothing if field is None
            default: If field does not exist for a parameter option, default_field is None, but default is not None, then the default 
                is returned as the selected field. Does nothing if field is None or default_field is not None

        Returns:
            A SelectParameterOption class object if no field is provided, or the type of the custom field
        """
        selected = next(x for x in self.options if x.identifier == self.selected_id)
        if field is not None:
            selected = selected.get_custom_field(field, default_field, default)
        return selected
    
    def get_selected_id(self) -> str:
        """
        Gets the ID of the selected option

        Returns:
            A string ID
        """
        return self.get_selected().identifier
    
    def get_selected_id_quoted(self) -> str:
        """
        Gets the ID of the selected option surrounded by single quotes

        Returns:
            A string
        """
        return self._enquote(self.get_selected_id())
    
    def get_selected_label(self) -> str:
        """
        Gets the label of the selected option

        Returns:
            A string
        """
        return self.get_selected().label
    
    def get_selected_label_quoted(self) -> str:
        """
        Gets the label of the selected option surrounded by single quotes

        Returns:
            A string
        """
        return self._enquote(self.get_selected_label())
    
    # Overriding for refresh method
    def _get_selected_ids_as_list(self) -> Sequence[str]:
        return (self.get_selected_id(),)
    
    def _get_default(self) -> str:
        default_id = next(self._get_default_iterator(), None)
        if default_id is None:
            default_id = self.options[0].identifier if len(self.options) > 0 else None
        return default_id
    
    def _set_default_as_selection_mutate(self) -> None:
        self.selected_id = self._get_default()

    def to_json_dict(self) -> Dict:
        """
        Converts this parameter as a JSON object for the parameters API response

        Returns:
            A dictionary for the JSON object
        """
        output = super().to_json_dict()
        output['selected_id'] = self.selected_id
        return output


@dataclass
class MultiSelectParameter(SelectionParameter):
    """
    Class to define attributes for multi-select parameter widgets.

    Attributes:
        widget_type: The type of widget parameter (and the name of the most-derived class)
        name: The name of the parameter
        label: The display label for the parameter
        all_options: A sequence of SelectParameterOption which defines the attribute for each dropdown option
        is_hidden: Whether the parameter is hidden in the parameters API response. Default is False.
        parent: The parent parameter that may cascade the options for this parameter. Default is no parent
        selected_ids: A sequence of selected options
        include_all: Whether applying no selection is equivalent to selecting all. Default is True
        order_matters: Whether the ordering of the selection matters. Default is False 
    """
    widget_type: str
    name: str
    label: str
    all_options: Sequence[po.ParameterOption]
    is_hidden: bool
    parent: Optional[SelectionParameter]
    selected_ids: Sequence[str]
    include_all: bool
    order_matters: bool

    def __init__(self, name: str, label: str, all_options: Sequence[po.SelectParameterOption], *, is_hidden = False,
                 parent: Optional[SelectionParameter] = None, include_all: bool = True, order_matters: bool = False) -> None:
        """
        Constructor for MultiSelectParameter class
        """
        super().__init__("MultiSelectParameter", name, label, all_options, is_hidden, parent)
        self.include_all = include_all
        self.order_matters = order_matters

    def with_selection(self, selection: str) -> MultiSelectParameter:
        """
        Applies the selected value(s) to this widget parameter

        Parameters:
            selection: A JSON string of list of strings representing IDs of selected values
        
        Returns:
            A new copy of MultiSelectParameter with the selection applied
        """
        param_copy = copy.copy(self)
        selection_split = u.load_json_or_comma_delimited_str_as_list(selection)
        param_copy.selected_ids = tuple(self._validate_selected_id_in_options(x) for x in selection_split)
        param_copy.children = [child.refresh(param_copy) for child in self.children]
        return param_copy
    
    def has_non_empty_selection(self) -> bool:
        """
        Returns True if more than zero options were selected. False otherwise.
        
        Note that even when this returns False, all "get_selected" functions would 
        return the full list of options if "include_all" is set to True

        Returns:
            A boolean
        """
        return len(self.selected_ids) > 0

    def get_selected_list(self, field: Optional[str] = None, *, default_field: Optional[str] = None,
                          default: Any = None) -> Sequence[Union[po.SelectParameterOption, Any]]:
        """
        Gets the sequence of the selected option(s) or a sequence of selected custom fields

        Parameters:
            field: If field is not None, the method gets this field from the "custom_fields" attribute of the selected options. 
                Otherwise, returns the class objects of the selected options
            default_field: If field does not exist for a parameter option and default_field is not None, the default_field is used 
                as the "field" instead. Does nothing if field is None
            default: If field does not exist for a parameter option, default_field is None, but default is not None, the default 
                is returned as the selected field. Does nothing if field is None or default_field is not None

        Returns:
            A sequence of SelectParameterOption class objects or sequence of type of custom field
        """
        if not self.has_non_empty_selection() and self.include_all:
            selected_list = self.options
        else:
            selected_list = (x for x in self.options if x.identifier in self.selected_ids)
        
        if field is not None:
            selected_list = [selected.get_custom_field(field, default_field, default) for selected in selected_list]
        
        return tuple(selected_list)

    def get_selected_ids_as_list(self) -> Sequence[str]:
        """
        Gets the sequence of ID(s) of the selected option(s)

        Returns:
            A sequence of strings
        """
        return tuple(x.identifier for x in self.get_selected_list())
    
    def get_selected_ids_joined(self) -> str:
        """
        Gets the ID(s) of the selected option(s) joined by comma

        Returns:
            A string
        """
        return ', '.join(self.get_selected_ids_as_list())
    
    def get_selected_ids_quoted_as_list(self) -> Sequence[str]:
        """
        Gets the sequence of ID(s) of the selected option(s) surrounded by single quotes

        Returns:
            A sequence of strings
        """
        return tuple(self._enquote(x) for x in self.get_selected_ids_as_list())
    
    def get_selected_ids_quoted_joined(self) -> str:
        """
        Gets the ID(s) of the selected option(s) surrounded by single quotes and joined by comma

        Returns:
            A string
        """
        return ', '.join(self.get_selected_ids_quoted_as_list())
    
    def get_selected_labels_as_list(self) -> Sequence[str]:
        """
        Gets the sequence of label(s) of the selected option(s)

        Returns:
            A sequence of strings
        """
        return tuple(x.label for x in self.get_selected_list())
    
    def get_selected_labels_joined(self) -> str:
        """
        Gets the label(s) of the selected option(s) joined by comma

        Returns:
            A string
        """
        return ', '.join(self.get_selected_labels_as_list())
    
    def get_selected_labels_quoted_as_list(self) -> Sequence[str]:
        """
        Gets the sequence of label(s) of the selected option(s) surrounded by single quotes

        Returns:
            A sequence of strings
        """
        return tuple(self._enquote(x) for x in self.get_selected_labels_as_list())
    
    def get_selected_labels_quoted_joined(self) -> str:
        """
        Gets the label(s) of the selected option(s) surrounded by single quotes and joined by comma

        Returns:
            A string
        """
        return ', '.join(self.get_selected_labels_quoted_as_list())
    
    def _get_selected_ids_as_list(self) -> Sequence[str]:
        return self.get_selected_ids_as_list()
    
    def _get_default(self) -> Sequence[str]:
        return tuple(self._get_default_iterator())
    
    def _set_default_as_selection_mutate(self):
        self.selected_ids = self._get_default()

    def to_json_dict(self):
        """
        Converts this parameter as a JSON object for the parameters API response

        Returns:
            A dictionary for the JSON object
        """
        output = super().to_json_dict()
        output['selected_ids'] = list(self.selected_ids)
        output['include_all'] = self.include_all
        output['order_matters'] = self.order_matters
        return output


class DateTypeParameter(Parameter):
    """
    Abstract class for date type parameter classes (single date, date range, etc)
    """
    @abstractmethod
    def __init__(self, widget_type: str, name: str, label: str, all_options: Sequence[po.DateTypeParameterOption], 
                 is_hidden: bool, parent: Optional[SelectionParameter]):
        super().__init__(widget_type, name, label, all_options, is_hidden, parent)


@dataclass
class DateParameter(DateTypeParameter):
    """
    Class to define attributes for date parameter widgets.
    """
    curr_option: po.DateParameterOption
    selected_date: datetime

    def __init__(self, name: str, label: str, default_date: Union[str, datetime], 
                 *, date_format: str = '%Y-%m-%d', is_hidden: bool = False) -> None:
        """
        Constructor for DateParameter class

        Parameters:
            name: The name of the parameter
            label: The display label for the parameter
            default_date: The default selected date
            date_format: The format of the default_date. Default is '%Y-%m-%d'
            is_hidden: Whether the parameter is hidden in the parameters API response. Default is False
        """
        self.curr_option = po.DateParameterOption(default_date, date_format)
        all_options = (self.curr_option,)
        super().__init__("DateParameter", name, label, all_options, is_hidden, None)
        self._set_default_as_selection_mutate()
    
    @staticmethod
    def WithParent(name: str, label: str, all_options: Sequence[po.DateParameterOption], parent: SingleSelectParameter, *, 
                   is_hidden: bool = False) -> DateParameter:
        """
        A factory method to construct a DateParameter with a parent parameter

        Parameters:
            name: The name of the parameter
            label: The display label for the parameter
            all_options: A sequence of DateParameterOption which contains various default dates linked to specific parent options
            parent: The parent parameter, which must be a SingleSelectParameter
            is_hidden: Whether the parameter is hidden in the parameters API response. Default is False
        """
        new_param = DateParameter(name, label, '2020-01-01', is_hidden=is_hidden) # dummy date in valid format
        return Parameter.WithParent(all_options, parent, new_param)
    
    def with_selection(self, selection: str):
        """
        Applies the selected date to this widget parameter

        Parameters:
            selection: The date string which must be in yyyy-mm-dd format (regardless of self.date_format value)
        
        Returns:
            A new copy of DateParameter with the selection applied
        """
        param_copy = copy.copy(self)
        try:
            param_copy.selected_date = datetime.strptime(selection, "%Y-%m-%d")
        except ValueError as e:
            self._raise_invalid_input_error(selection, 'Invalid selection for date.', e)
        return param_copy

    def get_selected_date(self, date_format: str = None) -> str:
        """
        Gets selected date as string

        Parameters:
            date_format: The date format (see Python's datetime formats). If not specified, self.date_format is used

        Returns:
            A string
        """
        date_format = self.curr_option.date_format if date_format is None else date_format
        return self.selected_date.strftime(date_format)

    def get_selected_date_quoted(self, date_format: str = None) -> str:
        """
        Gets selected date as string surrounded by single quotes

        Parameters:
            date_format: The date format (see Python's datetime formats). If not specified, self.date_format is used

        Returns:
            A string
        """
        return self._enquote(self.get_selected_date(date_format))
    
    def _set_default_as_selection_mutate(self) -> None:
        self.selected_date = self.curr_option.default_date
    
    def to_json_dict(self):
        """
        Converts this parameter as a JSON object for the parameters API response

        The "selected_date" field will always be in yyyy-mm-dd format

        Returns:
            A dictionary for the JSON object
        """
        output = super().to_json_dict()
        output['selected_date'] = self.get_selected_date("%Y-%m-%d")
        return output


@dataclass
class DateRangeParameter(DateTypeParameter):
    """
    Class to define attributes for date range parameter widgets.
    """
    curr_option: po.DateRangeParameterOption
    selected_start_date: datetime
    selected_end_date: datetime

    def __init__(self, name: str, label: str, default_start_date: Union[str, datetime], default_end_date: Union[str, datetime], 
                 *, date_format: str = '%Y-%m-%d', is_hidden: bool = False) -> None:
        """
        Constructor for DateRangeParameter class

        Parameters:
            name: The name of the parameter
            label: The display label for the parameter
            default_start_date: The default selected start date
            default_end_date: The default selected end date
            date_format: The format of the default_date. Default is '%Y-%m-%d'
            is_hidden: Whether the parameter is hidden in the parameters API response. Default is False
        """
        self.curr_option = po.DateRangeParameterOption(default_start_date, default_end_date, date_format)
        all_options = (self.curr_option,)
        super().__init__("DateRangeParameter", name, label, all_options, is_hidden, None)
        self._set_default_as_selection_mutate()@staticmethod
    
    def WithParent(name: str, label: str, all_options: Sequence[po.DateParameterOption], parent: SingleSelectParameter, *, 
                   is_hidden: bool = False) -> DateParameter:
        """
        A factory method to construct a DateParameter with a parent parameter

        Parameters:
            name: The name of the parameter
            label: The display label for the parameter
            all_options: A sequence of DateParameterOption which contains various default dates linked to specific parent options
            parent: The parent parameter, which must be a SingleSelectParameter
            is_hidden: Whether the parameter is hidden in the parameters API response. Default is False
        """
        new_param = DateRangeParameter(name, label, '2020-01-01', '2020-01-01', is_hidden=is_hidden) # dummy date in valid format
        return Parameter.WithParent(all_options, parent, new_param)
    
    def with_selection(self, selection: str):
        """
        Applies the selected date to this widget parameter

        Parameters:
            selection: The date string which must be in yyyy-mm-dd format (regardless of self.date_format value)
        
        Returns:
            A new copy of DateParameter with the selection applied
        """
        param_copy = copy.copy(self)
        try:
            param_copy.selected_date = datetime.strptime(selection, "%Y-%m-%d")
        except ValueError as e:
            self._raise_invalid_input_error(selection, 'Invalid selection for date.', e)
        return param_copy


class NumericParameter(Parameter):
    """
    Abstract class for numeric parameter classes (single number, number range, etc)
    """
    @abstractmethod
    def __init__(self, widget_type: str, name: str, label: str, all_options: Sequence[po.NumericParameterOption], 
                 is_hidden: bool, parent: Optional[SelectionParameter], curr_option: po.NumericParameterOption):
        super().__init__(widget_type, name, label, all_options, is_hidden, parent)
        self.curr_option = curr_option
    
    def to_json_dict(self):
        """
        Helper method to converts numeric parameters into JSON objects for the parameters API response

        Returns:
            A dictionary for the JSON object
        """
        output = super().to_json_dict()
        output['min_value'] = str(self.curr_option.min_value)
        output['max_value'] = str(self.curr_option.max_value)
        output['increment'] = str(self.curr_option.increment)
        return output


@dataclass
class NumberParameter(NumericParameter):
    """
    Class to define attributes for number slider parameter widgets.
    """
    selected_value: Decimal

    def __init__(self, name: str, label: str, min_value: po.Number, max_value: po.Number, increment: po.Number = 1, 
                 default_value: po.Number = None, *, is_hidden: bool = False) -> None:
        """
        Constructor for NumberParameter class

        Parameters:
            name: The name of the parameter
            label: The display label for the parameter
            min_value: The minimum bound for selection. Can be of type Decimal, integer, or number parsable string
            max_value: The maximum bound for selection. Can be of type Decimal, integer, or number parsable string
            increment: The increment for allowable selections. Can be of type Decimal, integer, or number parsable string. Default is 1
            default_value: The default selection. Can be of type Decimal, integer, or number parsable string. Default is min_value
            is_hidden: Whether the parameter is hidden in the parameters API response. Default is False
        """
        default_value = default_value if default_value is not None else min_value
        curr_option = po.NumberParameterOption(min_value, max_value, increment, default_value)
        all_options = (curr_option,)
        super().__init__("NumberParameter", name, label, all_options, is_hidden, None, curr_option)
        self._set_default_as_selection_mutate()
    
    @staticmethod
    def WithParent(name: str, label: str, all_options: Sequence[po.NumberParameterOption], parent: SingleSelectParameter, *, 
                   is_hidden: bool = False) -> DateParameter:
        """
        A factory method to construct a NumberParameter with a parent parameter

        Parameters:
            name: The name of the parameter
            label: The display label for the parameter
            all_options: A sequence of NumberParameterOption which contains various bounds and default values linked to specific parent options
            parent: The parent parameter, which must be a SingleSelectParameter
            is_hidden: Whether the parameter is hidden in the parameters API response. Default is False
        """
        new_param = NumberParameter(name, label, 0, 1, is_hidden=is_hidden) # dummy values
        return Parameter.WithParent(all_options, parent, new_param)
    
    def with_selection(self, selection: str):
        """
        Applies the selected number to this widget parameter

        Parameters:
            selection: The selected number (must be a string parsable as a number)
        
        Returns:
            A new copy of NumberParameter with the selection applied
        """
        param_copy = copy.copy(self)
        try:
            param_copy.selected_value = param_copy.curr_option._validate_value(selection)
        except u.ConfigurationError as e:
            self._raise_invalid_input_error(selection, 'Invalid selection for number parameter.', e)
        return param_copy

    def get_selected_value(self) -> str:
        """
        Get the selected number

        Returns:
            A number parsable string of the selected number
        """
        return str(self.selected_value)
    
    def _set_default_as_selection_mutate(self) -> None:
        self.curr_option: po.NumberParameterOption
        self.selected_value = self.curr_option.default_value
        
    def to_json_dict(self):
        """
        Converts this parameter as a JSON object for the parameters API response

        Returns:
            A dictionary for the JSON object
        """
        output = super().to_json_dict()
        output['selected_value'] = self.get_selected_value()
        return output


@dataclass
class NumRangeParameter(NumericParameter):
    """
    Class to define attributes for number range slider (double-ended) parameter widgets.
    """
    selected_lower_value: Decimal
    selected_upper_value: Decimal

    def __init__(self, name: str, label: str, min_value: po.Number, max_value: po.Number, increment: po.Number = 1, 
                 default_lower_value: po.Number = None, default_upper_value: po.Number = None, *, is_hidden: bool = False) -> None:
        """
        Constructor for NumberParameter class

        Parameters:
            name: The name of the parameter
            label: The display label for the parameter
            min_value: The minimum bound for selection. Can be of type Decimal, integer, or number parsable string
            max_value: The maximum bound for selection. Can be of type Decimal, integer, or number parsable string
            increment: The increment for allowable selections. Can be of type Decimal, integer, or number parsable string. Default is 1
            default_lower_value: The default lower selection. Can be of type Decimal, integer, or number parsable string. Default is min_value
            default_upper_value: The default upper selection. Can be of type Decimal, integer, or number parsable string. Default is max_value
            is_hidden: Whether the parameter is hidden in the parameters API response. Default is False
        """
        default_lower_value = default_lower_value if default_lower_value is not None else min_value
        default_upper_value = default_upper_value if default_upper_value is not None else max_value
        curr_option = po.NumRangeParameterOption(min_value, max_value, increment, default_lower_value, default_upper_value)
        all_options = (curr_option,)
        super().__init__("NumRangeParameter", name, label, all_options, is_hidden, None, curr_option)
        self._set_default_as_selection_mutate()
    
    @staticmethod
    def WithParent(name: str, label: str, all_options: Sequence[po.NumRangeParameterOption], parent: SingleSelectParameter, *, 
                   is_hidden: bool = False) -> DateParameter:
        """
        A factory method to construct a NumRangeParameter with a parent parameter

        Parameters:
            name: The name of the parameter
            label: The display label for the parameter
            all_options: A sequence of NumRangeParameterOption which contains various bounds and default values linked to specific parent options
            parent: The parent parameter, which must be a SingleSelectParameter
            is_hidden: Whether the parameter is hidden in the parameters API response. Default is False
        """
        new_param = NumRangeParameter(name, label, 0, 1, is_hidden=is_hidden) # dummy values
        return Parameter.WithParent(all_options, parent, new_param)
    
    def with_selection(self, selection: str):
        """
        Applies the selected numbers to this widget parameter

        Parameters:
            selection: The lower and upper selected numbers joined by comma (with no spaces)
        
        Returns:
            A new copy of NumRangeParameter with the selection applied
        """
        try:
            lower, upper = selection.split(',')
        except ValueError as e:
            self._raise_invalid_input_error(selection, "Range parameter selection must be two numbers joined by comma.", e)

        param_copy = copy.copy(self)
        try:
            param_copy.selected_lower_value = param_copy.curr_option._validate_value(lower)
            param_copy.selected_upper_value = param_copy.curr_option._validate_value(upper, param_copy.selected_lower_value)
        except u.ConfigurationError as e:
            self._raise_invalid_input_error(selection, 'Invalid selection for range parameter.', e)
        return param_copy

    def get_selected_lower_value(self) -> str:
        """
        Get the selected lower number

        Returns:
            A number parsable string of the selected number
        """
        return str(self.selected_lower_value)

    def get_selected_upper_value(self) -> str:
        """
        Get the selected upper number

        Returns:
            A number parsable string of the selected number
        """
        return str(self.selected_upper_value)
    
    def _set_default_as_selection_mutate(self) -> None:
        self.curr_option: po.NumRangeParameterOption
        self.selected_lower_value = self.curr_option.default_lower_value
        self.selected_upper_value = self.curr_option.default_upper_value

    def to_json_dict(self):
        """
        Converts this parameter as a JSON object for the parameters API response

        Returns:
            A dictionary for the JSON object
        """
        output = super().to_json_dict()
        output['selected_lower_value'] = self.get_selected_lower_value()
        output['selected_upper_value'] = self.get_selected_upper_value()
        return output


@dataclass
class DataSourceParameter(_ParameterBase):
    """
    Class for parameters that can use a lookup table to convert itself into another parameter
    """
    parameter_class: Type[Parameter]
    data_source: DataSource
    parent: Optional[Parameter] 

    def __init__(self, parameter_class: Type[Parameter], name: str, label: str, data_source: DataSource, *, 
                 is_hidden: bool = False, parent: Optional[Parameter] = None) -> None:
        """
        Constructor for DataSourceParameter, a Parameter that uses a DataSource to convert itself to another Parameter

        Parameters:
            parameter_class: The class of widget parameter to convert to
            name: The name of the parameter
            label: The display label for the parameter
            data_source: The lookup table to use for this parameter
            is_hidden: Whether the parameter is hidden in the parameters API response. Default is False
            parent: The parent parameter that may cascade the options for this parameter. Default is no parent
        """
        super().__init__("DataSourceParameter", name, label, None, is_hidden, None)
        self.parameter_class = parameter_class
        self.data_source = data_source
        self.parent = parent

    def convert(self, df: pd.DataFrame) -> Parameter:
        """
        Method to convert this DataSourceParameter into another parameter

        Parameters:
            df: The dataframe containing the parameter options data

        Returns:
            The converted parameter
        """
        return self.data_source.convert(self, df)
    
    def to_json_dict(self) -> Dict:
        """
        Converts this parameter as a JSON object for the parameters API response

        Returns:
            A dictionary for the JSON object
        """
        output = super().to_json_dict()
        output['widget_type'] = self.parameter_class.__name__
        output['data_source'] = self.data_source.__dict__
        return output
