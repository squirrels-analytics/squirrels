from __future__ import annotations
from typing import Dict, Sequence, Optional, Union, Any
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from abc import ABCMeta, abstractmethod

from . import _parameter_configs as pc, parameter_options as po, data_sources as d


@dataclass
class Parameter(metaclass=ABCMeta):
    """
    Abstract class for all parameter widgets
    """
    config: pc.ParameterConfig
    
    @classmethod
    def CreateFromSource(
        cls, name: str, label: str, data_source: d.DataSource, *, is_hidden: bool = False, user_attribute: Optional[str] = None, 
        parent_name: Optional[str] = None
    ) -> None:
        """
        Method for creating the configurations for any Parameter that uses a DataSource to received the options

        Parameters:
            name: The name of the parameter
            label: The display label for the parameter
            data_source: The lookup table to use for this parameter
            is_hidden: Whether the parameter is hidden in the parameters API response. Default is False
            user_attribute: The user attribute that may cascade the options for this parameter. Default is None
            parent_name: Name of parent parameter that may cascade the options for this parameter. Default is None (no parent)
        """
        param_config = pc.DataSourceParameterConfig(cls, name, label, data_source, is_hidden=is_hidden, user_attribute=user_attribute, 
                                                    parent_name=parent_name)
        pc.ParameterConfigsSetIO.obj.add(param_config)
        
    def _enquote(self, value: str) -> str:
        return "'" + value.replace("'", "''") + "'" 
    
    @abstractmethod
    def to_json_dict(self) -> Dict:
        """
        Helper method to convert the derived Parameter class into a JSON dictionary
        """
        return self.config.to_json_dict()


@dataclass
class _SelectionParameter(Parameter):
    config: pc.SelectionParameterConfig
    options: Sequence[po.SelectParameterOption]

    @abstractmethod
    def _get_selected_ids_as_list(self) -> Sequence[str]:
        pass

    def _validate_selected_id_in_options(self, selected_id):
        if selected_id not in (x.identifier for x in self.options):
            self.config._raise_invalid_input_error(selected_id)

    @abstractmethod
    def to_json_dict(self):
        """
        Helper method to convert the derived selection parameter class into a JSON object
        """
        output = super().to_json_dict()
        output['options'] = [x._to_json_dict() for x in self.options]
        return output


@dataclass
class SingleSelectParameter(_SelectionParameter):
    """
    Class for single-select parameter widgets.

    Attributes:
        config: The config for this widget parameter (for immutable attributes like name, label, all possible options, etc)
        options: The parameter options that are currently selectable
        selected_id: The ID of the selected option
    """
    config: pc.SingleSelectParameterConfig
    selected_id: str

    def __post_init__(self):
        self._validate_selected_id_in_options(self.selected_id)
    
    @staticmethod
    def Create(
        name: str, label: str, all_options: Sequence[po.SelectParameterOption], *, is_hidden: bool = False, 
        user_attribute: Optional[str] = None, parent_name: Optional[str] = None
    ) -> None:
        """
        Method for creating the configurations for a Parameter that may include user attribute or parent

        Parameters:
            name: The name of the parameter
            label: The display label for the parameter
            all_options: All options associated to this parameter regardless of the user group or parent parameter option they depend on
            is_hidden: Whether the parameter is hidden in the parameters API response. Default is False
            user_attribute: The user attribute that may cascade the options for this parameter. Default is None
            parent_name: Name of parent parameter that may cascade the options for this parameter. Default is None (no parent)
        """
        param_config = pc.SingleSelectParameterConfig(name, label, all_options, is_hidden=is_hidden, user_attribute=user_attribute, 
                                                      parent_name=parent_name)
        pc.ParameterConfigsSetIO.obj.add(param_config)
    
    @classmethod
    def CreateSimple(cls, name: str, label: str, all_options: Sequence[po.SelectParameterOption], *, is_hidden: bool = False) -> None:
        """
        Method for creating the configurations for a Parameter that doesn't involve user attributes or parent parameters

        Parameters:
            name: The name of the parameter
            label: The display label for the parameter
            all_options: All options associated to this parameter regardless of the user group or parent parameter option they depend on
            is_hidden: Whether the parameter is hidden in the parameters API response. Default is False
        """
        cls.Create(name, label, all_options, is_hidden=is_hidden)
    
    def get_selected(
        self, field: Optional[str] = None, *, default_field: Optional[str] = None, default: Any = None
    ) -> Union[po.SelectParameterOption, str]:
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
            selected = selected.get_custom_field(field, default_field=default_field, default=default)
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

    def _get_selected_ids_as_list(self) -> Sequence[str]:
        return (self.get_selected_id(),)

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
class MultiSelectParameter(_SelectionParameter):
    """
    Class for multi-select parameter widgets.

    Attributes:
        config: The config for this widget parameter (for immutable attributes like name, label, all possible options, etc)
        options: The parameter options that are currently selectable
        selected_ids: A sequence of IDs of the selected options
    """
    config: pc.MultiSelectParameterConfig
    selected_ids: Sequence[str]

    def __post_init__(self):
        for selected_id in self.selected_ids:
            self._validate_selected_id_in_options(selected_id)
    
    @staticmethod
    def Create(
        name: str, label: str, all_options: Sequence[po.SelectParameterOption], *, include_all: bool = True, order_matters: bool = False, 
        is_hidden: bool = False, user_attribute: Optional[str] = None, parent_name: Optional[str] = None
    ) -> None:
        """
        Method for creating the configurations for a Parameter that may include user attribute or parent

        Parameters:
            name: The name of the parameter
            label: The display label for the parameter
            all_options: All options associated to this parameter regardless of the user group or parent parameter option they depend on
            include_all: Whether having no options selected is equivalent to all selectable options selected
            order_matters: Whether the order of the selections made matter
            is_hidden: Whether the parameter is hidden in the parameters API response. Default is False
            user_attribute: The user attribute that may cascade the options for this parameter. Default is None
            parent_name: Name of parent parameter that may cascade the options for this parameter. Default is None (no parent)
        """
        param_config = pc.MultiSelectParameterConfig(name, label, all_options, include_all=include_all, order_matters=order_matters, 
                                                     is_hidden=is_hidden, user_attribute=user_attribute, parent_name=parent_name)
        pc.ParameterConfigsSetIO.obj.add(param_config)

    @classmethod
    def CreateSimple(
        cls, name: str, label: str, all_options: Sequence[po.SelectParameterOption], *, include_all: bool = True, 
        order_matters: bool = False, is_hidden: bool = False
    ) -> None:
        """
        Method for creating the configurations for a Parameter that doesn't involve user attributes or parent parameters

        Parameters:
            name: The name of the parameter
            label: The display label for the parameter
            all_options: All options associated to this parameter regardless of the user group or parent parameter option they depend on
            include_all: Whether having no options selected is equivalent to all selectable options selected
            order_matters: Whether the order of the selections made matter
            is_hidden: Whether the parameter is hidden in the parameters API response. Default is False
        """
        cls.Create(name, label, all_options, include_all=include_all, order_matters=order_matters, is_hidden=is_hidden)
    
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
        if not self.has_non_empty_selection() and self.config.include_all:
            selected_list = self.options
        else:
            selected_list = (x for x in self.options if x.identifier in self.selected_ids)
        
        if field is not None:
            selected_list = [selected.get_custom_field(field, default_field=default_field, default=default) for selected in selected_list]
        
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
        return ','.join(self.get_selected_ids_as_list())
    
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
        return ','.join(self.get_selected_ids_quoted_as_list())
    
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
        return ','.join(self.get_selected_labels_as_list())
    
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
        return ','.join(self.get_selected_labels_quoted_as_list())

    def _get_selected_ids_as_list(self) -> Sequence[str]:
        return self.get_selected_ids_as_list()
    
    def to_json_dict(self):
        """
        Converts this parameter as a JSON object for the parameters API response

        Returns:
            A dictionary for the JSON object
        """
        output = super().to_json_dict()
        output['selected_ids'] = list(self.selected_ids)
        return output


@dataclass
class DateParameter(Parameter):
    """
    Class for date parameter widgets.

    Attributes:
        config: The config for this widget parameter (for immutable attributes like name, label, all possible options, etc)
        curr_option: The current option showing for defaults based on user attribute and selection of parent
    """
    curr_option: po.DateParameterOption
    selected_date: datetime
    
    @staticmethod
    def Create(
        name: str, label: str, all_options: Sequence[po.DateParameterOption], *, is_hidden: bool = False, 
        user_attribute: Optional[str] = None, parent_name: Optional[str] = None
    ) -> None:
        """
        Method for creating the configurations for a Parameter that may include user attribute or parent

        Parameters:
            name: The name of the parameter
            label: The display label for the parameter
            all_options: All options associated to this parameter regardless of the user group or parent parameter option they depend on
            is_hidden: Whether the parameter is hidden in the parameters API response. Default is False
            user_attribute: The user attribute that may cascade the options for this parameter. Default is None
            parent_name: Name of parent parameter that may cascade the options for this parameter. Default is None (no parent)
        """
        param_config = pc.DateParameterConfig(name, label, all_options, is_hidden=is_hidden, user_attribute=user_attribute, 
                                              parent_name=parent_name)
        pc.ParameterConfigsSetIO.obj.add(param_config)

    @classmethod
    def CreateSimple(
        cls, name: str, label: str, default_date: Union[str, datetime], *, date_format: str = '%Y-%m-%d', is_hidden: bool = False
    ) -> None:
        """
        Method for creating the configurations for a Parameter that doesn't involve user attributes or parent parameters

        Parameters:
            name: The name of the parameter
            label: The display label for the parameter
            default_date: Default date for this option
            date_format: Format of the default date, default is '%Y-%m-%d'
            is_hidden: Whether the parameter is hidden in the parameters API response. Default is False
        """
        single_param_option = po.DateParameterOption(default_date, date_format=date_format)
        cls.Create(name, label, (single_param_option,), is_hidden=is_hidden)
    
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
    
    def to_json_dict(self):
        """
        Converts this parameter as a JSON object for the parameters API response

        The "selected_date" field will always be in yyyy-mm-dd format

        Returns:
            A dictionary for the JSON object
        """
        output = super().to_json_dict()
        output.update(self.curr_option._to_json_dict())
        output['selected_date'] = self.get_selected_date("%Y-%m-%d")
        return output


@dataclass
class DateRangeParameter(Parameter):
    """
    Class for date range parameter widgets.

    Attributes:
        config: The config for this widget parameter (for immutable attributes like name, label, all possible options, etc)
        curr_option: The current option showing for defaults based on user attribute and selection of parent
    """
    curr_option: po.DateRangeParameterOption
    selected_start_date: datetime
    selected_end_date: datetime
    
    @staticmethod
    def Create(
        name: str, label: str, all_options: Sequence[po.DateRangeParameterOption], *, is_hidden: bool = False, 
        user_attribute: Optional[str] = None, parent_name: Optional[str] = None
    ) -> None:
        """
        Method for creating the configurations for a Parameter that may include user attribute or parent

        Parameters:
            name: The name of the parameter
            label: The display label for the parameter
            all_options: All options associated to this parameter regardless of the user group or parent parameter option they depend on
            is_hidden: Whether the parameter is hidden in the parameters API response. Default is False
            user_attribute: The user attribute that may cascade the options for this parameter. Default is None
            parent_name: Name of parent parameter that may cascade the options for this parameter. Default is None (no parent)
        """
        param_config = pc.DateRangeParameterConfig(name, label, all_options, is_hidden=is_hidden, user_attribute=user_attribute, 
                                                   parent_name=parent_name)
        pc.ParameterConfigsSetIO.obj.add(param_config)

    @classmethod
    def CreateSimple(
        cls, name: str, label: str, default_start_date: Union[str, datetime], default_end_date: Union[str, datetime], 
        *, date_format: str = '%Y-%m-%d', is_hidden: bool = False
    ) -> None:
        """
        Method for creating the configurations for a Parameter that doesn't involve user attributes or parent parameters

        Parameters:
            name: The name of the parameter
            label: The display label for the parameter
            default_start_date: Default start date for this option
            default_end_date: Default end date for this option
            date_format: Format of the default date, default is '%Y-%m-%d'
            is_hidden: Whether the parameter is hidden in the parameters API response. Default is False
        """
        single_param_option = po.DateRangeParameterOption(default_start_date, default_end_date, date_format=date_format)
        cls.Create(name, label, (single_param_option,), is_hidden=is_hidden)
    
    def get_selected_start_date(self, date_format: str = None) -> str:
        """
        Gets selected start date as string

        Parameters:
            date_format: The date format (see Python's datetime formats). If not specified, self.date_format is used

        Returns:
            A string
        """
        date_format = self.curr_option.date_format if date_format is None else date_format
        return self.selected_start_date.strftime(date_format)

    def get_selected_start_date_quoted(self, date_format: str = None) -> str:
        """
        Gets selected start date as string surrounded by single quotes

        Parameters:
            date_format: The date format (see Python's datetime formats). If not specified, self.date_format is used

        Returns:
            A string
        """
        return self._enquote(self.get_selected_start_date(date_format))
    
    def get_selected_end_date(self, date_format: str = None) -> str:
        """
        Gets selected end date as string

        Parameters:
            date_format: The date format (see Python's datetime formats). If not specified, self.date_format is used

        Returns:
            A string
        """
        date_format = self.curr_option.date_format if date_format is None else date_format
        return self.selected_end_date.strftime(date_format)

    def get_selected_end_date_quoted(self, date_format: str = None) -> str:
        """
        Gets selected end date as string surrounded by single quotes

        Parameters:
            date_format: The date format (see Python's datetime formats). If not specified, self.date_format is used

        Returns:
            A string
        """
        return self._enquote(self.get_selected_end_date(date_format))
    
    def to_json_dict(self):
        """
        Converts this parameter as a JSON object for the parameters API response

        The "selected_date" field will always be in yyyy-mm-dd format

        Returns:
            A dictionary for the JSON object
        """
        output = super().to_json_dict()
        output.update(self.curr_option._to_json_dict())
        output['selected_start_date'] = self.get_selected_start_date("%Y-%m-%d")
        output['selected_end_date'] = self.get_selected_end_date("%Y-%m-%d")
        return output


@dataclass
class NumberParameter(Parameter):
    """
    Class for date range parameter widgets.

    Attributes:
        config: The config for this widget parameter (for immutable attributes like name, label, all possible options, etc)
        curr_option: The current option showing for defaults based on user attribute and selection of parent
    """
    curr_option: po.NumberParameterOption
    selected_value: Decimal
    
    @staticmethod
    def Create(
        name: str, label: str, all_options: Sequence[po.NumberParameterOption], *, is_hidden: bool = False, 
        user_attribute: Optional[str] = None, parent_name: Optional[str] = None
    ) -> None:
        """
        Method for creating the configurations for a Parameter that may include user attribute or parent

        Parameters:
            name: The name of the parameter
            label: The display label for the parameter
            all_options: All options associated to this parameter regardless of the user group or parent parameter option they depend on
            is_hidden: Whether the parameter is hidden in the parameters API response. Default is False
            user_attribute: The user attribute that may cascade the options for this parameter. Default is None
            parent_name: Name of parent parameter that may cascade the options for this parameter. Default is None (no parent)
        """
        param_config = pc.NumberParameterConfig(name, label, all_options, is_hidden=is_hidden, user_attribute=user_attribute, 
                                                parent_name=parent_name)
        pc.ParameterConfigsSetIO.obj.add(param_config)

    @classmethod
    def CreateSimple(
        cls, name: str, label: str, min_value: po.Number, max_value: po.Number, *, increment: po.Number = 1, 
        default_value: po.Number = None, is_hidden: bool = False
    ) -> None:
        """
        Method for creating the configurations for a Parameter that doesn't involve user attributes or parent parameters
        
        Parameters
            name: The name of the parameter
            label: The display label for the parameter
            min_value: Minimum selectable value
            max_value: Maximum selectable value
            increment: Increment of selectable values, and must fit evenly between min_value and max_value
            default_value: Default value for this option, and must be selectable based on min_value, max_value, and increment
            is_hidden: Whether the parameter is hidden in the parameters API response. Default is False
        """
        single_param_option = po.NumberParameterOption(min_value, max_value, increment=increment, default_value=default_value)
        cls.Create(name, label, (single_param_option,), is_hidden=is_hidden)
    
    def get_selected_value(self) -> str:
        """
        Get the selected number

        Returns:
            A number parsable string of the selected number
        """
        return str(self.selected_value)
        
    def to_json_dict(self):
        """
        Converts this parameter as a JSON object for the parameters API response

        Returns:
            A dictionary for the JSON object
        """
        output = super().to_json_dict()
        output.update(self.curr_option._to_json_dict())
        output['selected_value'] = self.get_selected_value()
        return output


@dataclass
class NumRangeParameter(Parameter):
    """
    Class for date range parameter widgets.

    Attributes:
        config: The config for this widget parameter (for immutable attributes like name, label, all possible options, etc)
        curr_option: The current option showing for defaults based on user attribute and selection of parent
    """
    curr_option: po.NumRangeParameterOption
    selected_lower_value: Decimal
    selected_upper_value: Decimal
    
    @staticmethod
    def Create(
        name: str, label: str, all_options: Sequence[po.NumRangeParameterOption], *, is_hidden: bool = False, 
        user_attribute: Optional[str] = None, parent_name: Optional[str] = None
    ) -> None:
        """
        Method for creating the configurations for a Parameter that may include user attribute or parent

        Parameters:
            name: The name of the parameter
            label: The display label for the parameter
            all_options: All options associated to this parameter regardless of the user group or parent parameter option they depend on
            is_hidden: Whether the parameter is hidden in the parameters API response. Default is False
            user_attribute: The user attribute that may cascade the options for this parameter. Default is None
            parent_name: Name of parent parameter that may cascade the options for this parameter. Default is None (no parent)
        """
        param_config = pc.NumRangeParameterConfig(name, label, all_options, is_hidden=is_hidden, user_attribute=user_attribute, 
                                                  parent_name=parent_name)
        pc.ParameterConfigsSetIO.obj.add(param_config)

    @classmethod
    def CreateSimple(
        cls, name: str, label: str, min_value: po.Number, max_value: po.Number, *, increment: po.Number = 1, 
        default_lower_value: po.Number = None, default_upper_value: po.Number = None, is_hidden: bool = False
    ) -> None:
        """
        Method for creating the configurations for a Parameter that doesn't involve user attributes or parent parameters

        Parameters:
            name: The name of the parameter
            label: The display label for the parameter
            min_value: Minimum selectable value
            max_value: Maximum selectable value
            increment: Increment of selectable values, and must fit evenly between min_value and max_value
            default_lower_value: Default lower value for this option, and must be selectable based on min_value, max_value, and increment
            default_upper_value: Default upper value for this option, and must be selectable based on min_value, max_value, and increment. 
                    Must also be greater than default_lower_value
            is_hidden: Whether the parameter is hidden in the parameters API response. Default is False
        """
        single_param_option = po.NumRangeParameterOption(min_value, max_value, increment=increment, default_lower_value=default_lower_value, 
                                                         default_upper_value=default_upper_value)
        cls.Create(name, label, (single_param_option,), is_hidden=is_hidden)
    
    def get_selected_lower_value(self) -> str:
        """
        Get the selected lower value number

        Returns:
            A number parsable string of the selected number
        """
        return str(self.selected_lower_value)

    def get_selected_upper_value(self) -> str:
        """
        Get the selected upper value number

        Returns:
            A number parsable string of the selected number
        """
        return str(self.selected_upper_value)

    def to_json_dict(self):
        """
        Converts this parameter as a JSON object for the parameters API response

        Returns:
            A dictionary for the JSON object
        """
        output = super().to_json_dict()
        output.update(self.curr_option._to_json_dict())
        output['selected_lower_value'] = self.get_selected_lower_value()
        output['selected_upper_value'] = self.get_selected_upper_value()
        return output
