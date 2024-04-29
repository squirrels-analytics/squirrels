from __future__ import annotations
from typing import Type, Sequence, Optional, Union, Any
from dataclasses import dataclass
from datetime import datetime, date
from decimal import Decimal
from abc import ABCMeta, abstractmethod

from . import _parameter_configs as pc, _parameter_sets as ps, parameter_options as po, data_sources as d
from . import _api_response_models as arm, _utils as u


@dataclass
class Parameter(metaclass=ABCMeta):
    """
    Abstract class for all parameter widgets
    """
    _config: pc.ParameterConfig

    @staticmethod
    @abstractmethod
    def _ParameterConfigType() -> Type:
        pass
    
    @classmethod
    def Create(
        cls, name: str, label: str, all_options: Sequence[Union[po.ParameterOption, dict]], *, description: str = "",
        user_attribute: Optional[str] = None, parent_name: Optional[str] = None, **kwargs
    ) -> None:
        """
        Method for creating the configurations for a Parameter that may include user attribute or parent

        Parameters:
            name: The name of the parameter
            label: The display label for the parameter
            all_options: All options associated to this parameter regardless of the user group or parent parameter option they depend on
            description: Explains the meaning of the parameter
            user_attribute: The user attribute that may cascade the options for this parameter. Default is None
            parent_name: Name of parent parameter that may cascade the options for this parameter. Default is None (no parent)
        """
        param_config_type = cls._ParameterConfigType()
        param_config = param_config_type(name, label, all_options, description=description, user_attribute=user_attribute, 
                                         parent_name=parent_name)
        ps.ParameterConfigsSetIO.obj.add(param_config)

    @classmethod
    @abstractmethod
    def CreateSimple(cls, name: str, label: str, *args, description: str = "", **kwargs) -> None:
        pass
    
    @classmethod
    def CreateFromSource(
        cls, name: str, label: str, data_source: Union[d.DataSource , dict], *, description: str = "", 
        user_attribute: Optional[str] = None, parent_name: Optional[str] = None, **kwargs
    ) -> None:
        """
        Method for creating the configurations for any Parameter that uses a DataSource to receive the options

        Parameters:
            name: The name of the parameter
            label: The display label for the parameter
            data_source: The lookup table to use for this parameter
            description: Explains the meaning of the parameter
            user_attribute: The user attribute that may cascade the options for this parameter. Default is None
            parent_name: Name of parent parameter that may cascade the options for this parameter. Default is None (no parent)
        """
        param_config_type = cls._ParameterConfigType()
        param_config = pc.DataSourceParameterConfig(param_config_type, name, label, data_source, description=description, 
                                                    user_attribute=user_attribute, parent_name=parent_name)
        ps.ParameterConfigsSetIO.obj.add(param_config)
        
    def _enquote(self, value: str) -> str:
        return "'" + value.replace("'", "''") + "'" 
    
    def _validate_date(self, input_date: str) -> date:
        try:
            return datetime.strptime(input_date.strip(), "%Y-%m-%d").date() if isinstance(input_date, str) else input_date
        except ValueError as e:
            self._config._raise_invalid_input_error(input_date, str(e), e)
    
    def _validate_number(self, input_number: po.Number, curr_option: po._NumericParameterOption) -> Decimal:
        try:
            return curr_option._validate_value(input_number)
        except u.ConfigurationError as e:
            self._config._raise_invalid_input_error(input_number, str(e), e)
    
    @abstractmethod
    def to_json_dict0(self) -> dict:
        """
        Helper method to convert the derived Parameter class into a JSON dictionary
        """
        return self._config.to_json_dict0()
    
    @abstractmethod
    def _get_response_model0(self) -> type[arm.ParameterModel]:
        pass
    
    def _to_api_response_model0(self) -> arm.ParameterModel:
        return self._get_response_model0().model_validate(self.to_json_dict0())


@dataclass
class _SelectionParameter(Parameter):
    _config: pc.SelectionParameterConfig
    _options: Sequence[po.SelectParameterOption]

    def __post_init__(self):
        self._options = tuple(self._options)

    @abstractmethod
    def _get_selected_ids_as_list(self) -> Sequence[str]:
        pass

    def _validate_selected_id_in_options(self, selected_id):
        if selected_id not in (x._identifier for x in self._options):
            self._config._raise_invalid_input_error(selected_id, f"The selected id {selected_id} does not exist in available options.")
    
    @abstractmethod
    def to_json_dict0(self) -> dict:
        """
        Helper method to convert the derived selection parameter class into a JSON object
        """
        output = super().to_json_dict0()
        output['options'] = [x._to_json_dict() for x in self._options]
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
    _config: pc.SingleSelectParameterConfig
    _selected_id: Optional[str]

    def __post_init__(self):
        super().__post_init__()
        if len(self._options) > 0:
            assert self._selected_id != None
            self._validate_selected_id_in_options(self._selected_id)
        else:
            self._selected_id = None
    
    @staticmethod
    def _ParameterConfigType():
        return pc.SingleSelectParameterConfig
    
    @classmethod
    def CreateSimple(
        cls, name: str, label: str, all_options: Sequence[po.SelectParameterOption], *, description: str = "", **kwargs
    ) -> None:
        """
        Method for creating the configurations for a Parameter that doesn't involve user attributes or parent parameters

        Parameters:
            name: The name of the parameter
            label: The display label for the parameter
            all_options: All options associated to this parameter regardless of the user group or parent parameter option they depend on
            description: Explains the meaning of the parameter
        """
        cls.Create(name, label, all_options, description=description)

    def get_selected(
        self, field: Optional[str] = None, *, default_field: Optional[str] = None, default: Any = None, **kwargs
    ) -> Union[po.SelectParameterOption, Any, None]:
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
        def get_selected_from_id(identifier: str):
            selected = next(x for x in self._options if x._identifier == identifier)
            if field is not None:
                selected = selected.get_custom_field(field, default_field=default_field, default=default)
            return selected
        return u.process_if_not_none(self._selected_id, get_selected_from_id)
    
    def get_selected_id(self, **kwargs) -> Optional[str]:
        """
        Gets the ID of the selected option

        Returns:
            A string ID or None if there are no selectable options
        """
        def get_id(x: po.SelectParameterOption): return x._identifier
        return u.process_if_not_none(self.get_selected(), get_id)
    
    def get_selected_id_quoted(self, **kwargs) -> Optional[str]:
        """
        Gets the ID of the selected option surrounded by single quotes

        Returns:
            A string or None if there are no selectable options
        """
        return u.process_if_not_none(self.get_selected_id(), self._enquote)
    
    def get_selected_label(self, **kwargs) -> Optional[str]:
        """
        Gets the label of the selected option

        Returns:
            A string or None if there are no selectable options
        """
        def get_label(x: po.SelectParameterOption): return x._label
        return u.process_if_not_none(self.get_selected(), get_label)
    
    def get_selected_label_quoted(self, **kwargs) -> Optional[str]:
        """
        Gets the label of the selected option surrounded by single quotes

        Returns:
            A string or None if there are no selectable options
        """
        return u.process_if_not_none(self.get_selected_label(), self._enquote)

    def _get_selected_ids_as_list(self) -> Sequence[str]:
        selected_id = self.get_selected_id()
        if selected_id is not None:
            return (self.get_selected_id(),)
        else:
            return tuple()
    
    def to_json_dict0(self) -> dict:
        """
        Converts this parameter as a JSON object for the parameters API response

        Returns:
            A dictionary for the JSON object
        """
        output = super().to_json_dict0()
        output['selected_id'] = self._selected_id
        return output
    
    def _get_response_model0(self):
        return arm.SingleSelectParameterModel


@dataclass
class MultiSelectParameter(_SelectionParameter):
    """
    Class for multi-select parameter widgets.

    Attributes:
        config: The config for this widget parameter (for immutable attributes like name, label, all possible options, etc)
        options: The parameter options that are currently selectable
        selected_ids: A sequence of IDs of the selected options
    """
    _config: pc.MultiSelectParameterConfig
    _selected_ids: Sequence[str]

    def __post_init__(self):
        super().__post_init__()
        self._selected_ids = tuple(self._selected_ids)
        for selected_id in self._selected_ids:
            self._validate_selected_id_in_options(selected_id)
    
    @staticmethod
    def _ParameterConfigType():
        return pc.MultiSelectParameterConfig
    
    @classmethod
    def Create(
        cls, name: str, label: str, all_options: Sequence[Union[po.SelectParameterOption, dict]], *, description: str = "",
        show_select_all: bool = True, is_dropdown: bool = True, order_matters: bool = False, none_is_all: bool = True,
        user_attribute: Optional[str] = None, parent_name: Optional[str] = None, **kwargs
    ) -> None:
        """
        Method for creating the configurations for a MultiSelectParameter that may include user attribute or parent

        Parameters:
            name: The name of the parameter
            label: The display label for the parameter
            all_options: All options associated to this parameter regardless of the user group or parent parameter option they depend on
            description: Explains the meaning of the parameter
            show_select_all: Communicate to front-end whether to include a "select all" option
            is_dropdown: Communicate to front-end whether the widget should be a dropdown with checkboxes
            order_matters: Communicate to front-end whether the order of the selections made matter
            none_is_all: Whether having no options selected is equivalent to all selectable options selected
            user_attribute: The user attribute that may cascade the options for this parameter. Default is None
            parent_name: Name of parent parameter that may cascade the options for this parameter. Default is None (no parent)
        """
        param_config = pc.MultiSelectParameterConfig(
            name, label, all_options, 
            show_select_all=show_select_all, is_dropdown=is_dropdown, order_matters=order_matters, none_is_all=none_is_all, 
            description=description, user_attribute=user_attribute, parent_name=parent_name
        )
        ps.ParameterConfigsSetIO.obj.add(param_config)

    @classmethod
    def CreateSimple(
        cls, name: str, label: str, all_options: Sequence[po.SelectParameterOption], *, description: str = "",
        show_select_all: bool = True, is_dropdown: bool = True, order_matters: bool = False, none_is_all: bool = True, **kwargs
    ) -> None:
        """
        Method for creating the configurations for a MultiSelectParameter that doesn't involve user attributes or parent parameters

        Parameters:
            name: The name of the parameter
            label: The display label for the parameter
            all_options: All options associated to this parameter regardless of the user group or parent parameter option they depend on
            description: Explains the meaning of the parameter
            show_select_all: Communicate to front-end whether to include a "select all" option
            is_dropdown: Communicate to front-end whether the widget should be a dropdown with checkboxes
            order_matters: Communicate to front-end whether the order of the selections made matter
            none_is_all: Whether having no options selected is equivalent to all selectable options selected
        """
        cls.Create(
            name, label, all_options, description=description,
            show_select_all=show_select_all, s_dropdown=is_dropdown, order_matters=order_matters, none_is_all=none_is_all
        )
    
    @classmethod
    def CreateFromSource(
        cls, name: str, label: str, data_source: Union[d.SelectDataSource, dict], *, description: str = "",
        show_select_all: bool = True, is_dropdown: bool = True, order_matters: bool = False, none_is_all: bool = True,
        user_attribute: Optional[str] = None, parent_name: Optional[str] = None, **kwargs
    ) -> None:
        """
        Method for creating the configurations for a MultiSelectParameter that uses a SelectDataSource to receive the options

        Parameters:
            name: The name of the parameter
            label: The display label for the parameter
            data_source: The lookup table to use for this parameter
            description: Explains the meaning of the parameter
            show_select_all: Communicate to front-end whether to include a "select all" option
            is_dropdown: Communicate to front-end whether the widget should be a dropdown with checkboxes
            order_matters: Communicate to front-end whether the order of the selections made matter
            none_is_all: Whether having no options selected is equivalent to all selectable options selected
            user_attribute: The user attribute that may cascade the options for this parameter. Default is None
            parent_name: Name of parent parameter that may cascade the options for this parameter. Default is None (no parent)
        """
        extra_args = {
            "show_select_all": show_select_all, "is_dropdown": is_dropdown, 
            "order_matters": order_matters, "none_is_all": none_is_all
        }
        param_config = pc.DataSourceParameterConfig(
            pc.MultiSelectParameterConfig, name, label, data_source, extra_args=extra_args, description=description, 
            user_attribute=user_attribute, parent_name=parent_name
        )
        ps.ParameterConfigsSetIO.obj.add(param_config)

    def has_non_empty_selection(self) -> bool:
        """
        Returns True if more than zero options were selected. False otherwise.
        
        Note that even when this returns False, all "get_selected" functions would 
        return the full list of options if "include_all" is set to True

        Returns:
            A boolean
        """
        return len(self._selected_ids) > 0

    def get_selected_list(
        self, field: Optional[str] = None, *, default_field: Optional[str] = None, default: Any = None, **kwargs
    ) -> Sequence[Union[po.SelectParameterOption, Any]]:
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
        if not self.has_non_empty_selection() and self._config.none_is_all:
            selected_list = self._options
        else:
            selected_list = (x for x in self._options if x._identifier in self._selected_ids)
        
        if field is not None:
            selected_list = [selected.get_custom_field(field, default_field=default_field, default=default) for selected in selected_list]
        
        return tuple(selected_list)

    def get_selected_ids_as_list(self, **kwargs) -> Sequence[str]:
        """
        Gets the sequence of ID(s) of the selected option(s)

        Returns:
            A sequence of strings
        """
        return tuple(x._identifier for x in self.get_selected_list())
    
    def get_selected_ids_joined(self, **kwargs) -> str:
        """
        Gets the ID(s) of the selected option(s) joined by comma

        Returns:
            A string
        """
        return ','.join(self.get_selected_ids_as_list())
    
    def get_selected_ids_quoted_as_list(self, **kwargs) -> Sequence[str]:
        """
        Gets the sequence of ID(s) of the selected option(s) surrounded by single quotes

        Returns:
            A sequence of strings
        """
        return tuple(self._enquote(x) for x in self.get_selected_ids_as_list())
    
    def get_selected_ids_quoted_joined(self, **kwargs) -> str:
        """
        Gets the ID(s) of the selected option(s) surrounded by single quotes and joined by comma

        Returns:
            A string
        """
        return ','.join(self.get_selected_ids_quoted_as_list())
    
    def get_selected_labels_as_list(self, **kwargs) -> Sequence[str]:
        """
        Gets the sequence of label(s) of the selected option(s)

        Returns:
            A sequence of strings
        """
        return tuple(x._label for x in self.get_selected_list())
    
    def get_selected_labels_joined(self, **kwargs) -> str:
        """
        Gets the label(s) of the selected option(s) joined by comma

        Returns:
            A string
        """
        return ','.join(self.get_selected_labels_as_list())
    
    def get_selected_labels_quoted_as_list(self, **kwargs) -> Sequence[str]:
        """
        Gets the sequence of label(s) of the selected option(s) surrounded by single quotes

        Returns:
            A sequence of strings
        """
        return tuple(self._enquote(x) for x in self.get_selected_labels_as_list())
    
    def get_selected_labels_quoted_joined(self, **kwargs) -> str:
        """
        Gets the label(s) of the selected option(s) surrounded by single quotes and joined by comma

        Returns:
            A string
        """
        return ','.join(self.get_selected_labels_quoted_as_list())

    def _get_selected_ids_as_list(self, **kwargs) -> Sequence[str]:
        return self.get_selected_ids_as_list()
    
    def to_json_dict0(self):
        """
        Converts this parameter as a JSON object for the parameters API response

        Returns:
            A dictionary for the JSON object
        """
        output = super().to_json_dict0()
        output['selected_ids'] = list(self._selected_ids)
        return output
    
    def _get_response_model0(self):
        return arm.MultiSelectParameterModel


@dataclass
class DateParameter(Parameter):
    """
    Class for date parameter widgets.

    Attributes:
        config: The config for this widget parameter (for immutable attributes like name, label, all possible options, etc)
        curr_option: The current option showing for defaults based on user attribute and selection of parent
        selected_date: The selected date
    """
    _curr_option: po.DateParameterOption
    _selected_date: Union[date, str]

    def __post_init__(self):
        self._selected_date: date = self._validate_date(self._selected_date)
    
    @staticmethod
    def _ParameterConfigType():
        return pc.DateParameterConfig

    @classmethod
    def CreateSimple(
        cls, name: str, label: str, default_date: Union[str, date], *, description: str = "", 
        date_format: str = '%Y-%m-%d', **kwargs
    ) -> None:
        """
        Method for creating the configurations for a Parameter that doesn't involve user attributes or parent parameters

        Parameters:
            name: The name of the parameter
            label: The display label for the parameter
            default_date: Default date for this option
            description: Explains the meaning of the parameter
            date_format: Format of the default date, default is '%Y-%m-%d'
        """
        single_param_option = po.DateParameterOption(default_date, date_format=date_format)
        cls.Create(name, label, (single_param_option,), description=description)
    
    def get_selected_date(self, *, date_format: str = None, **kwargs) -> str:
        """
        Gets selected date as string

        Parameters:
            date_format: The date format (see Python's datetime formats). If not specified, self.date_format is used

        Returns:
            A string
        """
        date_format = self._curr_option._date_format if date_format is None else date_format
        return self._selected_date.strftime(date_format)

    def get_selected_date_quoted(self, *, date_format: str = None, **kwargs) -> str:
        """
        Gets selected date as string surrounded by single quotes

        Parameters:
            date_format: The date format (see Python's datetime formats). If not specified, self.date_format is used

        Returns:
            A string
        """
        return self._enquote(self.get_selected_date(date_format=date_format))
    
    def to_json_dict0(self):
        """
        Converts this parameter as a JSON object for the parameters API response

        The "selected_date" field will always be in yyyy-mm-dd format

        Returns:
            A dictionary for the JSON object
        """
        output = super().to_json_dict0()
        output.update(self._curr_option._to_json_dict())
        output['selected_date'] = self.get_selected_date(date_format="%Y-%m-%d")
        return output
    
    def _get_response_model0(self):
        return arm.DateParameterModel


@dataclass
class DateRangeParameter(Parameter):
    """
    Class for date range parameter widgets.

    Attributes:
        config: The config for this widget parameter (for immutable attributes like name, label, all possible options, etc)
        curr_option: The current option showing for defaults based on user attribute and selection of parent
        selected_start_date: The selected start date
        selected_end_date: The selected end date
    """
    _curr_option: po.DateRangeParameterOption
    _selected_start_date: Union[date, str]
    _selected_end_date: Union[date, str]

    def __post_init__(self):
        self._selected_start_date: date = self._validate_date(self._selected_start_date)
        self._selected_end_date: date = self._validate_date(self._selected_end_date)
    
    @staticmethod
    def _ParameterConfigType():
        return pc.DateRangeParameterConfig

    @classmethod
    def CreateSimple(
        cls, name: str, label: str, default_start_date: Union[str, date], default_end_date: Union[str, date], *, 
        description: str = "", date_format: str = '%Y-%m-%d', **kwargs
    ) -> None:
        """
        Method for creating the configurations for a Parameter that doesn't involve user attributes or parent parameters

        Parameters:
            name: The name of the parameter
            label: The display label for the parameter
            default_start_date: Default start date for this option
            default_end_date: Default end date for this option
            description: Explains the meaning of the parameter
            date_format: Format of the default date, default is '%Y-%m-%d'
        """
        single_param_option = po.DateRangeParameterOption(default_start_date, default_end_date, date_format=date_format)
        cls.Create(name, label, (single_param_option,), description=description)
    
    def get_selected_start_date(self, *, date_format: str = None, **kwargs) -> str:
        """
        Gets selected start date as string

        Parameters:
            date_format: The date format (see Python's datetime formats). If not specified, self.date_format is used

        Returns:
            A string
        """
        date_format = self._curr_option._date_format if date_format is None else date_format
        return self._selected_start_date.strftime(date_format)

    def get_selected_start_date_quoted(self, *, date_format: str = None, **kwargs) -> str:
        """
        Gets selected start date as string surrounded by single quotes

        Parameters:
            date_format: The date format (see Python's datetime formats). If not specified, self.date_format is used

        Returns:
            A string
        """
        return self._enquote(self.get_selected_start_date(date_format=date_format))
    
    def get_selected_end_date(self, *, date_format: str = None, **kwargs) -> str:
        """
        Gets selected end date as string

        Parameters:
            date_format: The date format (see Python's datetime formats). If not specified, self.date_format is used

        Returns:
            A string
        """
        date_format = self._curr_option._date_format if date_format is None else date_format
        return self._selected_end_date.strftime(date_format)

    def get_selected_end_date_quoted(self, *, date_format: str = None, **kwargs) -> str:
        """
        Gets selected end date as string surrounded by single quotes

        Parameters:
            date_format: The date format (see Python's datetime formats). If not specified, self.date_format is used

        Returns:
            A string
        """
        return self._enquote(self.get_selected_end_date(date_format=date_format))
    
    def to_json_dict0(self):
        """
        Converts this parameter as a JSON object for the parameters API response

        The "selected_date" field will always be in yyyy-mm-dd format

        Returns:
            A dictionary for the JSON object
        """
        output = super().to_json_dict0()
        output.update(self._curr_option._to_json_dict())
        output['selected_start_date'] = self.get_selected_start_date(date_format="%Y-%m-%d")
        output['selected_end_date'] = self.get_selected_end_date(date_format="%Y-%m-%d")
        return output
    
    def _get_response_model0(self):
        return arm.DateRangeParameterModel


@dataclass
class NumberParameter(Parameter):
    """
    Class for number parameter widgets.

    Attributes:
        config: The config for this widget parameter (for immutable attributes like name, label, all possible options, etc)
        curr_option: The current option showing for defaults based on user attribute and selection of parent
        selected_value: The selected integer or decimal number
    """
    _curr_option: po.NumberParameterOption
    _selected_value: po.Number

    def __post_init__(self):
        self._selected_value: Decimal = self._validate_number(self._selected_value, self._curr_option)
    
    @staticmethod
    def _ParameterConfigType():
        return pc.NumberParameterConfig

    @classmethod
    def CreateSimple(
        cls, name: str, label: str, min_value: po.Number, max_value: po.Number, *, description: str = "", 
        increment: po.Number = 1, default_value: Optional[po.Number] = None, **kwargs
    ) -> None:
        """
        Method for creating the configurations for a Parameter that doesn't involve user attributes or parent parameters
        
        * Note that the "Number" type denotes an int, a Decimal (from decimal module), or a string that can be parsed to Decimal
        
        Parameters:
            name: The name of the parameter
            label: The display label for the parameter
            min_value: Minimum selectable value
            max_value: Maximum selectable value
            description: Explains the meaning of the parameter
            increment: Increment of selectable values, and must fit evenly between min_value and max_value
            default_value: Default value for this option, and must be selectable based on min_value, max_value, and increment
        """
        single_param_option = po.NumberParameterOption(min_value, max_value, increment=increment, default_value=default_value)
        cls.Create(name, label, (single_param_option,), description=description)
    
    def get_selected_value(self, **kwargs) -> str:
        """
        Get the selected number

        Returns:
            A number parsable string of the selected number
        """
        return str(self._selected_value)
        
    def to_json_dict0(self):
        """
        Converts this parameter as a JSON object for the parameters API response

        Returns:
            A dictionary for the JSON object
        """
        output = super().to_json_dict0()
        output.update(self._curr_option._to_json_dict())
        output['selected_value'] = self.get_selected_value()
        return output
    
    def _get_response_model0(self):
        return arm.NumberParameterModel


@dataclass
class NumberRangeParameter(Parameter):
    """
    Class for number range parameter widgets.

    Attributes:
        config: The config for this widget parameter (for immutable attributes like name, label, all possible options, etc)
        curr_option: The current option showing for defaults based on user attribute and selection of parent
        selected_lower_value: The selected lower integer or decimal number
        selected_upper_value: The selected upper integer or decimal number
    """
    _curr_option: po.NumberRangeParameterOption
    _selected_lower_value: po.Number
    _selected_upper_value: po.Number

    def __post_init__(self):
        self._selected_lower_value: Decimal = self._validate_number(self._selected_lower_value, self._curr_option)
        self._selected_upper_value: Decimal = self._validate_number(self._selected_upper_value, self._curr_option)
    
    @staticmethod
    def _ParameterConfigType():
        return pc.NumberRangeParameterConfig

    @classmethod
    def CreateSimple(
        cls, name: str, label: str, min_value: po.Number, max_value: po.Number, *, description: str = "",
        increment: po.Number = 1, default_lower_value: Optional[po.Number] = None, default_upper_value: Optional[po.Number] = None,
        **kwargs
    ) -> None:
        """
        Method for creating the configurations for a Parameter that doesn't involve user attributes or parent parameters
        
        * Note that the "Number" type denotes an int, a Decimal (from decimal module), or a string that can be parsed to Decimal

        Parameters:
            name: The name of the parameter
            label: The display label for the parameter
            min_value: Minimum selectable value
            max_value: Maximum selectable value
            description: Explains the meaning of the parameter
            increment: Increment of selectable values, and must fit evenly between min_value and max_value
            default_lower_value: Default lower value for this option, and must be selectable based on min_value, max_value, and increment
            default_upper_value: Default upper value for this option, and must be selectable based on min_value, max_value, and increment. 
                    Must also be greater than default_lower_value
        """
        single_param_option = po.NumberRangeParameterOption(min_value, max_value, increment=increment, default_lower_value=default_lower_value, 
                                                         default_upper_value=default_upper_value)
        cls.Create(name, label, (single_param_option,), description=description)
    
    def get_selected_lower_value(self, **kwargs) -> str:
        """
        Get the selected lower value number

        Returns:
            A number parsable string of the selected number
        """
        return str(self._selected_lower_value)

    def get_selected_upper_value(self, **kwargs) -> str:
        """
        Get the selected upper value number

        Returns:
            A number parsable string of the selected number
        """
        return str(self._selected_upper_value)

    def to_json_dict0(self):
        """
        Converts this parameter as a JSON object for the parameters API response

        Returns:
            A dictionary for the JSON object
        """
        output = super().to_json_dict0()
        output.update(self._curr_option._to_json_dict())
        output['selected_lower_value'] = self.get_selected_lower_value()
        output['selected_upper_value'] = self.get_selected_upper_value()
        return output
    
    def _get_response_model0(self):
        return arm.NumberRangeParameterModel
