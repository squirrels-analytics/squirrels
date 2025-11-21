from __future__ import annotations
from typing import Callable, Type, TypeVar, Sequence, Generic, Any
from dataclasses import dataclass
from datetime import datetime, date
from decimal import Decimal
from abc import ABCMeta, abstractmethod

from ._arguments.init_time_args import ParametersArgs
from ._schemas import response_models as rm
from . import _data_sources as d, _parameter_configs as pc, _parameter_options as po, _parameter_sets as ps
from . import _utils as u

IntOrFloat = TypeVar("IntOrFloat", int, float)

PC = TypeVar("PC", bound=pc.ParameterConfig)
PO = TypeVar("PO", bound=po.ParameterOption)
DS = TypeVar("DS", bound=d.DataSource)

@dataclass
class Parameter(Generic[PC, PO, DS], metaclass=ABCMeta):
    """
    Abstract class for all parameter widgets
    """
    _config: PC

    @abstractmethod
    def is_enabled(self) -> bool:
        return True

    @staticmethod
    @abstractmethod
    def _ParameterConfigType() -> Type[PC]: # Gets the actual type of the ParameterConfig TypeVar at runtime
        pass
    
    @staticmethod
    @abstractmethod
    def _ParameterOptionType() -> Type[PO]: # Gets the actual type of the ParameterOption TypeVar at runtime
        pass
    
    @staticmethod
    @abstractmethod
    def _DataSourceType() -> Type[DS]: # Gets the actual type of the DataSource TypeVar at runtime
        pass
    
    @classmethod
    def CreateWithOptions(
        cls, name: str, label: str, all_options: Sequence[PO | dict], *, description: str = "",
        user_attribute: str | None = None, parent_name: str | None = None, **kwargs
    ) -> PC:
        """
        Method for creating the configurations for a Parameter that may include user attribute or parent

        .. deprecated::
            Use the lowercase decorator form `create_with_options` instead.

        Arguments:
            name: The name of the parameter
            label: The display label for the parameter
            all_options: All options associated to this parameter regardless of the user group or parent parameter option they depend on
            description: Explains the meaning of the parameter
            user_attribute: The user attribute that may cascade the options for this parameter. Default is None
            parent_name: Name of parent parameter that may cascade the options for this parameter. Default is None (no parent)
        """
        param_option_type = cls._ParameterOptionType()
        if not isinstance(all_options, Sequence) or not all(isinstance(x, (param_option_type, dict)) for x in all_options):
            raise u.ConfigurationError(f"The parameter must take a sequence of {param_option_type.__name__} objects")
        
        param_config_type = cls._ParameterConfigType()
        param_config = param_config_type(
            name, label, all_options, description=description, user_attribute=user_attribute, parent_name=parent_name, **kwargs
        )
        return param_config
    
    @classmethod
    def create_with_options(
        cls, name: str, label: str, *, description: str = "", user_attribute: str | None = None, parent_name: str | None = None
    ):
        """
        Python decorator for creating the configurations for a Parameter that may include user attribute or parent.

        The decorated function must return a list of ParameterOption objects.

        Arguments:
            name: The name of the parameter
            label: The display label for the parameter
            description: Explains the meaning of the parameter
            user_attribute: The user attribute that may cascade the options for this parameter. Default is None
            parent_name: Name of parent parameter that may cascade the options for this parameter. Default is None (no parent)
        """
        def decorator(func: Callable[..., Sequence[PO]]):
            def wrapper(sqrl: ParametersArgs):
                options = u.call_func(func, sqrl=sqrl)
                return cls.CreateWithOptions(
                    name, label, options, description=description, 
                    user_attribute=user_attribute, parent_name=parent_name
                )
            ps.ParameterConfigsSetIO.param_factories.append(wrapper)
            return wrapper
        return decorator

    @classmethod
    @abstractmethod
    def CreateSimple(cls, name: str, label: str, *args, description: str = "", **kwargs) -> None:
        pass

    @classmethod
    @abstractmethod
    def create_simple(cls, name: str, label: str, *args, description: str = "", **kwargs) -> None:
        pass

    @classmethod
    def _CreateFromSourceHelper(
        cls, name: str, label: str, data_source: DS | dict, *, extra_args: dict = {}, description: str = "", 
        user_attribute: str | None = None, parent_name: str | None = None
    ):
        data_source_type = cls._DataSourceType()
        if not isinstance(data_source, (data_source_type, dict)):
            raise u.ConfigurationError(f"The data source must be a {data_source_type.__name__} object")
        
        param_config = pc.DataSourceParameterConfig(
            cls._ParameterConfigType(), name, label, data_source, description=description, user_attribute=user_attribute, 
            parent_name=parent_name, extra_args=extra_args
        )
        return param_config
    
    @classmethod
    def CreateFromSource(
        cls, name: str, label: str, data_source: DS | dict, *, description: str = "", 
        user_attribute: str | None = None, parent_name: str | None = None, **kwargs
    ):
        """
        Method for creating the configurations for any Parameter that uses a DataSource to receive the options

        .. deprecated::
            Use the lowercase decorator form `create_from_source` instead.

        Arguments:
            name: The name of the parameter
            label: The display label for the parameter
            data_source: The lookup table to use for this parameter
            description: Explains the meaning of the parameter
            user_attribute: The user attribute that may cascade the options for this parameter. Default is None
            parent_name: Name of parent parameter that may cascade the options for this parameter. Default is None (no parent)
        """
        return cls._CreateFromSourceHelper(name, label, data_source, description=description, user_attribute=user_attribute, parent_name=parent_name)
    
    @classmethod
    def create_from_source(
        cls, name: str, label: str, *, description: str = "", user_attribute: str | None = None, parent_name: str | None = None
    ):
        """
        Python decorator for creating the configurations for a Parameter that uses a DataSource to receive the options from a lookup table

        The decorated function must return a DataSource object.

        Arguments:
            name: The name of the parameter
            label: The display label for the parameter
            description: Explains the meaning of the parameter
            user_attribute: The user attribute that may cascade the options for this parameter. Default is None
            parent_name: Name of parent parameter that may cascade the options for this parameter. Default is None (no parent)
        """
        def decorator(func: Callable[..., DS]):
            def wrapper(sqrl: ParametersArgs):
                data_source = u.call_func(func, sqrl=sqrl)
                return cls.CreateFromSource(
                    name, label, data_source, description=description,
                    user_attribute=user_attribute, parent_name=parent_name
                )
            ps.ParameterConfigsSetIO.param_factories.append(wrapper)
            return wrapper
        return decorator
        
    def _enquote(self, value: str) -> str:
        return "'" + value.replace("'", "''") + "'" 
    
    def _validate_input_date(self, input_date: date | str, curr_option: po._DateTypeParameterOption) -> date:
        if isinstance(input_date, str):
            try:
                input_date = datetime.strptime(input_date.strip(), "%Y-%m-%d").date()
            except ValueError:
                raise self._config._invalid_input_error(str(input_date), "Must be a date in YYYY-MM-DD format.")
        
        try:
            return curr_option._validate_date(input_date)
        except u.ConfigurationError as e:
            raise self._config._invalid_input_error(str(input_date), str(e))
    
    def _validate_number(self, input_number: po.Number, curr_option: po._NumericParameterOption) -> Decimal:
        try:
            return curr_option._validate_value(input_number)
        except u.ConfigurationError as e:
            raise self._config._invalid_input_error(str(input_number), str(e))
    
    @abstractmethod
    def _to_json_dict0(self) -> dict:
        """
        Helper method to convert the derived Parameter class into a JSON dictionary
        """
        output = {
            "widget_type": self._config.widget_type(), "name": self._config.name, 
            "label": self._config.label, "description": self._config.description
        }
        if not self.is_enabled():
            output["widget_type"] = "disabled"
        return output
    
    @abstractmethod
    def _get_response_model0(self) -> type[rm.ParameterModelBase]:
        pass
    
    def _to_api_response_model0(self) -> rm.ParameterModelBase:
        return self._get_response_model0().model_validate(self._to_json_dict0())


SelectionPC = TypeVar("SelectionPC", bound=pc.SelectionParameterConfig)

@dataclass
class _SelectionParameter(Parameter[SelectionPC, po.SelectParameterOption, d.SelectDataSource], Generic[SelectionPC]):
    _options: Sequence[po.SelectParameterOption]

    def __post_init__(self):
        self._options = tuple(self._options)

    def is_enabled(self) -> bool:
        return len(self._options) > 0
    
    @abstractmethod
    def _get_selected_ids_as_list(self) -> Sequence[str]:
        pass

    def _validate_selected_id_in_options(self, selected_id):
        if selected_id not in (x._identifier for x in self._options):
            raise self._config._invalid_input_error(selected_id, f"The selected id {selected_id} does not exist in available options.")
    
    @abstractmethod
    def _to_json_dict0(self) -> dict:
        """
        Helper method to convert the derived selection parameter class into a JSON object
        """
        output = super()._to_json_dict0()
        output['trigger_refresh'] = self._config.trigger_refresh
        output['options'] = [x._to_json_dict() for x in self._options]
        return output


@dataclass
class SingleSelectParameter(_SelectionParameter[pc.SingleSelectParameterConfig]):
    """
    Class for single-select parameter widgets.

    Attributes:
        config: The config for this widget parameter (for immutable attributes like name, label, all possible options, etc)
        options: The parameter options that are currently selectable
        selected_id: The ID of the selected option
    """
    _selected_id: str | None

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
    
    @staticmethod
    def _ParameterOptionType():
        return po.SelectParameterOption
    
    @staticmethod
    def _DataSourceType():
        return d.SelectDataSource
    
    @classmethod
    def CreateSimple(
        cls, name: str, label: str, all_options: Sequence[po.SelectParameterOption | dict], *, description: str = "", **kwargs
    ):
        """
        Method for creating the configurations for a SingleSelectParameter that doesn't involve user attributes or parent parameters

        .. deprecated::
            Use the lowercase decorator form `create_simple` instead.

        Arguments:
            name: The name of the parameter
            label: The display label for the parameter
            all_options: All options associated to this parameter regardless of the user group or parent parameter option they depend on
            description: Explains the meaning of the parameter
        """
        return cls.CreateWithOptions(name, label, all_options, description=description)
    
    @classmethod
    def create_simple(cls, name: str, label: str, *, description: str = ""):
        """
        Python decorator for creating the configurations for a SingleSelectParameter that doesn't involve user attributes or parent parameters

        The decorated function must return a list of SelectParameterOption objects.

        Arguments:
            name: The name of the parameter
            label: The display label for the parameter
            description: Explains the meaning of the parameter
        """
        def decorator(func: Callable[..., Sequence[po.SelectParameterOption]]):
            def wrapper(sqrl: ParametersArgs):
                options = u.call_func(func, sqrl=sqrl)
                return cls.CreateSimple(name, label, options, description=description)
            ps.ParameterConfigsSetIO.param_factories.append(wrapper)
            return wrapper
        return decorator

    def get_selected(
        self, field: str | None = None, *, default_field: str | None = None, default: Any = None, **kwargs
    ) -> po.SelectParameterOption | Any | None:
        """
        Gets the selected single-select option or selected custom field

        Arguments:
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
    
    def get_selected_quoted(self, field: str, *, default_field: str | None = None, default: str | None = None, **kwargs) -> str | None:
        """
        Gets the selected single-select option surrounded by single quotes

        Arguments:
            field: The "custom_fields" attribute of the selected option.
            default_field: If field does not exist for a parameter option and default_field is not None, the default_field is used
                as the "field" instead.
            default: If field does not exist for a parameter option, default_field is None, but default is not None, then the default
                is returned as the selected field. Does nothing if default_field is not None

        Returns:
            A string surrounded by single quotes
        """
        selected_value = self.get_selected(field, default_field=default_field, default=default)
        
        def _enquote(x: Any) -> str:
            if not isinstance(selected_value, str):
                raise u.ConfigurationError(
                    f"Method 'get_selected_quoted' can only be used on fields with only string values"
                )
            return self._enquote(x)
        
        return u.process_if_not_none(selected_value, _enquote)
    
    def get_selected_id(self, **kwargs) -> str | None:
        """
        Gets the ID of the selected option

        Returns:
            A string ID or None if there are no selectable options
        """
        def get_id(x: po.SelectParameterOption): 
            return x._identifier
        return u.process_if_not_none(self.get_selected(), get_id)
    
    def get_selected_id_quoted(self, **kwargs) -> str | None:
        """
        Gets the ID of the selected option surrounded by single quotes

        Returns:
            A string or None if there are no selectable options
        """
        return u.process_if_not_none(self.get_selected_id(), self._enquote)
    
    def get_selected_label(self, **kwargs) -> str | None:
        """
        Gets the label of the selected option

        Returns:
            A string or None if there are no selectable options
        """
        def get_label(x: po.SelectParameterOption): return x._label
        return u.process_if_not_none(self.get_selected(), get_label)
    
    def get_selected_label_quoted(self, **kwargs) -> str | None:
        """
        Gets the label of the selected option surrounded by single quotes

        Returns:
            A string or None if there are no selectable options
        """
        return u.process_if_not_none(self.get_selected_label(), self._enquote)

    def _get_selected_ids_as_list(self) -> Sequence[str]:
        selected_id = self.get_selected_id()
        if selected_id is not None:
            return (selected_id,)
        else:
            return tuple()
    
    def _to_json_dict0(self) -> dict:
        """
        Converts this parameter as a JSON object for the parameters API response

        Returns:
            A dictionary for the JSON object
        """
        output = super()._to_json_dict0()
        output['selected_id'] = self._selected_id
        return output
    
    def _get_response_model0(self):
        return rm.SingleSelectParameterModel if self.is_enabled() else rm.NoneParameterModel


@dataclass
class MultiSelectParameter(_SelectionParameter[pc.MultiSelectParameterConfig]):
    """
    Class for multi-select parameter widgets.

    Attributes:
        config: The config for this widget parameter (for immutable attributes like name, label, all possible options, etc)
        options: The parameter options that are currently selectable
        selected_ids: A sequence of IDs of the selected options
    """
    _selected_ids: Sequence[str]

    def __post_init__(self):
        super().__post_init__()
        self._selected_ids = tuple(self._selected_ids)
        for selected_id in self._selected_ids:
            self._validate_selected_id_in_options(selected_id)
    
    @staticmethod
    def _ParameterConfigType():
        return pc.MultiSelectParameterConfig
    
    @staticmethod
    def _ParameterOptionType():
        return po.SelectParameterOption
    
    @staticmethod
    def _DataSourceType():
        return d.SelectDataSource
    
    @classmethod
    def CreateWithOptions(
        cls, name: str, label: str, all_options: Sequence[po.SelectParameterOption | dict], *, description: str = "",
        show_select_all: bool = True, order_matters: bool = False, none_is_all: bool = True,
        user_attribute: str | None = None, parent_name: str | None = None, **kwargs
    ):
        """
        Method for creating the configurations for a MultiSelectParameter that may include user attribute or parent

        .. deprecated::
            Use the lowercase decorator form `create_with_options` instead.

        Arguments:
            name: The name of the parameter
            label: The display label for the parameter
            all_options: All options associated to this parameter regardless of the user group or parent parameter option they depend on
            description: Explains the meaning of the parameter
            show_select_all: Communicate to front-end whether to include a "select all" option
            order_matters: Communicate to front-end whether the order of the selections made matter
            none_is_all: Whether having no options selected is equivalent to all selectable options selected
            user_attribute: The user attribute that may cascade the options for this parameter. Default is None
            parent_name: Name of parent parameter that may cascade the options for this parameter. Default is None (no parent)
        """
        return super().CreateWithOptions(
            name, label, all_options, description=description, user_attribute=user_attribute, parent_name=parent_name,
            show_select_all=show_select_all, order_matters=order_matters, none_is_all=none_is_all
        )

    @classmethod
    def create_with_options(
        cls, name: str, label: str, *, description: str = "", show_select_all: bool = True, order_matters: bool = False, 
        none_is_all: bool = True, user_attribute: str | None = None, parent_name: str | None = None
    ):
        """
        Python decorator for creating the configurations for a MultiSelectParameter that may include user attribute or parent

        The decorated function must return a list of SelectParameterOption objects.

        Arguments:
            name: The name of the parameter
            label: The display label for the parameter
            description: Explains the meaning of the parameter
            show_select_all: Communicate to front-end whether to include a "select all" option
            order_matters: Communicate to front-end whether the order of the selections made matter
            none_is_all: Whether having no options selected is equivalent to all selectable options selected
            user_attribute: The user attribute that may cascade the options for this parameter. Default is None
            parent_name: Name of parent parameter that may cascade the options for this parameter. Default is None (no parent)
        """
        def decorator(func: Callable[..., Sequence[po.SelectParameterOption]]):
            def wrapper(sqrl: ParametersArgs):
                options = u.call_func(func, sqrl=sqrl)
                return cls.CreateWithOptions(
                    name, label, options, description=description, user_attribute=user_attribute, parent_name=parent_name,
                    show_select_all=show_select_all, order_matters=order_matters, none_is_all=none_is_all
                )
            ps.ParameterConfigsSetIO.param_factories.append(wrapper)
            return wrapper
        return decorator
    
    @classmethod
    def CreateSimple(
        cls, name: str, label: str, all_options: Sequence[po.SelectParameterOption], *, description: str = "",
        show_select_all: bool = True, order_matters: bool = False, none_is_all: bool = True, **kwargs
    ):
        """
        Method for creating the configurations for a MultiSelectParameter that doesn't involve user attributes or parent parameters

        .. deprecated::
            Use the lowercase decorator form `create_simple` instead.

        Arguments:
            name: The name of the parameter
            label: The display label for the parameter
            all_options: All options associated to this parameter regardless of the user group or parent parameter option they depend on
            description: Explains the meaning of the parameter
            show_select_all: Communicate to front-end whether to include a "select all" option
            order_matters: Communicate to front-end whether the order of the selections made matter
            none_is_all: Whether having no options selected is equivalent to all selectable options selected
        """
        return cls.CreateWithOptions(
            name, label, all_options, description=description,
            show_select_all=show_select_all, order_matters=order_matters, none_is_all=none_is_all
        )
    
    @classmethod
    def create_simple(
        cls, name: str, label: str, *, description: str = "", 
        show_select_all: bool = True, order_matters: bool = False, none_is_all: bool = True
    ):
        """
        Python decorator for creating the configurations for a MultiSelectParameter that doesn't involve user attributes or parent parameters

        The decorated function must return a list of SelectParameterOption objects.

        Arguments:
            name: The name of the parameter
            label: The display label for the parameter
            description: Explains the meaning of the parameter
            show_select_all: Communicate to front-end whether to include a "select all" option
            order_matters: Communicate to front-end whether the order of the selections made matter
            none_is_all: Whether having no options selected is equivalent to all selectable options selected
        """
        def decorator(func: Callable[..., Sequence[po.SelectParameterOption]]):
            def wrapper(sqrl: ParametersArgs):
                options = u.call_func(func, sqrl=sqrl)
                return cls.CreateSimple(
                    name, label, options, description=description, 
                    show_select_all=show_select_all, order_matters=order_matters, none_is_all=none_is_all
                )
            ps.ParameterConfigsSetIO.param_factories.append(wrapper)
            return wrapper
        return decorator
    
    @classmethod
    def CreateFromSource(
        cls, name: str, label: str, data_source: d.SelectDataSource | dict, *, description: str = "",
        show_select_all: bool = True, order_matters: bool = False, none_is_all: bool = True,
        user_attribute: str | None = None, parent_name: str | None = None, **kwargs
    ):
        """
        Method for creating the configurations for a MultiSelectParameter that uses a SelectDataSource to receive the options

        .. deprecated::
            Use the lowercase decorator form `create_from_source` instead.

        Arguments:
            name: The name of the parameter
            label: The display label for the parameter
            data_source: The lookup table to use for this parameter
            description: Explains the meaning of the parameter
            show_select_all: Communicate to front-end whether to include a "select all" option
            order_matters: Communicate to front-end whether the order of the selections made matter
            none_is_all: Whether having no options selected is equivalent to all selectable options selected
            user_attribute: The user attribute that may cascade the options for this parameter. Default is None
            parent_name: Name of parent parameter that may cascade the options for this parameter. Default is None (no parent)
        """
        extra_args = {
            "show_select_all": show_select_all, "order_matters": order_matters, "none_is_all": none_is_all
        }
        return cls._CreateFromSourceHelper(
            name, label, data_source, extra_args=extra_args, description=description,
            user_attribute=user_attribute, parent_name=parent_name
        )
    
    @classmethod
    def create_from_source(
        cls, name: str, label: str, *, description: str = "",
        show_select_all: bool = True, order_matters: bool = False, none_is_all: bool = True,
        user_attribute: str | None = None, parent_name: str | None = None
    ):
        """
        Python decorator for creating the configurations for a MultiSelectParameter that uses a SelectDataSource to receive the options from a lookup table

        The decorated function must return a SelectDataSource object.
        
        Arguments:
            name: The name of the parameter
            label: The display label for the parameter
            data_source: The lookup table to use for this parameter
            description: Explains the meaning of the parameter
            show_select_all: Communicate to front-end whether to include a "select all" option
            order_matters: Communicate to front-end whether the order of the selections made matter
            none_is_all: Whether having no options selected is equivalent to all selectable options selected
            user_attribute: The user attribute that may cascade the options for this parameter. Default is None
            parent_name: Name of parent parameter that may cascade the options for this parameter. Default is None (no parent)
        """
        def decorator(func: Callable[..., d.SelectDataSource]):
            def wrapper(sqrl: ParametersArgs):
                data_source = u.call_func(func, sqrl=sqrl)
                return cls.CreateFromSource(
                    name, label, data_source, description=description,
                    show_select_all=show_select_all, order_matters=order_matters, none_is_all=none_is_all,
                    user_attribute=user_attribute, parent_name=parent_name
                )
            ps.ParameterConfigsSetIO.param_factories.append(wrapper)
            return wrapper
        return decorator

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
        self, field: str | None = None, *, default_field: str | None = None, default: Any = None, **kwargs
    ) -> Sequence[po.SelectParameterOption | Any]:
        """
        Gets the sequence of the selected option(s) or a sequence of selected custom fields

        Arguments:
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
    
    def _get_selected_list_of_strings(
        self, method: str, field: str, default_field: str | None, default: str | None, **kwargs
    ) -> list[str]:
        selected_list = self.get_selected_list(field, default_field=default_field, default=default)
        list_of_strings: list[str] = []
        for selected in selected_list:
            if not isinstance(selected, str):
                raise u.ConfigurationError(
                    f"Method '{method}' can only be used on fields with only string values"
                )
            list_of_strings.append(selected)
        return list_of_strings
    
    def get_selected_list_joined(self, field: str, *, default_field: str | None = None, default: str | None = None, **kwargs) -> str:
        """
        Gets the selected custom fields joined by comma

        Arguments:
            field: The "custom_fields" attribute of the selected options.
            default_field: If field does not exist for a parameter option and default_field is not None, the default_field is used
                as the "field" instead.
            default: If field does not exist for a parameter option, default_field is None, but default is not None, the default
                is returned as the selected field. Does nothing if default_field is not None

        Returns:
            A string
        """
        list_of_strings = self._get_selected_list_of_strings("get_selected_list_joined", field, default_field, default)
        return ','.join(list_of_strings)
    
    def get_selected_list_quoted(self, field: str, *, default_field: str | None = None, default: str | None = None, **kwargs) -> tuple[str, ...]:
        """
        Gets the selected custom fields surrounded by single quotes

        Arguments:
            field: The "custom_fields" attribute of the selected options.
            default_field: If field does not exist for a parameter option and default_field is not None, the default_field is used
                as the "field" instead.
            default: If field does not exist for a parameter option, default_field is None, but default is not None, the default
                is returned as the selected field. Does nothing if default_field is not None

        Returns:
            A tuple of strings
        """
        list_of_strings = self._get_selected_list_of_strings("get_selected_list_quoted", field, default_field, default)
        return tuple(self._enquote(x) for x in list_of_strings)
    
    def get_selected_list_quoted_joined(self, field: str, *, default_field: str | None = None, default: str | None = None, **kwargs) -> str:
        """
        Gets the selected custom fields surrounded by single quotes and joined by comma

        Arguments:
            field: The "custom_fields" attribute of the selected options.
            default_field: If field does not exist for a parameter option and default_field is not None, the default_field is used
                as the "field" instead.
            default: If field does not exist for a parameter option, default_field is None, but default is not None, the default
                is returned as the selected field. Does nothing if default_field is not None
        
        Returns:
            A string
        """
        list_of_strings = self._get_selected_list_of_strings("get_selected_list_quoted_joined", field, default_field, default)
        return ','.join(self._enquote(x) for x in list_of_strings)

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
    
    def _to_json_dict0(self):
        """
        Converts this parameter as a JSON object for the parameters API response

        Returns:
            A dictionary for the JSON object
        """
        output = super()._to_json_dict0()
        output['show_select_all'] = self._config.show_select_all
        output['order_matters'] = self._config.order_matters
        output['selected_ids'] = list(self._selected_ids)
        return output
    
    def _get_response_model0(self):
        return rm.MultiSelectParameterModel if self.is_enabled() else rm.NoneParameterModel


DatePO = TypeVar("DatePO", bound=po._DateTypeParameterOption)

@dataclass
class _DateTypeParameter(Parameter[PC, DatePO, DS], Generic[PC, DatePO, DS]):
    _curr_option: DatePO | None
    
    def is_enabled(self) -> bool:
        return self._curr_option is not None
    
    def _cast_optional_date_to_str(self, date: date | None) -> str | None:
        return None if date is None else date.strftime("%Y-%m-%d")

    def _to_json_dict0(self):
        output = super()._to_json_dict0()
        if self._curr_option is not None:
            output["min_date"] = self._cast_optional_date_to_str(self._curr_option._min_date)
            output["max_date"] = self._cast_optional_date_to_str(self._curr_option._max_date)
        return output


@dataclass
class DateParameter(_DateTypeParameter[pc.DateParameterConfig, po.DateParameterOption, d.DateDataSource]):
    """
    Class for date parameter widgets.

    Attributes:
        config: The config for this widget parameter (for immutable attributes like name, label, all possible options, etc)
        curr_option: The current option showing for defaults based on user attribute and selection of parent
        selected_date: The selected date
    """
    _selected_date: date | str | None

    def __post_init__(self):
        if self._curr_option is not None and self._selected_date is not None:
            self._selected_date = self._validate_input_date(self._selected_date, self._curr_option)
    
    def is_enabled(self) -> bool:
        return self._curr_option is not None
    
    @staticmethod
    def _ParameterConfigType():
        return pc.DateParameterConfig

    @staticmethod
    def _ParameterOptionType():
        return po.DateParameterOption

    @staticmethod
    def _DataSourceType():
        return d.DateDataSource

    @classmethod
    def CreateSimple(
        cls, name: str, label: str, default_date: str | date, *, description: str = "", 
        min_date: str | date | None = None, max_date: str | date | None = None, date_format: str = '%Y-%m-%d', **kwargs
    ):
        """
        Method for creating the configurations for a DateParameter that doesn't involve user attributes or parent parameters

        .. deprecated::
            Use the lowercase decorator form `create_simple` instead.

        Arguments:
            name: The name of the parameter
            label: The display label for the parameter
            default_date: Default date for this option
            description: Explains the meaning of the parameter
            min_date: Minimum selectable date
            max_date: Maximum selectable date
            date_format: Format of the default date, default is '%Y-%m-%d'
        """
        single_param_option = po.DateParameterOption(default_date, min_date=min_date, max_date=max_date, date_format=date_format)
        return cls.CreateWithOptions(name, label, (single_param_option,), description=description)

    @classmethod
    def create_simple(
        cls, name: str, label: str, default_date: str | date, *, description: str = "", 
        min_date: str | date | None = None, max_date: str | date | None = None, date_format: str = '%Y-%m-%d'
    ):
        """
        Python decorator for creating the configurations for a DateParameter that doesn't involve user attributes or parent parameters

        Arguments:
            name: The name of the parameter
            label: The display label for the parameter
            default_date: Default date for this option
            description: Explains the meaning of the parameter
            min_date: Minimum selectable date
            max_date: Maximum selectable date
            date_format: Format of the default date, default is '%Y-%m-%d'
        """
        def decorator(func: Callable[..., Any]):
            def wrapper(sqrl: ParametersArgs):
                return cls.CreateSimple(
                    name, label, default_date, description=description, 
                    min_date=min_date, max_date=max_date, date_format=date_format
                )
            ps.ParameterConfigsSetIO.param_factories.append(wrapper)
            return wrapper
        return decorator
    
    def get_selected_date(self, *, date_format: str | None = None, **kwargs) -> str:
        """
        Gets selected date as string

        Arguments:
            date_format: The date format (see Python's datetime formats). If not specified, self.date_format is used

        Returns:
            A string
        """
        assert self._curr_option is not None and isinstance(self._selected_date, date), "Parameter is not enabled"
        date_format = self._curr_option._date_format if date_format is None else date_format
        return self._selected_date.strftime(date_format)

    def get_selected_date_quoted(self, *, date_format: str | None = None, **kwargs) -> str:
        """
        Gets selected date as string surrounded by single quotes

        Arguments:
            date_format: The date format (see Python's datetime formats). If not specified, self.date_format is used

        Returns:
            A string
        """
        return self._enquote(self.get_selected_date(date_format=date_format))
    
    def _to_json_dict0(self):
        """
        Converts this parameter as a JSON object for the parameters API response

        The "selected_date" field will always be in yyyy-mm-dd format

        Returns:
            A dictionary for the JSON object
        """
        output = super()._to_json_dict0()
        if self.is_enabled():
            output["selected_date"] = self.get_selected_date(date_format="%Y-%m-%d")
        else:
            output["selected_date"] = ""
        return output
    
    def _get_response_model0(self):
        return rm.DateParameterModel if self.is_enabled() else rm.NoneParameterModel


@dataclass
class DateRangeParameter(_DateTypeParameter[pc.DateRangeParameterConfig, po.DateRangeParameterOption, d.DateRangeDataSource]):
    """
    Class for date range parameter widgets.

    Attributes:
        config: The config for this widget parameter (for immutable attributes like name, label, all possible options, etc)
        curr_option: The current option showing for defaults based on user attribute and selection of parent
        selected_start_date: The selected start date
        selected_end_date: The selected end date
    """
    _selected_start_date: date | str | None
    _selected_end_date: date | str | None

    def __post_init__(self):
        if self._curr_option is not None:
            if self._selected_start_date is not None:
                self._selected_start_date = self._validate_input_date(self._selected_start_date, self._curr_option)
            if self._selected_end_date is not None:
                self._selected_end_date = self._validate_input_date(self._selected_end_date, self._curr_option)
    
    def is_enabled(self) -> bool:
        return self._curr_option is not None
    
    @staticmethod
    def _ParameterConfigType():
        return pc.DateRangeParameterConfig
    
    @staticmethod
    def _ParameterOptionType():
        return po.DateRangeParameterOption

    @staticmethod
    def _DataSourceType():
        return d.DateRangeDataSource

    @classmethod
    def CreateSimple(
        cls, name: str, label: str, default_start_date: str | date, default_end_date: str | date, *, description: str = "", 
        min_date: str | date | None = None, max_date: str | date | None = None, date_format: str = '%Y-%m-%d', **kwargs
    ):
        """
        Method for creating the configurations for a DateRangeParameter that doesn't involve user attributes or parent parameters

        .. deprecated::
            Use the lowercase decorator form `create_simple` instead.

        Arguments:
            name: The name of the parameter
            label: The display label for the parameter
            default_start_date: Default start date for this option
            default_end_date: Default end date for this option
            description: Explains the meaning of the parameter
            min_date: Minimum selectable date
            max_date: Maximum selectable date
            date_format: Format of the default date, default is '%Y-%m-%d'
        """
        single_param_option = po.DateRangeParameterOption(
            default_start_date, default_end_date, min_date=min_date, max_date=max_date, date_format=date_format
        )
        return cls.CreateWithOptions(name, label, (single_param_option,), description=description)
    
    @classmethod
    def create_simple(
        cls, name: str, label: str, default_start_date: str | date, default_end_date: str | date, *, description: str = "", 
        min_date: str | date | None = None, max_date: str | date | None = None, date_format: str = '%Y-%m-%d'
    ):
        """
        Python decorator for creating the configurations for a DateRangeParameter that doesn't involve user attributes or parent parameters

        Arguments:
            name: The name of the parameter
            label: The display label for the parameter
            default_start_date: Default start date for this option
            default_end_date: Default end date for this option
            description: Explains the meaning of the parameter
            min_date: Minimum selectable date
            max_date: Maximum selectable date
            date_format: Format of the default date, default is '%Y-%m-%d'
        """
        def decorator(func: Callable[..., Any]):
            def wrapper(sqrl: ParametersArgs):
                return cls.CreateSimple(
                    name, label, default_start_date, default_end_date, description=description, 
                    min_date=min_date, max_date=max_date, date_format=date_format
                )
            ps.ParameterConfigsSetIO.param_factories.append(wrapper)
            return wrapper
        return decorator
    
    def get_selected_start_date(self, *, date_format: str | None = None, **kwargs) -> str:
        """
        Gets selected start date as string

        Arguments:
            date_format: The date format (see Python's datetime formats). If not specified, self.date_format is used

        Returns:
            A string
        """
        assert self._curr_option is not None and isinstance(self._selected_start_date, date), "Parameter is not enabled"
        date_format = self._curr_option._date_format if date_format is None else date_format
        return self._selected_start_date.strftime(date_format)

    def get_selected_start_date_quoted(self, *, date_format: str | None = None, **kwargs) -> str:
        """
        Gets selected start date as string surrounded by single quotes

        Arguments:
            date_format: The date format (see Python's datetime formats). If not specified, self.date_format is used

        Returns:
            A string
        """
        return self._enquote(self.get_selected_start_date(date_format=date_format))
    
    def get_selected_end_date(self, *, date_format: str | None = None, **kwargs) -> str:
        """
        Gets selected end date as string

        Arguments:
            date_format: The date format (see Python's datetime formats). If not specified, self.date_format is used

        Returns:
            A string
        """
        assert self._curr_option is not None and isinstance(self._selected_end_date, date), "Parameter is not enabled"
        date_format = self._curr_option._date_format if date_format is None else date_format
        return self._selected_end_date.strftime(date_format)

    def get_selected_end_date_quoted(self, *, date_format: str | None = None, **kwargs) -> str:
        """
        Gets selected end date as string surrounded by single quotes

        Arguments:
            date_format: The date format (see Python's datetime formats). If not specified, self.date_format is used

        Returns:
            A string
        """
        return self._enquote(self.get_selected_end_date(date_format=date_format))
    
    def _to_json_dict0(self):
        """
        Converts this parameter as a JSON object for the parameters API response

        The "selected_date" field will always be in yyyy-mm-dd format

        Returns:
            A dictionary for the JSON object
        """
        output = super()._to_json_dict0()
        if self.is_enabled():
            output["selected_start_date"] = self.get_selected_start_date(date_format="%Y-%m-%d")
            output["selected_end_date"] = self.get_selected_end_date(date_format="%Y-%m-%d")
        return output
    
    def _get_response_model0(self):
        return rm.DateRangeParameterModel if self.is_enabled() else rm.NoneParameterModel


NumericPO = TypeVar("NumericPO", bound=po._NumericParameterOption)

@dataclass
class _NumberTypeParameter(Parameter[PC, NumericPO, DS], Generic[PC, NumericPO, DS]):
    _curr_option: NumericPO | None
    
    def is_enabled(self) -> bool:
        return self._curr_option is not None

    def _to_json_dict0(self):
        output = super()._to_json_dict0()
        if self._curr_option is not None:
            output["min_value"] = float(self._curr_option._min_value)
            output["max_value"] = float(self._curr_option._max_value)
            output["increment"] = float(self._curr_option._increment)
        return output
    

@dataclass
class NumberParameter(_NumberTypeParameter[pc.NumberParameterConfig, po.NumberParameterOption, d.NumberDataSource]):
    """
    Class for number parameter widgets.

    Attributes:
        config: The config for this widget parameter (for immutable attributes like name, label, all possible options, etc)
        curr_option: The current option showing for defaults based on user attribute and selection of parent
        selected_value: The selected integer or decimal number
    """
    _selected_value: po.Number | None

    def __post_init__(self):
        if self._curr_option is not None and self._selected_value is not None:
            self._selected_value = self._validate_number(self._selected_value, self._curr_option)
    
    @staticmethod
    def _ParameterConfigType():
        return pc.NumberParameterConfig
    
    @staticmethod
    def _ParameterOptionType():
        return po.NumberParameterOption

    @staticmethod
    def _DataSourceType():
        return d.NumberDataSource

    @classmethod
    def CreateSimple(
        cls, name: str, label: str, min_value: po.Number, max_value: po.Number, *, description: str = "", 
        increment: po.Number = 1, default_value: po.Number | None = None, **kwargs
    ):
        """
        Method for creating the configurations for a NumberParameter that doesn't involve user attributes or parent parameters
        
        .. deprecated::
            Use the lowercase decorator form `create_simple` instead.
        
        * Note that the "Number" type denotes an int, a Decimal (from decimal module), or a string that can be parsed to Decimal
        
        Arguments:
            name: The name of the parameter
            label: The display label for the parameter
            min_value: Minimum selectable value
            max_value: Maximum selectable value
            description: Explains the meaning of the parameter
            increment: Increment of selectable values, and must fit evenly between min_value and max_value
            default_value: Default value for this option, and must be selectable based on min_value, max_value, and increment
        """
        single_param_option = po.NumberParameterOption(min_value, max_value, increment=increment, default_value=default_value)
        return cls.CreateWithOptions(name, label, (single_param_option,), description=description)

    @classmethod
    def create_simple(
        cls, name: str, label: str, min_value: po.Number, max_value: po.Number, *, description: str = "", 
        increment: po.Number = 1, default_value: po.Number | None = None
    ):
        """
        Python decorator for creating the configurations for a NumberParameter that doesn't involve user attributes or parent parameters

        * Note that the "Number" type denotes an int, a Decimal (from decimal module), or a string that can be parsed to Decimal

        Arguments:
            name: The name of the parameter
            label: The display label for the parameter
            min_value: Minimum selectable value
            max_value: Maximum selectable value
            description: Explains the meaning of the parameter
            increment: Increment of selectable values, and must fit evenly between min_value and max_value
            default_value: Default value for the parameter
        """
        def decorator(func: Callable[..., Any]):
            def wrapper(sqrl: ParametersArgs):
                return cls.CreateSimple(
                    name, label, min_value, max_value, description=description, increment=increment, default_value=default_value
                )
            ps.ParameterConfigsSetIO.param_factories.append(wrapper)
            return wrapper
        return decorator
    
    def get_selected_value(self, **kwargs) -> float:
        """
        Get the selected number (converted from Decimal to float)

        Returns:
            float
        """
        assert self._selected_value is not None, "Parameter is not enabled"
        return float(self._selected_value)
        
    def _to_json_dict0(self):
        """
        Converts this parameter as a JSON object for the parameters API response

        Returns:
            A dictionary for the JSON object
        """
        output = super()._to_json_dict0()
        if self.is_enabled():
            output["selected_value"] = self.get_selected_value()
        return output
    
    def _get_response_model0(self):
        return rm.NumberParameterModel if self.is_enabled() else rm.NoneParameterModel


@dataclass
class NumberRangeParameter(_NumberTypeParameter[pc.NumberRangeParameterConfig, po.NumberRangeParameterOption, d.NumberRangeDataSource]):
    """
    Class for number range parameter widgets.

    Attributes:
        config: The config for this widget parameter (for immutable attributes like name, label, all possible options, etc)
        curr_option: The current option showing for defaults based on user attribute and selection of parent
        selected_lower_value: The selected lower integer or decimal number
        selected_upper_value: The selected upper integer or decimal number
    """
    _selected_lower_value: po.Number | None
    _selected_upper_value: po.Number | None

    def __post_init__(self):
        if self._curr_option is not None:
            if self._selected_lower_value is not None:
                self._selected_lower_value = self._validate_number(self._selected_lower_value, self._curr_option)
            if self._selected_upper_value is not None:
                self._selected_upper_value = self._validate_number(self._selected_upper_value, self._curr_option)
    
    @staticmethod
    def _ParameterConfigType():
        return pc.NumberRangeParameterConfig
    
    @staticmethod
    def _ParameterOptionType():
        return po.NumberRangeParameterOption

    @staticmethod
    def _DataSourceType():
        return d.NumberRangeDataSource

    @classmethod
    def CreateSimple(
        cls, name: str, label: str, min_value: po.Number, max_value: po.Number, *, description: str = "",
        increment: po.Number = 1, default_lower_value: po.Number | None = None, default_upper_value: po.Number | None = None,**kwargs
    ):
        """
        Method for creating the configurations for a NumberRangeParameter that doesn't involve user attributes or parent parameters
        
        .. deprecated::
            Use the lowercase decorator form `create_simple` instead.
        
        * Note that the "Number" type denotes an int, a Decimal (from decimal module), or a string that can be parsed to Decimal

        Arguments:
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
        single_param_option = po.NumberRangeParameterOption(
            min_value, max_value, increment=increment, default_lower_value=default_lower_value, default_upper_value=default_upper_value
        )
        return cls.CreateWithOptions(name, label, (single_param_option,), description=description)
    
    @classmethod
    def create_simple(
        cls, name: str, label: str, min_value: po.Number, max_value: po.Number, *, description: str = "", 
        increment: po.Number = 1, default_lower_value: po.Number | None = None, default_upper_value: po.Number | None = None
    ):
        """
        Python decorator for creating the configurations for a NumberRangeParameter that doesn't involve user attributes or parent parameters

        * Note that the "Number" type denotes an int, a Decimal (from decimal module), or a string that can be parsed to Decimal

        Arguments:
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
        def decorator(func: Callable[..., Any]):
            def wrapper(sqrl: ParametersArgs):
                return cls.CreateSimple(
                    name, label, min_value, max_value, description=description, increment=increment, 
                    default_lower_value=default_lower_value, default_upper_value=default_upper_value
                )
            ps.ParameterConfigsSetIO.param_factories.append(wrapper)
            return wrapper
        return decorator
    
    def get_selected_lower_value(self, **kwargs) -> float:
        """
        Get the selected lower value number (converted from Decimal to float)

        Returns:
            float
        """
        assert self._selected_lower_value is not None, "Parameter is not enabled"
        return float(self._selected_lower_value)

    def get_selected_upper_value(self, **kwargs) -> float:
        """
        Get the selected upper value number (converted from Decimal to float)

        Returns:
            float
        """
        assert self._selected_upper_value is not None, "Parameter is not enabled"
        return float(self._selected_upper_value)

    def _to_json_dict0(self):
        """
        Converts this parameter as a JSON object for the parameters API response

        Returns:
            A dictionary for the JSON object
        """
        output = super()._to_json_dict0()
        if self.is_enabled():
            output['selected_lower_value'] = self.get_selected_lower_value()
            output['selected_upper_value'] = self.get_selected_upper_value()
        return output
    
    def _get_response_model0(self):
        return rm.NumberRangeParameterModel if self.is_enabled() else rm.NoneParameterModel


@dataclass
class TextValue:
    _value_do_not_touch: str

    def __repr__(self):
        raise u.ConfigurationError(
            "Cannot convert TextValue directly to string (to avoid SQL injection). Try using it through placeholders instead"
        )

    def apply(self, str_to_str_function: Callable[[str], str]) -> TextValue:
        """
        Transforms the entered text with a function that takes a string and returns a string. 
        
        This method returns a new object and leaves the original the same.

        Arguments:
            str_to_str_function: A function that accepts a string and returns a string

        Returns:
            A new TextValue with the transformed entered text
        """
        new_value = str_to_str_function(self._value_do_not_touch)
        if not isinstance(new_value, str):
            raise u.ConfigurationError("Function provided must return string")
        return TextValue(new_value)
    
    def apply_percent_wrap(self) -> TextValue:
        """
        Adds percent signs before and after the entered text, and returns a new object, leaving the original the same.

        Returns:
            A new TextValue with the transformed entered text
        """
        return self.apply(lambda x: "%"+x+"%")
    
    def apply_as_bool(self, str_to_bool_function: Callable[[str], bool]) -> bool:
        """
        Transforms the entered text with a function that takes a string and returns a boolean.

        Arguments:
            str_to_bool_function: A function that accepts a string and returns a boolean.

        Returns:
            A boolean for the transformed value
        """
        new_value = str_to_bool_function(self._value_do_not_touch)
        if not isinstance(new_value, bool):
            raise u.ConfigurationError("Function provided must return bool")
        return new_value
    
    def apply_as_number(self, str_to_num_function: Callable[[str], IntOrFloat]) ->  IntOrFloat:
        """
        Transforms the entered text with a function that takes a string and returns an int or float.

        Arguments:
            str_to_num_function: A function that accepts a string and returns an int or float.

        Returns:
            An int or float for the transformed value
        """
        new_value = str_to_num_function(self._value_do_not_touch)
        if not isinstance(new_value, (int, float)):
            raise u.ConfigurationError("Function provided must return a number")
        return new_value
    
    def apply_as_datetime(self, str_to_datetime_function: Callable[[str], datetime]) -> datetime:
        """
        Transforms the entered text with a function that takes a string and returns a datetime object.

        Arguments:
            str_to_datetime_function: A function that accepts a string and returns a datetime object.

        Returns:
            A datetime object for the transformed value
        """
        new_value = str_to_datetime_function(self._value_do_not_touch)
        if not isinstance(new_value, datetime):
            raise u.ConfigurationError("Function provided must return datetime")
        return new_value


@dataclass
class TextParameter(Parameter[pc.TextParameterConfig, po.TextParameterOption, d.TextDataSource]):
    """
    Class for text parameter widgets.
    """
    _curr_option: po.TextParameterOption | None
    _entered_text: str | None

    def __post_init__(self):
        if self.is_enabled() and isinstance(self._entered_text, str):
            try:
                self._entered_text = self._config.validate_entered_text(self._entered_text)
            except u.ConfigurationError as e:
                raise self._config._invalid_input_error(self._entered_text, str(e))
    
    def is_enabled(self) -> bool:
        return self._curr_option is not None
    
    @staticmethod
    def _ParameterConfigType():
        return pc.TextParameterConfig
    
    @staticmethod
    def _ParameterOptionType():
        return po.TextParameterOption
    
    @staticmethod
    def _DataSourceType():
        return d.TextDataSource

    @classmethod
    def CreateWithOptions(
        cls, name: str, label: str, all_options: Sequence[po.TextParameterOption | dict], *, description: str = "",
        input_type: str = "text", user_attribute: str | None = None, parent_name: str | None = None, **kwargs
    ):
        """
        Method for creating the configurations for a TextParameter that doesn't involve user attribute or parent

        .. deprecated::
            Use the lowercase decorator form `create_with_options` instead.

        Arguments:
            name: The name of the parameter
            label: The display label for the parameter
            all_options: All options associated to this parameter regardless of the user group or parent parameter option they depend on
            description: Explains the meaning of the parameter
            input_type: The type of input field to use. Must be one of "text", "textarea", "number", "color", "date", "datetime-local", "month", "time", and "password". Optional, default is "text". More information on input types other than "textarea" can be found at https://www.w3schools.com/html/html_form_input_types.asp. More information on "textarea" can be found at https://www.w3schools.com/tags/tag_textarea.asp
            user_attribute: The user attribute that may cascade the options for this parameter. Default is None
            parent_name: Name of parent parameter that may cascade the options for this parameter. Default is None (no parent)
        """
        return super().CreateWithOptions(
            name, label, all_options, description=description, input_type=input_type, 
            user_attribute=user_attribute, parent_name=parent_name
        )
    
    @classmethod
    def create_with_options(
        cls, name: str, label: str, *, description: str = "", 
        input_type: str = "text", user_attribute: str | None = None, parent_name: str | None = None
    ):
        """
        Python decorator for creating the configurations for a TextParameter that may include user attribute or parent

        The decorated function must return a list of TextParameterOption objects.

        Arguments:
            name: The name of the parameter
            label: The display label for the parameter
            description: Explains the meaning of the parameter
            input_type: The type of input field to use. Must be one of "text", "textarea", "number", "color", "date", "datetime-local", "month", "time", and "password". Optional, default is "text". More information on input types other than "textarea" can be found at https://www.w3schools.com/html/html_form_input_types.asp. More information on "textarea" can be found at https://www.w3schools.com/tags/tag_textarea.asp
            user_attribute: The user attribute that may cascade the options for this parameter. Default is None
            parent_name: Name of parent parameter that may cascade the options for this parameter. Default is None (no parent)
        """
        def decorator(func: Callable[..., Sequence[po.TextParameterOption]]):
            def wrapper(sqrl: ParametersArgs):
                options = u.call_func(func, sqrl=sqrl)
                return cls.CreateWithOptions(
                    name, label, options, description=description, input_type=input_type,
                    user_attribute=user_attribute, parent_name=parent_name
                )
            ps.ParameterConfigsSetIO.param_factories.append(wrapper)
            return wrapper
        return decorator
    
    @classmethod
    def CreateSimple(
        cls, name: str, label: str, *, description: str = "", default_text: str = "", input_type: str = "text", **kwargs
    ):
        """
        Method for creating the configurations for a TextParameter that doesn't involve user attributes or parent parameters
        
        .. deprecated::
            Use the lowercase decorator form `create_simple` instead.
        
        Arguments:
            name: The name of the parameter
            label: The display label for the parameter
            description: Explains the meaning of the parameter
            default_text: Default input text for this option. Optional, default is empty string.
            input_type: The type of input field to use. Must be one of "text", "textarea", "number", "color", "date", "datetime-local", "month", "time", and "password". Optional, default is "text". More information on input types other than "textarea" can be found at https://www.w3schools.com/html/html_form_input_types.asp. More information on "textarea" can be found at https://www.w3schools.com/tags/tag_textarea.asp
        """
        single_param_option = po.TextParameterOption(default_text=default_text)
        return cls.CreateWithOptions(name, label, (single_param_option,), description=description, input_type=input_type)
    
    @classmethod
    def create_simple(cls, name: str, label: str, *, description: str = "", default_text: str = "", input_type: str = "text"):
        """
        Python decorator for creating the configurations for a TextParameter that doesn't involve user attributes or parent parameters

        Arguments:
            name: The name of the parameter
            label: The display label for the parameter
            description: Explains the meaning of the parameter
            default_text: Default input text for this option. Optional, default is empty string.
            input_type: The type of input field to use. Must be one of "text", "textarea", "number", "color", "date", "datetime-local", "month", "time", and "password". Optional, default is "text". More information on input types other than "textarea" can be found at https://www.w3schools.com/html/html_form_input_types.asp. More information on "textarea" can be found at https://www.w3schools.com/tags/tag_textarea.asp
        """
        def decorator(func: Callable[..., Any]):
            def wrapper(sqrl: ParametersArgs):
                return cls.CreateSimple(name, label, description=description, default_text=default_text, input_type=input_type)
            ps.ParameterConfigsSetIO.param_factories.append(wrapper)
            return wrapper
        return decorator
    
    @classmethod
    def CreateFromSource(
        cls, name: str, label: str, data_source: d.TextDataSource | dict, *, description: str = "",
        input_type: str = "text", user_attribute: str | None = None, parent_name: str | None = None, **kwargs
    ):
        """
        Method for creating the configurations for a TextParameter that uses a TextDataSource to receive the options

        .. deprecated::
            Use the lowercase decorator form `create_from_source` instead.

        Arguments:
            name: The name of the parameter
            label: The display label for the parameter
            data_source: The lookup table to use for this parameter
            description: Explains the meaning of the parameter
            input_type: The type of input field to use. Options are one of "text", "textarea", "number", "color", "date", "datetime-local", "month", "time", and "password". Optional, default is "text". More information on input types other than "textarea" can be found at https://www.w3schools.com/html/html_form_input_types.asp. More information on "textarea" can be found at https://www.w3schools.com/tags/tag_textarea.asp
            user_attribute: The user attribute that may cascade the options for this parameter. Default is None
            parent_name: Name of parent parameter that may cascade the options for this parameter. Default is None (no parent)
        """
        extra_args = {
            "input_type": input_type
        }
        return cls._CreateFromSourceHelper(
            name, label, data_source, extra_args=extra_args, description=description, user_attribute=user_attribute, parent_name=parent_name
        )
    
    @classmethod
    def create_from_source(
        cls, name: str, label: str, data_source: d.TextDataSource | dict, *, description: str = "",
        input_type: str = "text", user_attribute: str | None = None, parent_name: str | None = None
    ):
        """
        Python decorator for creating the configurations for a TextParameter that uses a TextDataSource to receive the options from a lookup table

        The decorated function must return a TextDataSource object.

        Arguments:
            name: The name of the parameter
            label: The display label for the parameter
            data_source: The lookup table to use for this parameter
            description: Explains the meaning of the parameter
            input_type: The type of input field to use. Options are one of "text", "textarea", "number", "color", "date", "datetime-local", "month", "time", and "password". Optional, default is "text". More information on input types other than "textarea" can be found at https://www.w3schools.com/html/html_form_input_types.asp. More information on "textarea" can be found at https://www.w3schools.com/tags/tag_textarea.asp
            user_attribute: The user attribute that may cascade the options for this parameter. Default is None
            parent_name: Name of parent parameter that may cascade the options for this parameter. Default is None (no parent)
        """
        def decorator(func: Callable[..., d.TextDataSource]):
            def wrapper(sqrl: ParametersArgs):
                data_source = u.call_func(func, sqrl=sqrl)
                return cls.CreateFromSource(
                    name, label, data_source, description=description, 
                    input_type=input_type, user_attribute=user_attribute, parent_name=parent_name
                )
            ps.ParameterConfigsSetIO.param_factories.append(wrapper)
            return wrapper
        return decorator
    
    def get_entered_text(self, **kwargs) -> TextValue:
        """
        Get the entered text. Returns a TextValue object that cannot be converted to string except through placeholders.

        Returns:
            A TextValue object
        """
        assert isinstance(self._entered_text, str), "Parameter is not enabled"
        return TextValue(self._entered_text)
    
    def get_entered_int(self, **kwargs) -> int:
        """
        Get the entered integer. The TextParameter must be a "number" input type

        Returns: int
        """
        if self._config.input_type != "number":
            raise u.ConfigurationError("Method 'get_entered_int' requires TextParameter to have input type 'number'")
        text = self.get_entered_text()
        return text.apply_as_number(int)
    
    def get_entered_datetime(self, **kwargs) -> datetime:
        """
        Get the entered datetime. The TextParameter input type must be one of ["date", "datetime-local", "month", "time"]

        Returns: datetime
        """
        applicable_input_types = ["date", "datetime-local", "month", "time"]
        if self._config.input_type not in applicable_input_types:
            raise u.ConfigurationError(f"Method 'get_entered_datetime' requires TextParameter to have one of these input types: {applicable_input_types}")
        text = self.get_entered_text()

        date_formats = { "date": "%Y-%m-%d", "datetime-local": "%Y-%m-%dT%H:%M", "month": "%Y-%m", "time": "%H:%M" }
        return text.apply_as_datetime(lambda x: datetime.strptime(x, date_formats[self._config.input_type]))
        
    def _to_json_dict0(self):
        """
        Converts this parameter as a JSON object for the parameters API response

        Returns:
            A dictionary for the JSON object
        """
        output = super()._to_json_dict0()
        output['input_type'] = self._config.input_type
        if self.is_enabled():
            output['entered_text'] = self._entered_text
        return output
    
    def _get_response_model0(self):
        return rm.TextParameterModel if self.is_enabled() else rm.NoneParameterModel
