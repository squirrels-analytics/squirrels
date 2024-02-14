from __future__ import annotations
from typing import Type, Optional, Union, Sequence, Iterator, Any
from dataclasses import dataclass, field
from abc import ABCMeta, abstractmethod
from copy import copy
import pandas as pd

from . import parameter_options as po, parameters as p, data_sources as d, _utils as u
from .user_base import User


@dataclass
class ParameterConfigBase(metaclass=ABCMeta):
    """
    Abstract class for all parameter classes
    """
    name: str
    label: str
    is_hidden: bool # = field(default=False, kw_only=True)
    user_attribute: Optional[str] # = field(default=None, kw_only=True)
    parent_name: Optional[str] # = field(default=None, kw_only=True)

    @abstractmethod
    def __init__(
        self, widget_type: str, name: str, label: str, *, is_hidden: bool = False, user_attribute: Optional[str] = None, 
        parent_name: Optional[str] = None
    ) -> None:
        self.widget_type = widget_type
        self.name = name
        self.label = label
        self.is_hidden = is_hidden
        self.user_attribute = user_attribute
        self.parent_name = parent_name

    def _get_user_group(self, user: Optional[User]) -> Any:
        if self.user_attribute is not None:
            if user is None:
                raise u.ConfigurationError(f"Public datasets (which allows non-authenticated users) cannot use parameter " +
                                           f"'{self.name}' because 'user_attribute' is defined on this parameter.")
            return getattr(user, self.user_attribute)
        
    def copy(self):
        """
        Use for unit testing only
        """
        return copy(self)

    def to_json_dict0(self) -> dict:
        return {
            'widget_type': self.widget_type,
            'name': self.name,
            'label': self.label
        }


@dataclass
class ParameterConfig(ParameterConfigBase):
    """
    Abstract class for all parameter classes (except DataSourceParameters)
    """
    all_options: Sequence[po.ParameterOption] = field(repr=False)

    @abstractmethod
    def __init__(
        self, widget_type: str, name: str, label: str, all_options: Sequence[Union[po.ParameterOption, dict]], *, is_hidden: bool = False, 
        user_attribute: Optional[str] = None, parent_name: Optional[str] = None
    ) -> None:
        super().__init__(widget_type, name, label, is_hidden=is_hidden, user_attribute=user_attribute, 
                         parent_name=parent_name)
        self.all_options = tuple(self.__to_param_option(x) for x in all_options)

    def __to_param_option(self, option: Union[po.ParameterOption, dict]) -> po.ParameterOption:
        return self.__class__.ParameterOption(**option) if isinstance(option, dict) else option

    @staticmethod
    @abstractmethod
    def ParameterOption(*args, **kwargs) -> po.ParameterOption:
        pass

    @staticmethod
    @abstractmethod
    def DataSource(*args, **kwargs) -> d.DataSource:
        pass
    
    def _raise_invalid_input_error(self, selection: str, more_details: str = '', e: Exception = None) -> None:
        raise u.InvalidInputError(f'Selected value "{selection}" is not valid for parameter "{self.name}". ' + more_details) from e
    
    @abstractmethod
    def with_selection(
        self, selection: Optional[str], user: Optional[User], parent_param: Optional[p._SelectionParameter], 
        *, request_version: Optional[int] = None
    ) -> p.Parameter:
        pass
    
    def _get_options_iterator(self, user: Optional[User], parent_param: Optional[p._SelectionParameter]) -> Iterator[po.ParameterOption]:
        user_group = self._get_user_group(user)
        selected_parent_option_ids = frozenset(parent_param._get_selected_ids_as_list()) if parent_param else None
        return (x for x in self.all_options if x._is_valid(user_group, selected_parent_option_ids))


@dataclass
class SelectionParameterConfig(ParameterConfig):
    """
    Abstract class for select parameter classes (single-select, multi-select, etc)
    """
    children: dict[str, ParameterConfigBase] = field(default_factory=dict, init=False, repr=False)
    trigger_refresh: bool = field(default=False, init=False)

    @abstractmethod
    def __init__(
        self, widget_type: str, name: str, label: str, all_options: Sequence[Union[po.SelectParameterOption, dict]], *, is_hidden: bool = False, 
        user_attribute: Optional[str] = None, parent_name: Optional[str] = None
    ) -> None:
        super().__init__(widget_type, name, label, all_options, is_hidden=is_hidden, user_attribute=user_attribute, 
                         parent_name=parent_name)
        self.children: dict[str, ParameterConfigBase] = dict()
        self.trigger_refresh = False

    @staticmethod
    def ParameterOption(*args, **kwargs):
        return po.SelectParameterOption(*args, **kwargs)
    
    def _add_child_mutate(self, child: ParameterConfigBase):
        self.children[child.name] = child
        self.trigger_refresh = True
    
    def _get_options(self, user: Optional[User], parent_param: Optional[p._SelectionParameter]) -> Sequence[po.SelectParameterOption]:
        return tuple(self._get_options_iterator(user, parent_param))
    
    def _get_default_ids_iterator(self, options: Sequence[po.SelectParameterOption]) -> Iterator[str]:
        return (x._identifier for x in options if x._is_default)
    
    def copy(self) -> SelectionParameterConfig:
        """
        Use for unit testing only
        """
        other = super().copy()
        other.children = self.children.copy()
        return other

    def to_json_dict0(self) -> dict:
        output = super().to_json_dict0()
        output['trigger_refresh'] = self.trigger_refresh
        return output


@dataclass
class SingleSelectParameterConfig(SelectionParameterConfig):
    """
    Class to define configurations for single-select parameter widgets.
    """
    
    def __init__(
        self, name: str, label: str, all_options: Sequence[Union[po.SelectParameterOption, dict]], *, is_hidden: bool = False, 
        user_attribute: Optional[str] = None, parent_name: Optional[str] = None
    ) -> None:
        super().__init__("single_select", name, label, all_options, is_hidden=is_hidden, user_attribute=user_attribute, 
                         parent_name=parent_name)
     
    @staticmethod
    def DataSource(*args, **kwargs):
        return d.SingleSelectDataSource(*args, **kwargs)
    
    def with_selection(
        self, selection: Optional[str], user: Optional[User], parent_param: Optional[p._SelectionParameter],
        *, request_version: Optional[int] = None
    ) -> p.SingleSelectParameter:
        options = self._get_options(user, parent_param)
        if selection is None:
            selected_id = next(self._get_default_ids_iterator(options), None)
            if selected_id is None and len(options) > 0:
                selected_id = options[0]._identifier
        else:
            selected_id = selection
        return p.SingleSelectParameter(self, options, selected_id)


@dataclass
class MultiSelectParameterConfig(SelectionParameterConfig):
    """
    Class to define configurations for multi-select parameter widgets.
    """
    show_select_all: bool # = field(default=True, kw_only=True)
    is_dropdown: bool # = field(default=True, kw_only=True)
    order_matters: bool # = field(default=False, kw_only=True)
    none_is_all: bool # = field(default=True, kw_only=True)

    def __init__(
        self, name: str, label: str, all_options: Sequence[Union[po.SelectParameterOption, dict]], *, show_select_all: bool = True, 
        is_dropdown: bool = True, order_matters: bool = False, none_is_all: bool = True, is_hidden: bool = False, 
        user_attribute: Optional[str] = None, parent_name: Optional[str] = None
    ) -> None:
        super().__init__("multi_select", name, label, all_options, is_hidden=is_hidden, user_attribute=user_attribute, 
                         parent_name=parent_name)
        self.show_select_all = show_select_all
        self.is_dropdown = is_dropdown
        self.order_matters = order_matters
        self.none_is_all = none_is_all
    
    @staticmethod
    def DataSource(*args, **kwargs):
        return d.MultiSelectDataSource(*args, **kwargs)
    
    def with_selection(
        self, selection: Optional[str], user: Optional[User], parent_param: Optional[p._SelectionParameter],
        *, request_version: Optional[int] = None
    ) -> p.MultiSelectParameter:
        options = self._get_options(user, parent_param)
        if selection is None:
            selected_ids = tuple(self._get_default_ids_iterator(options))
        else:
            selected_ids = u.load_json_or_comma_delimited_str_as_list(selection)
        return p.MultiSelectParameter(self, options, selected_ids)

    def to_json_dict0(self) -> dict:
        output = super().to_json_dict0()
        output['show_select_all'] = self.show_select_all
        output['is_dropdown'] = self.is_dropdown
        output['order_matters'] = self.order_matters
        return output


@dataclass
class _DateTypeParameterConfig(ParameterConfig):
    """
    Abstract class for date and date range parameter configs
    """

    @abstractmethod
    def __init__(
        self, widget_type: str, name: str, label: str, all_options: Sequence[Union[po.ParameterOption, dict]], *, is_hidden: bool = False, 
        user_attribute: Optional[str] = None, parent_name: Optional[str] = None
    ) -> None:
        super().__init__(widget_type, name, label, all_options, is_hidden=is_hidden, user_attribute=user_attribute, 
                         parent_name=parent_name)


@dataclass
class DateParameterConfig(_DateTypeParameterConfig):
    """
    Class to define configurations for single-select parameter widgets.
    """
    
    def __init__(
        self, name: str, label: str, all_options: Sequence[Union[po.DateParameterOption, dict]], *, is_hidden: bool = False, 
        user_attribute: Optional[str] = None, parent_name: Optional[str] = None
    ) -> None:
        super().__init__("date", name, label, all_options, is_hidden=is_hidden, user_attribute=user_attribute, 
                         parent_name=parent_name)

    @staticmethod
    def ParameterOption(*args, **kwargs):
        return po.DateParameterOption(*args, **kwargs)
    
    @staticmethod
    def DataSource(*args, **kwargs):
        return d.DateDataSource(*args, **kwargs)
    
    def with_selection(
        self, selection: Optional[str], user: Optional[User], parent_param: Optional[p._SelectionParameter],
        *, request_version: Optional[int] = None
    ) -> p.DateParameter:
        curr_option: po.DateParameterOption = next(self._get_options_iterator(user, parent_param))
        selected_date = curr_option._default_date if selection is None else selection
        return p.DateParameter(self, curr_option, selected_date)


@dataclass
class DateRangeParameterConfig(_DateTypeParameterConfig):
    """
    Class to define configurations for single-select parameter widgets.
    """
    
    def __init__(
        self, name: str, label: str, all_options: Sequence[Union[po.DateRangeParameterOption, dict]], *, is_hidden: bool = False, 
        user_attribute: Optional[str] = None, parent_name: Optional[str] = None
    ) -> None:
        super().__init__("date_range", name, label, all_options, is_hidden=is_hidden, user_attribute=user_attribute, 
                         parent_name=parent_name)

    @staticmethod
    def ParameterOption(*args, **kwargs):
        return po.DateRangeParameterOption(*args, **kwargs)
    
    @staticmethod
    def DataSource(*args, **kwargs):
        return d.DateRangeDataSource(*args, **kwargs)
    
    def with_selection(
        self, selection: Optional[str], user: Optional[User], parent_param: Optional[p._SelectionParameter],
        *, request_version: Optional[int] = None
    ) -> p.DateParameter:
        curr_option: po.DateRangeParameterOption = next(self._get_options_iterator(user, parent_param))
        if selection is None:
            selected_start_date = curr_option._default_start_date
            selected_end_date = curr_option._default_end_date
        else:
            try:
                selected_start_date, selected_end_date = u.load_json_or_comma_delimited_str_as_list(selection)
            except ValueError as e:
                self._raise_invalid_input_error(selection, "Date range parameter selection must be two dates joined by comma.", e)
        return p.DateRangeParameter(self, curr_option, selected_start_date, selected_end_date)


@dataclass
class _NumericParameterConfig(ParameterConfig):
    """
    Abstract class for number and number range parameter configs
    """

    @abstractmethod
    def __init__(
        self, widget_type: str, name: str, label: str, all_options: Sequence[Union[po.ParameterOption, dict]], *, is_hidden: bool = False, 
        user_attribute: Optional[str] = None, parent_name: Optional[str] = None
    ) -> None:
        super().__init__(widget_type, name, label, all_options, is_hidden=is_hidden, user_attribute=user_attribute, 
                         parent_name=parent_name)


@dataclass
class NumberParameterConfig(_NumericParameterConfig):
    """
    Class to define configurations for single-select parameter widgets.
    """
    
    def __init__(
        self, name: str, label: str, all_options: Sequence[Union[po.NumberParameterOption, dict]], *, is_hidden: bool = False, 
        user_attribute: Optional[str] = None, parent_name: Optional[str] = None
    ) -> None:
        super().__init__("number", name, label, all_options, is_hidden=is_hidden, user_attribute=user_attribute, 
                         parent_name=parent_name)

    @staticmethod
    def ParameterOption(*args, **kwargs):
        return po.NumberParameterOption(*args, **kwargs)
    
    @staticmethod
    def DataSource(*args, **kwargs):
        return d.NumberDataSource(*args, **kwargs)
    
    def with_selection(
        self, selection: Optional[str], user: Optional[User], parent_param: Optional[p._SelectionParameter],
        *, request_version: Optional[int] = None
    ) -> p.NumberParameter:
        curr_option: po.NumberParameterOption = next(self._get_options_iterator(user, parent_param))
        selected_value = curr_option._default_value if selection is None else selection
        return p.NumberParameter(self, curr_option, selected_value)


@dataclass
class NumberRangeParameterConfig(_NumericParameterConfig):
    """
    Class to define configurations for single-select parameter widgets.
    """
    
    def __init__(
        self, name: str, label: str, all_options: Sequence[Union[po.NumberRangeParameterOption, dict]], *, is_hidden: bool = False, 
        user_attribute: Optional[str] = None, parent_name: Optional[str] = None
    ) -> None:
        super().__init__("number_range", name, label, all_options, is_hidden=is_hidden, user_attribute=user_attribute, 
                         parent_name=parent_name)

    @staticmethod
    def ParameterOption(*args, **kwargs):
        return po.NumberRangeParameterOption(*args, **kwargs)
    
    @staticmethod
    def DataSource(*args, **kwargs):
        return d.NumberRangeDataSource(*args, **kwargs)
    
    def with_selection(
        self, selection: Optional[str], user: Optional[User], parent_param: Optional[p._SelectionParameter],
        *, request_version: Optional[int] = None
    ) -> p.NumberRangeParameter:
        curr_option: po.NumberRangeParameterOption = next(self._get_options_iterator(user, parent_param))
        if selection is None:
            selected_lower_value = curr_option._default_lower_value
            selected_upper_value = curr_option._default_upper_value
        else:
            try:
                selected_lower_value, selected_upper_value = u.load_json_or_comma_delimited_str_as_list(selection)
            except ValueError as e:
                self._raise_invalid_input_error(selection, "Number range parameter selection must be two numbers joined by comma.", e)
        return p.NumberRangeParameter(self, curr_option, selected_lower_value, selected_upper_value)


@dataclass
class DataSourceParameterConfig(ParameterConfigBase):
    """
    Class to define configurations for parameter widgets whose options come from lookup tables
    """
    parameter_type: Type[ParameterConfig]
    data_source: d.DataSource

    def __init__(
        self, parameter_type: Type[ParameterConfig], name: str, label: str, data_source: Union[d.DataSource, dict], *, 
        is_hidden: bool = False, user_attribute: Optional[str] = None, parent_name: Optional[str] = None
    ) -> None:
        super().__init__("data_source", name, label, is_hidden=is_hidden, user_attribute=user_attribute, parent_name=parent_name)
        self.parameter_type = parameter_type
        if isinstance(data_source, dict):
            data_source = parameter_type.DataSource(**data_source)
        self.data_source = data_source

    def convert(self, df: pd.DataFrame) -> ParameterConfig:
        return self.data_source._convert(self, df)
