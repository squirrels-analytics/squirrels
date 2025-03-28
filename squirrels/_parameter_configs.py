from __future__ import annotations
from typing import Generic, TypeVar, Annotated, Sequence, Iterator, Any
from typing_extensions import Self
from datetime import datetime
from dataclasses import dataclass, field
from abc import ABCMeta, abstractmethod
from copy import copy
from fastapi import Query
from pydantic.fields import Field
import polars as pl, re

from . import parameter_options as _po, parameters as p, data_sources as d, _utils as u, _constants as c
from ._exceptions import InvalidInputError
from ._auth import BaseUser
from ._connection_set import ConnectionSet
from ._seeds import Seeds

ParamOptionType = TypeVar("ParamOptionType", bound=_po.ParameterOption)


@dataclass
class APIParamFieldInfo:
    name: str
    type: type
    title: str = ""
    description: str = ""
    examples: list[Any] | None = None
    pattern: str | None = None
    min_length: int | None = None
    max_length: int | None = None
    default: Any = None
    
    def as_query_info(self):
        query_info = Query(
            title=self.title, description=self.description, examples=self.examples, pattern=self.pattern,
            min_length=self.min_length, max_length=self.max_length
        )
        return (self.name, Annotated[self.type, query_info], self.default)
    
    def as_body_info(self):
        field_info = Field(self.default,
            title=self.title, description=self.description, examples=self.examples, pattern=self.pattern,
            min_length=self.min_length, max_length=self.max_length
        )
        return (self.type, field_info)


@dataclass
class ParameterConfigBase(metaclass=ABCMeta):
    """
    Abstract class for all parameter classes
    """
    name: str
    label: str
    description: str = field(default="", kw_only=True)
    user_attribute: str | None = field(default=None, kw_only=True)
    parent_name: str | None = field(default=None, kw_only=True)

    def _get_user_group(self, user: BaseUser | None) -> Any:
        if self.user_attribute is not None:
            if user is None:
                raise u.ConfigurationError(f"Public datasets (accessible without authentication) cannot use parameter " +
                                           f"'{self.name}' because 'user_attribute' is defined on this parameter.")
            return getattr(user, self.user_attribute)
        
    def copy(self):
        """
        Use for unit testing only
        """
        return copy(self)


@dataclass
class ParameterConfig(Generic[ParamOptionType], ParameterConfigBase):
    """
    Abstract class for all parameter classes (except DataSourceParameters)
    """
    _all_options: Sequence[ParamOptionType] = field(repr=False)

    @abstractmethod
    def __init__(
        self, name: str, label: str, all_options: Sequence[ParamOptionType | dict], *, description: str = "", 
        user_attribute: str | None = None, parent_name: str | None = None
    ) -> None:
        super().__init__(name, label, description=description, user_attribute=user_attribute, parent_name=parent_name)
        self._all_options = tuple(self._to_param_option(x) for x in all_options)

    def _to_param_option(self, option: ParamOptionType | dict) -> ParamOptionType:
        return self.ParameterOption(**option) if isinstance(option, dict) else option

    @property
    def all_options(self) -> Sequence[ParamOptionType]:
        return self._all_options

    @staticmethod
    @abstractmethod
    def widget_type() -> str:
        pass

    @staticmethod
    @abstractmethod
    def ParameterOption(*args, **kwargs) -> ParamOptionType:
        pass

    @staticmethod
    @abstractmethod
    def DataSource(*args, **kwargs) -> d.DataSource:
        pass
    
    def _invalid_input_error(self, selection: str, more_details: str = '') -> InvalidInputError:
        return InvalidInputError(200, f'Selected value "{selection}" is not valid for parameter "{self.name}". ' + more_details)
    
    @abstractmethod
    def with_selection(
        self, selection: str | None, user: BaseUser | None, parent_param: p._SelectionParameter | None
    ) -> p.Parameter:
        pass
    
    def _get_options_iterator(
        self, all_options: Sequence[ParamOptionType], user: BaseUser | None, parent_param: p._SelectionParameter | None
    ) -> Iterator[ParamOptionType]:
        user_group = self._get_user_group(user)
        selected_parent_option_ids = frozenset(parent_param._get_selected_ids_as_list()) if parent_param else None
        return (x for x in all_options if x._is_valid(user_group, selected_parent_option_ids))
    
    @abstractmethod
    def get_api_field_info(self) -> APIParamFieldInfo:
        pass


@dataclass
class SelectionParameterConfig(ParameterConfig[_po.SelectParameterOption]):
    """
    Abstract class for select parameter classes (single-select, multi-select, etc)
    """
    children: dict[str, ParameterConfigBase] = field(default_factory=dict, init=False, repr=False)
    trigger_refresh: bool = field(default=False, init=False)

    @abstractmethod
    def __init__(
        self, name: str, label: str, all_options: Sequence[_po.SelectParameterOption | dict], *,
        description: str = "", user_attribute: str | None = None, parent_name: str | None = None
    ) -> None:
        super().__init__(name, label, all_options, description=description, user_attribute=user_attribute, parent_name=parent_name)
        self.children: dict[str, ParameterConfigBase] = dict()
        self.trigger_refresh = False

    @staticmethod
    def ParameterOption(*args, **kwargs):
        return _po.SelectParameterOption(*args, **kwargs)
    
    def _add_child_mutate(self, child: ParameterConfigBase):
        self.children[child.name] = child
        self.trigger_refresh = True
    
    def _get_options(self, user: BaseUser | None, parent_param: p._SelectionParameter | None) -> Sequence[_po.SelectParameterOption]:
        return tuple(self._get_options_iterator(self.all_options, user, parent_param))
    
    def _get_default_ids_iterator(self, options: Sequence[_po.SelectParameterOption]) -> Iterator[str]:
        return (x._identifier for x in options if x._is_default)
    
    def copy(self) -> Self:
        """
        Use for unit testing only
        """
        other = super().copy()
        other.children = self.children.copy()
        return other


@dataclass
class SingleSelectParameterConfig(SelectionParameterConfig):
    """
    Class to define configurations for single-select parameter widgets.
    """
    
    def __init__(
        self, name: str, label: str, all_options: Sequence[_po.SelectParameterOption | dict], *, description: str = "", 
        user_attribute: str | None = None, parent_name: str | None = None
    ) -> None:
        super().__init__(name, label, all_options, description=description, user_attribute=user_attribute, parent_name=parent_name)
    
    @staticmethod
    def widget_type() -> str:
        return "single_select"
     
    @staticmethod
    def DataSource(*args, **kwargs):
        return d.SelectDataSource(*args, **kwargs)
    
    def with_selection(
        self, selection: str | None, user: BaseUser | None, parent_param: p._SelectionParameter | None
    ) -> p.SingleSelectParameter:
        options = self._get_options(user, parent_param)
        if selection is None:
            selected_id = next(self._get_default_ids_iterator(options), None)
            if selected_id is None and len(options) > 0:
                selected_id = options[0]._identifier
        else:
            selected_id = selection
        return p.SingleSelectParameter(self, options, selected_id)
    
    def get_api_field_info(self) -> APIParamFieldInfo:
        examples = [x._identifier for x in self.all_options]
        return APIParamFieldInfo(
            self.name, str, title=self.label, description=self.description, examples=examples
        )


@dataclass
class MultiSelectParameterConfig(SelectionParameterConfig):
    """
    Class to define configurations for multi-select parameter widgets.
    """
    show_select_all: bool = field(default=True, kw_only=True)
    order_matters: bool = field(default=False, kw_only=True)
    none_is_all: bool = field(default=True, kw_only=True)

    def __init__(
        self, name: str, label: str, all_options: Sequence[_po.SelectParameterOption | dict], *, description: str = "", 
        show_select_all: bool = True, order_matters: bool = False, none_is_all: bool = True,
        user_attribute: str | None = None, parent_name: str | None = None
    ) -> None:
        super().__init__(
            name, label, all_options, description=description, user_attribute=user_attribute, parent_name=parent_name
        )
        self.show_select_all = show_select_all
        self.order_matters = order_matters
        self.none_is_all = none_is_all
    
    @staticmethod
    def widget_type() -> str:
        return "multi_select"
    
    @staticmethod
    def DataSource(*args, **kwargs):
        return d.SelectDataSource(*args, **kwargs)
    
    def with_selection(
        self, selection: str | None, user: BaseUser | None, parent_param: p._SelectionParameter | None
    ) -> p.MultiSelectParameter:
        options = self._get_options(user, parent_param)
        if selection is None:
            selected_ids = tuple(self._get_default_ids_iterator(options))
        else:
            selected_ids = u.load_json_or_comma_delimited_str_as_list(selection)
        return p.MultiSelectParameter(self, options, selected_ids)
    
    def get_api_field_info(self) -> APIParamFieldInfo:
        identifiers = [x._identifier for x in self.all_options]
        return APIParamFieldInfo(
            self.name, list[str], title=self.label, description=self.description, examples=[identifiers]
        )


@dataclass
class _DateTypeParameterConfig(ParameterConfig[ParamOptionType]):
    """
    Abstract class for date and date range parameter configs
    """

    @abstractmethod
    def __init__(
        self, name: str, label: str, all_options: Sequence[ParamOptionType | dict], *, description: str = "", 
        user_attribute: str | None = None, parent_name: str | None = None
    ) -> None:
        super().__init__(name, label, all_options, description=description, user_attribute=user_attribute, parent_name=parent_name)


@dataclass
class DateParameterConfig(_DateTypeParameterConfig[_po.DateParameterOption]):
    """
    Class to define configurations for date parameter widgets.
    """
    _all_options: Sequence[_po.DateParameterOption] = field(repr=False)
    
    def __init__(
        self, name: str, label: str, all_options: Sequence[_po.DateParameterOption | dict], *, 
        description: str = "", user_attribute: str | None = None, parent_name: str | None = None
    ) -> None:
        super().__init__(name, label, all_options, description=description, user_attribute=user_attribute, parent_name=parent_name)
    
    @staticmethod
    def widget_type() -> str:
        return "date"

    @staticmethod
    def ParameterOption(*args, **kwargs):
        return _po.DateParameterOption(*args, **kwargs)
    
    @staticmethod
    def DataSource(*args, **kwargs):
        return d.DateDataSource(*args, **kwargs)
    
    def with_selection(
        self, selection: str | None, user: BaseUser | None, parent_param: p._SelectionParameter | None
    ) -> p.DateParameter:
        curr_option: _po.DateParameterOption | None = next(self._get_options_iterator(self.all_options, user, parent_param), None)
        selected_date = curr_option._default_date if selection is None and curr_option is not None else selection
        return p.DateParameter(self, curr_option, selected_date)
    
    def get_api_field_info(self) -> APIParamFieldInfo:
        examples = [str(x._default_date) for x in self.all_options]
        return APIParamFieldInfo(
            self.name, str, title=self.label, description=self.description, examples=examples, pattern=c.DATE_REGEX
        )


@dataclass
class DateRangeParameterConfig(_DateTypeParameterConfig[_po.DateRangeParameterOption]):
    """
    Class to define configurations for date range parameter widgets.
    """
    _all_options: Sequence[_po.DateRangeParameterOption] = field(repr=False)
    
    def __init__(
        self, name: str, label: str, all_options: Sequence[_po.DateRangeParameterOption | dict], *,
        description: str = "", user_attribute: str | None = None, parent_name: str | None = None
    ) -> None:
        super().__init__(name, label, all_options, description=description, user_attribute=user_attribute, parent_name=parent_name)
    
    @staticmethod
    def widget_type() -> str:
        return "date_range"

    @staticmethod
    def ParameterOption(*args, **kwargs):
        return _po.DateRangeParameterOption(*args, **kwargs)
    
    @staticmethod
    def DataSource(*args, **kwargs):
        return d.DateRangeDataSource(*args, **kwargs)
    
    def with_selection(
        self, selection: str | None, user: BaseUser | None, parent_param: p._SelectionParameter | None
    ) -> p.DateRangeParameter:
        curr_option: _po.DateRangeParameterOption | None = next(self._get_options_iterator(self.all_options, user, parent_param), None)
        if selection is None:
            if curr_option is not None:
                selected_start_date = curr_option._default_start_date
                selected_end_date = curr_option._default_end_date
            else:
                selected_start_date, selected_end_date = None, None
        else:
            try:
                selected_start_date, selected_end_date = u.load_json_or_comma_delimited_str_as_list(selection)
            except ValueError:
                raise self._invalid_input_error(selection, "Date range parameter selection must be two dates.")
        return p.DateRangeParameter(self, curr_option, selected_start_date, selected_end_date)
    
    def get_api_field_info(self) -> APIParamFieldInfo:
        examples = [[str(x._default_start_date), str(x._default_end_date)] for x in self.all_options]
        return APIParamFieldInfo(
            self.name, list[str], title=self.label, description=self.description, examples=examples, max_length=2
        )


@dataclass
class _NumericParameterConfig(ParameterConfig[ParamOptionType]):
    """
    Abstract class for number and number range parameter configs
    """

    @abstractmethod
    def __init__(
        self, name: str, label: str, all_options: Sequence[ParamOptionType | dict], *, description: str = "", 
        user_attribute: str | None = None, parent_name: str | None = None
    ) -> None:
        super().__init__(name, label, all_options, description=description, user_attribute=user_attribute, parent_name=parent_name)


@dataclass
class NumberParameterConfig(_NumericParameterConfig[_po.NumberParameterOption]):
    """
    Class to define configurations for number parameter widgets.
    """
    _all_options: Sequence[_po.NumberParameterOption] = field(repr=False)
    
    def __init__(
        self, name: str, label: str, all_options: Sequence[_po.NumberParameterOption | dict], *,
        description: str = "", user_attribute: str | None = None, parent_name: str | None = None
    ) -> None:
        super().__init__(name, label, all_options, description=description, user_attribute=user_attribute, parent_name=parent_name)
    
    @staticmethod
    def widget_type() -> str:
        return "number"
    
    @staticmethod
    def ParameterOption(*args, **kwargs):
        return _po.NumberParameterOption(*args, **kwargs)
    
    @staticmethod
    def DataSource(*args, **kwargs):
        return d.NumberDataSource(*args, **kwargs)
    
    def with_selection(
        self, selection: str | None, user: BaseUser | None, parent_param: p._SelectionParameter | None
    ) -> p.NumberParameter:
        curr_option: _po.NumberParameterOption | None = next(self._get_options_iterator(self.all_options, user, parent_param), None)
        selected_value = curr_option._default_value if selection is None and curr_option is not None else selection
        return p.NumberParameter(self, curr_option, selected_value)
    
    def get_api_field_info(self) -> APIParamFieldInfo:
        examples = [x._default_value for x in self.all_options]
        return APIParamFieldInfo(
            self.name, float, title=self.label, description=self.description, examples=examples
        )


@dataclass
class NumberRangeParameterConfig(_NumericParameterConfig[_po.NumberRangeParameterOption]):
    """
    Class to define configurations for number range parameter widgets.
    """
    _all_options: Sequence[_po.NumberRangeParameterOption] = field(repr=False)
    
    def __init__(
        self, name: str, label: str, all_options: Sequence[_po.NumberRangeParameterOption | dict], *, 
        description: str = "", user_attribute: str | None = None, parent_name: str | None = None
    ) -> None:
        super().__init__(name, label, all_options, description=description, user_attribute=user_attribute, parent_name=parent_name)
    
    @staticmethod
    def widget_type() -> str:
        return "number_range"

    @staticmethod
    def ParameterOption(*args, **kwargs):
        return _po.NumberRangeParameterOption(*args, **kwargs)
    
    @staticmethod
    def DataSource(*args, **kwargs):
        return d.NumberRangeDataSource(*args, **kwargs)
    
    def with_selection(
        self, selection: str | None, user: BaseUser | None, parent_param: p._SelectionParameter | None
    ) -> p.NumberRangeParameter:
        curr_option: _po.NumberRangeParameterOption | None = next(self._get_options_iterator(self.all_options, user, parent_param), None)
        if selection is None:
            if curr_option is not None:
                selected_lower_value = curr_option._default_lower_value
                selected_upper_value = curr_option._default_upper_value
            else:
                selected_lower_value, selected_upper_value = None, None
        else:
            try:
                selected_lower_value, selected_upper_value = u.load_json_or_comma_delimited_str_as_list(selection)
            except ValueError:
                raise self._invalid_input_error(selection, "Number range parameter selection must be two numbers.")
        return p.NumberRangeParameter(self, curr_option, selected_lower_value, selected_upper_value)
    
    def get_api_field_info(self) -> APIParamFieldInfo:
        examples = [[x._default_lower_value, x._default_upper_value] for x in self.all_options]
        return APIParamFieldInfo(
            self.name, list[str], title=self.label, description=self.description, examples=examples, max_length=2
        )


@dataclass
class TextParameterConfig(ParameterConfig[_po.TextParameterOption]):
    """
    Class to define configurations for text parameter widgets.
    """
    _all_options: Sequence[_po.TextParameterOption] = field(repr=False)
    input_type: str
    
    def __init__(
        self, name: str, label: str, all_options: Sequence[_po.TextParameterOption | dict], *, description: str = "", 
        input_type: str = "text", user_attribute: str | None = None, parent_name: str | None = None
    ) -> None:
        super().__init__(name, label, all_options, description=description, user_attribute=user_attribute, parent_name=parent_name)
        
        allowed_input_types = ["text", "textarea", "number", "date", "datetime-local", "month", "time", "color", "password"]
        if input_type not in allowed_input_types:
            raise u.ConfigurationError(f"Invalid input type '{input_type}' for text parameter '{name}'. Must be one of {allowed_input_types}.")

        self.input_type = input_type
        for option in self._all_options:
            self.validate_entered_text(option._default_text)
    
    def validate_entered_text(self, entered_text: str) -> str:
        if self.input_type == "number":
            try:
                float(entered_text)
            except ValueError:
                raise self._invalid_input_error(entered_text, "Must be a number")
        elif self.input_type == "date":
            try:
                datetime.strptime(entered_text, "%Y-%m-%d")
            except ValueError:
                raise self._invalid_input_error(entered_text, "Must be a date in YYYY-MM-DD format")
        elif self.input_type == "datetime-local":
            try:
                datetime.strptime(entered_text, "%Y-%m-%dT%H:%M")
            except ValueError:
                raise self._invalid_input_error(entered_text, "Must be a date in YYYY-MM-DDThh:mm format (e.g. 2020-01-01T07:00)")
        elif self.input_type == "month":
            try:
                datetime.strptime(entered_text, "%Y-%m")
            except ValueError:
                raise self._invalid_input_error(entered_text, "Must be a date in YYYY-MM format")
        elif self.input_type == "time":
            try:
                datetime.strptime(entered_text, "%H:%M")
            except ValueError:
                raise self._invalid_input_error(entered_text, "Must be a time in hh:mm format.")
        elif self.input_type == "color":
            if not re.match(c.COLOR_REGEX, entered_text):
                raise self._invalid_input_error(entered_text, "Must be a valid color hex code (e.g. #000000).")
        
        return entered_text
    
    @staticmethod
    def widget_type() -> str:
        return "text"

    @staticmethod
    def ParameterOption(*args, **kwargs):
        return _po.TextParameterOption(*args, **kwargs)
    
    @staticmethod
    def DataSource(*args, **kwargs):
        return d.TextDataSource(*args, **kwargs)
    
    def with_selection(
        self, selection: str | None, user: BaseUser | None, parent_param: p._SelectionParameter | None
    ) -> p.TextParameter:
        curr_option: _po.TextParameterOption | None = next(self._get_options_iterator(self.all_options, user, parent_param), None)
        entered_text = curr_option._default_text if selection is None and curr_option is not None else selection
        return p.TextParameter(self, curr_option, entered_text)
    
    def get_api_field_info(self) -> APIParamFieldInfo:
        examples = [x._default_text for x in self.all_options]
        return APIParamFieldInfo(
            self.name, str, title=self.label, description=self.description, examples=examples
        )


ParamConfigType = TypeVar("ParamConfigType", bound=ParameterConfig)

class DataSourceParameterConfig(Generic[ParamConfigType], ParameterConfigBase):
    """
    Class to define configurations for parameter widgets whose options come from lookup tables
    """
    def __init__(
        self, parameter_type: type[ParamConfigType], name: str, label: str, data_source: d.DataSource | dict, *, 
        extra_args: dict = {}, description: str = "", user_attribute: str | None = None, parent_name: str | None = None
    ) -> None:
        super().__init__(name, label, description=description, user_attribute=user_attribute, parent_name=parent_name)
        self.parameter_type = parameter_type
        if isinstance(data_source, dict):
            data_source = parameter_type.DataSource(**data_source)
        self.data_source = data_source
        self.extra_args = extra_args

    def convert(self, df: pl.DataFrame) -> ParamConfigType:
        return self.data_source._convert(self, df)
    
    def get_dataframe(self, default_conn_name: str, conn_set: ConnectionSet, seeds: Seeds) -> pl.DataFrame:
        datasource = self.data_source
        query = datasource._get_query()
        if datasource._is_from_seeds:
            df = seeds.run_query(query)
        else:
            try:
                conn_name = datasource._get_connection_name(default_conn_name)
                df = conn_set.run_sql_query_from_conn_name(query, conn_name)
            except RuntimeError as e:
                raise u.ConfigurationError(f'Error executing query for datasource parameter "{self.name}"') from e
        return df
