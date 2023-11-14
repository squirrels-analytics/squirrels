from __future__ import annotations
from typing import Type, Optional, Sequence, Iterator, Dict, Set, Tuple
from collections import OrderedDict
from dataclasses import dataclass, field
from abc import ABCMeta, abstractmethod
from datetime import datetime
import concurrent.futures

from . import parameter_options as po, parameters as p, data_sources as d, _utils as u, _constants as c
from ._timed_imports import pandas as pd
from ._authenticator import UserBase
from ._manifest import ManifestIO
from ._connection_set_io import ConnectionSetIO


@dataclass
class ParameterConfigBase(metaclass=ABCMeta):
    """
    Abstract class for all parameter classes
    """
    name: str
    label: str
    is_hidden: bool
    user_attribute: Optional[str]
    parent_name: Optional[str]

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

    def _get_user_group(self, user: Optional[UserBase]) -> Optional[str]:
        if self.user_attribute is not None:
            if user is None:
                raise u.ConfigurationError("Public datasets with non-authenticated users cannot use parameter named " +
                                           f"'{self.name}' because 'user_attribute' is defined on the parameter.")
            return getattr(user, self.user_attribute)

    def to_json_dict(self) -> Dict:
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
    all_options: Sequence[po.ParameterOption]

    @abstractmethod
    def __init__(
        self, widget_type: str, name: str, label: str, all_options: Sequence[po.ParameterOption], *, is_hidden: bool = False, 
        user_attribute: Optional[str] = None, parent_name: Optional[str] = None
    ) -> None:
        super().__init__(widget_type, name, label, is_hidden=is_hidden, user_attribute=user_attribute, 
                         parent_name=parent_name)
        self.all_options = tuple(all_options)
    
    def _raise_invalid_input_error(self, selection: str, more_details: str = '', e: Exception = None) -> None:
        raise u.InvalidInputError(f'Selected value "{selection}" is not valid for parameter "{self.name}". ' + more_details) from e
    
    @abstractmethod
    def with_selection(self, selection: Optional[str], user: Optional[UserBase], parent_param: Optional[p._SelectionParameter]) -> p.Parameter:
        pass
    
    def _get_options_iterator(self, user: Optional[UserBase], parent_param: Optional[p._SelectionParameter]) -> Iterator[po.ParameterOption]:
        user_group = self._get_user_group(user)
        selected_parent_option_ids = frozenset(parent_param._get_selected_ids_as_list()) if parent_param else None
        return (x for x in self.all_options if x._is_valid(user_group, selected_parent_option_ids))


@dataclass
class SelectionParameterConfig(ParameterConfig):
    """
    Abstract class for select parameter classes (single-select, multi-select, etc)
    """
    trigger_refresh: bool

    @abstractmethod
    def __init__(
        self, widget_type: str, name: str, label: str, all_options: Sequence[po.SelectParameterOption], *, is_hidden: bool = False, 
        user_attribute: Optional[str] = None, parent_name: Optional[str] = None
    ) -> None:
        super().__init__(widget_type, name, label, all_options, is_hidden=is_hidden, user_attribute=user_attribute, 
                         parent_name=parent_name)
        self.children: Dict[str, ParameterConfigBase] = dict()
        self.trigger_refresh = False
    
    def _add_child_mutate(self, child: ParameterConfigBase):
        self.children[child.name] = child
        self.trigger_refresh = True
    
    def _get_options(self, user: Optional[UserBase], parent_param: Optional[p._SelectionParameter]) -> Sequence[po.SelectParameterOption]:
        return tuple(self._get_options_iterator(user, parent_param))
    
    def _get_default_ids_iterator(self, options: Sequence[po.SelectParameterOption]) -> Iterator[str]:
        return (x.identifier for x in options if x.is_default)

    def to_json_dict(self) -> Dict:
        output = super().to_json_dict()
        output['trigger_refresh'] = self.trigger_refresh
        return output


@dataclass
class SingleSelectParameterConfig(SelectionParameterConfig):
    """
    Class to define configurations for single-select parameter widgets.
    """
    
    def __init__(
        self, name: str, label: str, all_options: Sequence[po.SelectParameterOption], *, is_hidden: bool = False, 
        user_attribute: Optional[str] = None, parent_name: Optional[str] = None
    ) -> None:
        super().__init__("SingleSelectParameter", name, label, all_options, is_hidden=is_hidden, user_attribute=user_attribute, 
                         parent_name=parent_name)
    
    def with_selection(
        self, selection: Optional[str], user: Optional[UserBase], parent_param: Optional[p._SelectionParameter]
    ) -> p.SingleSelectParameter:
        options = self._get_options(user, parent_param)
        if selection is None:
            selected_id = next(self._get_default_ids_iterator(options), None)
            if selected_id is None and len(options) > 0:
                selected_id = options[0].identifier
        else:
            selected_id = selection
        return p.SingleSelectParameter(self, options, selected_id)


@dataclass
class MultiSelectParameterConfig(SelectionParameterConfig):
    """
    Class to define configurations for multi-select parameter widgets.
    """
    include_all: bool 
    order_matters: bool

    def __init__(
        self, name: str, label: str, all_options: Sequence[po.SelectParameterOption], *, include_all: bool = True, 
        order_matters: bool = False, is_hidden: bool = False, user_attribute: Optional[str] = None, parent_name: Optional[str] = None
    ) -> None:
        super().__init__("MultiSelectParameter", name, label, all_options, is_hidden=is_hidden, user_attribute=user_attribute, 
                         parent_name=parent_name)
        self.include_all = include_all
        self.order_matters = order_matters
    
    def with_selection(
        self, selection: Optional[str], user: Optional[UserBase], parent_param: Optional[p._SelectionParameter]
    ) -> p.MultiSelectParameter:
        options = self._get_options(user, parent_param)
        if selection is None:
            selected_ids = tuple(self._get_default_ids_iterator(options))
        else:
            selected_ids = u.load_json_or_comma_delimited_str_as_list(selection)
        return p.MultiSelectParameter(self, options, selected_ids)

    def to_json_dict(self) -> Dict:
        output = super().to_json_dict()
        output['include_all'] = self.include_all
        output['order_matters'] = self.order_matters
        return output


@dataclass
class _DateTypeParameterConfig(ParameterConfig):
    """
    Abstract class for date and date range parameter configs
    """

    @abstractmethod
    def __init__(
        self, widget_type: str, name: str, label: str, all_options: Sequence[po.ParameterOption], *, is_hidden: bool = False, 
        user_attribute: Optional[str] = None, parent_name: Optional[str] = None
    ) -> None:
        super().__init__(widget_type, name, label, all_options, is_hidden=is_hidden, user_attribute=user_attribute, 
                         parent_name=parent_name)
    
    def _get_selected_date(self, selection: str) -> datetime:
        try:
            return datetime.strptime(selection, "%Y-%m-%d")
        except ValueError as e:
            self._raise_invalid_input_error(selection, 'Invalid selection for date.', e)


@dataclass
class DateParameterConfig(_DateTypeParameterConfig):
    """
    Class to define configurations for single-select parameter widgets.
    """
    
    def __init__(
        self, name: str, label: str, all_options: Sequence[po.DateParameterOption], *, is_hidden: bool = False, 
        user_attribute: Optional[str] = None, parent_name: Optional[str] = None
    ) -> None:
        super().__init__("DateParameter", name, label, all_options, is_hidden=is_hidden, user_attribute=user_attribute, 
                         parent_name=parent_name)
    
    def with_selection(
        self, selection: Optional[str], user: Optional[UserBase], parent_param: Optional[p._SelectionParameter]
    ) -> p.DateParameter:
        curr_option: po.DateParameterOption = next(self._get_options_iterator(user, parent_param))
        if selection is None:
            selected_date = curr_option.default_date
        else:
            selected_date = self._get_selected_date(selection)
        return p.DateParameter(self, curr_option, selected_date)


@dataclass
class DateRangeParameterConfig(_DateTypeParameterConfig):
    """
    Class to define configurations for single-select parameter widgets.
    """
    
    def __init__(
        self, name: str, label: str, all_options: Sequence[po.DateRangeParameterOption], *, is_hidden: bool = False, 
        user_attribute: Optional[str] = None, parent_name: Optional[str] = None
    ) -> None:
        super().__init__("DateRangeParameter", name, label, all_options, is_hidden=is_hidden, user_attribute=user_attribute, 
                         parent_name=parent_name)
    
    def with_selection(
        self, selection: Optional[str], user: Optional[UserBase], parent_param: Optional[p._SelectionParameter]
    ) -> p.DateParameter:
        curr_option: po.DateRangeParameterOption = next(self._get_options_iterator(user, parent_param))
        if selection is None:
            selected_start_date = curr_option.default_start_date
            selected_end_date = curr_option.default_end_date
        else:
            try:
                lower, upper = selection.split(',')
            except ValueError as e:
                self._raise_invalid_input_error(selection, "Date range parameter selection must be two dates joined by comma.", e)
            selected_start_date = self._get_selected_date(lower)
            selected_end_date = self._get_selected_date(upper)
        return p.DateRangeParameter(self, curr_option, selected_start_date, selected_end_date)


@dataclass
class _NumericParameterConfig(ParameterConfig):
    """
    Abstract class for number and number range parameter configs
    """

    @abstractmethod
    def __init__(
        self, widget_type: str, name: str, label: str, all_options: Sequence[po.ParameterOption], *, is_hidden: bool = False, 
        user_attribute: Optional[str] = None, parent_name: Optional[str] = None
    ) -> None:
        super().__init__(widget_type, name, label, all_options, is_hidden=is_hidden, user_attribute=user_attribute, 
                         parent_name=parent_name)
    
    def _get_selected_num(self, selection: str, curr_option: po._NumericParameterOption) -> datetime:
        try:
            return curr_option._validate_value(selection)
        except ValueError as e:
            self._raise_invalid_input_error(selection, 'Invalid selection for date.', e)


@dataclass
class NumberParameterConfig(_NumericParameterConfig):
    """
    Class to define configurations for single-select parameter widgets.
    """
    
    def __init__(
        self, name: str, label: str, all_options: Sequence[po.NumberParameterOption], *, is_hidden: bool = False, 
        user_attribute: Optional[str] = None, parent_name: Optional[str] = None
    ) -> None:
        super().__init__("NumberParameter", name, label, all_options, is_hidden=is_hidden, user_attribute=user_attribute, 
                         parent_name=parent_name)
    
    def with_selection(
        self, selection: Optional[str], user: Optional[UserBase], parent_param: Optional[p._SelectionParameter]
    ) -> p.NumberParameter:
        curr_option: po.NumberParameterOption = next(self._get_options_iterator(user, parent_param))
        if selection is None:
            selected_value = curr_option.default_value
        else:
            selected_value = self._get_selected_num(selection, curr_option)
        return p.NumberParameter(self, curr_option, selected_value)


@dataclass
class NumRangeParameterConfig(_NumericParameterConfig):
    """
    Class to define configurations for single-select parameter widgets.
    """
    
    def __init__(
        self, name: str, label: str, all_options: Sequence[po.NumRangeParameterOption], *, is_hidden: bool = False, 
        user_attribute: Optional[str] = None, parent_name: Optional[str] = None
    ) -> None:
        super().__init__("NumRangeParameter", name, label, all_options, is_hidden=is_hidden, user_attribute=user_attribute, 
                         parent_name=parent_name)
    
    def with_selection(
        self, selection: Optional[str], user: Optional[UserBase], parent_param: Optional[p._SelectionParameter]
    ) -> p.NumRangeParameter:
        curr_option: po.NumRangeParameterOption = next(self._get_options_iterator(user, parent_param))
        if selection is None:
            selected_lower_value = curr_option.default_lower_value
            selected_upper_value = curr_option.default_upper_value
        else:
            try:
                lower, upper = selection.split(',')
            except ValueError as e:
                self._raise_invalid_input_error(selection, "Number range parameter selection must be two numbers joined by comma.", e)
            selected_lower_value = self._get_selected_num(lower)
            selected_upper_value = self._get_selected_num(upper)
        return p.NumRangeParameter(self, curr_option, selected_lower_value, selected_upper_value)


@dataclass
class DataSourceParameterConfig(ParameterConfigBase):
    """
    Class to define configurations for parameter widgets whose options come from lookup tables
    """
    parameter_class: Type[p.Parameter]
    data_source: d.DataSource

    def __init__(
        self, parameter_class: Type[p.Parameter], name: str, label: str, data_source: d.DataSource, *, is_hidden: bool = False, 
        user_attribute: Optional[str] = None, parent_name: Optional[str] = None
    ) -> None:
        super().__init__("DataSourceParameter", name, label, is_hidden=is_hidden, user_attribute=user_attribute, parent_name=parent_name)
        self.parameter_class = parameter_class
        self.data_source = data_source

    def convert(self, df: pd.DataFrame) -> ParameterConfig:
        return self.data_source._convert(self, df)


@dataclass
class ParameterSet:
    """
    A wrapper class for a sequence of parameters with the selections applied as well
    """
    _parameters_dict: OrderedDict[str, p.Parameter]

    def get_parameters_as_dict(self) -> Dict[str, p.Parameter]:
        return self._parameters_dict.copy()

    def to_json_dict(self, *, debug: bool = False) -> Dict:
        parameters = []
        for x in self._parameters_dict.values():
            if not x.config.is_hidden or debug:
                parameters.append(x.to_json_dict())
        
        output = {
            "response_version": 0, 
            "parameters": parameters
        }
        return output


@dataclass
class _ParameterConfigsSet:
    """
    Pool of parameter configs, can create multiple for unit testing purposes
    """
    _data: Dict[str, ParameterConfigBase] = field(default_factory=OrderedDict)
    _data_source_param_names: Set[str] = field(default_factory=set)
        
    def get(self, name: Optional[str]) -> Optional[ParameterConfigBase]:
        return self._data[name] if name is not None else None

    def add(self, param_config: ParameterConfigBase) -> None:
        self._data[param_config.name] = param_config
        if isinstance(param_config, DataSourceParameterConfig):
            self._data_source_param_names.add(param_config.name)
    
    def _get_all_ds_param_configs(self) -> Dict[str, DataSourceParameterConfig]:
        return {key: self._data[key] for key in self._data_source_param_names}

    def __convert_datasource_params(self, df_dict: Dict[str, pd.DataFrame]) -> None:
        done = set()
        for curr_name in self._data_source_param_names:
            stack = [curr_name] # Note: parents must be converted first before children
            while stack:
                name = stack[-1]
                if name not in done:
                    ds_param: DataSourceParameterConfig = self._data[name]
                    parent_name = ds_param.parent_name
                    if parent_name is not None and parent_name not in done:
                        stack.append(parent_name)
                        continue
                    if name not in df_dict:
                        raise u.ConfigurationError('No reference data found for parameter "{name}"')
                    self._data[name] = ds_param.convert(df_dict[name])
                    done.add(name)
                stack.pop()
    
    def __validate_param_relationships(self) -> None:
        for param_config in self._data.values():
            assert isinstance(param_config, ParameterConfig)
            parent_name = param_config.parent_name
            parent = self._data.get(parent_name)
            if parent:
                if not isinstance(param_config, SelectionParameterConfig):
                    if not isinstance(parent, SingleSelectParameterConfig):
                        raise u.ConfigurationError(f'Parameter "{parent_name}" must be a single-select parameter ' +
                                                   "since it's a parent of a non-selection parameter")
                    seen = set()
                    for option in param_config.all_options:
                        assert isinstance(option, po.SelectParameterOption)
                        if not seen.isdisjoint(option.parent_option_ids):
                            raise u.ConfigurationError(f'Since parameter "{parent_name}" is a parent of non-selection parameter ' +
                                                       f'"{param_config.name}", the parent option {option._to_json_dict()} cannot have ' +
                                                       'multiple children options refering to it')
                        seen.update(option.parent_option_ids)
                
                if not isinstance(parent, SelectionParameterConfig):
                    raise u.ConfigurationError(f'Parameter "{parent_name}" must be a selection parameter since it is a parent')
                
                parent._add_child_mutate(param_config)
    
    def _post_process_params(self, df_dict: Dict[str, pd.DataFrame]) -> None:
        self.__convert_datasource_params(df_dict)
        self.__validate_param_relationships()
    
    def apply_selections(
        self, dataset_params: Optional[Sequence[str]], selections: Dict[str, str], user: Optional[UserBase], *, updates_only: bool = False
    ) -> ParameterSet:
        if dataset_params is None:
            dataset_params = self._data.keys()
        
        parameters_by_name: Dict[str, p.Parameter] = {}
        params_to_process = selections.keys() if selections and updates_only else dataset_params
        params_to_process_set = set(params_to_process)
        for some_name in params_to_process:
            stack = [some_name] # Note: process parent selections first (if applicable) before children
            while stack:
                curr_name = stack[-1]
                children = []
                if curr_name not in parameters_by_name:
                    param_conf: ParameterConfig = self._data[curr_name]
                    parent_name = param_conf.parent_name
                    if parent_name is not None:
                        if parent_name in params_to_process_set and parent_name not in parameters_by_name:
                            stack.append(parent_name)
                            continue
                        parent = parameters_by_name.get(parent_name)
                    else:
                        parent = None
                    param = param_conf.with_selection(selections.get(curr_name), user, parent)
                    parameters_by_name[curr_name] = param
                    if isinstance(param_conf, SelectionParameterConfig):
                        children = list(param_conf.children.keys())
                stack.pop()
                stack.extend(children)
        
        ordered_parameters = OrderedDict((key, parameters_by_name[key]) for key in dataset_params if key in parameters_by_name)
        return ParameterSet(ordered_parameters)


class ParameterConfigsSetIO:
    """
    Static class for the singleton object of __ParameterConfigsPoolData
    """
    obj = _ParameterConfigsSet()
    
    @classmethod
    def _GetDfDict(cls, excel_file_name: Optional[str]) -> Dict[str, pd.DataFrame]:
        if excel_file_name:
            excel_file = pd.ExcelFile(excel_file_name)
            df_dict = pd.read_excel(excel_file, None)
        else:
            def get_dataframe_from_query(item: Tuple[str, DataSourceParameterConfig]) -> pd.DataFrame:
                key, ds_param_config = item
                datasource = ds_param_config.data_source
                df = ConnectionSetIO.obj.get_dataframe_from_query(datasource.connection_name, datasource.get_query())
                return key, df
            
            ds_param_configs = cls.obj._get_all_ds_param_configs()
            with concurrent.futures.ThreadPoolExecutor() as executor:
                df_dict = dict(executor.map(get_dataframe_from_query, ds_param_configs.items()))
        
        return df_dict
    
    @classmethod
    def LoadFromFile(cls, *, excel_file_name: Optional[str] = None) -> None:
        proj_vars = ManifestIO.obj.get_proj_vars()
        u.run_module_main(c.PARAMETERS_FILE, {"proj": proj_vars})
        df_dict = cls._GetDfDict(excel_file_name)
        cls.obj._post_process_params(df_dict)
