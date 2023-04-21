from __future__ import annotations
import time, concurrent.futures
from dataclasses import dataclass
from collections import OrderedDict
from typing import List, Dict, Optional, Union, Type
from datetime import datetime
from enum import Enum
from decimal import Decimal
from squirrels import constants as c
from squirrels.db_conn import DbConnection
from squirrels.utils import timer


start = time.time()
import pandas as pd
timer.add_activity_time(c.IMPORT_PANDAS, start)


class WidgetType(Enum):
    SingleSelect = 1
    MultiSelect = 2
    DateField = 3
    NumberField = 4
    RangeField = 5


def _raiseParameterError(param_name: str, remaining_message: str, errorClass: Type[Exception] = RuntimeError):
    message = f'For parameter "{param_name}", {remaining_message}'
    raise errorClass(message)


@dataclass
class DataSource:
    table_or_query: str

    def get_query(self):
        if self.table_or_query.lower().startswith('select '):
            query = self.table_or_query
        else:
            query = f'SELECT * FROM {self.table_or_query}'
        return query
    
    def _convert(self, ds_param: DataSourceParameter, df: pd.DataFrame, from_sample: bool):
        raise RuntimeError(f'Must override "convert" method in all classes that override the "{self.__class__.__name__}" class')


@dataclass
class OptionsDataSource(DataSource):
    id_col: str
    options_col: str
    order_by_col: Optional[str]
    is_default_col: Optional[str]
    parent_id_col: Optional[str]
    is_cond_default_col: Optional[str]

    def __init__(self, table_or_query: str, id_col: str, options_col: str, order_by_col: Optional[str] = None, 
                is_default_col: Optional[str] = None, parent_id_col: Optional[str] = None, is_cond_default_col: Optional[str] = None):
        super().__init__(table_or_query)
        self.id_col = id_col 
        self.options_col = options_col
        self.order_by_col = id_col if order_by_col is None else order_by_col
        self.is_default_col = is_default_col
        self.parent_id_col = parent_id_col
        self.is_cond_default_col = is_cond_default_col

    def _convert(self, ds_param: DataSourceParameter, df: pd.DataFrame, from_sample: bool) -> Union[SingleSelectParameter, MultiSelectParameter]:
        id_col = 'id' if from_sample else self.id_col
        options_col = 'options' if from_sample else self.options_col
        order_by_col = 'ordering' if from_sample else self.order_by_col
        default_col = 'is_default' if from_sample else self.is_default_col
        parent_id_col = 'parent_id' if from_sample else self.parent_id_col
        cond_default_col = 'is_cond_default' if from_sample else self.is_cond_default_col
        
        def get_parent_id(row):
            if parent_id_col is not None and not pd.isnull(row[parent_id_col]):
                return str(row[parent_id_col])
            else:
                return None
        
        def is_cond_default(row):
            if cond_default_col is not None and not pd.isnull(row[cond_default_col]):
                return int(row[cond_default_col]) == 1
            else:
                return False
        
        df.sort_values(order_by_col, inplace=True)
        options = [ParameterOption(str(row[id_col]), str(row[options_col]), get_parent_id(row), is_cond_default(row)) for _, row in df.iterrows()]
        
        default_ids = []
        if default_col is not None:
            df_filtered = df.query(f'{default_col} == 1')
            default_ids = [str(x) for x in df_filtered[id_col].values.tolist()]
        
        if ds_param.widget_type == WidgetType.SingleSelect:
            default_id = default_ids[0] if len(default_ids) > 0 else None
            return SingleSelectParameter(ds_param.label, options, default_id=default_id, is_hidden=ds_param.is_hidden, 
                                         trigger_refresh=ds_param.trigger_refresh, parent=ds_param.parent)
        else:
            return MultiSelectParameter(ds_param.label, options, default_ids=default_ids, is_hidden=ds_param.is_hidden, 
                                         trigger_refresh=ds_param.trigger_refresh, parent=ds_param.parent)


@dataclass
class _NumericDataSource(DataSource):
    min_value_col: str
    max_value_col: str
    increment_col: str

    def _convert_helper(self, ds_param: DataSourceParameter, df: pd.DataFrame, from_sample: bool):
        min_value_col = 'min_value' if from_sample else self.min_value_col
        max_value_col = 'max_value' if from_sample else self.max_value_col
        increment_col = 'increment' if from_sample else self.increment_col
        return df[min_value_col].iloc[0], df[max_value_col].iloc[0], df[increment_col].iloc[0]


@dataclass
class NumberDataSource(_NumericDataSource):
    default_value_col: str

    def _convert(self, ds_param: DataSourceParameter, df: pd.DataFrame, from_sample: bool) -> NumberParameter:
        min_value, max_value, increment = self._convert_helper(ds_param, df, from_sample)
        default_value_col = 'default_value' if from_sample else self.default_value_col
        default_value = df[default_value_col].iloc[0]
        return NumberParameter(ds_param.label, min_value, max_value, increment, default_value, id_hidden=ds_param.is_hidden)


@dataclass
class RangeDataSource(_NumericDataSource):
    default_lower_value_col: str
    default_upper_value_col: str

    def _convert(self, ds_param: DataSourceParameter, df: pd.DataFrame, from_sample: bool) -> RangeParameter:
        min_value, max_value, increment = self._convert_helper(ds_param, df, from_sample)
        default_lower_value_col = 'default_lower_value' if from_sample else self.default_lower_value_col
        default_upper_value_col = 'default_upper_value' if from_sample else self.default_upper_value_col
        default_lower_value, default_upper_value = df[default_lower_value_col].iloc[0], df[default_upper_value_col].iloc[0]
        return RangeParameter(ds_param.label, min_value, max_value, increment, default_lower_value, default_upper_value, 
                              id_hidden=ds_param.is_hidden)


@dataclass
class DateDataSource(DataSource):
    default_date_col: str

    def _convert(self, ds_param: DataSourceParameter, df: pd.DataFrame, from_sample: bool) -> DateParameter:
        default_date_col = 'default_date' if from_sample else self.default_date_col
        selected_date = str(df[default_date_col].iloc[0])
        return DateParameter(ds_param.label, selected_date, is_hidden=ds_param.is_hidden)


@dataclass
class ParameterOption:
    identifier: str
    label: str
    parent_id: Optional[str] = None
    is_cond_default: bool = False

    def _to_dict(self):
        return {'id': self.identifier, 'label': self.label}


@dataclass
class Parameter:
    widget_type: WidgetType
    label: str
    is_hidden: bool

    def refresh(self, parameters: ParameterSet):
        pass # intentional, empty definition unless overwritten

    def set_selection(self, _: str):
        raise RuntimeError(f'Must override "set_selection" method in all classes that override the "{self.__class__.__name__}" class')

    def _to_dict(self, name):
        return {
            'widget_type': self.widget_type.name,
            'name': name,
            'label': self.label
        }


@dataclass
class _SelectionParameter(Parameter):
    options: List[ParameterOption]
    trigger_refresh: bool
    parent: Optional[str]

    def __post_init__(self):
        self.all_options = list(self.options)
        self.selected_parent_ids = set()

    def get_selected_ids_as_list(self):
        return []

    def refresh(self, parameters: ParameterSet):
        super().refresh(parameters)
        if self.parent is not None:
            parent_param: _SelectionParameter = parameters[self.parent]
            parent_param.trigger_refresh = True
            self.selected_parent_ids = set(parent_param.get_selected_ids_as_list())
            self.options = [x for x in self.all_options if x.parent_id in self.selected_parent_ids]

    def get_cond_default_iterator(self):
        return (x.identifier for x in self.options if x.is_cond_default)
    
    def is_selected_id_in_options(self, selected_id):
        return selected_id in [x.identifier for x in self.options]
    
    def verify_selected_id_in_options(self, selected_id):
        if not self.is_selected_id_in_options(selected_id):
            raise _raiseParameterError(self.label, f'the selected id "{selected_id}" is not selectable from options')
        
    def enquote(self, value):
        return "'" + value.replace("'", "''") + "'" 

    def _to_dict(self, name):
        output = super()._to_dict(name)
        output['options'] = [x._to_dict() for x in self.options]
        output['trigger_refresh'] = self.trigger_refresh
        return output


@dataclass
class SingleSelectParameter(_SelectionParameter):
    default_id: str
    selected_id: str

    def __init__(self, label: str, options: List[ParameterOption], *, default_id: Optional[str] = None, 
                 is_hidden: bool = False, trigger_refresh: bool = False, parent: Optional[str] = None):
        super().__init__(WidgetType.SingleSelect, label, is_hidden, options, trigger_refresh, parent)
        self.default_id = self.get_default_with_nullable_id(default_id)
        self.selected_id = self.default_id
        self.verify_selected_id_in_options(self.selected_id)
    
    def set_selection(self, selection: str):
        self.selected_id = selection if self.is_selected_id_in_options(selection) else self.default_id

    def get_selected(self) -> ParameterOption:
        return next(x for x in self.options if x.identifier == self.selected_id)
    
    def get_selected_id(self) -> str:
        return self.get_selected().identifier
    
    def get_selected_label_quoted(self) -> str:
        return self.enquote(self.get_selected().label)
    
    # Overriding for refresh method
    def get_selected_ids_as_list(self) -> List[str]:
        return [self.get_selected_id()]
    
    def get_default_with_nullable_id(self, default_id: str) -> str:
        return default_id if default_id is not None else self.options[0].identifier
    
    def refresh(self, parameters: ParameterSet):
        super().refresh(parameters)
        if self.parent is not None:
            default_id = next(self.get_cond_default_iterator(), None)
            self.default_id = self.get_default_with_nullable_id(default_id)
        self.selected_id = self.default_id

    def _to_dict(self, name):
        output = super()._to_dict(name)
        output['selected_id'] = self.selected_id
        return output


@dataclass
class MultiSelectParameter(_SelectionParameter):
    default_ids: List[str]
    selected_ids: List[str]
    include_all: bool
    order_matters: bool

    def __init__(self, label: str, options: List[ParameterOption], *, default_ids: List[str] = [], is_hidden = False,
                 trigger_refresh: bool = False, parent: Optional[str] = None, include_all: bool = True, order_matters: bool = False):
        super().__init__(WidgetType.MultiSelect, label, is_hidden, options, trigger_refresh, parent)
        self.default_ids = default_ids
        self.selected_ids = default_ids
        self.include_all = include_all
        self.order_matters = order_matters

    def set_selection(self, selection: str):
        selection_split = selection.split(',')
        self.selected_ids = [x for x in selection_split if self.is_selected_id_in_options(x)]
        if len(self.selected_ids) == 0:
            self.selected_ids = self.default_ids

    def get_selected_list(self) -> List[ParameterOption]:
        if len(self.selected_ids) == 0 and self.include_all:
            result = self.options
        else:
            result = [x for x in self.options if x.identifier in self.selected_ids]
        return result
    
    def get_selected_ids_as_list(self) -> List[str]:
        return [x.identifier for x in self.get_selected_list()]
    
    def get_selected_labels_quoted_as_list(self) -> List[str]:
        return [self.enquote(x.label) for x in self.get_selected_list()]
    
    def get_selected_ids(self) -> str:
        return ', '.join(self.get_selected_ids_as_list())
    
    def get_selected_labels_quoted(self) -> str:
        return ', '.join(self.get_selected_labels_quoted_as_list())
    
    def refresh(self, parameters: ParameterSet):
        super().refresh(parameters)
        if self.parent is not None:
            self.default_ids = list(self.get_cond_default_iterator())
        self.selected_ids = self.default_ids

    def _to_dict(self, name):
        output = super()._to_dict(name)
        output['selected_ids'] = self.selected_ids
        output['include_all'] = self.include_all
        output['order_matters'] = self.order_matters
        return output


@dataclass
class DateParameter(Parameter):
    selected_date: datetime
    format: str

    def __init__(self, label: str, selected_date: str, *, is_hidden: bool = False, format: str = '%Y-%m-%d'):
        super().__init__(WidgetType.DateField, label, is_hidden)
        self.format = format
        try:
            self.selected_date = datetime.strptime(selected_date, format)
        except ValueError:
            _raiseParameterError(label, f'the selected value "{selected_date}" could not be converted to a date')
    
    def set_selection(self, selection: str):
        self.selected_date = datetime.strptime(selection, self.format)

    def get_selected_date(self) -> str:
        return self.selected_date.strftime(self.format)

    def get_selected_date_quoted(self) -> str:
        return "'" + self.get_selected_date() + "'"
    
    def _to_dict(self, name):
        output = super()._to_dict(name)
        output['selected_date'] = self.get_selected_date()
        return output


@dataclass
class _NumericParameter(Parameter):
    min_value: Decimal
    max_value: Decimal
    increment: Decimal

    def __post_init__(self):
        self.min_value = Decimal(self.min_value)
        self.max_value = Decimal(self.max_value)
        self.increment = Decimal(self.increment)
        if self.min_value > self.max_value:
            _raiseParameterError(self.label, f'the min_value "{self.min_value}" must be less than the max_value "{self.max_value}"', ValueError)
        if (self.max_value - self.min_value) % self.increment != 0:
            _raiseParameterError(self.label, f'the increment "{self.increment}" must fit evenly between the min_value "{self.min_value}" and max_value "{self.max_value}"', ValueError)

    def value_in_range(self, value):
        return self.min_value <= value <= self.max_value
    
    def value_on_increment(self, value, min_value=None):
        min_value = self.min_value if min_value is None else min_value
        diff = (value - min_value)
        return diff >= 0 and diff % self.increment == 0

    def validate_value(self, value, min_value=None):
        if not self.value_in_range(value):
            _raiseParameterError(self.label, f'the selected value "{value}" is out of bounds', ValueError)
        if not self.value_on_increment(value, min_value):
            _raiseParameterError(self.label, f'the difference between selected value "{value}" and min_value "{self.min_value}" must be a multiple of increment "{self.increment}"', ValueError)

    def _to_dict(self, name):
        output = super()._to_dict(name)
        output['min_value'] = str(self.min_value)
        output['max_value'] = str(self.max_value)
        output['increment'] = str(self.increment)
        return output


@dataclass
class NumberParameter(_NumericParameter):
    default_value: Decimal
    selected_value: Decimal

    def __init__(self, label: str, min_value: Union[Decimal, int, str], max_value: Union[Decimal, int, str], 
                 increment: Union[Decimal, int, str], default_value: Union[Decimal, int, str], *, is_hidden: bool = False):
        super().__init__(WidgetType.NumberField, label, is_hidden, min_value, max_value, increment)
        self.default_value = Decimal(default_value)
        self.selected_value = self.default_value
        self.validate_value(default_value)
    
    def set_selection(self, selection: str):
        selection_decimal = Decimal(selection)
        self.selected_value = selection_decimal if self.value_in_range(selection_decimal) and self.value_on_increment(selection_decimal) else self.default_value

    def get_selected_value(self) -> str:
        return str(self.selected_value)
        
    def _to_dict(self, name):
        output = super()._to_dict(name)
        output['selected_value'] = self.get_selected_value()
        return output


@dataclass
class RangeParameter(_NumericParameter):
    default_lower_value: Decimal
    default_upper_value: Decimal
    selected_lower_value: Decimal
    selected_upper_value: Decimal

    def __init__(self, label: str, min_value: Union[Decimal, int, str], max_value: Union[Decimal, int, str], increment: Union[Decimal, int, str], 
                 default_lower_value: Union[Decimal, int, str], default_upper_value: Union[Decimal, int, str], *, is_hidden: bool = False):
        super().__init__(WidgetType.RangeField, label, is_hidden, min_value, max_value, increment)
        self.default_lower_value = Decimal(default_lower_value)
        self.default_upper_value = Decimal(default_upper_value)
        self.selected_lower_value = self.default_lower_value
        self.selected_upper_value = self.default_upper_value
        self.validate_value(default_lower_value)
        self.validate_value(default_upper_value, default_lower_value)
    
    def set_selection(self, selection: str):
        lower, upper = [Decimal(x) for x in selection.split(',')]
        if self.value_in_range(lower) and self.value_in_range(upper) and self.value_on_increment(lower) and self.value_on_increment(upper, lower):
            self.selected_lower_value = lower
            self.selected_upper_value = upper
        else:
            self.selected_lower_value = self.default_lower_value
            self.selected_upper_value = self.default_upper_value

    def get_selected_lower_value(self) -> str:
        return str(self.selected_lower_value)

    def get_selected_upper_value(self) -> str:
        return str(self.selected_upper_value)

    def _to_dict(self, name):
        output = super()._to_dict(name)
        output['selected_lower_value'] = self.get_selected_lower_value()
        output['selected_upper_value'] = self.get_selected_upper_value()
        return output


@dataclass
class DataSourceParameter(Parameter):
    data_source: OptionsDataSource
    trigger_refresh: bool
    parent: Optional[str]

    def __init__(self, widget_type: str, label: str, data_source: OptionsDataSource, *, 
                 is_hidden: bool = False, trigger_refresh: bool = False, parent: Optional[str] = None):
        super().__init__(widget_type, label, is_hidden)
        self.data_source = data_source
        self.trigger_refresh = trigger_refresh
        self.parent = parent

    def _convert(self, profile_name: str, df: pd.DataFrame = None) -> Parameter:
        from_sample = (df is not None)
        if not from_sample:
            conn = DbConnection(profile_name)
            df = conn.get_dataframe_from_query(self.data_source.get_query())
        
        return self.data_source._convert(self, df, from_sample)
    
    def _to_dict(self, name):
        output = super()._to_dict(name)
        output['data_source'] = self.data_source.__dict__
        return output


class ParameterSet:
    def __init__(self, parameters: Dict[str, Parameter]):
        self._parameters_dict: OrderedDict[str, Parameter] = OrderedDict()
        self._data_source_params: OrderedDict[str, DataSourceParameter] = OrderedDict()
        for key, param in parameters.items():
            self._parameters_dict[key] = param
            if isinstance(param, DataSourceParameter):
                self._data_source_params[key] = param
            param.refresh(self)

    def get_parameter_by_name(self, param_name: str) -> Parameter:
        if param_name in self._parameters_dict:
            return self._parameters_dict[param_name]
        else:
            raise KeyError(f'No such parameter exists called "{param_name}" (yet)')
    
    def __getitem__(self, param_name: str) -> Parameter:
        return self.get_parameter_by_name(param_name)

    def _convert_datasource_params(self, profile_name: str, test_file: str = None):
        df_all = pd.read_csv(test_file) if test_file is not None else None
        
        def convert(item):
            key, ds_param = item
            df = df_all.query(f'parameter == "{key}"') if df_all is not None else None
            return key, ds_param._convert(profile_name, df)

        with concurrent.futures.ThreadPoolExecutor() as executor:
            converted_params = list(executor.map(convert, self._data_source_params.items()))
            for key, parameter in converted_params:
                self._parameters_dict[key] = parameter
                parameter.refresh(self)
        self._data_source_params.clear()
    
    def _to_dict(self, debug: bool = False):
        output = {'parameters': [x._to_dict(key) for key, x in self._parameters_dict.items() if not x.is_hidden or debug]}
        return output
