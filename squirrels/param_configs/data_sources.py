from __future__ import annotations
from typing import Tuple, Dict, Optional
from dataclasses import dataclass

from squirrels.param_configs import parameters as p, parameter_options as po
from squirrels.timed_imports import pandas as pd
from squirrels import utils


@dataclass
class DataSource:
    connection_name: str
    table_or_query: str
    
    def __post_init__(self) -> None:
        if not hasattr(self, 'parent_id_col'):
            self.parent_id_col = None

    def get_query(self):
        if self.table_or_query.lower().startswith('select '):
            query = self.table_or_query
        else:
            query = f'SELECT * FROM {self.table_or_query}'
        return query
    
    def convert(self, ds_param: DataSourceParameter, df: pd.DataFrame) -> p.Parameter:
        raise NotImplementedError(f'Must override the "convert" method')
    
    def _get_parent(self, row):
        return str(utils.get_row_value(row, self.parent_id_col)) if self.parent_id_col is not None else None


@dataclass
class SelectionDataSource(DataSource):
    id_col: str
    options_col: str
    order_by_col: str = None
    is_default_col: Optional[str] = None
    parent_id_col: Optional[str] = None

    def __post_init__(self):
        self.order_by_col = self.order_by_col if self.order_by_col is not None else self.id_col

    def convert(self, ds_param: DataSourceParameter, df: pd.DataFrame) -> p.Parameter:
        def is_default(row):
            return int(utils.get_row_value(row, self.is_default_col)) == 1 if self.is_default_col is not None else False
        
        try:
            df.sort_values(self.order_by_col, inplace=True)
        except KeyError as e:
            raise utils.ConfigurationError(f'Could not sort on column name "{self.order_by_col}" as it does not exist')
        
        options = tuple(
            po.SelectParameterOption(str(utils.get_row_value(row, self.id_col)), str(utils.get_row_value(row, self.options_col)), is_default(row), 
                                     parent_option_id=self._get_parent(row))
            for _, row in df.iterrows()
        )
        
        if ds_param.widget_type == p.WidgetType.SingleSelect:
            return p.SingleSelectParameter(ds_param.name, ds_param.label, options, 
                                           is_hidden=ds_param.is_hidden, parent=ds_param.parent)
        else:
            return p.MultiSelectParameter(ds_param.name, ds_param.label, options, 
                                          is_hidden=ds_param.is_hidden, parent=ds_param.parent)


@dataclass
class DateDataSource(DataSource):
    default_date_col: str
    format_col: str = None
    parent_id_col: Optional[str] = None

    def convert(self, ds_param: DataSourceParameter, df: pd.DataFrame) -> p.DateParameter:
        def get_format(row: pd.Series) -> str:
            return str(utils.get_row_value(row, self.format_col)) if self.format_col is not None else '%Y-%m-%d'
        
        def get_date(row: pd.Series) -> str:
            return str(utils.get_row_value(row, self.default_date_col))
        
        def create_date_param_option(row: pd.Series) -> po.DateParameterOption:
            return po.DateParameterOption(get_date(row), get_format(row), self._get_parent(row))
        
        if ds_param.parent is None:
            row = df.iloc[0]
            return p.DateParameter(ds_param.name, ds_param.label, get_date(row), get_format(row), 
                                   is_hidden=ds_param.is_hidden)
        else:
            all_options = tuple(create_date_param_option(row) for _, row in df.iterrows())
            return p.DateParameter.WithParent(ds_param.name, ds_param.label, all_options, ds_param.parent,
                                              is_hidden=ds_param.is_hidden)


@dataclass
class _NumericDataSource(DataSource):
    min_value_col: str
    max_value_col: str
    increment_col: Optional[str] = None

    def _convert_helper(self, row: pd.Series) -> Tuple[str]:
        min_val = str(utils.get_row_value(row, self.min_value_col))
        max_val = str(utils.get_row_value(row, self.max_value_col))
        incr_val = str(utils.get_row_value(row, self.increment_col)) if self.increment_col is not None else '1'
        return min_val, max_val, incr_val

@dataclass
class NumberDataSource(_NumericDataSource):
    default_value_col: Optional[str] = None
    parent_id_col: Optional[str] = None

    def convert(self, ds_param: DataSourceParameter, df: pd.DataFrame) -> p.NumberParameter:
        def _get_default_value(row: pd.Series) -> str:
            return str(utils.get_row_value(row, self.default_value_col)) if self.default_value_col is not None \
                else str(utils.get_row_value(row, self.min_value_col))
        
        def _create_num_param_option(row: pd.Series) -> po.NumberParameterOption:
            min_value, max_value, increment = self._convert_helper(row)
            return po.NumberParameterOption(min_value, max_value, increment, _get_default_value(row), 
                                            self._get_parent(row)) 

        if ds_param.parent is None:
            row = df.iloc[0]
            min_value, max_value, increment = self._convert_helper(row)
            return p.NumberParameter(ds_param.name, ds_param.label, min_value, max_value, increment, 
                                     _get_default_value(row), is_hidden=ds_param.is_hidden)
        else:
            all_options = tuple(_create_num_param_option(row) for _, row in df.iterrows())
            return p.NumberParameter.WithParent(ds_param.name, ds_param.label, all_options, ds_param.parent,
                                                is_hidden=ds_param.is_hidden)


@dataclass
class RangeDataSource(_NumericDataSource):
    default_lower_value_col: Optional[str] = None
    default_upper_value_col: Optional[str] = None
    parent_id_col: Optional[str] = None

    def convert(self, ds_param: DataSourceParameter, df: pd.DataFrame) -> p.NumRangeParameter:
        def _get_default_lower_upper_values(row: pd.Series) -> Tuple[str]:
            lower_value = str(utils.get_row_value(row, self.default_lower_value_col)) if self.default_lower_value_col is not None \
                else str(utils.get_row_value(row, self.min_value_col))
            upper_value = str(utils.get_row_value(row, self.default_upper_value_col)) if self.default_upper_value_col is not None \
                else str(utils.get_row_value(row, self.max_value_col))
            return lower_value, upper_value
        
        def _create_range_param_option(row: pd.Series) -> po.NumRangeParameterOption:
            min_value, max_value, increment = self._convert_helper(row)
            lower_value, upper_value = _get_default_lower_upper_values(row)
            return po.NumRangeParameterOption(min_value, max_value, increment, lower_value, upper_value, 
                                           self._get_parent(row)) 

        if ds_param.parent is None:
            row = df.iloc[0]
            min_value, max_value, increment = self._convert_helper(row)
            lower_value, upper_value = _get_default_lower_upper_values(row)
            return p.NumRangeParameter(ds_param.name, ds_param.label, min_value, max_value, increment, 
                                    lower_value, upper_value, is_hidden=ds_param.is_hidden)
        else:
            all_options = tuple(_create_range_param_option(row) for _, row in df.iterrows())
            return p.NumRangeParameter.WithParent(ds_param.name, ds_param.label, all_options, ds_param.parent,
                                               is_hidden=ds_param.is_hidden)


@dataclass
class DataSourceParameter(p.Parameter):
    data_source: DataSource
    parent: Optional[p.Parameter] 

    def __init__(self, widget_type: p.WidgetType, name: str, label: str, data_source: DataSource, *, 
                 is_hidden: bool = False, parent: Optional[p.Parameter] = None) -> None:
        super().__init__(widget_type, name, label, None, is_hidden, None)
        self.data_source = data_source
        self.parent = parent

    def convert(self, df: pd.DataFrame) -> p.Parameter:
        return self.data_source.convert(self, df)
    
    def to_dict(self) -> Dict:
        output = super().to_dict()
        output['data_source'] = self.data_source.__dict__
        return output
