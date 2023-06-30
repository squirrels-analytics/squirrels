from __future__ import annotations
from typing import Type, Dict, Tuple, List, Optional
from dataclasses import dataclass, field

from squirrels import parameters as p, parameter_options as po
from squirrels._timed_imports import pandas as pd
from squirrels import _constants as c, _utils


@dataclass
class DataSource:
    """
    Abstract class for lookup tables coming from a database
    """
    table_or_query: str
    
    def __post_init__(self) -> None:
        if not hasattr(self, 'parent_id_col'):
            self.parent_id_col = None
        if not hasattr(self, 'connection_name'):
            self.connection_name = c.DEFAULT_DB_CONN

    def get_query(self) -> str:
        """
        Get the "table_or_query" attribute as a select query

        Returns:
            str: The converted select query
        """
        if self.table_or_query.strip().lower().startswith('select '):
            query = self.table_or_query
        else:
            query = f'SELECT * FROM {self.table_or_query}'
        return query
    
    def convert(self, ds_param: p.DataSourceParameter, df: pd.DataFrame) -> p.Parameter:
        """
        An abstract method for converting itself into a parameter

        Args:
            ds_param: The parameter to convert
            df: The dataframe containing the parameter options data

        Returns:
            The converted parameter
        """
        raise _utils.AbstractMethodCallError(self.__class__, "convert")
    
    def _get_parent(self, row):
        return str(_utils.get_row_value(row, self.parent_id_col)) if self.parent_id_col is not None else None
    
    def _validate_parameter_class(self, ds_param: p.DataSourceParameter, parameter_classes: List[Type[p.Parameter]]) -> None:
        if ds_param.parameter_class not in parameter_classes:
            parameter_class_name = ds_param.parameter_class.__name__
            datasource_class_name = self.__class__.__name__
            raise _utils.ConfigurationError(f'Invalid widget type "{parameter_class_name}" for {datasource_class_name}')


@dataclass
class SelectionDataSource(DataSource):
    """
    Lookup table for selection parameters (single and multi)

    Attributes:
        connection_name: Name of the connection to use defined in connections.py
        table_or_query: Either the name of the table to use, or a query to run
        id_col: The column name of the id
        options_col: The column name of the options
        order_by_col: The column name to order the options by. Orders by the id_col instead if this is None
        is_default_col: The column name that indicates which options are the default
        parent_id_col: The column name of the parent option id that this option belongs to
        custom_cols: Dictionary of attribute to column name for custom fields for the SelectParameterOption
    """
    id_col: str
    options_col: str
    order_by_col: Optional[str] = None
    is_default_col: Optional[str] = None
    parent_id_col: Optional[str] = None
    custom_cols: Dict[str, str] = field(default_factory=dict)
    connection_name: str = c.DEFAULT_DB_CONN

    def __post_init__(self):
        self.order_by_col = self.order_by_col if self.order_by_col is not None else self.id_col

    def convert(self, ds_param: p.DataSourceParameter, df: pd.DataFrame) -> p.Parameter:
        """
        Method to convert the associated DataSourceParameter into a SingleSelect or MultiSelect Parameter

        Parameters:
            ds_param: The parameter to convert
            df: The dataframe containing the parameter options data

        Returns:
            The converted parameter
        """
        self._validate_parameter_class(ds_param, [p.SingleSelectParameter, p.MultiSelectParameter])

        def is_default(row):
            return int(_utils.get_row_value(row, self.is_default_col)) == 1 if self.is_default_col is not None else False
        
        def get_custom_fields(row):
            result = {}
            for key, val in self.custom_cols.items():
                result[key] = _utils.get_row_value(row, val)
            return result
        
        try:
            df.sort_values(self.order_by_col, inplace=True)
        except KeyError as e:
            raise _utils.ConfigurationError(f'Could not sort on column name "{self.order_by_col}" as it does not exist')
        
        options = tuple(
            po.SelectParameterOption(str(_utils.get_row_value(row, self.id_col)), str(_utils.get_row_value(row, self.options_col)), 
                                     is_default=is_default(row), parent_option_id=self._get_parent(row), custom_fields=get_custom_fields(row))
            for _, row in df.iterrows()
        )
        
        return ds_param.parameter_class(ds_param.name, ds_param.label, options, is_hidden=ds_param.is_hidden, parent=ds_param.parent)


@dataclass
class DateDataSource(DataSource):
    """
    Lookup table for date parameter default options

    Attributes:
        connection_name: Name of the connection to use defined in connections.py
        table_or_query: Either the name of the table to use, or a query to run
        default_date_col: The column name of the default date
        date_format: The format of the default date(s). Defaults to '%Y-%m-%d'
        parent_id_col: The column name of the parent option id that the default date belongs to
    """
    default_date_col: str
    parent_id_col: Optional[str] = None
    date_format: Optional[str] = '%Y-%m-%d'
    connection_name: str = c.DEFAULT_DB_CONN

    def convert(self, ds_param: p.DataSourceParameter, df: pd.DataFrame) -> p.DateParameter:
        """
        Method to convert the associated DataSourceParameter into a DateParameter

        Parameters:
            ds_param: The parameter to convert
            df: The dataframe containing the parameter options data

        Returns:
            The converted parameter
        """
        self._validate_parameter_class(ds_param, [p.DateParameter])
        
        def get_date(row: pd.Series) -> str:
            return str(_utils.get_row_value(row, self.default_date_col))
        
        def create_date_param_option(row: pd.Series) -> po.DateParameterOption:
            return po.DateParameterOption(get_date(row), self.date_format, self._get_parent(row))
        
        if ds_param.parent is None:
            row = df.iloc[0]
            return p.DateParameter(ds_param.name, ds_param.label, get_date(row), self.date_format, 
                                   is_hidden=ds_param.is_hidden)
        else:
            all_options = tuple(create_date_param_option(row) for _, row in df.iterrows())
            return p.DateParameter.WithParent(ds_param.name, ds_param.label, all_options, ds_param.parent,
                                              is_hidden=ds_param.is_hidden)


@dataclass
class _NumericDataSource(DataSource):
    """
    Abstract class for number or number range data sources
    """
    min_value_col: str
    max_value_col: str
    increment_col: Optional[str] = None

    def _convert_helper(self, row: pd.Series) -> Tuple[str, str, str]:
        min_val = str(_utils.get_row_value(row, self.min_value_col))
        max_val = str(_utils.get_row_value(row, self.max_value_col))
        incr_val = str(_utils.get_row_value(row, self.increment_col)) if self.increment_col is not None else '1'
        return min_val, max_val, incr_val

@dataclass
class NumberDataSource(_NumericDataSource):
    """
    Lookup table for number parameter default options

    Attributes:
        connection_name: Name of the connection to use defined in connections.py
        table_or_query: Either the name of the table to use, or a query to run
        min_value_col: The column name of the minimum value
        max_value_col: The column name of the maximum value
        increment_col: The column name of the increment value. Defaults to column of 1's if None
        default_value_col: The column name of the default value. Defaults to min_value_col if None
        parent_id_col: The column name of the parent option id that the default value belongs to
    """
    default_value_col: Optional[str] = None
    parent_id_col: Optional[str] = None
    connection_name: str = c.DEFAULT_DB_CONN

    def convert(self, ds_param: p.DataSourceParameter, df: pd.DataFrame) -> p.NumberParameter:
        """
        Method to convert the associated DataSourceParameter into a NumberParameter

        Parameters:
            ds_param: The parameter to convert
            df: The dataframe containing the parameter options data

        Returns:
            The converted parameter
        """
        self._validate_parameter_class(ds_param, [p.NumberParameter])

        def _get_default_value(row: pd.Series) -> str:
            return str(_utils.get_row_value(row, self.default_value_col)) if self.default_value_col is not None \
                else str(_utils.get_row_value(row, self.min_value_col))
        
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
class NumRangeDataSource(_NumericDataSource):
    """
    Lookup table for number range parameter default options

    Attributes:
        connection_name: Name of the connection to use defined in connections.py
        table_or_query: Either the name of the table to use, or a query to run
        min_value_col: The column name of the minimum value
        max_value_col: The column name of the maximum value
        increment_col: The column name of the increment value. Defaults to column of 1's if None
        default_lower_value_col: The column name of the default lower value. Defaults to min_value_col if None
        default_upper_value_col: The column name of the default upper value. Defaults to max_value_col if None
        parent_id_col: The column name of the parent option id that the default value belongs to
    """
    default_lower_value_col: Optional[str] = None
    default_upper_value_col: Optional[str] = None
    parent_id_col: Optional[str] = None
    connection_name: str = c.DEFAULT_DB_CONN

    def convert(self, ds_param: p.DataSourceParameter, df: pd.DataFrame) -> p.NumRangeParameter:
        """
        Method to convert the associated DataSourceParameter into a NumRangeParameter

        Parameters:
            ds_param: The parameter to convert
            df: The dataframe containing the parameter options data

        Returns:
            The converted parameter
        """
        self._validate_parameter_class(ds_param, [p.NumRangeParameter])

        def _get_default_lower_upper_values(row: pd.Series) -> Tuple[str, str]:
            lower_value_col = self.default_lower_value_col if self.default_lower_value_col is not None \
                else self.min_value_col
            upper_value_col = self.default_upper_value_col if self.default_upper_value_col is not None \
                else self.max_value_col
            lower_value = str(_utils.get_row_value(row, lower_value_col))
            upper_value = str(_utils.get_row_value(row, upper_value_col))
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
