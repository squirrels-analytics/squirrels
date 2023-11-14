from __future__ import annotations
from typing import Type, Dict, Sequence, Iterable, Optional, Any
from dataclasses import dataclass
from abc import ABCMeta, abstractmethod

from . import _parameter_configs as pc, parameter_options as po, parameters as p, _constants as c, _utils as u
from squirrels._timed_imports import pandas as pd


@dataclass
class DataSource(metaclass=ABCMeta):
    """
    Abstract class for lookup tables coming from a database
    """
    table_or_query: str
    id_col: str
    user_group_col: Optional[str]
    parent_id_col: Optional[str]
    connection_name: str

    @abstractmethod
    def __init__(
        self, table_or_query: str, id_col: str, *, user_group_col: Optional[str] = None, parent_id_col: Optional[str] = None,
        connection_name: str = c.DEFAULT_DB_CONN
    ) -> None:
        self.table_or_query = table_or_query
        self.id_col = id_col
        self.user_group_col = user_group_col
        self.parent_id_col = parent_id_col
        self.connection_name = connection_name

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
    
    @abstractmethod
    def _convert(self, ds_param: pc.DataSourceParameterConfig, df: pd.DataFrame) -> pc.ParameterConfig:
        """
        An abstract method for converting itself into a parameter
        """
        pass
    
    def _validate_parameter_class(self, ds_param: pc.DataSourceParameterConfig, target_parameter_class: Type[p.Parameter]) -> None:
        if ds_param.parameter_class != target_parameter_class:
            parameter_class_name = ds_param.parameter_class.__name__
            datasource_class_name = self.__class__.__name__
            raise u.ConfigurationError(f'Invalid widget type "{parameter_class_name}" for {datasource_class_name}')
    
    def _get_aggregated_df(self, df: pd.DataFrame, columns_to_include: Iterable[str]) -> pd.DataFrame:
        agg_rules = {}
        for column in columns_to_include:
            if column is not None:
                agg_rules[column] = "first"
        if self.user_group_col is not None:
            agg_rules[self.user_group_col] = list
        if self.parent_id_col is not None:
            agg_rules[self.parent_id_col] = list

        try:
            df_agg = df.groupby(self.id_col).agg(agg_rules)
        except KeyError as e:
            raise u.ConfigurationError(e)
        
        return df_agg
        
    def _get_key_from_record(self, key: Optional[str], record: Dict[str, Any], default: Any) -> Any:
        return record[key] if key is not None else default
    
    def _get_key_from_record_as_list(self, key: Optional[str], record: Dict[str, Any]) -> Iterable[str]:
        value = self._get_key_from_record(key, record, list())
        return [str(x) for x in value]


@dataclass
class _SelectionDataSource(DataSource):
    """
    Abstract class for selection parameter data sources
    """
    options_col: str
    order_by_col: Optional[str]
    is_default_col: Optional[str]
    custom_cols: Dict[str, str]

    @abstractmethod
    def __init__(
        self, table_or_query: str, id_col: str, options_col: str, *, order_by_col: Optional[str] = None, 
        is_default_col: Optional[str] = None, custom_cols: Dict[str, str] = {}, user_group_col: Optional[str] = None, 
        parent_id_col: Optional[str] = None, connection_name: str = c.DEFAULT_DB_CONN
    ) -> None:
        super().__init__(table_or_query, id_col, user_group_col=user_group_col, parent_id_col=parent_id_col, connection_name=connection_name)
        self.options_col = options_col
        self.order_by_col = order_by_col
        self.is_default_col = is_default_col
        self.custom_cols = custom_cols

    def _get_all_options(self, df: pd.DataFrame) -> Sequence[po.SelectParameterOption]:
        columns = [self.options_col, self.order_by_col, self.is_default_col, *self.custom_cols.values()]
        df_agg = self._get_aggregated_df(df, columns)

        if self.order_by_col is None:
            df_agg.sort_index(inplace=True)
        else:
            df_agg.sort_values(self.order_by_col, inplace=True)

        def get_is_default(record: Dict[str, Any]) -> bool:
            return int(record[self.is_default_col]) == 1 if self.is_default_col is not None else False

        def get_custom_fields(record: Dict[str, Any]) -> Dict[str, Any]:
            result = {}
            for key, val in self.custom_cols.items():
                result[key] = record[val]
            return result
        
        records: Dict[str, Dict[str, Any]] = df_agg.to_dict("index")
        return tuple(
            po.SelectParameterOption(str(id), str(record[self.options_col]), 
                                     is_default=get_is_default(record), custom_fields=get_custom_fields(record),
                                     user_groups=self._get_key_from_record_as_list(self.user_group_col, record), 
                                     parent_option_ids=self._get_key_from_record_as_list(self.parent_id_col, record))
            for id, record in records.items()
        )


@dataclass
class SingleSelectDataSource(_SelectionDataSource):
    """
    Lookup table for single select parameter options

    Attributes:
        table_or_query: Either the name of the table to use, or a query to run
        id_col: The column name of the id
        options_col: The column name of the options
        order_by_col: The column name to order the options by. Orders by the id_col instead if this is None
        is_default_col: The column name that indicates which options are the default
        custom_cols: Dictionary of attribute to column name for custom fields for the SelectParameterOption
        user_group_col: The column name of the user group that the user is in for this option to be valid
        parent_id_col: The column name of the parent option id that must be selected for this option to be valid
        connection_name: Name of the connection to use defined in connections.py
    """

    def __init__(
            self, table_or_query: str, id_col: str, options_col: str, *, order_by_col: Optional[str] = None, 
            is_default_col: Optional[str] = None, custom_cols: Dict[str, str] = {}, user_group_col: Optional[str] = None, 
            parent_id_col: Optional[str] = None, connection_name: str = c.DEFAULT_DB_CONN
        ) -> None:
        """
        Constructor for SingleSelectDataSource

        Parameters:
            ...see Attributes of SingleSelectDataSource
        """
        super().__init__(table_or_query, id_col, options_col, order_by_col=order_by_col, is_default_col=is_default_col,
                         custom_cols=custom_cols, user_group_col=user_group_col, parent_id_col=parent_id_col, 
                         connection_name=connection_name)
    
    def _convert(self, ds_param: pc.DataSourceParameterConfig, df: pd.DataFrame) -> pc.SingleSelectParameterConfig:
        """
        Method to convert the associated DataSourceParameter into a SingleSelectParameterConfig

        Parameters:
            ds_param: The parameter to convert
            df: The dataframe containing the parameter options data

        Returns:
            The converted parameter
        """
        self._validate_parameter_class(ds_param, p.SingleSelectParameter)
        all_options = self._get_all_options(df)
        return pc.SingleSelectParameterConfig(ds_param.name, ds_param.label, all_options, is_hidden=ds_param.is_hidden, 
                                              user_attribute=ds_param.user_attribute, parent_name=ds_param.parent_name)


@dataclass
class MultiSelectDataSource(_SelectionDataSource):
    """
    Lookup table for single select parameter options

    Attributes:
        table_or_query: Either the name of the table to use, or a query to run
        id_col: The column name of the id
        options_col: The column name of the options
        order_by_col: The column name to order the options by. Orders by the id_col instead if this is None
        is_default_col: The column name that indicates which options are the default
        custom_cols: Dictionary of attribute to column name for custom fields for the SelectParameterOption
        include_all: Whether applying no selection is equivalent to selecting all. Default is True
        order_matters: Whether the ordering of the selection matters. Default is False 
        user_group_col: The column name of the user group that the user is in for this option to be valid
        parent_id_col: The column name of the parent option id that must be selected for this option to be valid
        connection_name: Name of the connection to use defined in connections.py
    """
    include_all: bool
    order_matters: bool 

    def __init__(
            self, table_or_query: str, id_col: str, options_col: str, *, order_by_col: Optional[str] = None, 
            is_default_col: Optional[str] = None, custom_cols: Dict[str, str] = {}, include_all: bool = True, order_matters: bool = False, 
            user_group_col: Optional[str] = None, parent_id_col: Optional[str] = None, connection_name: str = c.DEFAULT_DB_CONN
        ) -> None:
        """
        Constructor for SingleSelectDataSource

        Parameters:
            ...see Attributes of SingleSelectDataSource
        """
        super().__init__(table_or_query, id_col, options_col, order_by_col=order_by_col, is_default_col=is_default_col,
                         custom_cols=custom_cols, user_group_col=user_group_col, parent_id_col=parent_id_col, 
                         connection_name=connection_name)
        self.include_all = include_all
        self.order_matters = order_matters
    
    def _convert(self, ds_param: pc.DataSourceParameterConfig, df: pd.DataFrame) -> pc.MultiSelectParameterConfig:
        """
        Method to convert the associated DataSourceParameter into a MultiSelectParameterConfig

        Parameters:
            ds_param: The parameter to convert
            df: The dataframe containing the parameter options data

        Returns:
            The converted parameter
        """
        self._validate_parameter_class(ds_param, p.MultiSelectParameter)
        all_options = self._get_all_options(df)
        return pc.MultiSelectParameterConfig(ds_param.name, ds_param.label, all_options, include_all=self.include_all, 
                                             order_matters=self.order_matters, is_hidden=ds_param.is_hidden, 
                                             user_attribute=ds_param.user_attribute, parent_name=ds_param.parent_name)


@dataclass
class DateDataSource(DataSource):
    """
    Lookup table for date parameter default options

    Attributes:
        table_or_query: Either the name of the table to use, or a query to run
        id_col: The column name of the id
        default_date_col: The column name of the default date
        date_format: The format of the default date(s). Defaults to '%Y-%m-%d'
        user_group_col: The column name of the user group that the user is in for this option to be valid
        parent_id_col: The column name of the parent option id that the default date belongs to
        connection_name: Name of the connection to use defined in connections.py
    """
    default_date_col: str
    date_format: str

    def __init__(
        self, table_or_query: str, id_col: str, default_date_col: str, *, date_format: str = '%Y-%m-%d', 
        user_group_col: Optional[str] = None, parent_id_col: Optional[str] = None, connection_name: str = c.DEFAULT_DB_CONN
    ) -> None:
        """
        Constructor for DateDataSource

        Parameters:
            ...see Attributes of DateDataSource
        """
        super().__init__(table_or_query, id_col, user_group_col=user_group_col, parent_id_col=parent_id_col, 
                         connection_name=connection_name)
        self.default_date_col = default_date_col
        self.date_format = date_format

    def _convert(self, ds_param: pc.DataSourceParameterConfig, df: pd.DataFrame) -> pc.DateParameterConfig:
        """
        Method to convert the associated DataSourceParameter into a DateParameterConfig

        Parameters:
            ds_param: The parameter to convert
            df: The dataframe containing the parameter options data

        Returns:
            The converted parameter
        """
        self._validate_parameter_class(ds_param, p.DateParameter)

        columns = [self.default_date_col]
        df_agg = self._get_aggregated_df(df, columns)

        records: Dict[str, Dict[str, Any]] = df_agg.to_dict("index")
        options = tuple(
            po.DateParameterOption(str(record[self.default_date_col]), date_format=self.date_format, 
                                   user_groups=self._get_key_from_record_as_list(self.user_group_col, record), 
                                   parent_option_ids=self._get_key_from_record_as_list(self.parent_id_col, record))
            for _, record in records.items()
        )
        return pc.DateParameterConfig(ds_param.name, ds_param.label, options, is_hidden=ds_param.is_hidden, 
                                      user_attribute=ds_param.user_attribute, parent_name=ds_param.parent_name)


@dataclass
class DateRangeDataSource(DataSource):
    """
    Lookup table for date parameter default options

    Attributes:
        table_or_query: Either the name of the table to use, or a query to run
        id_col: The column name of the id
        default_start_date_col: The column name of the default start date
        default_end_date_col: The column name of the default end date
        date_format: The format of the default date(s). Defaults to '%Y-%m-%d'
        user_group_col: The column name of the user group that the user is in for this option to be valid
        parent_id_col: The column name of the parent option id that the default date belongs to
        connection_name: Name of the connection to use defined in connections.py
    """
    default_start_date_col: str
    default_end_date_col: str
    date_format: str

    def __init__(self, table_or_query: str, id_col: str, default_start_date_col: str, default_end_date_col: str, *, 
                 date_format: str = '%Y-%m-%d', user_group_col: Optional[str] = None, parent_id_col: Optional[str] = None, 
                 connection_name: str = c.DEFAULT_DB_CONN) -> None:
        """
        Constructor for DateRangeDataSource

        Parameters:
            ...see Attributes of DateRangeDataSource
        """
        super().__init__(table_or_query, id_col, user_group_col=user_group_col, parent_id_col=parent_id_col, 
                         connection_name=connection_name)
        self.default_start_date_col = default_start_date_col
        self.default_end_date_col = default_end_date_col
        self.date_format = date_format

    def _convert(self, ds_param: pc.DataSourceParameterConfig, df: pd.DataFrame) -> pc.DateRangeParameterConfig:
        """
        Method to convert the associated DataSourceParameter into a DateRangeParameterConfig

        Parameters:
            ds_param: The parameter to convert
            df: The dataframe containing the parameter options data

        Returns:
            The converted parameter
        """
        self._validate_parameter_class(ds_param, p.DateRangeParameter)

        columns = [self.default_start_date_col, self.default_end_date_col]
        df_agg = self._get_aggregated_df(df, columns)

        records: Dict[str, Dict[str, Any]] = df_agg.to_dict("index")
        options = tuple(
            po.DateRangeParameterOption(str(record[self.default_start_date_col]), str(record[self.default_end_date_col]),
                                        date_format=self.date_format, 
                                        user_groups=self._get_key_from_record_as_list(self.user_group_col, record), 
                                        parent_option_ids=self._get_key_from_record_as_list(self.parent_id_col, record))
            for _, record in records.items()
        )
        return pc.DateRangeParameterConfig(ds_param.name, ds_param.label, options, is_hidden=ds_param.is_hidden, 
                                           user_attribute=ds_param.user_attribute, parent_name=ds_param.parent_name)


@dataclass
class _NumericDataSource(DataSource):
    """
    Abstract class for number or number range data sources
    """
    min_value_col: str
    max_value_col: str
    increment_col: Optional[str]
    
    @abstractmethod
    def __init__(self, table_or_query: str, id_col: str, min_value_col: str, max_value_col: str, *, increment_col: Optional[str] = None, 
                 user_group_col: Optional[str] = None, parent_id_col: Optional[str] = None, connection_name: str = c.DEFAULT_DB_CONN) -> None:
        super().__init__(table_or_query, id_col, user_group_col=user_group_col, parent_id_col=parent_id_col, connection_name=connection_name)
        self.min_value_col = min_value_col
        self.max_value_col = max_value_col
        self.increment_col = increment_col


@dataclass
class NumberDataSource(_NumericDataSource):
    """
    Lookup table for number parameter default options

    Attributes:
        table_or_query: Either the name of the table to use, or a query to run
        id_col: The column name of the id
        min_value_col: The column name of the minimum value
        max_value_col: The column name of the maximum value
        increment_col: The column name of the increment value. Defaults to column of 1's if None
        default_value_col: The column name of the default value. Defaults to min_value_col if None
        user_group_col: The column name of the user group that the user is in for this option to be valid
        parent_id_col: The column name of the parent option id that the default value belongs to
        connection_name: Name of the connection to use defined in connections.py
    """
    default_value_col: Optional[str]

    def __init__(self, table_or_query: str, id_col: str, min_value_col: str, max_value_col: str, *, increment_col: Optional[str] = None,
                 default_value_col: Optional[str] = None, user_group_col: Optional[str] = None, parent_id_col: Optional[str] = None, 
                 connection_name: str = c.DEFAULT_DB_CONN) -> None:
        """
        Constructor for NumberDataSource

        Parameters:
            ...see Attributes of NumberDataSource
        """
        super().__init__(table_or_query, id_col, min_value_col, max_value_col, increment_col=increment_col, user_group_col=user_group_col, 
                         parent_id_col=parent_id_col, connection_name=connection_name)
        self.default_value_col = default_value_col

    def _convert(self, ds_param: pc.DataSourceParameterConfig, df: pd.DataFrame) -> pc.NumberParameterConfig:
        """
        Method to convert the associated DataSourceParameter into a NumberParameterConfig

        Parameters:
            ds_param: The parameter to convert
            df: The dataframe containing the parameter options data

        Returns:
            The converted parameter
        """
        self._validate_parameter_class(ds_param, p.NumberParameter)

        columns = [self.min_value_col, self.max_value_col, self.increment_col, self.default_value_col]
        df_agg = self._get_aggregated_df(df, columns)

        records: Dict[str, Dict[str, Any]] = df_agg.to_dict("index")
        options = tuple(
            po.NumberParameterOption(record[self.min_value_col], record[self.max_value_col], 
                                     increment=self._get_key_from_record(self.increment_col, record, 1),
                                     default_value=self._get_key_from_record(self.default_value_col, record, None),
                                     user_groups=self._get_key_from_record_as_list(self.user_group_col, record), 
                                     parent_option_ids=self._get_key_from_record_as_list(self.parent_id_col, record))
            for _, record in records.items()
        )
        return pc.NumberParameterConfig(ds_param.name, ds_param.label, options, is_hidden=ds_param.is_hidden,
                                        user_attribute=ds_param.user_attribute, parent_name=ds_param.parent_name)


@dataclass
class NumRangeDataSource(_NumericDataSource):
    """
    Lookup table for number range parameter default options

    Attributes:
        table_or_query: Either the name of the table to use, or a query to 
        id_col: The column name of the id
        min_value_col: The column name of the minimum value
        max_value_col: The column name of the maximum value
        increment_col: The column name of the increment value. Defaults to column of 1's if None
        default_lower_value_col: The column name of the default lower value. Defaults to min_value_col if None
        default_upper_value_col: The column name of the default upper value. Defaults to max_value_col if None
        user_group_col: The column name of the user group that the user is in for this option to be valid
        parent_id_col: The column name of the parent option id that the default value belongs to
        connection_name: Name of the connection to use defined in connections.py
    """
    default_lower_value_col: Optional[str]
    default_upper_value_col: Optional[str]

    def __init__(self, table_or_query: str, id_col: str, min_value_col: str, max_value_col: str, *, increment_col: Optional[str] = None,
                 default_lower_value_col: Optional[str] = None, default_upper_value_col: Optional[str] = None, user_group_col: Optional[str] = None,
                 parent_id_col: Optional[str] = None, connection_name: str = c.DEFAULT_DB_CONN) -> None:
        """
        Constructor for NumRangeDataSource

        Parameters:
            ...see Attributes of NumRangeDataSource
        """
        super().__init__(table_or_query, id_col, min_value_col, max_value_col, increment_col=increment_col, user_group_col=user_group_col, 
                         parent_id_col=parent_id_col, connection_name=connection_name)
        self.default_lower_value_col = default_lower_value_col
        self.default_upper_value_col = default_upper_value_col

    def _convert(self, ds_param: pc.DataSourceParameterConfig, df: pd.DataFrame) -> pc.NumRangeParameterConfig:
        """
        Method to convert the associated DataSourceParameter into a NumRangeParameterConfig

        Parameters:
            ds_param: The parameter to convert
            df: The dataframe containing the parameter options data

        Returns:
            The converted parameter
        """
        self._validate_parameter_class(ds_param, p.NumRangeParameter)

        columns = [self.min_value_col, self.max_value_col, self.increment_col, self.default_lower_value_col, self.default_upper_value_col]
        df_agg = self._get_aggregated_df(df, columns)

        records: Dict[str, Any] = df_agg.to_dict("index")
        options = tuple(
            po.NumRangeParameterOption(record[self.min_value_col], record[self.max_value_col], 
                                       increment=self._get_key_from_record(self.increment_col, record, 1),
                                       default_lower_value=self._get_key_from_record(self.default_lower_value_col, record, None),
                                       default_upper_value=self._get_key_from_record(self.default_upper_value_col, record, None),
                                       user_groups=self._get_key_from_record_as_list(self.user_group_col, record), 
                                       parent_option_ids=self._get_key_from_record_as_list(self.parent_id_col, record))
            for _, record in records.items()
        )
        return pc.NumRangeParameterConfig(ds_param.name, ds_param.label, options, is_hidden=ds_param.is_hidden,
                                          user_attribute=ds_param.user_attribute, parent_name=ds_param.parent_name)
