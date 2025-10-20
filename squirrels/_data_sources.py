from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
import polars as pl, typing as t, abc

from . import _parameter_configs as pc, _parameter_options as po
from ._exceptions import ConfigurationError

class SourceEnum(Enum):
    CONNECTION = "connection"
    SEEDS = "seeds"
    VDL = "vdl"


@dataclass
class DataSource(metaclass=abc.ABCMeta):
    """
    Abstract class for lookup tables coming from a database
    """
    _table_or_query: str
    _id_col: str | None
    _source: SourceEnum
    _user_group_col: str | None
    _parent_id_col: str | None
    _connection: str | None

    @abc.abstractmethod
    def __init__(
        self, table_or_query: str, *, id_col: str | None = None, source: SourceEnum = SourceEnum.CONNECTION, 
        user_group_col: str | None = None, parent_id_col: str | None = None, connection: str | None = None, **kwargs
    ) -> None:
        self._table_or_query = table_or_query
        self._id_col = id_col
        self._source = source
        self._user_group_col = user_group_col
        self._parent_id_col = parent_id_col
        self._connection = connection
    
    def _get_connection_name(self, default_conn_name: str) -> str:
        return self._connection if self._connection is not None else default_conn_name

    def _get_query(self) -> str:
        """
        Get the "table_or_query" attribute as a select query

        Returns:
            str: The converted select query
        """
        if self._table_or_query.strip().lower().startswith('select '):
            query = self._table_or_query
        else:
            query = f'SELECT * FROM {self._table_or_query}'
        return query
    
    @abc.abstractmethod
    def _convert(self, ds_param: pc.DataSourceParameterConfig, df: pl.DataFrame) -> pc.ParameterConfig:
        """
        An abstract method for converting itself into a parameter
        """
        pass
    
    def _validate_parameter_type(self, ds_param: pc.DataSourceParameterConfig, target_parameter_type: t.Type[pc.ParameterConfig]) -> None:
        if ds_param.parameter_type != target_parameter_type:
            parameter_type_name = ds_param.parameter_type.__name__
            datasource_type_name = self.__class__.__name__
            raise ConfigurationError(f'Invalid widget type "{parameter_type_name}" for {datasource_type_name}')
    
    def _get_aggregated_df(self, df: pl.DataFrame, columns_to_include: t.Iterable[str]) -> pl.DataFrame:
        if self._id_col is None:
            return df
        
        agg_rules = []
        for column in columns_to_include:
            if column is not None:
                agg_rules.append(pl.first(column))
        if self._user_group_col is not None:
            agg_rules.append(pl.col(self._user_group_col))
        if self._parent_id_col is not None:
            agg_rules.append(pl.col(self._parent_id_col))

        try:
            df_agg = df.group_by(self._id_col).agg(agg_rules).sort(by=self._id_col)
        except pl.exceptions.ColumnNotFoundError as e:
            raise ConfigurationError(e)
        
        return df_agg
        
    def _get_key_from_record(self, key: str | None, record: dict[t.Hashable, t.Any], default: t.Any) -> t.Any:
        return record[key] if key is not None else default
    
    def _get_key_from_record_as_list(self, key: str | None, record: dict[t.Hashable, t.Any]) -> t.Iterable[str]:
        value = self._get_key_from_record(key, record, list())
        return [str(x) for x in value]


@dataclass
class _SelectionDataSource(DataSource):
    """
    Abstract class for selection parameter data sources
    """
    _options_col: str
    _order_by_col: str | None
    _is_default_col: str | None
    _custom_cols: dict[str, str]

    @abc.abstractmethod
    def __init__(
        self, table_or_query: str, id_col: str, options_col: str, *, order_by_col: str | None = None, 
        is_default_col: str | None = None, custom_cols: dict[str, str] = {}, source: SourceEnum = SourceEnum.CONNECTION, 
        user_group_col: str | None = None, parent_id_col: str | None = None, connection: str | None = None, 
        **kwargs
    ) -> None:
        super().__init__(
            table_or_query, id_col=id_col, source=source, user_group_col=user_group_col, parent_id_col=parent_id_col, 
            connection=connection
        )
        self._options_col = options_col
        self._order_by_col = order_by_col
        self._is_default_col = is_default_col
        self._custom_cols = custom_cols

    def _get_all_options(self, df: pl.DataFrame) -> t.Sequence[po.SelectParameterOption]:
        columns = [self._options_col, self._order_by_col, self._is_default_col, *self._custom_cols.values()]
        df_agg = self._get_aggregated_df(df, columns)

        if self._order_by_col is None:
            df_agg = df_agg.sort(by=self._id_col)
        else:
            df_agg = df_agg.sort(by=self._order_by_col)

        def get_is_default(record: dict[t.Hashable, t.Any]) -> bool:
            return int(record[self._is_default_col]) == 1 if self._is_default_col is not None else False

        def get_custom_fields(record: dict[t.Hashable, t.Any]) -> dict[str, t.Any]:
            result = {}
            for key, val in self._custom_cols.items():
                result[key] = record[val]
            return result
        
        records = df_agg.to_pandas().to_dict("records")
        return tuple(
            po.SelectParameterOption(
                str(record[self._id_col]), str(record[self._options_col]), 
                is_default=get_is_default(record), custom_fields=get_custom_fields(record),
                user_groups=self._get_key_from_record_as_list(self._user_group_col, record), 
                parent_option_ids=self._get_key_from_record_as_list(self._parent_id_col, record)
            )
            for record in records
        )


@dataclass
class SelectDataSource(_SelectionDataSource):
    """
    Lookup table for select parameter options
    """

    def __init__(
            self, table_or_query: str, id_col: str, options_col: str, *, order_by_col: str | None = None, 
            is_default_col: str | None = None, custom_cols: dict[str, str] = {}, source: SourceEnum = SourceEnum.CONNECTION, 
            user_group_col: str | None = None, parent_id_col: str | None = None, connection: str | None = None, 
            **kwargs
        ) -> None:
        """
        Constructor for SelectDataSource

        Arguments:
            table_or_query: Either the name of the table to use, or a query to run
            id_col: The column name of the id
            options_col: The column name of the options
            order_by_col: The column name to order the options by. Orders by the id_col instead if this is None
            is_default_col: The column name that indicates which options are the default
            custom_cols: Dictionary of attribute to column name for custom fields for the SelectParameterOption
            source: The source to fetch data from. Must be "connection", "seeds", or "vdl". Defaults to "connection"
            user_group_col: The column name of the user group that the user is in for this option to be valid
            parent_id_col: The column name of the parent option id that must be selected for this option to be valid
            connection: Name of the connection to use defined in connections.py
        """
        super().__init__(
            table_or_query, id_col, options_col, order_by_col=order_by_col, is_default_col=is_default_col, custom_cols=custom_cols,
            source=source, user_group_col=user_group_col, parent_id_col=parent_id_col, connection=connection
        )

    def _convert(self, ds_param: pc.DataSourceParameterConfig, df: pl.DataFrame) -> pc.SelectionParameterConfig:
        """
        Method to convert the associated DataSourceParameterConfig into a SingleSelectParameterConfig or MultiSelectParameterConfig

        Arguments:
            ds_param: The parameter to convert
            df: The dataframe containing the parameter options data

        Returns:
            The converted parameter
        """
        all_options = self._get_all_options(df)
        if ds_param.parameter_type == pc.SingleSelectParameterConfig:
            return pc.SingleSelectParameterConfig(
                ds_param.name, ds_param.label, all_options, description=ds_param.description, 
                user_attribute=ds_param.user_attribute, parent_name=ds_param.parent_name, **ds_param.extra_args
            )
        elif ds_param.parameter_type == pc.MultiSelectParameterConfig:
            return pc.MultiSelectParameterConfig(
                ds_param.name, ds_param.label, all_options, description=ds_param.description, 
                user_attribute=ds_param.user_attribute, parent_name=ds_param.parent_name, **ds_param.extra_args
            )
        else:
            raise ConfigurationError(f'Invalid widget type "{ds_param.parameter_type}" for SelectDataSource')


@dataclass
class DateDataSource(DataSource):
    """
    Lookup table for date parameter default options
    """
    _default_date_col: str
    _date_format: str

    def __init__(
        self, table_or_query: str, default_date_col: str, *, min_date_col: str | None = None, 
        max_date_col: str | None = None, date_format: str = '%Y-%m-%d', id_col: str | None = None, 
        source: SourceEnum = SourceEnum.CONNECTION, user_group_col: str | None = None, parent_id_col: str | None = None, 
        connection: str | None = None, **kwargs
    ) -> None:
        """
        Constructor for DateDataSource

        Arguments:
            table_or_query: Either the name of the table to use, or a query to run
            default_date_col: The column name of the default date
            date_format: The format of the default date(s). Defaults to '%Y-%m-%d'
            id_col: The column name of the id
            source: The source to fetch data from. Must be "connection", "seeds", or "vdl". Defaults to "connection"
            user_group_col: The column name of the user group that the user is in for this option to be valid
            parent_id_col: The column name of the parent option id that the default date belongs to
            connection: Name of the connection to use defined in connections.py
        """
        super().__init__(
            table_or_query, id_col=id_col, source=source, user_group_col=user_group_col, parent_id_col=parent_id_col, 
            connection=connection
        )
        self._default_date_col = default_date_col
        self._min_date_col = min_date_col
        self._max_date_col = max_date_col
        self._date_format = date_format

    def _convert(self, ds_param: pc.DataSourceParameterConfig, df: pl.DataFrame) -> pc.DateParameterConfig:
        """
        Method to convert the associated DataSourceParameterConfig into a DateParameterConfig

        Arguments:
            ds_param: The parameter to convert
            df: The dataframe containing the parameter options data

        Returns:
            The converted parameter
        """
        self._validate_parameter_type(ds_param, pc.DateParameterConfig)

        columns = [self._default_date_col, self._min_date_col, self._max_date_col]
        df_agg = self._get_aggregated_df(df, columns)

        records = df_agg.to_pandas().to_dict("records")
        options = tuple(
            po.DateParameterOption(
                str(record[self._default_date_col]), date_format=self._date_format, 
                min_date = str(record[self._min_date_col]) if self._min_date_col else None,
                max_date = str(record[self._max_date_col]) if self._max_date_col else None,
                user_groups=self._get_key_from_record_as_list(self._user_group_col, record), 
                parent_option_ids=self._get_key_from_record_as_list(self._parent_id_col, record)
            )
            for record in records
        )
        return pc.DateParameterConfig(
            ds_param.name, ds_param.label, options, description=ds_param.description, user_attribute=ds_param.user_attribute, 
            parent_name=ds_param.parent_name, **ds_param.extra_args
        )


@dataclass
class DateRangeDataSource(DataSource):
    """
    Lookup table for date parameter default options
    """
    _default_start_date_col: str
    _default_end_date_col: str
    _date_format: str

    def __init__(
        self, table_or_query: str, default_start_date_col: str, default_end_date_col: str, *, date_format: str = '%Y-%m-%d',
        min_date_col: str | None = None, max_date_col: str | None = None, id_col: str | None = None, source: SourceEnum = SourceEnum.CONNECTION, 
        user_group_col: str | None = None, parent_id_col: str | None = None, connection: str | None = None, **kwargs
    ) -> None:
        """
        Constructor for DateRangeDataSource

        Arguments:
            table_or_query: Either the name of the table to use, or a query to run
            default_start_date_col: The column name of the default start date
            default_end_date_col: The column name of the default end date
            date_format: The format of the default date(s). Defaults to '%Y-%m-%d'
            id_col: The column name of the id
            source: The source to fetch data from. Must be "connection", "seeds", or "vdl". Defaults to "connection"
            user_group_col: The column name of the user group that the user is in for this option to be valid
            parent_id_col: The column name of the parent option id that the default date belongs to
            connection: Name of the connection to use defined in connections.py
        """
        super().__init__(
            table_or_query, id_col=id_col, source=source, user_group_col=user_group_col, parent_id_col=parent_id_col, 
            connection=connection
        )
        self._default_start_date_col = default_start_date_col
        self._default_end_date_col = default_end_date_col
        self._min_date_col = min_date_col
        self._max_date_col = max_date_col
        self._date_format = date_format

    def _convert(self, ds_param: pc.DataSourceParameterConfig, df: pl.DataFrame) -> pc.DateRangeParameterConfig:
        """
        Method to convert the associated DataSourceParameterConfig into a DateRangeParameterConfig

        Arguments:
            ds_param: The parameter to convert
            df: The dataframe containing the parameter options data

        Returns:
            The converted parameter
        """
        self._validate_parameter_type(ds_param, pc.DateRangeParameterConfig)

        columns = [self._default_start_date_col, self._default_end_date_col, self._min_date_col, self._max_date_col]
        df_agg = self._get_aggregated_df(df, columns)

        records = df_agg.to_pandas().to_dict("records")
        options = tuple(
            po.DateRangeParameterOption(
                str(record[self._default_start_date_col]), str(record[self._default_end_date_col]),
                min_date=str(record[self._min_date_col]) if self._min_date_col else None, 
                max_date=str(record[self._max_date_col]) if self._max_date_col else None, 
                date_format=self._date_format, 
                user_groups=self._get_key_from_record_as_list(self._user_group_col, record), 
                parent_option_ids=self._get_key_from_record_as_list(self._parent_id_col, record)
            )
            for record in records
        )
        return pc.DateRangeParameterConfig(
            ds_param.name, ds_param.label, options, description=ds_param.description, user_attribute=ds_param.user_attribute, 
            parent_name=ds_param.parent_name, **ds_param.extra_args
        )


@dataclass
class _NumericDataSource(DataSource):
    """
    Abstract class for number or number range data sources
    """
    _min_value_col: str
    _max_value_col: str
    _increment_col: str | None
    
    @abc.abstractmethod
    def __init__(
        self, table_or_query: str, min_value_col: str, max_value_col: str, *, increment_col: str | None = None, 
        id_col: str | None = None, source: SourceEnum = SourceEnum.CONNECTION, user_group_col: str | None = None, 
        parent_id_col: str | None = None, connection: str | None = None, **kwargs
    ) -> None:
        super().__init__(
            table_or_query, id_col=id_col, source=source, user_group_col=user_group_col, parent_id_col=parent_id_col, 
            connection=connection
        )
        self._min_value_col = min_value_col
        self._max_value_col = max_value_col
        self._increment_col = increment_col


@dataclass
class NumberDataSource(_NumericDataSource):
    """
    Lookup table for number parameter default options
    """
    _default_value_col: str | None

    def __init__(
        self, table_or_query: str, min_value_col: str, max_value_col: str, *, increment_col: str | None = None,
        default_value_col: str | None = None, id_col: str | None = None, source: SourceEnum = SourceEnum.CONNECTION, 
        user_group_col: str | None = None, parent_id_col: str | None = None, connection: str | None = None, **kwargs
    ) -> None:
        """
        Constructor for NumberDataSource

        Arguments:
            table_or_query: Either the name of the table to use, or a query to run
            min_value_col: The column name of the minimum value
            max_value_col: The column name of the maximum value
            increment_col: The column name of the increment value. Defaults to column of 1's if None
            default_value_col: The column name of the default value. Defaults to min_value_col if None
            id_col: The column name of the id
            source: The source to fetch data from. Must be "connection", "seeds", or "vdl". Defaults to "connection"
            user_group_col: The column name of the user group that the user is in for this option to be valid
            parent_id_col: The column name of the parent option id that the default value belongs to
            connection: Name of the connection to use defined in connections.py
        """
        super().__init__(
            table_or_query, min_value_col, max_value_col, increment_col=increment_col, id_col=id_col, source=source,
            user_group_col=user_group_col, parent_id_col=parent_id_col, connection=connection
        )
        self._default_value_col = default_value_col

    def _convert(self, ds_param: pc.DataSourceParameterConfig, df: pl.DataFrame) -> pc.NumberParameterConfig:
        """
        Method to convert the associated DataSourceParameterConfig into a NumberParameterConfig

        Arguments:
            ds_param: The parameter to convert
            df: The dataframe containing the parameter options data

        Returns:
            The converted parameter
        """
        self._validate_parameter_type(ds_param, pc.NumberParameterConfig)

        columns = [self._min_value_col, self._max_value_col, self._increment_col, self._default_value_col]
        df_agg = self._get_aggregated_df(df, columns)

        records = df_agg.to_pandas().to_dict("records")
        options = tuple(
            po.NumberParameterOption(
                record[self._min_value_col], record[self._max_value_col], 
                increment=self._get_key_from_record(self._increment_col, record, 1),
                default_value=self._get_key_from_record(self._default_value_col, record, None),
                user_groups=self._get_key_from_record_as_list(self._user_group_col, record), 
                parent_option_ids=self._get_key_from_record_as_list(self._parent_id_col, record)
            )
            for record in records
        )
        return pc.NumberParameterConfig(
            ds_param.name, ds_param.label, options, description=ds_param.description, user_attribute=ds_param.user_attribute, 
            parent_name=ds_param.parent_name, **ds_param.extra_args
        )


@dataclass
class NumberRangeDataSource(_NumericDataSource):
    """
    Lookup table for number range parameter default options
    """
    _default_lower_value_col: str | None
    _default_upper_value_col: str | None

    def __init__(
        self, table_or_query: str, min_value_col: str, max_value_col: str, *, increment_col: str | None = None,
        default_lower_value_col: str | None = None, default_upper_value_col: str | None = None, id_col: str | None = None, 
        source: SourceEnum = SourceEnum.CONNECTION, user_group_col: str | None = None, parent_id_col: str | None = None, 
        connection: str | None = None, **kwargs
    ) -> None:
        """
        Constructor for NumRangeDataSource

        Arguments:
            table_or_query: Either the name of the table to use, or a query to 
            min_value_col: The column name of the minimum value
            max_value_col: The column name of the maximum value
            increment_col: The column name of the increment value. Defaults to column of 1's if None
            default_lower_value_col: The column name of the default lower value. Defaults to min_value_col if None
            default_upper_value_col: The column name of the default upper value. Defaults to max_value_col if None
            id_col: The column name of the id
            source: The source to fetch data from. Must be "connection", "seeds", or "vdl". Defaults to "connection"
            user_group_col: The column name of the user group that the user is in for this option to be valid
            parent_id_col: The column name of the parent option id that the default value belongs to
            connection: Name of the connection to use defined in connections.py
        """
        super().__init__(
            table_or_query, min_value_col, max_value_col, increment_col=increment_col, id_col=id_col, source=source, 
            user_group_col=user_group_col, parent_id_col=parent_id_col, connection=connection
        )
        self._default_lower_value_col = default_lower_value_col
        self._default_upper_value_col = default_upper_value_col

    def _convert(self, ds_param: pc.DataSourceParameterConfig, df: pl.DataFrame) -> pc.NumberRangeParameterConfig:
        """
        Method to convert the associated DataSourceParameterConfig into a NumberRangeParameterConfig

        Arguments:
            ds_param: The parameter to convert
            df: The dataframe containing the parameter options data

        Returns:
            The converted parameter
        """
        self._validate_parameter_type(ds_param, pc.NumberRangeParameterConfig)

        columns = [self._min_value_col, self._max_value_col, self._increment_col, self._default_lower_value_col, self._default_upper_value_col]
        df_agg = self._get_aggregated_df(df, columns)

        records = df_agg.to_pandas().to_dict("records")
        options = tuple(
            po.NumberRangeParameterOption(
                record[self._min_value_col], record[self._max_value_col], 
                increment=self._get_key_from_record(self._increment_col, record, 1),
                default_lower_value=self._get_key_from_record(self._default_lower_value_col, record, None),
                default_upper_value=self._get_key_from_record(self._default_upper_value_col, record, None),
                user_groups=self._get_key_from_record_as_list(self._user_group_col, record), 
                parent_option_ids=self._get_key_from_record_as_list(self._parent_id_col, record)
            )
            for record in records
        )
        return pc.NumberRangeParameterConfig(
            ds_param.name, ds_param.label, options, description=ds_param.description, user_attribute=ds_param.user_attribute, 
            parent_name=ds_param.parent_name, **ds_param.extra_args
        )


@dataclass
class TextDataSource(DataSource):
    """
    Lookup table for text parameter default options
    """
    _default_text_col: str

    def __init__(
        self, table_or_query: str, default_text_col: str, *, id_col: str | None = None, source: SourceEnum = SourceEnum.CONNECTION, 
        user_group_col: str | None = None, parent_id_col: str | None = None, connection: str | None = None,
        **kwargs
    ) -> None:
        """
        Constructor for TextDataSource

        Arguments:
            table_or_query: Either the name of the table to use, or a query to run
            default_text_col: The column name of the default text
            id_col: The column name of the id
            source: The source to fetch data from. Must be "connection", "seeds", or "vdl". Defaults to "connection"
            user_group_col: The column name of the user group that the user is in for this option to be valid
            parent_id_col: The column name of the parent option id that the default date belongs to
            connection: Name of the connection to use defined in connections.py
        """
        super().__init__(
            table_or_query, id_col=id_col, source=source, user_group_col=user_group_col, parent_id_col=parent_id_col, 
            connection=connection
        )
        self._default_text_col = default_text_col

    def _convert(self, ds_param: pc.DataSourceParameterConfig, df: pl.DataFrame) -> pc.TextParameterConfig:
        """
        Method to convert the associated DataSourceParameterConfig into a TextParameterConfig

        Arguments:
            ds_param: The parameter to convert
            df: The dataframe containing the parameter options data

        Returns:
            The converted parameter
        """
        self._validate_parameter_type(ds_param, pc.TextParameterConfig)

        columns = [self._default_text_col]
        df_agg = self._get_aggregated_df(df, columns)

        records = df_agg.to_pandas().to_dict("records")
        options = tuple(
            po.TextParameterOption(
                default_text=str(record[self._default_text_col]), 
                user_groups=self._get_key_from_record_as_list(self._user_group_col, record), 
                parent_option_ids=self._get_key_from_record_as_list(self._parent_id_col, record)
            )
            for record in records
        )
        return pc.TextParameterConfig(
            ds_param.name, ds_param.label, options, description=ds_param.description, user_attribute=ds_param.user_attribute, 
            parent_name=ds_param.parent_name, **ds_param.extra_args
        )
