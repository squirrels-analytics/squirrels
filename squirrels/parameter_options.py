from typing import Iterable, Optional, Union, Dict, Any
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation as InvalidDecimalConversion
from datetime import datetime
from abc import ABCMeta, abstractmethod

from squirrels._utils import ConfigurationError

Number = Union[Decimal, int, str]


class ParameterOption(metaclass=ABCMeta):
    """
    Abstract class for parameter options
    """
    @abstractmethod
    def __init__(self, parent_option_id: Optional[str] = None, parent_option_ids: Iterable[str] = frozenset()) -> None:
        self.parent_option_id = parent_option_id
        self.parent_option_ids = frozenset(
            {self.parent_option_id} if self.parent_option_id is not None \
            else parent_option_ids
        )

    def _validate_lower_upper_values(self, lower_label: str, lower_value, upper_label: str, upper_value):
        if lower_value > upper_value:
            raise ConfigurationError(f'The {lower_label} "{lower_value}" must be ' +
                f'less than or equal to the {upper_label} "{upper_value}"')

    def is_valid(self, selected_parent_option_ids: Optional[Iterable[str]] = None):
        """
        Checks if this option is valid given the selected parent options.
        
        Parameters:
            selected_parent_option_ids: List of selected option ids from the parent parameter
        
        Returns:
            True if valid, False otherwise
        """
        if selected_parent_option_ids is not None:
            return not self.parent_option_ids.isdisjoint(selected_parent_option_ids)
        else:
            return True


@dataclass
class SelectParameterOption(ParameterOption):
    """
    Parameter option for a select parameter

    Attributes:
        identifier: Unique identifier for this option that never changes over time
        label: Human readable label that gets shown as a dropdown option
        is_default: True if this is a default option, False otherwise
        custom_fields: Dictionary to associate custom attributes to the parameter option
        parent_option_id: Identifier of the parent option, or None if this is a top-level option (hidden from print)
        parent_option_ids: Set of parent option ids (only used if parent_option_id is None)
    """
    identifier: str
    label: str
    is_default: bool
    custom_fields: Dict[str, Any]
    parent_option_ids: Iterable[str]

    def __init__(self, identifier: str, label: str, *, is_default: bool = False, 
                 parent_option_id: Optional[str] = None, parent_option_ids: Iterable[str] = frozenset(), 
                 custom_fields: Dict[str, Any] = {}, **kwargs):
        """
        Constructor for SelectParameterOption

        Parameters:
            ...see Attributes of SelectParameterOption
            **kwargs: Any additional keyword arguments specified (except the ones above) gets included into custom_fields as well
        """
        super().__init__(parent_option_id, parent_option_ids)
        self.identifier = identifier
        self.label = label
        self.is_default = is_default
        self.custom_fields = {
            **kwargs, **custom_fields, "id": identifier, "label": label
        }

    def get_custom_field(self, field: str, default_field: Optional[str] = None, default: Any = None) -> Any:
        """
        Get field value from the custom_fields attribute

        Parameters:
            field: The key to use to fetch the custom field from "custom_fields"
            default_field: If field does not exist in "custom_fields", then this is used instead as the field (if not None)
            default: If field does not exist and default_field is None, then this value is used as default
        
        Returns:
            The type of the custom field
        """
        if default_field is not None:
            default = self.get_custom_field(default_field)
        try:
            if default is not None:
                selected_field = self.custom_fields.get(field, default)
            else:
                selected_field = self.custom_fields[field]
        except KeyError as e:
            raise ConfigurationError(f"Field '{field}' must exist for parameter option '{self.to_dict()}'") from e
        
        return selected_field
    
    def to_dict(self):
        return {'id': self.identifier, 'label': self.label}


class DateTypeParameterOption(ParameterOption, metaclass=ABCMeta):
    """
    Abstract class (or type) for date type parameter options
    """
    @abstractmethod
    def __init__(self, date_format: str = '%Y-%m-%d', parent_option_id: Optional[str] = None, 
                 parent_option_ids: Iterable[str] = frozenset()) -> None:
        self.date_format = date_format
        super().__init__(parent_option_id, parent_option_ids)
    
    def _validate_date(self, date_str: Union[str, datetime]) -> datetime:
        try:
            return datetime.strptime(date_str, self.date_format) if isinstance(date_str, str) else date_str
        except ValueError as e:
            raise ConfigurationError(f'Invalid format for date "{date_str}".') from e


@dataclass
class DateParameterOption(DateTypeParameterOption):
    """
    Parameter option for default dates if it varies based on selection of another parameter

    Attributes:
        default_date: Default date for this option
        date_format: Format of the default date, default is '%Y-%m-%d'
        parent_option_id: Identifier of the parent option, or None if this is a top-level option (hidden from print)
        parent_option_ids: Set of parent option ids (only used if parent_option_id is None)
    """
    default_date: Union[str, datetime]
    date_format: str = '%Y-%m-%d'
    parent_option_ids: Iterable[str] = frozenset()

    def __init__(self, default_date: Union[str, datetime], date_format: str = '%Y-%m-%d', 
                 parent_option_id: Optional[str] = None, parent_option_ids: Iterable[str] = frozenset()) -> None:
        super().__init__(date_format, parent_option_id, parent_option_ids)
        self.default_date = self._validate_date(default_date)


@dataclass
class DateRangeParameterOption(DateTypeParameterOption):
    """
    Parameter option for default dates if it varies based on selection of another parameter

    Attributes:
        default_start_date: Default start date for this option
        default_end_date: Default end date for this option
        date_format: Format of the default date, default is '%Y-%m-%d'
        parent_option_id: Identifier of the parent option, or None if this is a top-level option (hidden from print)
        parent_option_ids: Set of parent option ids (only used if parent_option_id is None)
    """
    default_start_date: Union[str, datetime]
    default_end_date: Union[str, datetime]
    date_format: str = '%Y-%m-%d'
    parent_option_ids: Iterable[str] = frozenset()

    def __init__(self, default_start_date: Union[str, datetime], default_end_date: Union[str, datetime], 
                 date_format: str = '%Y-%m-%d', parent_option_id: Optional[str] = None, 
                 parent_option_ids: Iterable[str] = frozenset()) -> None:
        super().__init__(date_format, parent_option_id, parent_option_ids)
        self.default_start_date = self._validate_date(default_start_date)
        self.default_end_date = self._validate_date(default_end_date)
        self._validate_lower_upper_values("default_start_date", self.default_start_date, 
                                    "default_end_date", self.default_end_date)


class NumericParameterOption(ParameterOption, metaclass=ABCMeta):
    """
    Abstract class (or type) for numeric parameter options
    """
    @abstractmethod
    def __init__(self, min_value: Number, max_value: Number, increment: Number,
                 parent_option_id: Optional[str] = None, parent_option_ids: Iterable[str] = frozenset()) -> None:
        super().__init__(parent_option_id, parent_option_ids)
        try:
            self.min_value = Decimal(min_value)
            self.max_value = Decimal(max_value)
            self.increment = Decimal(increment)
        except InvalidDecimalConversion as e:
            raise ConfigurationError(f'Could not convert either min, max, or increment to number') from e
        
        self._validate_lower_upper_values("min_value", self.min_value, "max_value", self.max_value)

        if (self.max_value - self.min_value) % self.increment != 0:
            raise ConfigurationError(f'The increment "{self.increment}" must fit evenly between ' + 
                f'the min_value "{self.min_value}" and max_value "{self.max_value}"')

    def __value_in_range(self, value: Decimal, min_value: Decimal) -> bool:
        return min_value <= value <= self.max_value
    
    def __value_on_increment(self, value: Decimal, min_value: Decimal) -> bool:
        diff = (value - min_value)
        return diff >= 0 and diff % self.increment == 0

    def _validate_value(self, value: Number, min_value: Number = None) -> Decimal:
        min_value = self.min_value if min_value is None else Decimal(min_value)
        try:
            value = Decimal(value)
        except InvalidDecimalConversion as e:
            raise ConfigurationError(f'Could not convert "{value}" to number', e)
        
        if not self.__value_in_range(value, min_value):
            raise ConfigurationError(f'The selected value "{value}" is outside of bounds ' +
                '"{min_value}" and "{self.max_value}"')
        if not self.__value_on_increment(value, min_value):
            raise ConfigurationError(f'The difference between selected value "{value}" and lower value ' +
                '"{min_value}" must be a multiple of increment "{self.increment}"')
        return value


@dataclass
class NumberParameterOption(NumericParameterOption):
    """
    Parameter option for default numbers if it varies based on selection of another parameter

    Attributes:
        min_value: Minimum selectable value
        max_value: Maximum selectable value
        increment: Increment of selectable values, and must fit evenly between min_value and max_value
        default_value: Default value for this option, and must be selectable based on min_value, max_value, and increment
        parent_option_id: Identifier of the parent option, or None if this is a top-level option (hidden from print)
        parent_option_ids: Set of parent option ids (only used if parent_option_id is None)
    """
    min_value: Decimal
    max_value: Decimal
    increment: Decimal
    default_value: Decimal
    parent_option_ids: Iterable[str] = frozenset()

    def __init__(self, min_value: Number, max_value: Number, increment: Number, default_value: Number,
                 parent_option_id: Optional[str] = None, parent_option_ids: Iterable[str] = frozenset()) -> None:
        super().__init__(min_value, max_value, increment, parent_option_id, parent_option_ids)
        self.default_value = self._validate_value(default_value)


@dataclass
class NumRangeParameterOption(NumericParameterOption):
    """
    Parameter option for default numeric ranges if it varies based on selection of another parameter
    
    Attributes:
        min_value: Minimum selectable value
        max_value: Maximum selectable value
        increment: Increment of selectable values, and must fit evenly between min_value and max_value
        default_lower_value: Default lower value for this option, and must be selectable based on min_value, max_value, and increment
        default_upper_value: Default upper value for this option, and must be selectable based on min_value, max_value, and increment. 
                Must also be greater than default_lower_value
        parent_option_id: Identifier of the parent option, or None if this is a top-level option (hidden from print)
        parent_option_ids: Set of parent option ids (only used if parent_option_id is None)
    """
    min_value: Decimal
    max_value: Decimal
    increment: Decimal
    default_lower_value: Decimal
    default_upper_value: Decimal
    parent_option_ids: Iterable[str] = frozenset()

    def __init__(self, min_value: Number, max_value: Number, increment: Number, 
                 default_lower_value: Number, default_upper_value: Number,
                 parent_option_id: Optional[str] = None, parent_option_ids: Iterable[str] = frozenset()) -> None:
        super().__init__(min_value, max_value, increment, parent_option_id, parent_option_ids)
        self.default_lower_value = self._validate_value(default_lower_value)
        self.default_upper_value = self._validate_value(default_upper_value, default_lower_value)
        self._validate_lower_upper_values("default_lower_value", self.default_lower_value, 
                                    "default_upper_value", self.default_upper_value)
