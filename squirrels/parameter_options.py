from typing import Iterable, Optional, Union, Dict, Any
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation as InvalidDecimalConversion
from datetime import datetime

from squirrels._utils import ConfigurationError

Number = Union[Decimal, int, str]


@dataclass
class ParameterOption:
    """
    Abstract class (or type) for parameter options
    """
    def __post_init__(self) -> None:
        if not hasattr(self, "parent_option_ids"):
            self.parent_option_ids = frozenset()
        
        self.parent_option_ids = frozenset(
            {self.parent_option_id} \
            if hasattr(self, "parent_option_id") and self.parent_option_id is not None \
            else self.parent_option_ids
        )

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
    """
    identifier: str
    label: str
    is_default: bool = False
    parent_option_id: Optional[str] = field(default=None, repr=False)
    parent_option_ids: Iterable[str] = frozenset()

    def __init__(self, identifier: str, label: str, *, is_default: bool = False, 
                 parent_option_id: Optional[str] = None, parent_option_ids: Iterable[str] = frozenset(), 
                 custom_fields: Dict[str, Any] = {}, **kwargs):
        """
        Constructor for SelectParameterOption

        Parameters:
            identifier: Unique identifier for this option that never changes over time
            label: Human readable label that gets shown as a dropdown option
            is_default: True if this is a default option, False otherwise
            parent_option_id: Identifier of the parent option, or None if this is a top-level option
            parent_option_ids: Set of parent option ids (only used if parent_option_id is None)
            custom_fields: Dictionary to associate custom attributes to the parameter option
            **kwargs: Any additional keyword arguments specified (except the ones above) gets included into custom_fields as well
        """
        self.identifier = identifier
        self.label = label
        self.is_default = is_default
        self.parent_option_id = parent_option_id
        self.parent_option_ids = parent_option_ids
        self.custom_fields = {
            **kwargs, **custom_fields, "id": identifier, "label": label
        }
        super().__post_init__()

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


@dataclass
class DateParameterOption(ParameterOption):
    """
    Parameter option for default dates if it varies based on selection of another parameter

    Attributes:
        default_date: Default date for this option
        date_format: Format of the default date, default is '%Y-%m-%d'
        parent_option_id: Identifier of the parent option, or None if this is a top-level option
        parent_option_ids: Set of parent option ids (only used if parent_option_id is None)
    """
    default_date: Union[str, datetime]
    date_format: str = '%Y-%m-%d'
    parent_option_id: Optional[str] = field(default=None, repr=False)
    parent_option_ids: Iterable[str] = frozenset()

    def __post_init__(self) -> None:
        super().__post_init__()
        self.default_date = self._validate_date(self.default_date) \
            if isinstance(self.default_date, str) else self.default_date
    
    def _validate_date(self, date_str: str) -> datetime:
        try:
            return datetime.strptime(date_str, self.date_format)
        except ValueError as e:
            raise ConfigurationError(f'Invalid format for date "{date_str}".') from e


@dataclass
class _NumericParameterOption(ParameterOption):
    """
    Abstract class (or type) for numeric parameter options
    """
    min_value: Decimal
    max_value: Decimal
    increment: Decimal

    def __post_init__(self) -> None:
        super().__post_init__()
        try:
            self.min_value = Decimal(self.min_value)
            self.max_value = Decimal(self.max_value)
            self.increment = Decimal(self.increment)
        except InvalidDecimalConversion as e:
            raise ConfigurationError(f'Could not convert either min, max, or increment to number') from e
        
        if self.min_value > self.max_value:
            raise ConfigurationError(f'The min_value "{self.min_value}" must be less than or equal to ' + 
                'the max_value "{self.max_value}"')
        if (self.max_value - self.min_value) % self.increment != 0:
            raise ConfigurationError(f'The increment "{self.increment}" must fit evenly between ' + 
                'the min_value "{self.min_value}" and max_value "{self.max_value}"')

    def _value_in_range(self, value: Decimal, min_value: Decimal) -> bool:
        return min_value <= value <= self.max_value
    
    def _value_on_increment(self, value: Decimal, min_value: Decimal) -> bool:
        diff = (value - min_value)
        return diff >= 0 and diff % self.increment == 0

    def _validate_value(self, value: Number, min_value: Optional[Decimal] = None) -> Decimal:
        min_value = self.min_value if min_value is None else min_value
        try:
            value = Decimal(value)
        except InvalidDecimalConversion as e:
            raise ConfigurationError(f'Could not convert "{value}" to number', e)
        
        if not self._value_in_range(value, min_value):
            raise ConfigurationError(f'The selected value "{value}" is outside of bounds ' +
                '"{min_value}" and "{self.max_value}"')
        if not self._value_on_increment(value, min_value):
            raise ConfigurationError(f'The difference between selected value "{value}" and lower value ' +
                '"{min_value}" must be a multiple of increment "{self.increment}"')
        return value


@dataclass
class NumberParameterOption(_NumericParameterOption):
    """
    Parameter option for default numbers if it varies based on selection of another parameter

    Attributes:
        min_value: Minimum selectable value
        max_value: Maximum selectable value
        increment: Increment of selectable values, and must fit evenly between min_value and max_value
        default_value: Default value for this option, and must be selectable based on min_value, max_value, and increment
        parent_option_id: Identifier of the parent option, or None if this is a top-level option
        parent_option_ids: Set of parent option ids (only used if parent_option_id is None)
    """
    default_value: Decimal
    parent_option_id: Optional[str] = field(default=None, repr=False)
    parent_option_ids: Iterable[str] = frozenset()

    def __post_init__(self) -> None:
        super().__post_init__()
        self.default_value = self._validate_value(self.default_value)


@dataclass
class NumRangeParameterOption(_NumericParameterOption):
    """
    Parameter option for default numeric ranges if it varies based on selection of another parameter
    
    Attributes:
        min_value: Minimum selectable value
        max_value: Maximum selectable value
        increment: Increment of selectable values, and must fit evenly between min_value and max_value
        default_lower_value: Default lower value for this option, and must be selectable based on min_value, max_value, and increment
        default_upper_value: Default upper value for this option, and must be selectable based on min_value, max_value, and increment. 
                Must also be greater than default_lower_value
        parent_option_id: Identifier of the parent option, or None if this is a top-level option
        parent_option_ids: Set of parent option ids (only used if parent_option_id is None)
    """
    default_lower_value: Decimal
    default_upper_value: Decimal
    parent_option_id: Optional[str] = field(default=None, repr=False)
    parent_option_ids: Iterable[str] = frozenset()

    def __post_init__(self) -> None:
        super().__post_init__()
        self.default_lower_value = self._validate_value(self.default_lower_value)
        self.default_upper_value = self._validate_value(self.default_upper_value, self.default_lower_value)


# Types:
NumericParameterOption = Union[NumberParameterOption, NumRangeParameterOption]
