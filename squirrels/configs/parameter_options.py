from typing import Iterable, Set, Optional, Union
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation as InvalidDecimalConversion
from datetime import datetime

from squirrels.utils import ConfigurationError

Number = Union[Decimal, int, str]


@dataclass
class ParameterOption:
    def __post_init__(self) -> None:
        if not hasattr(self, "parent_option_ids"):
            self.parent_option_ids = frozenset()
        
        self.parent_option_ids = frozenset({self.parent_option_id}) \
            if hasattr(self, "parent_option_id") and self.parent_option_id is not None \
            else self.parent_option_ids

    def is_valid(self, selected_parent_option_ids: Optional[Iterable[str]] = None):
        if selected_parent_option_ids is not None:
            return not self.parent_option_ids.isdisjoint(selected_parent_option_ids)
        else:
            return True


@dataclass
class SelectParameterOption(ParameterOption):
    identifier: str
    label: str
    is_default: bool = False
    parent_option_id: Optional[str] = field(default=None, repr=False)
    parent_option_ids: Set[str] = frozenset()

    def to_dict(self):
        return {'id': self.identifier, 'label': self.label}


@dataclass
class DateParameterOption(ParameterOption):
    default_date: Union[str, datetime]
    format: str = '%Y-%m-%d'
    parent_option_id: Optional[str] = field(default=None, repr=False)
    parent_option_ids: Set[str] = frozenset()

    def __post_init__(self) -> None:
        super().__post_init__()
        self.default_date = self._validate_date(self.default_date) \
            if isinstance(self.default_date, str) else self.default_date
    
    def _validate_date(self, date_str: str) -> datetime:
        try:
            return datetime.strptime(date_str, self.format)
        except ValueError as e:
            raise ConfigurationError(f'Invalid format for date "{date_str}".') from e


@dataclass
class _NumericParameterOption(ParameterOption):
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
            raise ConfigurationError(f'The min_value "{self.min_value}" must be less than or equal to \
                                     the max_value "{self.max_value}"')
        if (self.max_value - self.min_value) % self.increment != 0:
            raise ConfigurationError(f'The increment "{self.increment}" must fit evenly between \
                                     the min_value "{self.min_value}" and max_value "{self.max_value}"')

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
            raise ConfigurationError(f'The selected value "{value}" is outside of bounds \
                                     "{min_value}" and "{self.max_value}"')
        if not self._value_on_increment(value, min_value):
            raise ConfigurationError(f'The difference between selected value "{value}" and lower value \
                                     "{min_value}" must be a multiple of increment "{self.increment}"')
        return value


@dataclass
class NumberParameterOption(_NumericParameterOption):
    default_value: Decimal
    parent_option_id: Optional[str] = field(default=None, repr=False)
    parent_option_ids: Set[str] = frozenset()

    def __post_init__(self) -> None:
        super().__post_init__()
        self.default_value = self._validate_value(self.default_value)


@dataclass
class RangeParameterOption(_NumericParameterOption):
    default_lower_value: Decimal
    default_upper_value: Decimal
    parent_option_id: Optional[str] = field(default=None, repr=False)
    parent_option_ids: Set[str] = frozenset()

    def __post_init__(self) -> None:
        super().__post_init__()
        self.default_lower_value = self._validate_value(self.default_lower_value)
        self.default_upper_value = self._validate_value(self.default_upper_value, self.default_lower_value)


# Types:
NumericParameterOption = Union[NumberParameterOption, RangeParameterOption]
