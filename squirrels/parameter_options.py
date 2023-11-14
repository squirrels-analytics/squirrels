from typing import Set, Iterable, Optional, Union, Dict, Any
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation as InvalidDecimalConversion
from datetime import datetime
from abc import ABCMeta, abstractmethod

from ._utils import ConfigurationError

Number = Union[Decimal, int, str]


@dataclass
class ParameterOption(metaclass=ABCMeta):
    """
    Abstract class for parameter options
    """
    user_groups: Set[str]
    parent_option_ids: Set[str]

    @abstractmethod
    def __init__(self, *, user_groups: Iterable[str] = frozenset(), parent_option_ids: Iterable[str] = frozenset()) -> None:
        self.user_groups = frozenset(user_groups)
        self.parent_option_ids = frozenset(parent_option_ids)

    def _validate_lower_upper_values(self, lower_label: str, lower_value: Union[Decimal, datetime], 
                                     upper_label: str, upper_value: Union[Decimal, datetime]):
        if lower_value > upper_value:
            raise ConfigurationError(f'The {lower_label} "{lower_value}" must be less than or equal to the {upper_label} "{upper_value}"')

    def _is_valid(self, user_group: Optional[str], selected_parent_option_ids: Optional[Iterable[str]]) -> bool:
        """
        Checks if this option is valid given the selected parent options and user group of user if applicable.
        
        Parameters:
            user_group: The value of the user's "user group attribute". Only None when "user_group_attr" is not specified
                for the Parameter object. Note that when user is None but "user_group_attr" is specified, an error is thrown
            selected_parent_option_ids: List of selected option ids from the parent parameter. Only None when the Parameter
                object has no parent parameter.
        
        Returns:
            True if valid, False otherwise
        """
        if user_group is not None and user_group not in self.user_groups:
            return False

        if selected_parent_option_ids is not None and self.parent_option_ids.isdisjoint(selected_parent_option_ids):
            return False
        
        return True
    
    @abstractmethod
    def _to_json_dict(self):
        return {}


@dataclass
class SelectParameterOption(ParameterOption):
    """
    Parameter option for a select parameter

    Attributes:
        identifier: Unique identifier for this option that never changes over time
        label: Human readable label that gets shown as a dropdown option
        is_default: True if this is a default option, False otherwise
        user_groups: The user groups this parameter option would show for if "user_group_attr" is specified in the Parameter object
        parent_option_ids: Set of parent option ids this parameter option would show for if "parent" is specified in the Parameter object
        custom_fields: Dictionary to associate custom attributes to the parameter option
    """
    identifier: str
    label: str
    is_default: bool
    custom_fields: Dict[str, Any]

    def __init__(self, identifier: str, label: str, *, is_default: bool = False, user_groups: Iterable[str] = frozenset(), 
                 parent_option_ids: Iterable[str] = frozenset(), custom_fields: Dict[str, Any] = {}, **kwargs):
        """
        Constructor for SelectParameterOption

        Parameters:
            ...see Attributes of SelectParameterOption
            **kwargs: Any additional keyword arguments specified (except the ones above) gets included into custom_fields as well
        """
        super().__init__(user_groups=user_groups, parent_option_ids=parent_option_ids)
        self.identifier = identifier
        self.label = label
        self.is_default = is_default
        self.custom_fields = {
            **kwargs, **custom_fields, **self._to_json_dict()
        }

    def get_custom_field(self, field: str, *, default_field: Optional[str] = None, default: Any = None) -> Any:
        """
        Get field value from the custom_fields attribute

        Parameters:
            field: The key to use to fetch the custom field from "custom_fields"
            default_field: If value at "field" key does not exist in "custom_fields", then this is used instead as the field (if not None)
            default: If value at "field" or "default_field" (if not None) key does not exist in "custom_fields", then this value 
                is used as default, or throws an error if None
        
        Returns:
            The type of the custom field
        """
        if default_field is not None:
            default = self.get_custom_field(default_field, default=default)
        
        if default is not None:
            selected_field = self.custom_fields.get(field, default)
        else:
            try:
                selected_field = self.custom_fields[field]
            except KeyError as e:
                raise ConfigurationError(f"Field '{field}' must exist for parameter option {self._to_json_dict()}") from e
        
        return selected_field
    
    def _to_json_dict(self):
        return {'id': self.identifier, 'label': self.label}


@dataclass
class _DateTypeParameterOption(ParameterOption):
    """
    Abstract class (or type) for date type parameter options
    """
    date_format: str

    @abstractmethod
    def __init__(self, *, date_format: str = '%Y-%m-%d', user_groups: Iterable[str] = frozenset(), 
                 parent_option_ids: Iterable[str] = frozenset()) -> None:
        super().__init__(user_groups=user_groups, parent_option_ids=parent_option_ids)
        self.date_format = date_format
    
    def _validate_date(self, date_str: Union[str, datetime]) -> datetime:
        try:
            return datetime.strptime(date_str, self.date_format) if isinstance(date_str, str) else date_str
        except ValueError as e:
            raise ConfigurationError(f'Invalid format for date "{date_str}".') from e
    
    def _to_json_dict(self):
        return {}


@dataclass
class DateParameterOption(_DateTypeParameterOption):
    """
    Parameter option for default dates if it varies based on selection of another parameter

    Attributes:
        default_date: Default date for this option
        date_format: Format of the default date, default is '%Y-%m-%d'
        user_groups: The user groups this parameter option would show for if "user_group_attr" is specified in the Parameter object
        parent_option_ids: Set of parent option ids this parameter option would show for if "parent" is specified in the Parameter object
    """
    default_date: datetime

    def __init__(self, default_date: Union[str, datetime], *, date_format: str = '%Y-%m-%d', user_groups: Iterable[str] = frozenset(), 
                 parent_option_ids: Iterable[str] = frozenset()) -> None:
        """
        Constructor for DateParameterOption

        Parameters:
            ...see Attributes of DateParameterOption
        """
        super().__init__(date_format=date_format, user_groups=user_groups, parent_option_ids=parent_option_ids)
        self.default_date = self._validate_date(default_date)


@dataclass
class DateRangeParameterOption(_DateTypeParameterOption):
    """
    Parameter option for default dates if it varies based on selection of another parameter

    Attributes:
        default_start_date: Default start date for this option
        default_end_date: Default end date for this option
        date_format: Format of the default date, default is '%Y-%m-%d'
        user_groups: The user groups this parameter option would show for if "user_group_attr" is specified in the Parameter object
        parent_option_ids: Set of parent option ids this parameter option would show for if "parent" is specified in the Parameter object
    """
    default_start_date: datetime
    default_end_date: datetime

    def __init__(self, default_start_date: Union[str, datetime], default_end_date: Union[str, datetime], *, date_format: str = '%Y-%m-%d', 
                 user_groups: Iterable[str] = frozenset(), parent_option_ids: Iterable[str] = frozenset()) -> None:
        """
        Constructor for DateRangeParameterOption

        Parameters:
            ...see Attributes of DateRangeParameterOption
        """
        super().__init__(date_format=date_format, user_groups=user_groups, parent_option_ids=parent_option_ids)
        self.default_start_date = self._validate_date(default_start_date)
        self.default_end_date = self._validate_date(default_end_date)
        self._validate_lower_upper_values("default_start_date", self.default_start_date, "default_end_date", self.default_end_date)


@dataclass
class _NumericParameterOption(ParameterOption):
    """
    Abstract class (or type) for numeric parameter options
    """
    min_value: Decimal
    max_value: Decimal
    increment: Decimal

    @abstractmethod
    def __init__(self, min_value: Number, max_value: Number, *, increment: Number = 1, user_groups: Iterable[str] = frozenset(),
                 parent_option_ids: Iterable[str] = frozenset()) -> None:
        super().__init__(user_groups=user_groups, parent_option_ids=parent_option_ids)
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

    def __value_in_range(self, value: Decimal) -> bool:
        return self.min_value <= value <= self.max_value
    
    def __value_on_increment(self, value: Decimal) -> bool:
        diff = (value - self.min_value)
        return diff >= 0 and diff % self.increment == 0

    def _validate_value(self, value: Number) -> Decimal:
        try:
            value = Decimal(value)
        except InvalidDecimalConversion as e:
            raise ConfigurationError(f'Could not convert "{value}" to number', e)
        
        if not self.__value_in_range(value):
            raise ConfigurationError(f'The selected value "{value}" is outside of bounds ' +
                '"{min_value}" and "{self.max_value}"')
        if not self.__value_on_increment(value):
            raise ConfigurationError(f'The difference between selected value "{value}" and lower value ' +
                '"{min_value}" must be a multiple of increment "{self.increment}"')
        return value
    
    def _to_json_dict(self):
        return {
            "min_value": str(self.min_value),
            "max_value": str(self.max_value),
            "increment": str(self.increment)
        }


@dataclass
class NumberParameterOption(_NumericParameterOption):
    """
    Parameter option for default numbers if it varies based on selection of another parameter

    Attributes:
        min_value: Minimum selectable value
        max_value: Maximum selectable value
        increment: Increment of selectable values, and must fit evenly between min_value and max_value
        default_value: Default value for this option, and must be selectable based on min_value, max_value, and increment
        user_groups: The user groups this parameter option would show for if "user_group_attr" is specified in the Parameter object
        parent_option_ids: Set of parent option ids this parameter option would show for if "parent" is specified in the Parameter object
    """
    default_value: Decimal

    def __init__(self, min_value: Number, max_value: Number, *, increment: Number = 1, default_value: Optional[Number] = None,
                 user_groups: Iterable[str] = frozenset(), parent_option_ids: Iterable[str] = frozenset()) -> None:
        """
        Constructor for NumberParameterOption

        Parameters:
            ...see Attributes of NumberParameterOption
        """
        super().__init__(min_value, max_value, increment=increment, user_groups=user_groups, parent_option_ids=parent_option_ids)
        self.default_value = self._validate_value(default_value) if default_value is not None else self.min_value


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
        user_groups: The user groups this parameter option would show for if "user_group_attr" is specified in the Parameter object
        parent_option_ids: Set of parent option ids this parameter option would show for if "parent" is specified in the Parameter object
    """
    default_lower_value: Decimal
    default_upper_value: Decimal

    def __init__(self, min_value: Number, max_value: Number, *, increment: Number = 1, default_lower_value: Optional[Number] = None, 
                 default_upper_value: Optional[Number] = None, user_groups: Iterable[str] = frozenset(), 
                 parent_option_ids: Iterable[str] = frozenset()) -> None:
        """
        Constructor for NumRangeParameterOption

        Parameters:
            ...see Attributes of NumRangeParameterOption
        """
        super().__init__(min_value, max_value, increment=increment, user_groups=user_groups, parent_option_ids=parent_option_ids)
        self.default_lower_value = self._validate_value(default_lower_value) if default_lower_value is not None else self.min_value
        self.default_upper_value = self._validate_value(default_upper_value) if default_upper_value is not None else self.max_value
        self._validate_lower_upper_values("default_lower_value", self.default_lower_value, "default_upper_value", self.default_upper_value)
