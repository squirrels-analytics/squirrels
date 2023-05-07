from typing import Sequence
from dataclasses import dataclass
from datetime import datetime
from dateutil.relativedelta import relativedelta
from enum import Enum

from squirrels import utils


class DayOfWeek(Enum):
    Sunday = 0
    Monday = 1
    Tuesday = 2
    Wednesday = 3
    Thursday = 4
    Friday = 5
    Saturday = 6

class Month(Enum):
    January = 1
    February = 2
    March = 3
    April = 4
    May = 5
    June = 6
    July = 7
    August = 8
    September = 9
    October = 10
    November = 11
    December = 12


@dataclass
class DateModifier:
    """
    Interface for all Date modification classes, and declares a "modify" method
    """

    def modify(self, date: datetime) -> datetime:
        """
        Method to be overwritten, modifies the input date

        Parameters:
            date: The input date to modify.

        Returns:
            The modified date.
        """
        raise utils.AbstractMethodCallError(self.__class__, "modify")


@dataclass
class _DayIdxOfCalendarUnit(DateModifier):
    """
    Interface for adjusting a date to some day of calendar unit
    """
    idx: int

    def __post_init__(self):
        if self.idx == 0:
            raise utils.ConfigurationError("The idx attribute of any DateModifier object cannot be zero")
        self.incr = self.idx - 1 if self.idx > 0 else self.idx


@dataclass
class DayIdxOfMonthsCycle(_DayIdxOfCalendarUnit):
    """
    DateModifier class to get the idx-th day of a cycle of months for an input date

    Attributes:
        idx: 1 for first, 2 for second, etc. Or, -1 for last, -2 for second last, etc. Must not be 0
        num_months_in_cycle: 2 for one 6th of year, 3 for Quarter, 4 for one 3rd of year, 6 for half year, 12 for full year. Must fit evenly in 12
        first_month_of_cycle: The first month of months cycle of year. Default is January
    """
    num_months_in_cycle: int
    first_month_of_cycle: Month = Month.January

    def __post_init__(self):
        super().__post_init__()
        if 12 % self.num_months_in_cycle != 0:
            raise utils.ConfigurationError("Value X must fit evenly in 12")
        self.first_month_of_first_cycle = (self.first_month_of_cycle.value - 1) % self.num_months_in_cycle + 1

    def modify(self, date: datetime) -> datetime:
        current_cycle = (date.month - self.first_month_of_first_cycle) % 12 // self.num_months_in_cycle
        first_month_of_curr_cycle = current_cycle * self.num_months_in_cycle + self.first_month_of_first_cycle
        year = date.year if date.month >= first_month_of_curr_cycle else date.year - 1
        first_day = datetime(year, first_month_of_curr_cycle, 1)
        ref_date = first_day if self.idx > 0 else first_day + relativedelta(months=self.num_months_in_cycle)
        return ref_date + relativedelta(days=self.incr)


@dataclass
class DayIdxOfYear(DayIdxOfMonthsCycle):
    """
    DateModifier class to get the idx-th day of year of an input date

    Attributes:
        idx: 1 for first, 2 for second, etc. Or, -1 for last, -2 for second last, etc. Must not be 0
        first_month_of_year: The first month of year. Default is January
    """

    def __init__(self, idx: int, first_month_of_year: Month = Month.January):
        super().__init__(idx, num_months_in_cycle=12, first_month_of_cycle=first_month_of_year)


@dataclass
class DayIdxOfQuarter(DayIdxOfMonthsCycle):
    """
    DateModifier class to get the idx-th day of quarter of an input date

    Attributes:
        idx: 1 for first, 2 for second, etc. Or, -1 for last, -2 for second last, etc. Must not be 0
        first_month_of_quarter: The first month of first quarter. Default is January
    """

    def __init__(self, idx: int, first_month_of_quarter: Month = Month.January):
        super().__init__(idx, num_months_in_cycle=3, first_month_of_cycle=first_month_of_quarter)


@dataclass
class DayIdxOfMonth(_DayIdxOfCalendarUnit):
    """
    DateModifier class to get the idx-th day of month of an input date

    Attributes:
        idx: 1 for first, 2 for second, etc. Or, -1 for last, -2 for second last, etc. Must not be 0
    """

    def modify(self, date: datetime) -> datetime:
        first_day = datetime(date.year, date.month, 1)
        ref_date = first_day if self.idx > 0 else first_day + relativedelta(months=1)
        return ref_date + relativedelta(days=self.incr)


@dataclass
class DayIdxOfWeek(_DayIdxOfCalendarUnit):
    """
    DateModifier class to get the idx-th day of week of an input date

    Attributes:
        idx: 1 for first, 2 for second, etc. Or, -1 for last, -2 for second last, etc. Must not be 0
        first_day_of_week: The day of week identified as the "first". Default is Monday
    """
    first_day_of_week: DayOfWeek = DayOfWeek.Monday

    def __post_init__(self):
        super().__post_init__()
        self.first_dow_num = self.first_day_of_week.value
    
    def modify(self, date: datetime) -> datetime:
        distance_from_first_day = (1 + date.weekday() - self.first_dow_num) % 7
        total_incr = -distance_from_first_day + (7 if self.idx < 0 else 0) + self.incr
        return date + relativedelta(days=total_incr)


@dataclass
class _OffsetUnits(DateModifier):
    """
    Abstract DateModifier class to offset an input date by some number of some calendar unit
    """
    offset: int


@dataclass
class OffsetYears(_OffsetUnits):
    """
    DateModifier class to offset an input date by some number of years

    Attributes:
        offset: The number of years to offset the input date.
    """

    def modify(self, date: datetime) -> datetime:
        return date + relativedelta(years=self.offset)
    

@dataclass
class OffsetMonths(_OffsetUnits):
    """
    DateModifier class to offset an input date by some number of months

    Attributes:
        offset: The number of months to offset the input date.
    """

    def modify(self, date: datetime) -> datetime:
        return date + relativedelta(months=self.offset)


@dataclass
class OffsetWeeks(_OffsetUnits):
    """
    DateModifier class to offset an input date by some number of weeks

    Attributes:
        offset: The number of weeks to offset the input date.
    """

    def modify(self, date: datetime) -> datetime:
        return date + relativedelta(weeks=self.offset)


@dataclass
class OffsetDays(_OffsetUnits):
    """
    DateModifier class to offset an input date by some number of days

    Attributes:
        offset: The number of days to offset the input date.
    """

    def modify(self, date: datetime) -> datetime:
        return date + relativedelta(days=self.offset)


@dataclass
class DateModPipeline(DateModifier):
    """
    DateModifier class to apply a list of date modifiers to an input date

    Attributes:
        modifiers: The list of DateModifier's to apply in sequence.
    """
    modifiers: Sequence[DateModifier]

    def __post_init__(self):
        self.modifiers = tuple(self.modifiers)
    
    def modify(self, date: datetime) -> datetime:
        for modifier in self.modifiers:
            date = modifier.modify(date)
        return date


@dataclass
class _DateRepresentationModifier:
    """
    Abstract class for modifying other representations of dates (such as string or unix timestemp)
    """
    def __init__(self, date_modifiers: Sequence[DateModifier]):
        self.date_modifier = DateModPipeline(date_modifiers)
    
    def _get_joined_modifiers(self, date_modifiers: Sequence[DateModifier]) -> Sequence[DateModifier]:
        joined_modifiers = self.date_modifier.modifiers + tuple(date_modifiers)
        return joined_modifiers

    def with_more_modifiers(self, date_modifiers: Sequence[DateModifier]):
        raise utils.AbstractMethodCallError(self.__class__, "with_more_modifiers")


@dataclass
class DateStringModifier(_DateRepresentationModifier):
    """
    Class to modify a string representation of a date given a DateModifier

    Attributes:
        date_modifier: The DateModifier to apply on datetime objects
        date_format: Format of the date string. Default is '%Y-%m-%d'
    """
    def __init__(self, date_modifiers: Sequence[DateModifier], date_format: str = '%Y-%m-%d'):
        super().__init__(date_modifiers)
        self.date_format = date_format

    def with_more_modifiers(self, date_modifiers: Sequence[DateModifier]):
        joined_modifiers = self._get_joined_modifiers(date_modifiers)
        return DateStringModifier(joined_modifiers, self.date_format)

    def modify(self, date_str: str) -> str:
        date_obj = datetime.strptime(date_str, self.date_format)
        return self.date_modifier.modify(date_obj).strftime(self.date_format)


@dataclass
class TimestampModifier(_DateRepresentationModifier):
    """
    Class to modify a numeric representation of a date (as Unix/Epoch/POSIX timestamp) given a DateModifier

    Attributes:
        date_modifier: The DateModifier to apply on datetime objects
        date_format: Format of the date string. Default is '%Y-%m-%d'
    """
    def __init__(self, date_modifiers: Sequence[DateModifier]):
        super().__init__(date_modifiers)

    def with_more_modifiers(self, date_modifiers: Sequence[DateModifier]):
        joined_modifiers = self._get_joined_modifiers(date_modifiers)
        return TimestampModifier(joined_modifiers)

    def modify(self, timestamp: float) -> float:
        date_obj = datetime.fromtimestamp(timestamp)
        return self.date_modifier.modify(date_obj).timestamp()
