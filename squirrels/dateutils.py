from typing import Sequence, Type
from dataclasses import dataclass
from datetime import date as Date, datetime
from dateutil.relativedelta import relativedelta
from abc import ABCMeta, abstractmethod
from enum import Enum

from . import _utils as u


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


class DateModifier(metaclass=ABCMeta):
    """
    Interface for all Date modification classes, and declares a "modify" method
    """

    @abstractmethod
    def modify(self, date: Date) -> Date:
        """
        Method to be overwritten, modifies the input date

        Parameters:
            date: The input date to modify.

        Returns:
            The modified date.
        """
        pass

    def _get_date(self, datetype: Type, year: int, month: int, day: int) -> Date:
        return datetype(year, month, day)


class _DayIdxOfCalendarUnit(DateModifier):
    """
    Interface for adjusting a date to some day of calendar unit
    """
    def __init__(self, idx: int) -> None:
        super().__init__()
        self.idx = idx
        if self.idx == 0:
            raise u.ConfigurationError(f"For constructors of class names that start with DayIdxOf_, idx cannot be zero")
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
    _num_months_in_cycle: int
    _first_month_of_cycle: Month

    def __init__(self, idx: int, num_months_in_cycle: int, first_month_of_cycle: Month = Month.January) -> None:
        super().__init__(idx)
        self._num_months_in_cycle = num_months_in_cycle
        self._first_month_of_cycle = first_month_of_cycle
        if 12 % self._num_months_in_cycle != 0:
            raise u.ConfigurationError(f"Value X must fit evenly in 12")
        self.first_month_of_first_cycle = (self._first_month_of_cycle.value - 1) % self._num_months_in_cycle + 1

    def modify(self, date: Date) -> Date:
        current_cycle = (date.month - self.first_month_of_first_cycle) % 12 // self._num_months_in_cycle
        first_month_of_curr_cycle = current_cycle * self._num_months_in_cycle + self.first_month_of_first_cycle
        year = date.year if date.month >= first_month_of_curr_cycle else date.year - 1
        first_day = self._get_date(type(date), year, first_month_of_curr_cycle, 1)
        ref_date = first_day if self.idx > 0 else first_day + relativedelta(months=self._num_months_in_cycle)
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

    def __init__(self, idx: int) -> None:
        super().__init__(idx)

    def modify(self, date: Date) -> Date:
        first_day = self._get_date(type(date), date.year, date.month, 1)
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
    _first_day_of_week: DayOfWeek

    def __init__(self, idx: int, first_day_of_week: DayOfWeek = DayOfWeek.Monday) -> None:
        super().__init__(idx)
        self._first_day_of_week = first_day_of_week
        self.first_dow_num = self._first_day_of_week.value
    
    def modify(self, date: Date) -> Date:
        distance_from_first_day = (1 + date.weekday() - self.first_dow_num) % 7
        total_incr = -distance_from_first_day + (7 if self.idx < 0 else 0) + self.incr
        return date + relativedelta(days=total_incr)


class _OffsetUnits(DateModifier):
    """
    Abstract DateModifier class to offset an input date by some number of some calendar unit
    """
    def __init__(self, offset: int) -> None:
        super().__init__()
        self.offset = offset


@dataclass
class OffsetYears(_OffsetUnits):
    """
    DateModifier class to offset an input date by some number of years

    Attributes:
        offset: The number of years to offset the input date.
    """

    def __init__(self, offset: int) -> None:
        super().__init__(offset)

    def modify(self, date: Date) -> Date:
        return date + relativedelta(years=self.offset)
    

@dataclass
class OffsetMonths(_OffsetUnits):
    """
    DateModifier class to offset an input date by some number of months

    Attributes:
        offset: The number of months to offset the input date.
    """

    def __init__(self, offset: int) -> None:
        super().__init__(offset)

    def modify(self, date: Date) -> Date:
        return date + relativedelta(months=self.offset)


@dataclass
class OffsetWeeks(_OffsetUnits):
    """
    DateModifier class to offset an input date by some number of weeks

    Attributes:
        offset: The number of weeks to offset the input date.
    """

    def __init__(self, offset: int) -> None:
        super().__init__(offset)

    def modify(self, date: Date) -> Date:
        return date + relativedelta(weeks=self.offset)


@dataclass
class OffsetDays(_OffsetUnits):
    """
    DateModifier class to offset an input date by some number of days

    Attributes:
        offset: The number of days to offset the input date.
    """

    def __init__(self, offset: int) -> None:
        super().__init__(offset)

    def modify(self, date: Date) -> Date:
        return date + relativedelta(days=self.offset)


@dataclass
class DateModPipeline(DateModifier):
    """
    DateModifier class to apply a list of date modifiers to an input date

    Attributes:
        modifiers: The list of DateModifier's to apply in sequence.
    """
    _date_modifiers: Sequence[DateModifier]

    def __init__(self, date_modifiers: Sequence[DateModifier]) -> None:
        super().__init__()
        self._date_modifiers = tuple(date_modifiers)
    
    def modify(self, date: Date) -> Date:
        for modifier in self._date_modifiers:
            date = modifier.modify(date)
        return date
    
    def get_joined_modifiers(self, date_modifiers: Sequence[DateModifier]) -> Sequence[DateModifier]:
        """
        Create a new sequence of DateModifier by joining the date modifiers in this class 
        with the input date_modifiers

        Parameters:
            date_modifiers: The new date modifier sequence to join

        Returns:
            A new sequence of DateModifier
        """
        joined_modifiers = self._date_modifiers + tuple(date_modifiers)
        return joined_modifiers

    def with_more_modifiers(self, date_modifiers: Sequence[DateModifier]):
        """
        Create a new DateModPipeline with more date modifiers

        Parameters:
            date_modifiers: The additional date modifiers to add

        Returns:
            A new DateModPipeline
        """
        joined_modifiers = self.get_joined_modifiers(date_modifiers)
        return DateModPipeline(joined_modifiers)
    
    def get_date_list(self, start_date: Date, step: _OffsetUnits) -> Sequence[Date]:
        """
        This method modifies the input date, and returns all dates from the input date to the modified date, 
        incremented by a DateModifier step.

        If the step is positive and start date is less than end date, then it'll return an increasing list of
        dates starting from the start date. If the step is positive and start date is less than end date, 
        then it'll return a decreasing list of dates starting from the start date. Otherwise, an empty list
        is returned.

        Parameters:
            start_date: The input date (it's the first date in the output list if step moves towards end date)
            step: The increment to take (specified as an offset DateModifier). Offset cannot be zero

        Returns:
            A list of datetime objects
        """
        if step.offset == 0:
            raise u.ConfigurationError(f"The length of 'step' must not be zero")
        
        output = []
        end_date = self.modify(start_date)
        curr_date = start_date
        is_not_done_positive_step = lambda: curr_date <= end_date and step.offset > 0
        is_not_done_negative_step = lambda: curr_date >= end_date and step.offset < 0
        while is_not_done_positive_step() or is_not_done_negative_step():
            output.append(curr_date)
            curr_date = step.modify(curr_date)
        return output


class _DateRepresentationModifier(metaclass=ABCMeta):
    """
    Abstract class for modifying other representations of dates (such as string or unix timestemp)
    """
    def __init__(self, date_modifiers: Sequence[DateModifier]):
        self.date_modifier = DateModPipeline(date_modifiers)

    @abstractmethod
    def with_more_modifiers(self, date_modifiers: Sequence[DateModifier]):
        pass


@dataclass
class DateStringModifier(_DateRepresentationModifier):
    """
    Class to modify a string representation of a date given a DateModifier

    Attributes:
        date_modifier: The DateModifier to apply on datetime objects
        date_format: Format of the output date string. Default is '%Y-%m-%d'
    """
    _date_modifiers: Sequence[DateModifier]
    _date_format: str

    def __init__(self, date_modifiers: Sequence[DateModifier], date_format: str = '%Y-%m-%d'):
        super().__init__(date_modifiers)
        self._date_format = date_format

    def with_more_modifiers(self, date_modifiers: Sequence[DateModifier]):
        """
        Create a new DateStringModifier with more date modifiers

        Parameters:
            date_modifiers: The additional date modifiers to add

        Returns:
            A new DateStringModifier
        """
        joined_modifiers = self.date_modifier.get_joined_modifiers(date_modifiers)
        return DateStringModifier(joined_modifiers, self._date_format)
    
    def _get_input_date_obj(self, date_str: str, input_format: str = None) -> Date:
        input_format = self._date_format if input_format is None else input_format
        return datetime.strptime(date_str, input_format).date()

    def modify(self, date_str: str, input_format: str = None) -> str:
        """
        Modifies the input date string with the date modifiers

        Parameters:
            date_str: The input date string
            input_format: The input date format. Defaults to the same as output date format
        
        Returns:
            The resulting date string
        """
        date_obj = self._get_input_date_obj(date_str, input_format)
        return self.date_modifier.modify(date_obj).strftime(self._date_format)
    
    def get_date_list(self, start_date_str: str, step: DateModifier, input_format: str = None) -> Sequence[str]:
        """
        This method modifies the input date string, and returns all dates as strings from the input date 
        to the modified date, incremented by a DateModifier step.

        If the step is positive and start date is less than end date, then it'll return an increasing list of
        dates starting from the start date. If the step is positive and start date is less than end date, 
        then it'll return a decreasing list of dates starting from the start date. Otherwise, an empty list
        is returned.

        Parameters:
            start_date_str: The input date string (it's the first date in the output list if step moves towards end date)
            step: The increment to take (specified as an offset DateModifier). Offset cannot be zero
            input_format: The input date format. Defaults to the same as output date format

        Returns:
            A list of date strings
        """
        curr_date = self._get_input_date_obj(start_date_str, input_format)
        output = self.date_modifier.get_date_list(curr_date, step)
        return [x.strftime(self._date_format) for x in output]


@dataclass
class TimestampModifier(_DateRepresentationModifier):
    """
    Class to modify a numeric representation of a date (as Unix/Epoch/POSIX timestamp) given a DateModifier

    Attributes:
        date_modifier: The DateModifier to apply on datetime objects
        date_format: Format of the date string. Default is '%Y-%m-%d'
    """
    _date_modifiers: Sequence[DateModifier]

    def __init__(self, date_modifiers: Sequence[DateModifier]):
        super().__init__(date_modifiers)

    def with_more_modifiers(self, date_modifiers: Sequence[DateModifier]):
        """
        Create a new TimestampModifier with more date modifiers

        Parameters:
            date_modifiers: The additional date modifiers to add

        Returns:
            A new TimestampModifier
        """
        joined_modifiers = self.date_modifier.get_joined_modifiers(date_modifiers)
        return TimestampModifier(joined_modifiers)

    def modify(self, timestamp: float) -> float:
        """
        Modifies the input timestamp with the date modifiers

        Parameters:
            timestamp: The input timestamp as float
        
        Returns:
            The resulting timestamp
        """
        date_obj = datetime.fromtimestamp(timestamp)
        modified_date: datetime = self.date_modifier.modify(date_obj)
        return modified_date.timestamp()
    
    def get_date_list(self, start_timestamp: float, step: DateModifier) -> Sequence[float]:
        """
        This method modifies the input timestamp, and returns all dates as timestampes/floats from the input date 
        to the modified date, incremented by a DateModifier step.

        If the step is positive and start date is less than end date, then it'll return an increasing list of
        dates starting from the start date. If the step is positive and start date is less than end date, 
        then it'll return a decreasing list of dates starting from the start date. Otherwise, an empty list
        is returned.

        Parameters:
            start_timestamp: The input timestamp as float (it's the first date in the output list if step moves towards end date)
            step: The increment to take (specified as an offset DateModifier). Offset cannot be zero

        Returns:
            A list of timestamp as floats
        """
        curr_date = datetime.fromtimestamp(start_timestamp)
        output: Sequence[datetime] = self.date_modifier.get_date_list(curr_date, step)
        return [x.timestamp() for x in output]
