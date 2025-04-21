from typing import Sequence, Type
from dataclasses import dataclass
from datetime import date as Date, datetime
from dateutil.relativedelta import relativedelta
from abc import ABCMeta, abstractmethod

from ._enums import DayOfWeekEnum, MonthEnum


class DateModifier(metaclass=ABCMeta):
    """
    Interface for all Date modification classes, and declares a "modify" method
    """

    @abstractmethod
    def modify(self, date: Date) -> Date:
        """
        Method to be overwritten, modifies the input date

        Arguments:
            date: The input date to modify.

        Returns:
            The modified date.
        """
        pass

    def _get_date(self, datetype: Type, year: int, month: int, day: int) -> Date:
        return datetype(year, month, day)


@dataclass
class DayIdxOfCalendarUnit(DateModifier):
    """
    Interface for adjusting a date to some day of calendar unit
    """
    idx: int

    def __post_init__(self) -> None:
        if self.idx == 0:
            raise ValueError(f"For constructors of class names that start with DayIdxOf_, idx cannot be zero")
        self.incr = self.idx - 1 if self.idx > 0 else self.idx


@dataclass
class DayIdxOfMonthsCycle(DayIdxOfCalendarUnit):
    """
    DateModifier class to get the idx-th day of a cycle of months for an input date

    Attributes:
        idx: 1 for first, 2 for second, etc. Or, -1 for last, -2 for second last, etc. Must not be 0
        num_months_in_cycle: 2 for one 6th of year, 3 for Quarter, 4 for one 3rd of year, 6 for half year, 12 for full year. Must fit evenly in 12
        first_month_of_cycle: The first month of months cycle of year. Default is January
    """
    num_months_in_cycle: int
    first_month_of_cycle: MonthEnum = MonthEnum.January

    def __post_init__(self) -> None:
        super().__post_init__()
        if 12 % self.num_months_in_cycle != 0:
            raise ValueError(f"Argument 'num_months_in_cycle' must fit evenly in 12")
        self.first_month_of_first_cycle = (self.first_month_of_cycle.value - 1) % self.num_months_in_cycle + 1

    def modify(self, date: Date) -> Date:
        current_cycle = (date.month - self.first_month_of_first_cycle) % 12 // self.num_months_in_cycle
        first_month_of_curr_cycle = current_cycle * self.num_months_in_cycle + self.first_month_of_first_cycle
        year = date.year if date.month >= first_month_of_curr_cycle else date.year - 1
        first_day = self._get_date(type(date), year, first_month_of_curr_cycle, 1)
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

    def __init__(self, idx: int, first_month_of_year: MonthEnum = MonthEnum.January):
        super().__init__(idx, num_months_in_cycle=12, first_month_of_cycle=first_month_of_year)


@dataclass
class DayIdxOfQuarter(DayIdxOfMonthsCycle):
    """
    DateModifier class to get the idx-th day of quarter of an input date

    Attributes:
        idx: 1 for first, 2 for second, etc. Or, -1 for last, -2 for second last, etc. Must not be 0
        first_month_of_quarter: The first month of first quarter. Default is January
    """

    def __init__(self, idx: int, first_month_of_quarter: MonthEnum = MonthEnum.January):
        super().__init__(idx, num_months_in_cycle=3, first_month_of_cycle=first_month_of_quarter)


@dataclass
class DayIdxOfMonth(DayIdxOfCalendarUnit):
    """
    DateModifier class to get the idx-th day of month of an input date

    Attributes:
        idx: 1 for first, 2 for second, etc. Or, -1 for last, -2 for second last, etc. Must not be 0
    """

    def modify(self, date: Date) -> Date:
        first_day = self._get_date(type(date), date.year, date.month, 1)
        ref_date = first_day if self.idx > 0 else first_day + relativedelta(months=1)
        return ref_date + relativedelta(days=self.incr)


@dataclass
class DayIdxOfWeek(DayIdxOfCalendarUnit):
    """
    DateModifier class to get the idx-th day of week of an input date

    Attributes:
        idx: 1 for first, 2 for second, etc. Or, -1 for last, -2 for second last, etc. Must not be 0
        first_day_of_week: The day of week identified as the "first". Default is Monday
    """
    first_day_of_week: DayOfWeekEnum = DayOfWeekEnum.Monday

    def __post_init__(self) -> None:
        super().__post_init__()
        self.first_dow_num = self.first_day_of_week.value
    
    def modify(self, date: Date) -> Date:
        distance_from_first_day = (1 + date.weekday() - self.first_dow_num) % 7
        total_incr = -distance_from_first_day + (7 if self.idx < 0 else 0) + self.incr
        return date + relativedelta(days=total_incr)


@dataclass
class OffsetUnits(DateModifier):
    """
    Abstract DateModifier class to offset an input date by some number of some calendar unit
    """
    offset: int


@dataclass
class OffsetYears(OffsetUnits):
    """
    DateModifier class to offset an input date by some number of years

    Attributes:
        offset: The number of years to offset the input date.
    """

    def modify(self, date: Date) -> Date:
        return date + relativedelta(years=self.offset)
    

@dataclass
class OffsetMonths(OffsetUnits):
    """
    DateModifier class to offset an input date by some number of months

    Attributes:
        offset: The number of months to offset the input date.
    """

    def modify(self, date: Date) -> Date:
        return date + relativedelta(months=self.offset)


@dataclass
class OffsetWeeks(OffsetUnits):
    """
    DateModifier class to offset an input date by some number of weeks

    Attributes:
        offset: The number of weeks to offset the input date.
    """

    def modify(self, date: Date) -> Date:
        return date + relativedelta(weeks=self.offset)


@dataclass
class OffsetDays(OffsetUnits):
    """
    DateModifier class to offset an input date by some number of days

    Attributes:
        offset: The number of days to offset the input date.
    """

    def modify(self, date: Date) -> Date:
        return date + relativedelta(days=self.offset)


@dataclass
class DateModPipeline(DateModifier):
    """
    DateModifier class to apply a list of date modifiers to an input date

    Attributes:
        modifiers: The list of DateModifier's to apply in sequence.
    """
    date_modifiers: Sequence[DateModifier]

    def modify(self, date: Date) -> Date:
        for modifier in self.date_modifiers:
            date = modifier.modify(date)
        return date
    
    def get_joined_modifiers(self, date_modifiers: Sequence[DateModifier]) -> Sequence[DateModifier]:
        """
        Create a new sequence of DateModifier by joining the date modifiers in this class 
        with the input date_modifiers

        Arguments:
            date_modifiers: The new date modifier sequence to join

        Returns:
            A new sequence of DateModifier
        """
        joined_modifiers = tuple(self.date_modifiers) + tuple(date_modifiers)
        return joined_modifiers

    def with_more_modifiers(self, date_modifiers: Sequence[DateModifier]):
        """
        Create a new DateModPipeline with more date modifiers

        Arguments:
            date_modifiers: The additional date modifiers to add

        Returns:
            A new DateModPipeline
        """
        joined_modifiers = self.get_joined_modifiers(date_modifiers)
        return DateModPipeline(joined_modifiers)
    
    def get_date_list(self, start_date: Date, step: DateModifier) -> Sequence[Date]:
        """
        This method modifies the input date, and returns all dates from the input date to the modified date, 
        incremented by a DateModifier step.

        If the step is positive and start date is less than end date, then it'll return an increasing list of
        dates starting from the start date. If the step is negative and start date is greater than end date, 
        then it'll return a decreasing list of dates starting from the start date. Otherwise, an empty list
        is returned.

        Arguments:
            start_date: The input date (it's the first date in the output list if step moves towards end date)
            step: The increment to take (specified as an offset DateModifier). Offset cannot be zero

        Returns:
            A list of datetime objects
        """
        assert isinstance(step, OffsetUnits)
        if step.offset == 0:
            raise ValueError(f"The length of 'step' must not be zero")
        
        output: Sequence[Date] = []
        end_date = self.modify(start_date)
        curr_date = start_date
        is_not_done_positive_step = lambda: curr_date <= end_date and step.offset > 0
        is_not_done_negative_step = lambda: curr_date >= end_date and step.offset < 0
        while is_not_done_positive_step() or is_not_done_negative_step():
            output.append(curr_date)
            curr_date = step.modify(curr_date)
        return output


@dataclass
class DateRepresentationModifier(metaclass=ABCMeta):
    """
    Abstract class for modifying other representations of dates (such as string or unix timestemp)
    """
    date_modifiers: Sequence[DateModifier]

    def __post_init__(self) -> None:
        self.date_mod_pipeline = DateModPipeline(self.date_modifiers)

    @abstractmethod
    def with_more_modifiers(self, date_modifiers: Sequence[DateModifier]):
        pass


@dataclass
class DateStringModifier(DateRepresentationModifier):
    """
    Class to modify a string representation of a date given a DateModifier

    Attributes:
        date_modifier: The DateModifier to apply on datetime objects
        date_format: Format of the output date string. Default is '%Y-%m-%d'
    """
    date_format: str = '%Y-%m-%d'

    def with_more_modifiers(self, date_modifiers: Sequence[DateModifier]):
        """
        Create a new DateStringModifier with more date modifiers

        Arguments:
            date_modifiers: The additional date modifiers to add

        Returns:
            A new DateStringModifier
        """
        joined_modifiers = self.date_mod_pipeline.get_joined_modifiers(date_modifiers)
        return DateStringModifier(joined_modifiers, self.date_format)
    
    def _get_input_date_obj(self, date_str: str, input_format: str | None = None) -> Date:
        input_format = self.date_format if input_format is None else input_format
        return datetime.strptime(date_str, input_format).date()

    def modify(self, date_str: str, input_format: str | None = None) -> str:
        """
        Modifies the input date string with the date modifiers

        Arguments:
            date_str: The input date string
            input_format: The input date format. Defaults to the same as output date format
        
        Returns:
            The resulting date string
        """
        date_obj = self._get_input_date_obj(date_str, input_format)
        return self.date_mod_pipeline.modify(date_obj).strftime(self.date_format)
    
    def get_date_list(self, start_date_str: str, step: DateModifier, input_format: str | None = None) -> Sequence[str]:
        """
        This method modifies the input date string, and returns all dates as strings from the input date 
        to the modified date, incremented by a DateModifier step.

        If the step is positive and start date is less than end date, then it'll return an increasing list of
        dates starting from the start date. If the step is negative and start date is greater than end date, 
        then it'll return a decreasing list of dates starting from the start date. Otherwise, an empty list
        is returned.

        Arguments:
            start_date_str: The input date string (it's the first date in the output list if step moves towards end date)
            step: The increment to take (specified as an offset DateModifier). Offset cannot be zero
            input_format: The input date format. Defaults to the same as output date format

        Returns:
            A list of date strings
        """
        assert isinstance(step, OffsetUnits)
        curr_date = self._get_input_date_obj(start_date_str, input_format)
        output = self.date_mod_pipeline.get_date_list(curr_date, step)
        return [x.strftime(self.date_format) for x in output]


@dataclass
class TimestampModifier(DateRepresentationModifier):
    """
    Class to modify a numeric representation of a date (as Unix/Epoch/POSIX timestamp) given a DateModifier

    Attributes:
        date_modifier: The DateModifier to apply on datetime objects
        date_format: Format of the date string. Default is '%Y-%m-%d'
    """

    def with_more_modifiers(self, date_modifiers: Sequence[DateModifier]):
        """
        Create a new TimestampModifier with more date modifiers

        Arguments:
            date_modifiers: The additional date modifiers to add

        Returns:
            A new TimestampModifier
        """
        joined_modifiers = self.date_mod_pipeline.get_joined_modifiers(date_modifiers)
        return TimestampModifier(joined_modifiers)

    def modify(self, timestamp: float) -> float:
        """
        Modifies the input timestamp with the date modifiers

        Arguments:
            timestamp: The input timestamp as float
        
        Returns:
            The resulting timestamp
        """
        date_obj = datetime.fromtimestamp(timestamp).date()
        modified_date = self.date_mod_pipeline.modify(date_obj)
        modified_datetime = datetime.combine(modified_date, datetime.min.time())
        return modified_datetime.timestamp()
    
    def get_date_list(self, start_timestamp: float, step: DateModifier) -> Sequence[float]:
        """
        This method modifies the input timestamp, and returns all dates as timestampes/floats from the input date 
        to the modified date, incremented by a DateModifier step.

        If the step is positive and start date is less than end date, then it'll return an increasing list of
        dates starting from the start date. If the step is negative and start date is greater than end date, 
        then it'll return a decreasing list of dates starting from the start date. Otherwise, an empty list
        is returned.

        Arguments:
            start_timestamp: The input timestamp as float (it's the first date in the output list if step moves towards end date)
            step: The increment to take (specified as an offset DateModifier). Offset cannot be zero

        Returns:
            A list of timestamp as floats
        """
        curr_date = datetime.fromtimestamp(start_timestamp).date()
        output = self.date_mod_pipeline.get_date_list(curr_date, step)
        return [datetime.combine(x, datetime.min.time()).timestamp() for x in output]
