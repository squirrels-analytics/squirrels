from datetime import datetime, date
from functools import partial
import pytest

from squirrels import dateutils as d


class TestDayIdxOfMonthsCycle:
    @pytest.mark.parametrize('num_months_in_cycle,first_month,input_date,expected_first_date,expected_last_date', [
        (4, d.Month.March, date(2024,2,15), date(2023,11,1), date(2024,2,29)),
        (4, d.Month.March, datetime(2023,6,15), datetime(2023,3,1), datetime(2023,6,30)),
        (6, d.Month.October, datetime(2023,10,1), datetime(2023,10,1), datetime(2024,3,31)),
        (6, d.Month.October, date(2023,1,25), date(2022,10,1), date(2023,3,31)),
        (6, d.Month.October, datetime(2023,9,30), datetime(2023,4,1), datetime(2023,9,30))
    ])
    def test_modify(self, num_months_in_cycle: int, first_month: d.Month, input_date: date, expected_first_date: date, 
                    expected_last_date: date):
        PartialClass = partial(d.DayIdxOfMonthsCycle, num_months_in_cycle=num_months_in_cycle, first_month_of_cycle=first_month)
        assert PartialClass(idx=1).modify(input_date) == expected_first_date
        assert PartialClass(idx=-1).modify(input_date) == expected_last_date


class TestDayIdxOfYear:
    @pytest.mark.parametrize('first_month,input_date,expected_first_date,expected_last_date', [
        (d.Month.March, date(2024,2,15), date(2023,3,1), date(2024,2,29)),
        (d.Month.March, datetime(2023,6,15), datetime(2023,3,1), datetime(2024,2,29)),
        (d.Month.November, datetime(2023,11,1), datetime(2023,11,1), datetime(2024,10,31)),
        (d.Month.June, date(2023,1,25), date(2022,6,1), date(2023,5,31)),
        (d.Month.February, datetime(2023,1,31), datetime(2022,2,1), datetime(2023,1,31))
    ])
    def test_modify(self, first_month: d.Month, input_date: date, expected_first_date: date, expected_last_date: date):
        PartialClass = partial(d.DayIdxOfYear, first_month_of_year=first_month)
        assert PartialClass(idx=1).modify(input_date) == expected_first_date
        assert PartialClass(idx=-1).modify(input_date) == expected_last_date


class TestDayIdxOfQuarter:
    @pytest.mark.parametrize('first_month,input_date,expected_first_date,expected_last_date', [
        (d.Month.March, date(2024,2,15), date(2023,12,1), date(2024,2,29)),
        (d.Month.March, datetime(2023,6,15), datetime(2023,6,1), datetime(2023,8,31)),
        (d.Month.November, datetime(2023,11,1), datetime(2023,11,1), datetime(2024,1,31)),
        (d.Month.June, date(2023,1,25), date(2022,12,1), date(2023,2,28)),
        (d.Month.May, datetime(2023,1,31), datetime(2022,11,1), datetime(2023,1,31))
    ])
    def test_modify(self, first_month: d.Month, input_date: date, expected_first_date: date, expected_last_date: date):
        PartialClass = partial(d.DayIdxOfQuarter, first_month_of_quarter=first_month)
        assert PartialClass(idx=1).modify(input_date) == expected_first_date
        assert PartialClass(idx=-1).modify(input_date) == expected_last_date


class TestDayIdxOfMonth:
    @pytest.mark.parametrize('input_date,expected_first_date,expected_last_date', [
        (datetime(2024,2,15), datetime(2024,2,1), datetime(2024,2,29)),
        (date(2023,2,1), date(2023,2,1), date(2023,2,28)),
        (datetime(2023,11,30), datetime(2023,11,1), datetime(2023,11,30))
    ])
    def test_modify(self, input_date: date, expected_first_date: date, expected_last_date: date):
        assert d.DayIdxOfMonth(idx=1).modify(input_date) == expected_first_date
        assert d.DayIdxOfMonth(idx=-1).modify(input_date) == expected_last_date


class TestDayIdxOfWeek:
    @pytest.mark.parametrize('first_day_of_week,input_date,expected_first_date,expected_last_date', [
        (d.DayOfWeek.Monday, datetime(2023,5,3), datetime(2023,5,1), datetime(2023,5,7)),
        (d.DayOfWeek.Wednesday, date(2023,5,3), date(2023,5,3), date(2023,5,9)),
        (d.DayOfWeek.Thursday, datetime(2023,5,3), datetime(2023,4,27), datetime(2023,5,3))
    ])
    def test_modify(self, first_day_of_week: d.DayOfWeek, input_date: date, expected_first_date: date, expected_last_date: date):
        PartialClass = partial(d.DayIdxOfWeek, first_day_of_week=first_day_of_week)
        assert PartialClass(idx=1).modify(input_date) == expected_first_date
        assert PartialClass(idx=-1).modify(input_date) == expected_last_date


class TestDateModPipeline:
    @pytest.mark.parametrize('modifiers,input_date,expected_date', [
        ([d.DayIdxOfQuarter(1), d.DayIdxOfWeek(-1), d.OffsetMonths(-2)], datetime(2023,5,15), datetime(2023,2,2)),
    ])
    def test_modify(self, modifiers: list[d.DateModifier], input_date: date, expected_date: date):
        assert d.DateModPipeline(modifiers).modify(input_date) == expected_date


class TestDateStringModifier:
    @pytest.mark.parametrize('modifiers,input_format,output_format,input_date,expected_date', [
        ([d.DayIdxOfQuarter(1), d.DayIdxOfWeek(-1), d.OffsetMonths(-2)], "%m-%d-%Y", "%Y%m%d", "05-15-2023", "20230202"),
        ([d.DayIdxOfQuarter(1), d.DayIdxOfWeek(-1), d.OffsetMonths(-2)], None, "%Y%m%d", "20230515", "20230202"),
    ])
    def test_modify(self, modifiers: list[d.DateModifier], input_format: str, output_format: str, input_date: str, expected_date: str):
        assert d.DateStringModifier(modifiers, output_format).modify(input_date, input_format) == expected_date
    
    @pytest.mark.parametrize('modifiers,more_modifiers,input_date,expected_date1,expected_date2', [
        ([d.DayIdxOfQuarter(1)], (d.DayIdxOfWeek(-1), d.OffsetMonths(-2)), "2023-05-15", "2023-04-01", "2023-02-02"),
    ])
    def test_with_more_modifiers(self, modifiers: list[d.DateModifier], more_modifiers: list[d.DateModifier], 
                                 input_date: str, expected_date1: str, expected_date2: str):
        date_str_modifier = d.DateStringModifier(modifiers)
        new_date_str_modifier = date_str_modifier.with_more_modifiers(more_modifiers)
        assert date_str_modifier.modify(input_date) == expected_date1
        assert new_date_str_modifier.modify(input_date) == expected_date2
    
    @pytest.mark.parametrize('modifiers,step,input_date,expected_dates', [
        ([d.DayIdxOfWeek(-1), d.OffsetMonths(1)], d.OffsetWeeks(1), "2023-05-17", 
         ["2023-05-17", "2023-05-24", "2023-05-31", "2023-06-07", "2023-06-14", "2023-06-21"]),
        ([d.DayIdxOfWeek(-1), d.OffsetMonths(1)], d.OffsetWeeks(1), "2023-05-18", 
         ["2023-05-18", "2023-05-25", "2023-06-01", "2023-06-08", "2023-06-15"]),
        ([d.DayIdxOfWeek(-1), d.OffsetMonths(-1)], d.OffsetWeeks(1), "2023-06-14", []),
        ([d.DayIdxOfWeek(-1), d.OffsetMonths(-1)], d.OffsetWeeks(-1), "2023-06-14", 
         ["2023-06-14", "2023-06-07", "2023-05-31", "2023-05-24"]),
        ([d.DayIdxOfWeek(-1), d.OffsetMonths(-1)], d.OffsetWeeks(-1), "2023-06-15", 
         ["2023-06-15", "2023-06-08", "2023-06-01", "2023-05-25", "2023-05-18"]),
    ])
    def test_get_date_list(self, modifiers: list[d.DateModifier], step: d.DateModifier, 
                           input_date: str, expected_dates: list[str]):
        date_str_modifier = d.DateStringModifier(modifiers)
        assert date_str_modifier.get_date_list(input_date, step) == expected_dates


class TestTimestampModifier:
    @pytest.mark.parametrize('modifiers,input_date,expected_date', [
        ([d.DayIdxOfQuarter(1), d.DayIdxOfWeek(-1), d.OffsetMonths(-2)], 1684123200, 1675314000),
    ])
    def test_modify(self, modifiers: list[d.DateModifier], input_date: float, expected_date: float):
        assert d.TimestampModifier(modifiers).modify(input_date) == expected_date
    
    @pytest.mark.parametrize('modifiers,more_modifiers,input_date,expected_date1,expected_date2', [
        ([d.DayIdxOfQuarter(1)], (d.DayIdxOfWeek(-1), d.OffsetMonths(-2)), 1684123200, 1680321600, 1675314000),
    ])
    def test_with_more_modifiers(self, modifiers: list[d.DateModifier], more_modifiers: list[d.DateModifier], 
                                 input_date: float, expected_date1: float, expected_date2: float):
        date_str_modifier = d.TimestampModifier(modifiers)
        new_date_str_modifier = date_str_modifier.with_more_modifiers(more_modifiers)
        assert date_str_modifier.modify(input_date) == expected_date1
        assert new_date_str_modifier.modify(input_date) == expected_date2
