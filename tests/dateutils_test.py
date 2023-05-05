from typing import List
from datetime import datetime
from functools import partial
import pytest

from squirrels import dateutils as d


class TestDayIdxOfMonthsCycle:
    @pytest.mark.parametrize('num_months_in_cycle,first_month,input_date,expected_first_date,expected_last_date', [
        (4, d.Month.March, datetime(2024,2,15), datetime(2023,11,1), datetime(2024,2,29)),
        (4, d.Month.March, datetime(2023,6,15), datetime(2023,3,1), datetime(2023,6,30)),
        (6, d.Month.October, datetime(2023,10,1), datetime(2023,10,1), datetime(2024,3,31)),
        (6, d.Month.October, datetime(2023,1,25), datetime(2022,10,1), datetime(2023,3,31)),
        (6, d.Month.October, datetime(2023,9,30), datetime(2023,4,1), datetime(2023,9,30))
    ])
    def test_modify(self, num_months_in_cycle: int, first_month: d.Month, input_date: datetime, 
                    expected_first_date: datetime, expected_last_date: datetime):
        PartialClass = partial(d.DayIdxOfMonthsCycle, num_months_in_cycle=num_months_in_cycle, first_month_of_cycle=first_month)
        assert PartialClass(idx=1).modify(input_date) == expected_first_date
        assert PartialClass(idx=-1).modify(input_date) == expected_last_date


class TestDayIdxOfYear:
    @pytest.mark.parametrize('first_month,input_date,expected_first_date,expected_last_date', [
        (d.Month.March, datetime(2024,2,15), datetime(2023,3,1), datetime(2024,2,29)),
        (d.Month.March, datetime(2023,6,15), datetime(2023,3,1), datetime(2024,2,29)),
        (d.Month.November, datetime(2023,11,1), datetime(2023,11,1), datetime(2024,10,31)),
        (d.Month.June, datetime(2023,1,25), datetime(2022,6,1), datetime(2023,5,31)),
        (d.Month.February, datetime(2023,1,31), datetime(2022,2,1), datetime(2023,1,31))
    ])
    def test_modify(self, first_month: d.Month, input_date: datetime, 
                    expected_first_date: datetime, expected_last_date: datetime):
        PartialClass = partial(d.DayIdxOfYear, first_month_of_year=first_month)
        assert PartialClass(idx=1).modify(input_date) == expected_first_date
        assert PartialClass(idx=-1).modify(input_date) == expected_last_date


class TestDayIdxOfQuarter:
    @pytest.mark.parametrize('first_month,input_date,expected_first_date,expected_last_date', [
        (d.Month.March, datetime(2024,2,15), datetime(2023,12,1), datetime(2024,2,29)),
        (d.Month.March, datetime(2023,6,15), datetime(2023,6,1), datetime(2023,8,31)),
        (d.Month.November, datetime(2023,11,1), datetime(2023,11,1), datetime(2024,1,31)),
        (d.Month.June, datetime(2023,1,25), datetime(2022,12,1), datetime(2023,2,28)),
        (d.Month.May, datetime(2023,1,31), datetime(2022,11,1), datetime(2023,1,31))
    ])
    def test_modify(self, first_month: d.Month, input_date: datetime, 
                    expected_first_date: datetime, expected_last_date: datetime):
        PartialClass = partial(d.DayIdxOfQuarter, first_month_of_quarter=first_month)
        assert PartialClass(idx=1).modify(input_date) == expected_first_date
        assert PartialClass(idx=-1).modify(input_date) == expected_last_date


class TestDayIdxOfMonth:
    @pytest.mark.parametrize('input_date,expected_first_date,expected_last_date', [
        (datetime(2024,2,15), datetime(2024,2,1), datetime(2024,2,29)),
        (datetime(2023,2,1), datetime(2023,2,1), datetime(2023,2,28)),
        (datetime(2023,11,30), datetime(2023,11,1), datetime(2023,11,30))
    ])
    def test_modify(self, input_date: datetime, expected_first_date: datetime, expected_last_date: datetime):
        assert d.DayIdxOfMonth(idx=1).modify(input_date) == expected_first_date
        assert d.DayIdxOfMonth(idx=-1).modify(input_date) == expected_last_date


class TestDayIdxOfWeek:
    @pytest.mark.parametrize('first_day_of_week,input_date,expected_first_date,expected_last_date', [
        (d.DayOfWeek.Monday, datetime(2023,5,3), datetime(2023,5,1), datetime(2023,5,7)),
        (d.DayOfWeek.Wednesday, datetime(2023,5,3), datetime(2023,5,3), datetime(2023,5,9)),
        (d.DayOfWeek.Thursday, datetime(2023,5,3), datetime(2023,4,27), datetime(2023,5,3))
    ])
    def test_modify(self, first_day_of_week: d.DayOfWeek, input_date: datetime, 
                    expected_first_date: datetime, expected_last_date: datetime):
        PartialClass = partial(d.DayIdxOfWeek, first_day_of_week=first_day_of_week)
        assert PartialClass(idx=1).modify(input_date) == expected_first_date
        assert PartialClass(idx=-1).modify(input_date) == expected_last_date


class TestDateModPipeline:
    @pytest.mark.parametrize('modifiers,input_date,expected_date', [
        ([d.DayIdxOfQuarter(1), d.DayIdxOfWeek(-1), d.OffsetMonths(-2)], datetime(2023,5,15), datetime(2023,2,2)),
    ])
    def test_modify(self, modifiers: List[d.DateModifier], input_date: datetime, expected_date: datetime):
        assert d.DateModPipeline(modifiers).modify(input_date) == expected_date


class TestDateStringModifier:
    @pytest.mark.parametrize('modifiers,input_date,expected_date', [
        ([d.DayIdxOfQuarter(1), d.DayIdxOfWeek(-1), d.OffsetMonths(-2)], "2023-05-15", "2023-02-02"),
    ])
    def test_modify(self, modifiers: List[d.DateModifier], input_date: str, expected_date: str):
        assert d.DateStringModifier(modifiers).modify(input_date) == expected_date


class TestTimestampModifier:
    @pytest.mark.parametrize('modifiers,input_date,expected_date', [
        ([d.DayIdxOfQuarter(1), d.DayIdxOfWeek(-1), d.OffsetMonths(-2)], 1684123200, 1675314000),
    ])
    def test_modify(self, modifiers: List[d.DateModifier], input_date: str, expected_date: str):
        assert d.TimestampModifier(modifiers).modify(input_date) == expected_date
