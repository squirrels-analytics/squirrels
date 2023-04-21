from typing import Callable
from datetime import datetime
from dateutil.relativedelta import relativedelta


def operate_on_str_date(reference_date: str, operation: Callable[[datetime], datetime]):
    format = '%Y-%m-%d'
    date = datetime.strptime(reference_date, format)
    return operation(date).strftime(format)


def get_start_of_days(reference_date: str, num_days: str):
    operation = lambda x: x - relativedelta(days=int(num_days)-1)
    return operate_on_str_date(reference_date, operation)


def get_start_of_weeks(reference_date: str, num_weeks: str):
    operation = lambda x: x - relativedelta(days=x.weekday()) - relativedelta(weeks=int(num_weeks)-1)
    return operate_on_str_date(reference_date, operation)


def get_start_of_months(reference_date: str, num_months: str):
    operation = lambda x: datetime(x.year, x.month, 1) - relativedelta(months=int(num_months)-1)
    return operate_on_str_date(reference_date, operation)


def get_start_of_quarters(reference_date: str, num_quarters: str):
    
    def get_quarter_start(d: datetime) -> datetime:
        quarter_start_month = 3 * ((d.month - 1) // 3) + 1
        return datetime(d.year, quarter_start_month, 1)
    
    operation = lambda x: get_quarter_start(x) - relativedelta(months=(int(num_quarters)-1)*3)
    return operate_on_str_date(reference_date, operation)


def get_start_of_years(reference_date: str, num_years: str):
    operation = lambda x: datetime(x.year, 1, 1) - relativedelta(years=int(num_years)-1)
    return operate_on_str_date(reference_date, operation)


def get_today():
    return '2023-01-31'
