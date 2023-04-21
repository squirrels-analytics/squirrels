from functions import *
import squirrels as sq


# Define options for Date Range parameter
class TimeUnitParameterOption(sq.ParameterOption):
    def __init__(self, identifier: str, label: str, get_start_date: Callable[[str, str], str], date_bucket_column: str, 
                 min_num_periods: int, max_num_periods: int, default_num_periods: int, num_periods_increment: int = 1):
        super().__init__(identifier, label)
        self.get_start_date = get_start_date
        self.date_bucket_column = date_bucket_column
        self.min_num_periods = min_num_periods
        self.max_num_periods = max_num_periods
        self.default_num_periods = default_num_periods
        self.num_periods_increment = num_periods_increment

time_unit_options = [
    TimeUnitParameterOption('0', 'Days', get_start_of_days, 'trading_date', 7, 364, 28, 7),
    TimeUnitParameterOption('1', 'Weeks', get_start_of_weeks, 'week_value', 1, 260, 52),
    TimeUnitParameterOption('2', 'Months', get_start_of_months, 'month_value', 1, 120, 12),
    TimeUnitParameterOption('3', 'Quarters', get_start_of_quarters, 'quarter_value', 1, 40, 8),
    TimeUnitParameterOption('4', 'Years', get_start_of_years, 'year_value', 1, 10, 5)
]


# Define data source for Ticker parameter
ticker_data_source = sq.OptionsDataSource('lu_tickers', 'ticker_id', 'ticker', 'ticker_order')


# Define options for time of year
class TimeOfYearOption(sq.ParameterOption):
    def __init__(self, identifier: str, label: str, applicable_months: str, parent_id: str):
        super().__init__(identifier, label, parent_id=parent_id)
        self.applicable_months = applicable_months

time_of_year_options = [
    TimeOfYearOption('1',  'January',  '01', parent_id='2'),
    TimeOfYearOption('2',  'February', '02', parent_id='2'),
    TimeOfYearOption('3',  'March',    '03', parent_id='2'),
    TimeOfYearOption('4',  'April',    '04', parent_id='2'),
    TimeOfYearOption('5',  'May',      '05', parent_id='2'),
    TimeOfYearOption('6',  'June',     '06', parent_id='2'),
    TimeOfYearOption('7',  'July',     '07', parent_id='2'),
    TimeOfYearOption('8',  'August',   '08', parent_id='2'),
    TimeOfYearOption('9',  'September','09', parent_id='2'),
    TimeOfYearOption('10', 'October',  '10', parent_id='2'),
    TimeOfYearOption('11', 'November', '11', parent_id='2'),
    TimeOfYearOption('12', 'December', '12', parent_id='2'),
    TimeOfYearOption('13', 'Q1', '01,02,03', parent_id='3'),
    TimeOfYearOption('14', 'Q2', '04,05,06', parent_id='3'),
    TimeOfYearOption('15', 'Q3', '07,08,09', parent_id='3'),
    TimeOfYearOption('16', 'Q4', '10,11,12', parent_id='3')
]