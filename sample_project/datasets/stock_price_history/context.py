from typing import Dict, Any, Callable
from datasets.stock_price_history.parameters import NumPeriodsParameter, TimeOfYearParameter, TimeUnitParameterOption
import squirrels as sq

def main(prms: Callable[[str], sq.Parameter]) -> Dict[str, Any]:
    time_unit_param: sq.SingleSelectParameter = prms('time_unit')
    reference_date_param: sq.DateParameter = prms('reference_date')
    num_periods_param: NumPeriodsParameter = prms('num_periods')
    time_of_year_param: TimeOfYearParameter = prms('time_of_year')

    selected_time_unit: TimeUnitParameterOption = time_unit_param.get_selected()
    date_bucket: str = selected_time_unit.date_bucket_column
    curr_date: str = reference_date_param.get_selected_date()
    length: NumPeriodsParameter = num_periods_param.get_selected_value()
    start_date: str = selected_time_unit.get_start_date(curr_date, length)
    applicable_months: str = time_of_year_param.get_selected_months()

    return {
        'date_bucket': date_bucket,
        'start_date':  start_date,
        'applicable_months': applicable_months
    }