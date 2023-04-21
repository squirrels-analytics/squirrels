from typing import List, Dict
from parameter_options import *

# Define time of year parameter class
class TimeOfYearParameter(sq.MultiSelectParameter):
    def __init__(self, label: str, options: List[TimeOfYearOption], parent: str):
        super().__init__(label, options, parent=parent)
    
    def get_selected_months(self):
        def quote_months(comma_values: str):
            return ','.join(f"'{val}'" for val in comma_values.split(','))
        return ', '.join(quote_months(x.applicable_months) for x in self.get_selected_list())


# Define number of periods parameter class
class NumPeriodsParameter(sq.NumberParameter):
    def __init__(self, label: str):
        super().__init__(label, 1, 1, 1, 1) # most parameters taken care of in refresh method
        self.parent = 'time_unit'
    
    def refresh(self, all_parameters: sq.ParameterSet):
        super().refresh(all_parameters)
        time_unit: sq.SingleSelectParameter = all_parameters[self.parent]
        time_unit.trigger_refresh = True
        selected_time_unit: TimeUnitParameterOption = time_unit.get_selected()
        self.min_value = selected_time_unit.min_num_periods
        self.max_value = selected_time_unit.max_num_periods
        self.increment = selected_time_unit.num_periods_increment
        self.default_value = selected_time_unit.default_num_periods
        self.selected_value = self.default_value


# Define parameters
def main() -> Dict[str, sq.Parameter]:
    return {
        'reference_date': sq.DateParameter('Reference Date', get_today()),
        'time_unit':      sq.SingleSelectParameter('Time Unit', time_unit_options),
        'num_periods':    NumPeriodsParameter('Number of Time Units'),
        'time_of_year':   TimeOfYearParameter('Time of Year', time_of_year_options, parent='time_unit'),
        'ticker':         sq.DataSourceParameter(sq.WidgetType.MultiSelect, 'Ticker', ticker_data_source)
    }
