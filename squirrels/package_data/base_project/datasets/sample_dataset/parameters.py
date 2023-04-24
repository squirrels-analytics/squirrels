import squirrels as sq


def main(*args, **kwargs) -> sq.ParameterSet:
    select_options = (
        sq.SelectParameterOption('x0', 'Red'),
        sq.SelectParameterOption('x1', 'Green'),
        sq.SelectParameterOption('x2', 'Blue')
    )
    
    return sq.ParameterSet([
        sq.SingleSelectParameter('single_select_example', 'Single Select Color', select_options),
        sq.MultiSelectParameter('multi_select_example', 'Multi Select Colors', select_options),
        sq.DateParameter('date_example', 'As Of Date', '2020-01-01'),
        sq.NumberParameter('number_example', 'Upper Bound', min_value=1, max_value=10, increment=1, 
                           default_value=5),
        sq.RangeParameter('range_example', 'Some Range', min_value=1, max_value=10, increment=1, 
                          default_lower_value=1, default_upper_value=6)
    ])
