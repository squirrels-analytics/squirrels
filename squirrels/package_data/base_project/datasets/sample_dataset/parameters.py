from typing import Dict, Any
import squirrels as sr


def main(args: Dict[str, Any], *p_args, **kwargs) -> sr.ParameterSet:
    single_select_options = (
        sr.SelectParameterOption('a0', 'Primary Colors'),
        sr.SelectParameterOption('a1', 'Secondary Colors')
    )
    
    multi_select_options = (
        sr.SelectParameterOption('x0', 'Red',    parent_option_id='a0'),
        sr.SelectParameterOption('x1', 'Yellow', parent_option_id='a0'),
        sr.SelectParameterOption('x2', 'Blue',   parent_option_id='a0'),
        sr.SelectParameterOption('x3', 'Green',  parent_option_id='a1'),
        sr.SelectParameterOption('x4', 'Orange', parent_option_id='a1'),
        sr.SelectParameterOption('x5', 'Purple', parent_option_id='a1')
    )

    single_select_example = sr.SingleSelectParameter('color_type', 'Color Type', single_select_options)

    multi_select_example = sr.MultiSelectParameter('colors', 'Colors', multi_select_options,
                                                   parent=single_select_example)
    
    date_example = sr.DateParameter('as_of_date', 'As Of Date', '2020-01-01')
    
    number_example = sr.NumberParameter('upper_bound', 'Upper Bound', min_value=1, max_value=10, 
                                        increment=1, default_value=5)
    
    return sr.ParameterSet(
        [single_select_example, multi_select_example, date_example, number_example]
    )
