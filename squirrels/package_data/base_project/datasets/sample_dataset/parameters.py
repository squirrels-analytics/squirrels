from typing import Dict, Any
import squirrels as sq


def main(args: Dict[str, Any], *p_args, **kwargs) -> sq.ParameterSet:
    single_select_options = (
        sq.SelectParameterOption('a0', 'Primary Colors'),
        sq.SelectParameterOption('a1', 'Secondary Colors')
    )
    
    multi_select_options = (
        sq.SelectParameterOption('x0', 'Red',    parent_option_id='a0'),
        sq.SelectParameterOption('x1', 'Yellow', parent_option_id='a0'),
        sq.SelectParameterOption('x2', 'Blue',   parent_option_id='a0'),
        sq.SelectParameterOption('x3', 'Green',  parent_option_id='a1'),
        sq.SelectParameterOption('x4', 'Orange', parent_option_id='a1'),
        sq.SelectParameterOption('x5', 'Purple', parent_option_id='a1')
    )

    single_select_example = sq.SingleSelectParameter('color_type', 'Color Type', single_select_options)

    multi_select_example = sq.MultiSelectParameter('colors', 'Colors', multi_select_options,
                                                   parent=single_select_example)
    
    date_example = sq.DateParameter('as_of_date', 'As Of Date', '2020-01-01')
    
    number_example = sq.NumberParameter('upper_bound', 'Upper Bound', min_value=1, max_value=10, 
                                        increment=1, default_value=5)
    
    return sq.ParameterSet(
        [single_select_example, multi_select_example, date_example, number_example]
    )
