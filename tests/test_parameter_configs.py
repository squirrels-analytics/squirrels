from squirrels.parameter_configs import *
import importlib

param_module = importlib.import_module("datasets.stock_price_history.parameters")


def test_parameters_to_dict():
    parameters = ParameterSet(param_module.main())

    name = 'reference_date'
    reference_date_dict = {
        'widget_type': 'DateField',
        'name': name,
        'label': 'Reference Date',
        'selected_date': '2023-01-31'
    }
    assert(parameters.get_parameter_by_name(name)._to_dict(name) == reference_date_dict)

    name = 'time_unit'
    time_unit_dict = {
        'widget_type': 'SingleSelect',
        'name': name,
        'label': 'Time Unit',
        'options': [
            {'id': '0', 'label': 'Days'}, 
            {'id': '1', 'label': 'Weeks'}, 
            {'id': '2', 'label': 'Months'}, 
            {'id': '3', 'label': 'Quarters'}, 
            {'id': '4', 'label': 'Years'}
        ],
        'selected_id': '0',
        'trigger_refresh': True
    }
    assert(parameters.get_parameter_by_name(name)._to_dict(name) == time_unit_dict)
    
    name = 'num_periods'
    num_periods_dict = {
        'widget_type': 'NumberField',
        'name': name,
        'label': 'Number of Time Units',
        'min_value': '7',
        'max_value': '364',
        'increment': '7',
        'selected_value': '28'
    }
    assert(parameters.get_parameter_by_name(name)._to_dict(name) == num_periods_dict)

    name = 'time_of_year'
    time_of_year_dict = {
        'widget_type': 'MultiSelect',
        'name': name,
        'label': 'Time of Year',
        'options': [],
        'selected_ids': [],
        'trigger_refresh': False,
        'include_all': True,
        'order_matters': False
    }
    assert(parameters.get_parameter_by_name(name)._to_dict(name) == time_of_year_dict)

    name = 'ticker'
    ticker_dict = {
        'widget_type': 'MultiSelect',
        'name': name,
        'label': 'Ticker',
        'data_source': {
            'table_or_query': 'lu_tickers',
            'id_col': 'ticker_id',
            'options_col': 'ticker',
            'order_by_col': 'ticker_order',
            'is_default_col': None,
            'parent_id_col': None,
            'is_cond_default_col': None
        }
    }
    assert(parameters.get_parameter_by_name(name)._to_dict(name) == ticker_dict)

    parameters_dict = { 'parameters': [
        reference_date_dict, time_unit_dict, num_periods_dict, time_of_year_dict, ticker_dict
    ] }
    assert(parameters._to_dict() == parameters_dict)


def test_convert_datasource_params():
    parameters = ParameterSet(param_module.main())
    parameters._convert_datasource_params('product_profile')

    ticker_dict = {
        'widget_type': 'MultiSelect',
        'name': 'ticker',
        'label': 'Ticker',
        'options': [
            {'id': '0', 'label': 'AAPL'}, 
            {'id': '1', 'label': 'AMZN'}, 
            {'id': '2', 'label': 'GOOG'}, 
            {'id': '3', 'label': 'MSFT'}
        ],
        'selected_ids': [],
        'trigger_refresh': False,
        'include_all': True,
        'order_matters': False
    }
    assert(parameters.get_parameter_by_name('ticker')._to_dict('ticker') == ticker_dict)


def test_refresh():
    parameters = ParameterSet(param_module.main())
    parent_parm: SingleSelectParameter = parameters.get_parameter_by_name('time_unit')
    parent_parm.set_selection('3')
    time_of_year_parm: MultiSelectParameter = parameters.get_parameter_by_name('time_of_year')
    time_of_year_parm.refresh(parameters)
    expected_time_of_year_options =  [
        ParameterOption('13', 'Q1'),
        ParameterOption('14', 'Q2'),
        ParameterOption('15', 'Q3'),
        ParameterOption('16', 'Q4')
    ]
    assert([x._to_dict() for x in time_of_year_parm.options] == [x._to_dict() for x in expected_time_of_year_options])
    assert(time_of_year_parm.get_selected_ids() == '13, 14, 15, 16')
    