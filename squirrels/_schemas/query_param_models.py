"""
Query model generation utilities for API routes
"""
from typing import Annotated
from dataclasses import make_dataclass
from fastapi import Depends
from pydantic import create_model

from .._parameter_configs import APIParamFieldInfo


def _get_query_models_helper(widget_parameters: list[str] | None, predefined_params: list[APIParamFieldInfo], param_fields: dict):
    """Helper function to generate query models"""
    if widget_parameters is None:
        widget_parameters = list(param_fields.keys())
    
    QueryModelForGetRaw = make_dataclass("QueryParams", [
        param_fields[param].as_query_info() for param in widget_parameters
    ] + [param.as_query_info() for param in predefined_params])
    QueryModelForGet = Annotated[QueryModelForGetRaw, Depends()]

    field_definitions = {param: param_fields[param].as_body_info() for param in widget_parameters}
    for param in predefined_params:
        field_definitions[param.name] = param.as_body_info()
    QueryModelForPost = create_model("RequestBodyParams", **field_definitions) # type: ignore
    return QueryModelForGet, QueryModelForPost


def get_query_models_for_parameters(widget_parameters: list[str] | None, param_fields: dict):
    """Generate query models for parameter endpoints"""
    predefined_params = [
        APIParamFieldInfo("x_verify_params", bool, default=False, description="If true, the query parameters are verified to be valid for the dataset"),
        APIParamFieldInfo("x_parent_param", str, description="The parameter name used for parameter updates. If not provided, then all parameters are retrieved"),
    ]
    return _get_query_models_helper(widget_parameters, predefined_params, param_fields)


def get_query_models_for_dataset(widget_parameters: list[str] | None, param_fields: dict):
    """Generate query models for dataset endpoints"""
    predefined_params = [
        APIParamFieldInfo("x_verify_params", bool, default=False, description="If true, the query parameters are verified to be valid for the dataset"),
        APIParamFieldInfo("x_orientation", str, default="records", description="The orientation of the data to return, one of: 'records', 'rows', or 'columns'"),
        APIParamFieldInfo("x_select", list[str], examples=[[]], description="The columns to select from the dataset. All are returned if not specified"), 
        APIParamFieldInfo("x_offset", int, default=0, description="The number of rows to skip before returning data (applied after data caching)"),
        APIParamFieldInfo("x_limit", int, default=1000, description="The maximum number of rows to return (applied after data caching and offset)"),
    ]
    return _get_query_models_helper(widget_parameters, predefined_params, param_fields)


def get_query_models_for_dashboard(widget_parameters: list[str] | None, param_fields: dict):
    """Generate query models for dashboard endpoints"""
    predefined_params = [
        APIParamFieldInfo("x_verify_params", bool, default=False, description="If true, the query parameters are verified to be valid for the dashboard"),
    ]
    return _get_query_models_helper(widget_parameters, predefined_params, param_fields)


def get_query_models_for_querying_models(param_fields: dict):
    """Generate query models for querying data models"""
    predefined_params = [
        APIParamFieldInfo("x_verify_params", bool, default=False, description="If true, the query parameters are verified to be valid"),
        APIParamFieldInfo("x_orientation", str, default="records", description="The orientation of the data to return, one of: 'records', 'rows', or 'columns'"),
        APIParamFieldInfo("x_offset", int, default=0, description="The number of rows to skip before returning data (applied after data caching)"),
        APIParamFieldInfo("x_limit", int, default=1000, description="The maximum number of rows to return (applied after data caching and offset)"),
        APIParamFieldInfo("x_sql_query", str, description="The SQL query to execute on the data models"),
    ]
    return _get_query_models_helper(None, predefined_params, param_fields) 
