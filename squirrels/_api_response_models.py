from typing import Annotated, Literal
from pydantic import BaseModel, Field
from datetime import datetime, date

from . import _model_configs as mc, _sources as s


class LoginReponse(BaseModel):
    access_token: Annotated[str, Field(examples=["encoded_jwt_token"], description="An encoded JSON web token to use subsequent API requests")]
    token_type: Annotated[str, Field(examples=["bearer"], description='Always "bearer" for Bearer token')]
    username: Annotated[str, Field(examples=["johndoe"], description='The username authenticated with from the form data')]
    is_admin: Annotated[bool, Field(examples=[False], description="A boolean for whether the user is an admin")]
    expiry_time: Annotated[datetime, Field(examples=["2023-08-01T12:00:00.000000Z"], description="The expiry time of the access token in yyyy-MM-dd'T'hh:mm:ss.SSSSSS'Z' format")]


## Parameters Response Models

class ParameterOptionModel(BaseModel):
    id: Annotated[str, Field(examples=["my_option_id"], description="The unique identifier for the option")]
    label: Annotated[str, Field(examples=["My Option"], description="The human-friendly display name for the option")]

class ParameterModelBase(BaseModel):
    widget_type: Annotated[str, Field(examples=["disabled"], description="The parameter type")]
    name: Annotated[str, Field(examples=["my_unique_param_name"], description="The name of the parameter. Use this as the key when providing the API request parameters")]
    label: Annotated[str, Field(examples=["My Parameter"], description="The human-friendly display name for the parameter")]
    description: Annotated[str, Field(examples=[""], description="The description of the parameter")]

class NoneParameterModel(ParameterModelBase):
    pass

class SelectParameterModel(ParameterModelBase):
    options: Annotated[list[ParameterOptionModel], Field(description="The list of dropdown options as JSON objects containing 'id' and 'label' fields")]
    trigger_refresh: Annotated[bool, Field(description="A boolean that's set to true for parent parameters that require a new parameters API call when the selection changes")]

class SingleSelectParameterModel(SelectParameterModel):
    widget_type: Annotated[str, Field(examples=["single_select"], description="The parameter type (set to 'single_select' for this model)")]
    selected_id: Annotated[str | None, Field(examples=["my_option_id"], description="The ID of the selected / default option")]

class MultiSelectParameterModel(SelectParameterModel):
    widget_type: Annotated[str, Field(examples=["multi_select"], description="The parameter type (set to 'multi_select' for this model)")]
    show_select_all: Annotated[bool, Field(description="A boolean for whether there should be a toggle to select all options")]
    order_matters: Annotated[bool, Field(description="A boolean for whether the ordering of the input selections would affect the result of the dataset")]
    selected_ids: Annotated[list[str], Field(examples=[["my_option_id"]], description="A list of ids of the selected / default options")]

class _DateTypeParameterModel(ParameterModelBase):
    min_date: Annotated[date | None, Field(examples=["2023-01-01"], description='A string in "yyyy-MM-dd" format for the minimum date')]
    max_date: Annotated[date | None, Field(examples=["2023-12-31"], description='A string in "yyyy-MM-dd" format for the maximum date')]

class DateParameterModel(_DateTypeParameterModel):
    widget_type: Annotated[str, Field(examples=["date"], description="The parameter type (set to 'date' for this model)")]
    selected_date: Annotated[date, Field(examples=["2023-01-01"], description='A string in "yyyy-MM-dd" format for the selected / default date')]

class DateRangeParameterModel(_DateTypeParameterModel):
    widget_type: Annotated[str, Field(examples=["date_range"], description="The parameter type (set to 'date_range' for this model)")]
    selected_start_date: Annotated[date, Field(examples=["2023-01-01"], description='A string in "yyyy-MM-dd" format for the selected / default start date')]
    selected_end_date: Annotated[date, Field(examples=["2023-12-31"], description='A string in "yyyy-MM-dd" format for the selected / default end date')]

class _NumericParameterModel(ParameterModelBase):
    min_value: Annotated[float, Field(examples=[0], description="A number for the lower bound of the selectable number")]
    max_value: Annotated[float, Field(examples=[10], description="A number for the upper bound of the selectable number")]
    increment: Annotated[float, Field(examples=[1], description="A number for the selectable increments between the lower bound and upper bound")]

class NumberParameterModel(_NumericParameterModel):
    widget_type: Annotated[str, Field(examples=["number"], description="The parameter type (set to 'number' for this model)")]
    selected_value: Annotated[float, Field(examples=[2], description="A number for the selected / default number")]

class NumberRangeParameterModel(_NumericParameterModel):
    widget_type: Annotated[str, Field(examples=["number_range"], description="The parameter type (set to 'number_range' for this model)")]
    selected_lower_value: Annotated[float, Field(examples=[2], description="A number for the selected / default lower number")]
    selected_upper_value: Annotated[float, Field(examples=[8], description="A number for the selected / default upper number")]

class TextParameterModel(ParameterModelBase):
    widget_type: Annotated[str, Field(examples=["text"], description="The parameter type (set to 'text' for this model)")]
    entered_text: Annotated[str, Field(examples=["sushi"], description="A string for the default entered text")]
    input_type: Annotated[str, Field(
        examples=["text", "textarea", "number", "date", "datetime-local", "month", "time", "color", "password"],
        description='A string for the input type (one of "text", "textarea", "number", "date", "datetime-local", "month", "time", "color", or "password")'
    )]

ParametersListType = list[
    NoneParameterModel | SingleSelectParameterModel | MultiSelectParameterModel | DateParameterModel | DateRangeParameterModel | 
    NumberParameterModel | NumberRangeParameterModel | TextParameterModel
]

class ParametersModel(BaseModel):
    parameters: Annotated[ParametersListType, Field(description="The list of parameters for the dataset / dashboard")]


## Datasets / Dashboards Catalog Response Models

name_description = "The name of the dataset / dashboard (usually in snake case)"
label_description = "The human-friendly display name for the dataset / dashboard"
description_description = "The description of the dataset / dashboard"
parameters_path_description = "The API path to the parameters for the dataset / dashboard"
metadata_path_description = "The API path to the metadata (i.e., description and schema) for the dataset"
result_path_description = "The API path to the results for the dataset / dashboard"

class ColumnModel(BaseModel):
    name: Annotated[str, Field(examples=["mycol"], description="Name of column")]
    type: Annotated[str, Field(examples=["string", "integer", "boolean", "datetime"], description='Column type (such as "string", "integer", "boolean", "datetime", etc.)')]
    description: Annotated[str, Field(examples=["My column description"], description="The description of the column")]
    category: Annotated[str, Field(examples=["dimension", "measure", "misc"], description="The category of the column (such as 'dimension', 'measure', or 'misc')")]

class ColumnWithConditionModel(ColumnModel):
    condition: Annotated[str | None, Field(None, examples=["My condition"], description="The condition of when the column is included (such as based on a parameter selection)")]

class SchemaModel(BaseModel):
    fields: Annotated[list[ColumnModel], Field(description="A list of JSON objects containing the 'name' and 'type' for each of the columns in the result")]

class SchemaWithConditionModel(BaseModel):
    fields: Annotated[list[ColumnWithConditionModel], Field(description="A list of JSON objects containing the 'name' and 'type' for each of the columns in the result")]

class DatasetItemModel(BaseModel):
    name: Annotated[str, Field(examples=["mydataset"], description=name_description)]
    label: Annotated[str, Field(examples=["My Dataset"], description=label_description)]
    description: Annotated[str, Field(examples=[""], description=description_description)]
    parameters: Annotated[list[str], Field(examples=["myparam1", "myparam2"], description="The list of parameter names used by the dataset")]
    data_schema: Annotated[SchemaWithConditionModel, Field(alias="schema", description="JSON object describing the schema of the dataset")]
    parameters_path: Annotated[str, Field(examples=["/squirrels-v0/myproject/v1/dataset/mydataset/parameters"], description=parameters_path_description)]
    result_path: Annotated[str, Field(examples=["/squirrels-v0/myproject/v1/dataset/mydataset"], description=result_path_description)]

class DashboardItemModel(ParametersModel):
    name: Annotated[str, Field(examples=["mydashboard"], description=name_description)]
    label: Annotated[str, Field(examples=["My Dashboard"], description=label_description)]
    description: Annotated[str, Field(examples=[""], description=description_description)]
    parameters: Annotated[list[str], Field(examples=["myparam1", "myparam2"], description="The list of parameter names used by the dashboard")]
    parameters_path: Annotated[str, Field(examples=["/squirrels-v0/myproject/v1/dashboard/mydashboard/parameters"], description=parameters_path_description)]
    result_path: Annotated[str, Field(examples=["/squirrels-v0/myproject/v1/dashboard/mydashboard"], description=result_path_description)]
    result_format: Annotated[str, Field(examples=["png", "html"], description="The format of the dashboard's result API response (one of 'png' or 'html')")]

ModelConfigType = mc.ModelConfig | s.Source | mc.SeedConfig | mc.BuildModelConfig | mc.DbviewModelConfig | mc.FederateModelConfig

class ConnectionItemModel(BaseModel):
    name: Annotated[str, Field(examples=["myconnection"], description="The name of the connection")]
    label: Annotated[str, Field(examples=["My Connection"], description="The human-friendly display name for the connection")]

class DataModelItem(BaseModel):
    name: Annotated[str, Field(examples=["model_name"], description="The name of the model")]
    model_type: Annotated[Literal["source", "dbview", "federate", "seed", "build"], Field(
        examples=["source", "dbview", "federate", "seed", "build"], description="The type of the model"
    )]
    config: Annotated[ModelConfigType, Field(description="The configuration of the model")]
    is_queryable: Annotated[bool, Field(examples=[True], description="Whether the model is queryable")]

class LineageNode(BaseModel):
    name: str
    type: Literal["model", "dataset", "dashboard"]

class LineageRelation(BaseModel):
    type: Literal["buildtime", "runtime"]
    source: LineageNode
    target: LineageNode

class CatalogModel(BaseModel):
    parameters: Annotated[ParametersListType, Field(description="The list of all parameters in the project")]
    datasets: Annotated[list[DatasetItemModel], Field(description="The list of accessible datasets")]
    dashboards: Annotated[list[DashboardItemModel], Field(description="The list of accessible dashboards")]
    connections: Annotated[list[ConnectionItemModel], Field(description="The list of connections in the project (only provided for admin users)")]
    models: Annotated[list[DataModelItem], Field(description="The list of data models in the project (only provided for admin users)")]
    lineage: Annotated[list[LineageRelation], Field(description="The lineage information between data assets (only provided for admin users)")]


## Dataset Results Response Models

class DataDetailsModel(BaseModel):
    num_rows: Annotated[int, Field(examples=[2], description="The number of rows in the data field")]
    orientation: Annotated[Literal["records", "rows", "columns"], Field(examples=["records", "rows", "columns"], description="The orientation of the data field")]

class DatasetResultModel(BaseModel):
    data_schema: Annotated[SchemaModel, Field(alias="schema", description="JSON object describing the schema of the dataset")]
    total_num_rows: Annotated[int, Field(examples=[2], description="The total number of rows for the dataset")]
    data_details: Annotated[DataDetailsModel, Field(description="A JSON object containing the details of the data field")]
    data: Annotated[list[dict] | list[list] | dict[str, list], Field(
        examples=[[{"mycol": "col_value1"}, {"mycol": "col_value2"}], [["col_value1"], ["col_value2"]], {"mycol": ["col_value1", "col_value2"]}],
        description="A list of JSON objects where each object is a row of the tabular results. The keys and values of the object are column names (described in fields) and values of the row."
    )]


## Project Metadata Response Models

class ProjectVersionModel(BaseModel):
    major_version: Annotated[int, Field(examples=[1])]
    data_catalog_path: Annotated[str, Field(examples=["/squirrels-v0/project/myproject/v1/data-catalog"])]

class ProjectModel(BaseModel):
    name: Annotated[str, Field(examples=["myproject"])]
    version: Annotated[str, Field(examples=["v1"])]
    label: Annotated[str, Field(examples=["My Project"])]
    description: Annotated[str, Field(examples=["My project description"])]
    squirrels_version: Annotated[str, Field(examples=["0.1.0"])]
