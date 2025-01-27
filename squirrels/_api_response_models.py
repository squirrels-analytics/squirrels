from typing import Annotated
from pydantic import BaseModel, Field
from datetime import datetime, date


class LoginReponse(BaseModel):
    access_token: Annotated[str, Field(examples=["encoded_jwt_token"], description="An encoded JSON web token to use subsequent API requests")]
    token_type: Annotated[str, Field(examples=["bearer"], description='Always "bearer" for Bearer token')]
    username: Annotated[str, Field(examples=["johndoe"], description='The username authenticated with from the form data')]
    expiry_time: Annotated[datetime, Field(examples=["2023-08-01T12:00:00.000000Z"], description="The expiry time of the access token in yyyy-MM-dd'T'hh:mm:ss.SSSSSS'Z' format")]


## Parameters Response Models

class ParameterOptionModel(BaseModel):
    id: Annotated[str, Field(examples=["my_option_id"], description="The unique identifier for the option")]
    label: Annotated[str, Field(examples=["My Option"], description="The human-friendly display name for the option")]

class ParameterModelBase(BaseModel):
    widget_type: Annotated[str, Field(examples=["none"], description="The parameter type (set to 'none' for this model)")]
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

class ParametersModel(BaseModel):
    parameters: list[
        NoneParameterModel | SingleSelectParameterModel | MultiSelectParameterModel | DateParameterModel | DateRangeParameterModel | 
        NumberParameterModel | NumberRangeParameterModel | TextParameterModel
    ]


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
    parameters: list[
        NoneParameterModel | SingleSelectParameterModel | MultiSelectParameterModel | DateParameterModel | DateRangeParameterModel | 
        NumberParameterModel | NumberRangeParameterModel | TextParameterModel
    ]
    data_schema: Annotated[SchemaWithConditionModel, Field(alias="schema", description="JSON object describing the schema of the dataset")]
    parameters_path: Annotated[str, Field(examples=["/squirrels-v0/myproject/v1/dataset/mydataset/parameters"], description=parameters_path_description)]
    result_path: Annotated[str, Field(examples=["/squirrels-v0/myproject/v1/dataset/mydataset"], description=result_path_description)]

class DashboardItemModel(ParametersModel):
    name: Annotated[str, Field(examples=["mydashboard"], description=name_description)]
    label: Annotated[str, Field(examples=["My Dashboard"], description=label_description)]
    description: Annotated[str, Field(examples=[""], description=description_description)]
    parameters_path: Annotated[str, Field(examples=["/squirrels-v0/myproject/v1/dashboard/mydashboard/parameters"], description=parameters_path_description)]
    result_path: Annotated[str, Field(examples=["/squirrels-v0/myproject/v1/dashboard/mydashboard"], description=result_path_description)]
    result_format: Annotated[str, Field(examples=["png", "html"], description="The format of the dashboard's result API response (one of 'png' or 'html')")]

class CatalogModel(BaseModel):
    datasets: Annotated[list[DatasetItemModel], Field(description="The list of accessible datasets")]
    dashboards: Annotated[list[DashboardItemModel], Field(description="The list of accessible dashboards")]


## Dataset Results Response Models

class DatasetResultModel(BaseModel):
    data_schema: Annotated[SchemaModel, Field(alias="schema", description="JSON object describing the schema of the dataset")]
    total_num_rows: Annotated[int, Field(description="The total number of rows for the dataset")]
    data: Annotated[list[dict] | dict[str, list], Field(
        examples=[[{"col_name": "col_value1"}, {"col_name": "col_value2"}], {"col_name": ["col_value1", "col_value2"]}],
        description="A list of JSON objects where each object is a row of the tabular results. The keys and values of the object are column names (described in fields) and values of the row."
    )]


## Project Metadata Response Models

class ProjectVersionModel(BaseModel):
    major_version: int
    created_at: datetime
    updated_at: datetime
    token_path: Annotated[str, Field(examples=["/squirrels-v0/myproject/v1/token"])]
    data_catalog_path: Annotated[str, Field(examples=["/squirrels-v0/myproject/v1/datasets"])]

class ProjectModel(BaseModel):
    name: Annotated[str, Field(examples=["myproject"])]
    label: Annotated[str, Field(examples=["My Project"])]
    description: Annotated[str, Field(examples=["My project description"])]
    versions: list[ProjectVersionModel]
