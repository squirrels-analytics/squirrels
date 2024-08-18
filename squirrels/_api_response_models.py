from typing import Annotated, Union, Optional
from pydantic import BaseModel, Field
from datetime import datetime, date


class LoginReponse(BaseModel):
    access_token: Annotated[str, Field(examples=["encoded_jwt_token"])]
    token_type: Annotated[str, Field(examples=["bearer"])]
    username: Annotated[str, Field(examples=["johndoe"])]
    expiry_time: datetime


## Datasets Catalog Response Models

class DatasetInfoModel(BaseModel):
    name: Annotated[str, Field(examples=["mydataset"])]
    label: Annotated[str, Field(examples=["My Dataset"])]
    parameters_path: Annotated[str, Field(examples=["/squirrels-v0/myproject/v1/dataset/mydataset/parameters"])]
    result_path: Annotated[str, Field(examples=["/squirrels-v0/myproject/v1/dataset/mydataset"])]

class DatasetsCatalogModel(BaseModel):
    datasets: list[DatasetInfoModel]


## Parameters Response Models

class ParameterOptionModel(BaseModel):
    id: str
    label: str

class ParameterModelBase(BaseModel):
    widget_type: str
    name: str
    label: str
    description: str

class SelectParameterModel(ParameterModelBase):
    options: list[ParameterOptionModel]
    trigger_refresh: bool

class SingleSelectParameterModel(SelectParameterModel):
    widget_type: Annotated[str, Field(examples=["single_select"])]
    selected_id: Optional[str]

class MultiSelectParameterModel(SelectParameterModel):
    widget_type: Annotated[str, Field(examples=["multi_select"])]
    show_select_all: bool
    order_matters: bool
    selected_ids: list[str]

class DateParameterModel(ParameterModelBase):
    widget_type: Annotated[str, Field(examples=["date"])]
    selected_date: date

class DateRangeParameterModel(ParameterModelBase):
    widget_type: Annotated[str, Field(examples=["date_range"])]
    selected_start_date: date
    selected_end_date: date

class NumericParameterModel(ParameterModelBase):
    min_value: Annotated[float, Field(examples=[0])]
    max_value: Annotated[float, Field(examples=[10])]
    increment: Annotated[float, Field(examples=[1])]

class NumberParameterModel(NumericParameterModel):
    widget_type: Annotated[str, Field(examples=["number"])]
    selected_value: Annotated[float, Field(examples=[2])]

class NumberRangeParameterModel(NumericParameterModel):
    widget_type: Annotated[str, Field(examples=["number_range"])]
    selected_lower_value: Annotated[float, Field(examples=[2])]
    selected_upper_value: Annotated[float, Field(examples=[8])]

class TextParameterModel(ParameterModelBase):
    widget_type: Annotated[str, Field(examples=["text"])]
    entered_text: str
    input_type: Annotated[str, Field(examples=["text", "textarea", "number", "date", "datetime-local", "month", "time", "color", "password"])]

class ParametersModel(BaseModel):
    parameters: list[
        Union[
            ParameterModelBase, SingleSelectParameterModel, MultiSelectParameterModel, DateParameterModel, DateRangeParameterModel,
            NumberParameterModel, NumberRangeParameterModel, TextParameterModel
        ]
    ]


## Dataset Results Response Models

class ColumnModel(BaseModel):
    name: Annotated[str, Field(examples=["mycol"])]
    type: str

class SchemaModel(BaseModel):
    fields: list[ColumnModel]
    dimensions: Annotated[list[str], Field(examples=[["mycol"]])]

class DatasetResultModel(BaseModel):
    data_schema: Annotated[SchemaModel, Field(alias='schema')]
    data: Annotated[list[dict], Field(examples=[[{"mycol": "myval"}]])]


## Catalog Response Models

class ProjectVersionModel(BaseModel):
    major_version: int
    minor_versions: list[int]
    token_path: Annotated[str, Field(examples=["/squirrels-v0/myproject/v1/token"])]
    datasets_path: Annotated[str, Field(examples=["/squirrels-v0/myproject/v1/datasets"])]

class ProjectModel(BaseModel):
    name: Annotated[str, Field(examples=["myproject"])]
    label: Annotated[str, Field(examples=["My Project"])]
    versions: list[ProjectVersionModel]

class ProjectMetadataModel(BaseModel):
    projects: list[ProjectModel]
