from typing import Annotated, Literal, Any
from textwrap import dedent
from pydantic import BaseModel, Field
from datetime import date

from .. import _model_configs as mc, _sources as s, _constants as c


## Simple Auth Response Models

class ApiKeyResponse(BaseModel):
    api_key: Annotated[str, Field(examples=["sqrl-12345678"], description="The API key to use subsequent API requests")]

class ProviderResponse(BaseModel):
    name: Annotated[str, Field(examples=["my_provider"], description="The name of the provider")]
    label: Annotated[str, Field(examples=["My Provider"], description="The human-friendly display name for the provider")]
    icon: Annotated[str, Field(examples=["https://example.com/my_provider_icon.png"], description="The URL of the provider's icon. If the path starts with '/public/', it is assumed to be a static file in the project's 'resources/public/' directory.")]
    login_url: Annotated[str, Field(examples=["https://example.com/my_provider_login"], description="The URL to redirect to for provider login")]


## Parameters Response Models

class ParameterOptionModel(BaseModel):
    id: Annotated[str, Field(description="The unique identifier for the option")]
    label: Annotated[str, Field(description="The human-friendly display name for the option")]

class ParameterModelBase(BaseModel):
    widget_type: Annotated[str, Field(description="The parameter type")]
    name: Annotated[str, Field(description="The name of the parameter. Use this as the key when providing the API request parameters")]
    label: Annotated[str, Field(description="The human-friendly display name for the parameter")]
    description: Annotated[str, Field(description="The description of the parameter")]

class NoneParameterModel(ParameterModelBase):
    pass

class SelectParameterModel(ParameterModelBase):
    options: Annotated[list[ParameterOptionModel], Field(description="The list of dropdown options as JSON objects containing 'id' and 'label' fields")]
    trigger_refresh: Annotated[bool, Field(description="A boolean that's set to true for parent parameters that require a new parameters API call when the selection changes")]

class SingleSelectParameterModel(SelectParameterModel):
    widget_type: Annotated[Literal["single_select"], Field(description="The parameter type")]
    selected_id: Annotated[str | None, Field(description="The ID of the selected / default option")]

class MultiSelectParameterModel(SelectParameterModel):
    widget_type: Annotated[Literal["multi_select"], Field(description="The parameter type")]
    show_select_all: Annotated[bool, Field(description="A boolean for whether there should be a toggle to select all options")]
    order_matters: Annotated[bool, Field(description="A boolean for whether the ordering of the input selections would affect the result of the dataset")]
    selected_ids: Annotated[list[str], Field(description="A list of ids of the selected / default options")]

class _DateTypeParameterModel(ParameterModelBase):
    min_date: Annotated[date | None, Field(description='A string in "yyyy-MM-dd" format for the minimum date')]
    max_date: Annotated[date | None, Field(description='A string in "yyyy-MM-dd" format for the maximum date')]

class DateParameterModel(_DateTypeParameterModel):
    widget_type: Annotated[Literal["date"], Field(description="The parameter type")]
    selected_date: Annotated[date, Field(description='A string in "yyyy-MM-dd" format for the selected / default date')]

class DateRangeParameterModel(_DateTypeParameterModel):
    widget_type: Annotated[Literal["date_range"], Field(description="The parameter type")]
    selected_start_date: Annotated[date, Field(description='A string in "yyyy-MM-dd" format for the selected / default start date')]
    selected_end_date: Annotated[date, Field(description='A string in "yyyy-MM-dd" format for the selected / default end date')]

class _NumericParameterModel(ParameterModelBase):
    min_value: Annotated[float, Field(description="A number for the lower bound of the selectable number")]
    max_value: Annotated[float, Field(description="A number for the upper bound of the selectable number")]
    increment: Annotated[float, Field(description="A number for the selectable increments between the lower bound and upper bound")]

class NumberParameterModel(_NumericParameterModel):
    widget_type: Annotated[Literal["number"], Field(description="The parameter type")]
    selected_value: Annotated[float, Field(description="A number for the selected / default number")]

class NumberRangeParameterModel(_NumericParameterModel):
    widget_type: Annotated[Literal["number_range"], Field(description="The parameter type")]
    selected_lower_value: Annotated[float, Field(description="A number for the selected / default lower number")]
    selected_upper_value: Annotated[float, Field(description="A number for the selected / default upper number")]

class TextParameterModel(ParameterModelBase):
    widget_type: Annotated[Literal["text"], Field(description="The parameter type")]
    entered_text: Annotated[str, Field(description="A string for the default entered text")]
    input_type: Annotated[str, Field(
        description='A string for the input type (one of "text", "textarea", "number", "date", "datetime-local", "month", "time", "color", or "password")'
    )]

ParametersListType = list[
    NoneParameterModel | SingleSelectParameterModel | MultiSelectParameterModel | DateParameterModel | DateRangeParameterModel | 
    NumberParameterModel | NumberRangeParameterModel | TextParameterModel
]

class ParametersModel(BaseModel):
    parameters: Annotated[ParametersListType, Field(description="The list of parameters for the dataset / dashboard")]


## Datasets / Dashboards Catalog Response Models

name_description = "The name of the dataset / dashboard (usually in snake case with underscores)"
name_for_api_description = "The name of the dataset / dashboard for the API (with dashes instead of underscores)"
label_description = "The human-friendly display name for the dataset / dashboard"
description_description = "The description of the dataset / dashboard"
parameters_path_description = "The API path to the parameters for the dataset / dashboard"
metadata_path_description = "The API path to the metadata (i.e., description and schema) for the dataset"
result_path_description = "The API path to the results for the dataset / dashboard"

class ConfigurableOverrideModel(BaseModel):
    name: str
    default: str

class ConfigurableItemModel(ConfigurableOverrideModel):
    label: str
    description: str

class ColumnModel(BaseModel):
    name: Annotated[str, Field(examples=["mycol"], description="Name of column")]
    type: Annotated[str, Field(examples=["string", "integer", "boolean", "datetime"], description='Column type (such as "string", "integer", "boolean", "datetime", etc.)')]
    description: Annotated[str, Field(examples=["My column description"], description="The description of the column")]
    category: Annotated[str, Field(examples=["dimension", "measure", "misc"], description="The category of the column (such as 'dimension', 'measure', or 'misc')")]

class ColumnWithConditionModel(ColumnModel):
    condition: Annotated[list[str] | None, Field(default=None, examples=[["My condition"]], description="The condition(s) of when the field is included (such as based on a parameter selection)")]

class SchemaModel(BaseModel):
    fields: Annotated[list[ColumnModel], Field(description="A list of JSON objects containing the 'name' and 'type' for each of the columns in the result")]

class SchemaWithConditionModel(BaseModel):
    fields: Annotated[list[ColumnWithConditionModel], Field(description="A list of JSON objects containing the 'name' and 'type' for each of the columns in the result")]

class DatasetItemModelForMcp(BaseModel):
    name: Annotated[str, Field(examples=["my_dataset"], description=name_description)]
    label: Annotated[str, Field(examples=["My Dataset"], description=label_description)]
    description: Annotated[str, Field(examples=[""], description=description_description)]
    parameters: Annotated[list[str], Field(examples=[["myparam1", "myparam2"]], description="The list of parameter names used by the dataset. If the list is empty, the dataset does not accept any parameters.")]
    data_schema: Annotated[SchemaWithConditionModel, Field(alias="schema", description="JSON object describing the schema of the dataset")]
 
class DatasetItemModel(DatasetItemModelForMcp):
    name_for_api: Annotated[str, Field(examples=["my-dataset"], description=name_for_api_description)]
    configurables: Annotated[list[ConfigurableOverrideModel], Field(default_factory=list, description="The list of configurables with their default values")]

class DashboardItemModel(ParametersModel):
    name: Annotated[str, Field(examples=["mydashboard"], description=name_description)]
    name_for_api: Annotated[str, Field(examples=["my-dashboard"], description=name_for_api_description)]
    label: Annotated[str, Field(examples=["My Dashboard"], description=label_description)]
    description: Annotated[str, Field(examples=[""], description=description_description)]
    configurables: Annotated[list[ConfigurableOverrideModel], Field(default_factory=list, description="The list of configurables with their default values")]
    parameters: Annotated[list[str], Field(examples=[["myparam1", "myparam2"]], description="The list of parameter names used by the dashboard")]
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

class CatalogModelForMcp(BaseModel):
    parameters: Annotated[ParametersListType, Field(description="The list of all parameters in the project. It is possible that not all parameters are used by a dataset.")]
    datasets: Annotated[list[DatasetItemModelForMcp], Field(description="The list of accessible datasets")]
    
class CatalogModel(CatalogModelForMcp):
    datasets: Annotated[list[DatasetItemModel], Field(description="The list of accessible datasets")]
    dashboards: Annotated[list[DashboardItemModel], Field(description="The list of accessible dashboards")]
    connections: Annotated[list[ConnectionItemModel], Field(description="The list of connections in the project (only provided for admin users)")]
    models: Annotated[list[DataModelItem], Field(description="The list of data models in the project (only provided for admin users)")]
    lineage: Annotated[list[LineageRelation], Field(description="The lineage information between data assets (only provided for admin users)")]
    configurables: Annotated[list[ConfigurableItemModel], Field(description="The list of configurables (only provided for admin users)")]


## Dataset Results Response Models

class DataDetailsModel(BaseModel):
    num_rows: Annotated[int, Field(description="The number of rows in the data field")]
    orientation: Annotated[Literal["records", "rows", "columns"], Field(description="The orientation of the data field")]

class DatasetResultModel(BaseModel):
    data_schema: Annotated[SchemaModel, Field(alias="schema", description="JSON object describing the schema of the dataset")]
    total_num_rows: Annotated[int, Field(description="The total number of rows for the dataset")]
    data_details: Annotated[DataDetailsModel, Field(description="A JSON object containing the details of the data field")]
    data: Annotated[list[dict] | list[list] | dict[str, list], Field(
        description=dedent("""
        The data payload. 
        - If orientation is 'records', this is a list of JSON objects. 
        - If orientation is 'rows', this is a list of rows. Each row is a list of values in the same order as the columns in `schema`.
        - If orientation is 'columns', this is a JSON object where keys are column names and values are columns. Each column is a list of values from the first to last row.
        """).strip()
    )]


## Compiled Query Response Model

class CompiledQueryModel(BaseModel):
    language: Annotated[Literal["sql", "python"], Field(description="The language of the data model query: 'sql' or 'python'")]
    definition: Annotated[str, Field("", description="The compiled SQL or Python definition of the data model.")]
    placeholders: Annotated[dict[str, Any], Field({}, description="The placeholders for the data model.")]


## Project Metadata Response Models

SAMPLE_BASE_URL = "https://example.com/analytics/project/v1"
SAMPLE_BASE_URL_API = f"{SAMPLE_BASE_URL}{c.LATEST_API_VERSION_MOUNT_PATH}"

class PasswordRequirements(BaseModel):
    min_length: Annotated[int, Field(default=8, description="The minimum length of the password")]
    max_length: Annotated[int, Field(default=64, description="The maximum length of the password")] # For bcrypt, the max length must be 72 or less

class ApiRoutesModel(BaseModel):
    # Data catalog and assets routes
    get_data_catalog_url: Annotated[str, Field(examples=[f"{SAMPLE_BASE_URL_API}/data-catalog"])]
    get_parameters_url: Annotated[str, Field(examples=[f"{SAMPLE_BASE_URL_API}/parameters"])]
    get_dataset_parameters_url: Annotated[str, Field(examples=[f"{SAMPLE_BASE_URL_API}/datasets/{{dataset_name}}/parameters"])]
    get_dataset_results_url: Annotated[str, Field(examples=[f"{SAMPLE_BASE_URL_API}/datasets/{{dataset_name}}"])]
    get_dashboard_parameters_url: Annotated[str, Field(examples=[f"{SAMPLE_BASE_URL_API}/dashboards/{{dashboard_name}}/parameters"])]
    get_dashboard_results_url: Annotated[str, Field(examples=[f"{SAMPLE_BASE_URL_API}/dashboards/{{dashboard_name}}"])]

    # Data management routes
    trigger_build_url: Annotated[str, Field(examples=[f"{SAMPLE_BASE_URL_API}/build"])]
    get_query_result_url: Annotated[str, Field(examples=[f"{SAMPLE_BASE_URL_API}/query-result"])]
    get_compiled_model_url: Annotated[str, Field(examples=[f"{SAMPLE_BASE_URL_API}/compiled-models/{{model_name}}"])]

    # Authentication routes
    get_user_session_url: Annotated[str | None, Field(default=None, examples=[f"{SAMPLE_BASE_URL_API}/auth/user-session"])]
    list_providers_url: Annotated[str | None, Field(default=None, examples=[f"{SAMPLE_BASE_URL_API}/auth/providers"])]
    login_url: Annotated[str | None, Field(default=None, examples=[f"{SAMPLE_BASE_URL_API}/auth/login"])]
    logout_url: Annotated[str | None, Field(default=None, examples=[f"{SAMPLE_BASE_URL_API}/auth/logout"])]
    change_password_url: Annotated[str | None, Field(default=None, examples=[f"{SAMPLE_BASE_URL_API}/auth/password"])]
    list_api_keys_url: Annotated[str | None, Field(default=None, examples=[f"{SAMPLE_BASE_URL_API}/auth/api-keys"])]
    create_api_key_url: Annotated[str | None, Field(default=None, examples=[f"{SAMPLE_BASE_URL_API}/auth/api-keys"])]
    revoke_api_key_url: Annotated[str | None, Field(default=None, examples=[f"{SAMPLE_BASE_URL_API}/auth/api-keys/{{key_id}}"])]

    # User management routes
    list_user_fields_url: Annotated[str | None, Field(default=None, examples=[f"{SAMPLE_BASE_URL_API}/auth/user-management/user-fields"])]
    list_users_url: Annotated[str | None, Field(default=None, examples=[f"{SAMPLE_BASE_URL_API}/auth/user-management/users"])]
    add_user_url: Annotated[str | None, Field(default=None, examples=[f"{SAMPLE_BASE_URL_API}/auth/user-management/users"])]
    update_user_url: Annotated[str | None, Field(default=None, examples=[f"{SAMPLE_BASE_URL_API}/auth/user-management/users/{{username}}"])]
    delete_user_url: Annotated[str | None, Field(default=None, examples=[f"{SAMPLE_BASE_URL_API}/auth/user-management/users/{{username}}"])]

class ProjectMetadataModel(BaseModel):
    name: Annotated[str, Field(examples=["my_project"], description="The name of the project (usually in snake case with underscores)")]
    name_for_api: Annotated[str, Field(examples=["my-project"], description="The name of the project for the API (with dashes instead of underscores)")]
    version: Annotated[str, Field(examples=["1"], description="The version of the project. Should be a stringified integer.")]
    label: Annotated[str, Field(examples=["My Project"], description="The human-friendly display name for the project")]
    description: Annotated[str, Field(examples=["My project description"], description="The description of the project")]
    auth_strategy: Annotated[Literal["managed", "external"], Field(examples=["managed"], description="The authentication strategy for the project")]
    auth_type: Annotated[Literal["optional", "required"], Field(examples=["optional"], description="The authentication type for the project")]
    password_requirements: Annotated[PasswordRequirements | None, Field(description="The password requirements for the project")]
    elevated_access_level: Annotated[Literal["admin", "member", "guest"], Field(
        examples=["admin"], description="The access level required to access elevated features (such as configurables and data lineage)"
    )]
    api_routes: Annotated[ApiRoutesModel, Field(description="The API routes for the project")]


class DocumentationRoutesModel(BaseModel):
    swagger_url: Annotated[str, Field(examples=[f"{SAMPLE_BASE_URL}/docs"], description="The URL to the Swagger UI")]
    redoc_url: Annotated[str, Field(examples=[f"{SAMPLE_BASE_URL}/redoc"], description="The URL to the ReDoc UI")]
    openapi_url: Annotated[str, Field(examples=[f"{SAMPLE_BASE_URL}/openapi.json"], description="The URL to the OpenAPI specification")]

class APIVersionMetadataModel(BaseModel):
    project_metadata_url: Annotated[str, Field(examples=[SAMPLE_BASE_URL_API], description="The URL to the project metadata endpoint")]
    documentation_routes: Annotated[DocumentationRoutesModel, Field(description="The API documentation for this version of the API contract")]

class ExploreEndpointsModel(BaseModel):
    health_url: Annotated[str, Field(examples=[f"{SAMPLE_BASE_URL}/health"], description="The URL to the health check endpoint")]
    api_versions: Annotated[dict[str, APIVersionMetadataModel], Field(description="The set of API versions and their metadata paths")]
    documentation_routes: Annotated[DocumentationRoutesModel, Field(description="The API documentation for the base API application")]
    mcp_server_url: Annotated[str, Field(examples=[f"{SAMPLE_BASE_URL}/mcp"], description="The URL to the MCP server")]
    studio_url: Annotated[str, Field(examples=[f"{SAMPLE_BASE_URL}/studio"], description="The URL to the Squirrels Studio UI")]


class OAuthProtectedResourceMetadata(BaseModel):
    """OAuth 2.1 Protected Resource Metadata (RFC 9728)"""
    resource: str = Field(description="The resource identifier URL")
    authorization_servers: list[str] = Field(description="List of authorization server issuer identifier URLs")
    scopes_supported: list[str] = Field(description="List of OAuth 2.1 scope values supported")
