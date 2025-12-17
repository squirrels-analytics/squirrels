"""
MCP Server implementation using the official MCP Python SDK low-level APIs.

This module provides the MCP server for Squirrels projects, exposing:
- Tools: get_data_catalog, get_dataset_parameters, get_dataset_results
- Resources: sqrl://data-catalog
"""
from typing import Mapping
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from textwrap import dedent
from typing import Any, Callable, Coroutine
from pydantic import AnyUrl
from starlette.applications import Starlette
from starlette.routing import Mount
from starlette.types import ASGIApp
from mcp.server.lowlevel import Server
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
import mcp.types as types
import json

from ._schemas.auth_models import AbstractUser
from ._exceptions import InvalidInputError
from ._schemas import response_models as rm
from ._dataset_types import DatasetResult, DatasetResultFormat
from ._api_routes.base import RouteBase


class McpServerBuilder:
    """
    Builder for the MCP server that exposes Squirrels tools and resources.
    
    This class is responsible for:
    - Creating the low-level MCP Server
    - Registering list_tools, call_tool, list_resources, read_resource handlers
    - Creating the StreamableHTTPSessionManager for HTTP transport
    - Providing the ASGI app and lifespan manager
    """
    
    def __init__(
        self,
        project_name: str,
        project_label: str,
        max_rows_for_ai: int,
        get_user_from_headers: Callable[[Mapping[str, str]], AbstractUser],
        get_data_catalog_for_mcp: Callable[[AbstractUser], Coroutine[Any, Any, rm.CatalogModelForMcp]],
        get_dataset_parameters_for_mcp: Callable[
            [str, str, str | list[str], AbstractUser], # dataset, parameter_name, selected_ids, user
            Coroutine[Any, Any, rm.ParametersModel]
        ],
        get_dataset_results_for_mcp: Callable[
            [str, str, str | None, AbstractUser, dict[str, str]], # dataset, parameters, sql_query, user, headers
            Coroutine[Any, Any, DatasetResult]
        ],
    ):
        """
        Initialize the MCP server builder.
        
        Args:
            project_name: The name of the Squirrels project
            project_label: The human-readable label of the project
            max_rows_for_ai: Maximum number of rows to return for AI tools
            get_data_catalog_for_mcp: Async function to get the data catalog
            get_dataset_parameters_for_mcp: Async function to get dataset parameters
            get_dataset_results_for_mcp: Async function to get dataset results
        """
        self.project_name = project_name
        self.project_label = project_label
        self.max_rows_for_ai = max_rows_for_ai
        self.default_for_limit = min(self.max_rows_for_ai, 10)

        self.get_user_from_headers = get_user_from_headers
        self._get_data_catalog_for_mcp = get_data_catalog_for_mcp
        self._get_dataset_parameters_for_mcp = get_dataset_parameters_for_mcp
        self._get_dataset_results_for_mcp = get_dataset_results_for_mcp
        
        # Tool names
        self.catalog_tool_name = f"get_data_catalog_from_{project_name}"
        self.parameters_tool_name = f"get_dataset_parameters_from_{project_name}"
        self.results_tool_name = f"get_dataset_results_from_{project_name}"
        
        # Resource URI
        self.catalog_resource_uri = "sqrl://data-catalog"
        self.catalog_resource_name = f"data_catalog_from_{project_name}"
        
        # Build the server
        self._server = self._build_server()
        self._session_manager = StreamableHTTPSessionManager(
            app=self._server,
            stateless=True,
            json_response=True,
        )
    
    def _build_server(self) -> Server:
        """Build and configure the low-level MCP Server."""
        server = Server("Squirrels")
        
        # Register handlers
        server.list_tools()(self._list_tools)
        server.call_tool()(self._call_tool)
        server.list_resources()(self._list_resources)
        server.read_resource()(self._read_resource)
        
        return server
    
    def _get_request_headers(self) -> dict[str, str]:
        """
        Get HTTP headers from the current MCP request context.
        
        Uses server.request_context.request.headers to access headers
        from the underlying HTTP request.
        """
        try:
            request = self._server.request_context.request
            if request is not None and hasattr(request, 'headers'):
                return dict(request.headers)
        except (AttributeError, LookupError):
            pass
        return {}

    def _get_feature_flags(self, *, headers: dict[str, str] | None = None) -> set[str]:
        """
        Get the feature flags from the request headers.
        """
        headers = headers or self._get_request_headers()
        return set(x.strip() for x in headers.get("x-feature-flags", "").split(","))
    
    async def _list_tools(self) -> list[types.Tool]:
        """Return the list of available MCP tools."""
        feature_flags = self._get_feature_flags()
        full_result_flag = "mcp-full-dataset-v1" in feature_flags
        
        dataset_results_extended_description = dedent("""
            The "offset" and "limit" arguments affect the "content" field, but not the "structuredContent" field, of this tool's result. Assume that you (the AI model) can only see the "content" field, but accessing this tool's result through code execution (if applicable) uses the "structuredContent" field. Note that the "sql_query" and "orientation" arguments still apply to both the "content" and "structuredContent" fields.
        """).strip() if full_result_flag else ""

        return [
            types.Tool(
                name=self.catalog_tool_name,
                title=f"Getting Data Catalog For {self.project_label}",
                description=dedent(f"""
                    Use this tool to get the details of all datasets and parameters you can access in the Squirrels project '{self.project_name}'.
                    
                    Unless the data catalog for this project has already been provided, use this tool at the start of each conversation.
                """).strip(),
                inputSchema={
                    "type": "object",
                    "properties": {},
                    "required": [],
                },
                # outputSchema=rm.CatalogModelForMcp.model_json_schema(),
            ),
            types.Tool(
                name=self.parameters_tool_name,
                title=f"Setting Dataset Parameters For {self.project_label}",
                description=dedent(f"""
                    Use this tool to get updates for dataset parameters in the Squirrels project "{self.project_name}" when a selection is to be made on a parameter with `"trigger_refresh": true`.

                    For example, suppose there are two parameters, "country" and "city", and the user selects "United States" for "country". If "country" has the "trigger_refresh" field as true, then this tool should be called to get the updates for other parameters such as "city".

                    Do not use this tool on parameters that do not have `"trigger_refresh": true`.
                """).strip(),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "dataset": {
                            "type": "string",
                            "description": "The name of the dataset whose parameters the trigger parameter will update",
                        },
                        "selected_ids": {
                            "type": "string",
                            "description": dedent("""
                                A JSON object (as string) with one key-value pair. The key is the name of the parameter triggering the refresh, and the value is the ID(s) of the selected option(s) for the parameter.
                                - If the parameter's widget_type is single_select, use a string for the ID of the selected option
                                - If the parameter's widget_type is multi_select, use an array of strings for the IDs of the selected options

                                An error is raised if this JSON object does not have exactly one key-value pair.
                            """).strip(),
                        },
                    },
                    "required": ["dataset", "selected_ids"],
                },
                # outputSchema=rm.ParametersModel.model_json_schema(),
            ),
            types.Tool(
                name=self.results_tool_name,
                title=f"Getting Dataset Results For {self.project_label}",
                description=dedent(f"""
                    Use this tool to get the dataset results as a JSON object for a dataset in the Squirrels project "{self.project_name}".

                    {dataset_results_extended_description}
                """).strip(),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "dataset": {
                            "type": "string",
                            "description": "The name of the dataset to get results for",
                        },
                        "parameters": {
                            "type": "string",
                            "description": dedent("""
                                A JSON object (as string) containing key-value pairs for parameter name and selected value. The selected value to provide depends on the parameter widget type:
                                - If the parameter's widget_type is single_select, use a string for the ID of the selected option
                                - If the parameter's widget_type is multi_select, use an array of strings for the IDs of the selected options
                                - If the parameter's widget_type is date, use a string like "YYYY-MM-DD"
                                - If the parameter's widget_type is date_range, use array of strings like ["YYYY-MM-DD","YYYY-MM-DD"]
                                - If the parameter's widget_type is number, use a number like 1
                                - If the parameter's widget_type is number_range, use array of numbers like [1,100]
                                - If the parameter's widget_type is text, use a string for the text value
                                - Complex objects are NOT supported
                            """).strip(),
                        },
                        "sql_query": {
                            "type": ["string", "null"],
                            "description": dedent("""
                                A custom Polars SQL query to execute on the final dataset result. 
                                - Use table name 'result' to reference the dataset result.
                                - Use this to apply transformations to the dataset result if needed (such as filtering, sorting, or selecting columns).
                                - If not provided, the dataset result is returned as is.
                            """).strip(),
                            "default": None,
                        },
                        "orientation": {
                            "type": "string",
                            "enum": ["rows", "columns", "records"],
                            "description": "The orientation of the dataset result. Options are 'rows', 'columns', and 'records'. Default is 'rows'.",
                            "default": "rows",
                        },
                        "offset": {
                            "type": "integer",
                            "description": "The number of rows to skip from first row. Applied after the sql_query. Default is 0.",
                            "default": 0,
                        },
                        "limit": {
                            "type": "integer",
                            "description": dedent(f"""
                                The maximum number of rows to return. Applied after the sql_query. 
                                Default is {self.default_for_limit}. Maximum allowed value is {self.max_rows_for_ai}.
                            """).strip(),
                            "default": self.default_for_limit,
                        },
                    },
                    "required": ["dataset", "parameters"],
                },
                outputSchema=rm.DatasetResultModel.model_json_schema(),
            ),
        ]
    
    def _get_dataset_and_parameters(self, arguments: dict[str, Any], *, params_key: str = "parameters") -> tuple[str, dict[str, Any]]:
        """Get dataset and parameters from arguments.

        Args:
            arguments: The arguments from the tool call
            params_key: The key of the parameters in the arguments
        
        Returns:
            A tuple of the dataset and parameters

        Raises:
            InvalidInputError: If the dataset or parameters are invalid
        """
        try:
            dataset = str(arguments["dataset"])
        except KeyError:
            raise InvalidInputError(400, "invalid_dataset", "The 'dataset' argument is required.")

        parameters_arg = str(arguments.get(params_key, "{}"))
        
        # validate parameters argument
        try:
            parameters = json.loads(parameters_arg)
        except json.JSONDecodeError:
            parameters = None  # error handled below
        
        if not isinstance(parameters, dict):
            raise InvalidInputError(400, "invalid_parameters", f"The '{params_key}' argument must be a JSON object.")
        
        return dataset, parameters
    
    async def _call_tool(self, name: str, arguments: dict[str, Any] | None) -> types.CallToolResult:
        """Handle tool calls by dispatching to the appropriate function.
        
        Returns structured data (dict) directly for successful calls, which the MCP
        framework will serialize to JSON. For errors, returns CallToolResult with isError=True.
        """
        arguments = arguments or {}
        
        try:
            headers = self._get_request_headers()
            user = self.get_user_from_headers(headers)
            
            feature_flags = self._get_feature_flags(headers=headers)
            full_result_flag = "mcp-full-dataset-v1" in feature_flags

            if name == self.catalog_tool_name:
                result = await self._get_data_catalog_for_mcp(user)
                return types.CallToolResult(
                    content=[types.TextContent(type="text", text=result.model_dump_json(by_alias=True))],
                    structuredContent=result.model_dump(mode="json", by_alias=True),
                )
            
            elif name == self.parameters_tool_name:
                dataset, parameters = self._get_dataset_and_parameters(arguments, params_key="selected_ids")

                # validate parameters is a single key-value pair
                if len(parameters) != 1:
                    raise InvalidInputError(
                        400, "invalid_selected_ids", 
                        "The 'selected_ids' argument must have exactly one key-value pair."
                    )
                
                # validate selected ids is a string or list of strings
                parameter_name, selected_ids = next(iter(parameters.items()))
                if not isinstance(selected_ids, (str, list)):
                    raise InvalidInputError(
                        400, "invalid_selected_ids", 
                        f"The selected ids of the parameter '{parameter_name}' must be a string or list of strings."
                    )
                
                # get dataset parameters
                result = await self._get_dataset_parameters_for_mcp(dataset, parameter_name, selected_ids, user)
                return types.CallToolResult(
                    content=[types.TextContent(type="text", text=result.model_dump_json(by_alias=True))],
                    structuredContent=result.model_dump(mode="json", by_alias=True),
                )
            
            elif name == self.results_tool_name:
                dataset, parameters = self._get_dataset_and_parameters(arguments, params_key="parameters")
            
                # validate sql_query argument
                sql_query_arg = arguments.get("sql_query")
                sql_query = str(sql_query_arg) if sql_query_arg else None

                # validate orientation argument
                result_format = RouteBase.extract_orientation_offset_and_limit(arguments, key_prefix="", default_orientation="rows", default_limit=self.default_for_limit)
                orientation, limit = result_format.orientation, result_format.limit
                if limit > self.max_rows_for_ai:
                    raise InvalidInputError(400, "invalid_limit", f"The 'limit' argument must be less than or equal to {self.max_rows_for_ai}.")
                
                # get dataset result object
                result_obj = await self._get_dataset_results_for_mcp(
                    dataset, parameters, sql_query, user, headers
                )

                # format dataset result object
                structured_result = result_obj.to_json(result_format)
                result_model = rm.DatasetResultModel(**structured_result)
                
                if full_result_flag:
                    full_result_format = DatasetResultFormat(orientation, 0, None)
                    structured_result = result_obj.to_json(full_result_format)
                
                return types.CallToolResult(
                    content=[types.TextContent(type="text", text=result_model.model_dump_json(by_alias=True))],
                    structuredContent=structured_result,
                )
            
            else:
                return types.CallToolResult(
                    content=[types.TextContent(type="text", text=f"Unknown tool: {name}")],
                    isError=True
                )
        
        except InvalidInputError as e:
            return types.CallToolResult(
                content=[types.TextContent(type="text", text=f"Error: {e.error_description}")],
                isError=True
            )
        except Exception as e:
            return types.CallToolResult(
                content=[types.TextContent(type="text", text=f"Error: {str(e)}")],
                isError=True
            )
    
    async def _list_resources(self) -> list[types.Resource]:
        """Return the list of available MCP resources."""
        return [
            types.Resource(
                uri=AnyUrl(self.catalog_resource_uri),
                name=self.catalog_resource_name,
                description=f"Details of all datasets and parameters you can access in the Squirrels project '{self.project_name}'.",
            ),
        ]
    
    async def _read_resource(self, uri: AnyUrl) -> str | bytes:
        """Read the content of a resource."""
        headers = self._get_request_headers()
        
        if str(uri) == self.catalog_resource_uri:
            user = self.get_user_from_headers(headers)
            result = await self._get_data_catalog_for_mcp(user)
            return result.model_dump_json(by_alias=True)
        else:
            raise ValueError(f"Unknown resource URI: {uri}")
    
    @asynccontextmanager
    async def lifespan(self) -> AsyncIterator[None]:
        """
        Async context manager for the MCP session manager lifecycle.
        
        Use this in the FastAPI app lifespan to ensure proper startup/shutdown.
        """
        async with self._session_manager.run():
            yield

    def get_asgi_app(self) -> ASGIApp:
        """
        Get the ASGI app for the MCP server.
        """
        app = Starlette(
            routes=[
                Mount("/", app=self._session_manager.handle_request),
            ],
            lifespan=self.lifespan,
        )
        return app
    