"""
MCP Server implementation using the official MCP Python SDK low-level APIs.

This module provides the MCP server for Squirrels projects, exposing:
- Tools: get_data_catalog, get_dataset_parameters, get_dataset_results
- Resources: sqrl://data-catalog
"""
from typing import Any, Protocol
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from textwrap import dedent
from pydantic import AnyUrl
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.routing import Mount
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp
from mcp.server.lowlevel import Server
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
import mcp.types as types
import json

from . import _utils as u
from ._schemas.auth_models import AbstractUser
from ._schemas.request_models import McpRequestHeaders
from ._exceptions import InvalidInputError
from ._http_error_responses import invalid_input_error_to_json_response
from ._schemas import response_models as rm
from ._dataset_types import DatasetResult, DatasetResultFormat
from ._api_routes.base import RouteBase


class GetUserFromHeaders(Protocol):
    def __call__(self, api_key: str | None, bearer_token: str | None) -> tuple[AbstractUser, float | None]:
        ...

class GetDataCatalogForMcp(Protocol):
    async def __call__(self, user: AbstractUser) -> rm.CatalogModelForMcp:
        ...

class GetDatasetParametersForMcp(Protocol):
    async def __call__(
        self, dataset: str, parameter_name: str, selected_ids: str | list[str], user: AbstractUser
    ) -> rm.ParametersModel:
        ...

class GetDatasetResultsForMcp(Protocol):
    async def __call__(
        self, dataset: str, parameters: dict[str, Any], sql_query: str | None, user: AbstractUser, configurables: tuple[tuple[str, str], ...]
    ) -> DatasetResult:
        ...


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
        get_user_from_headers: GetUserFromHeaders,
        get_data_catalog_for_mcp: GetDataCatalogForMcp,
        get_dataset_parameters_for_mcp: GetDatasetParametersForMcp,
        get_dataset_results_for_mcp: GetDatasetResultsForMcp,
        *,
        enforce_oauth_bearer: bool = False,
        oauth_resource_metadata_path: str = "/.well-known/oauth-protected-resource",
        www_authenticate_strip_path_suffix: str = "/mcp",
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

        self.enforce_oauth_bearer = enforce_oauth_bearer
        self.oauth_resource_metadata_path = oauth_resource_metadata_path
        self.www_authenticate_strip_path_suffix = www_authenticate_strip_path_suffix

        self._get_user_from_headers = get_user_from_headers
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
    
    def _get_tool_annotations(
        self, title: str, *, read_only: bool = True, destructive: bool = False,
        idempotent: bool = True, open_world: bool = False
    ) -> types.ToolAnnotations:
        """Get the tool annotations for the given title."""
        return types.ToolAnnotations(
            title=title,
            readOnlyHint=read_only,
            destructiveHint=destructive,
            idempotentHint=idempotent,
            openWorldHint=open_world,
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
    
    def _get_request_headers(self) -> McpRequestHeaders:
        """
        Get HTTP headers from the current MCP request context.
        
        Uses server.request_context.request.headers to access headers
        from the underlying HTTP request.
        """
        try:
            request = self._server.request_context.request
            if request is not None and hasattr(request, 'headers'):
                return McpRequestHeaders(raw_headers=request.headers)
        except (AttributeError, LookupError):
            pass
        
        return McpRequestHeaders()
    
    def _get_request_metadata(self) -> dict[str, Any]:
        """
        Metadata of the current MCP request as a dictionary.
        
        Returns:
            A dictionary of the request metadata
        """
        request_metadata = self._server.request_context.meta
        if request_metadata is None:
            return {}
        return request_metadata.model_dump(mode="json")

    def _get_configurables(self, mcp_headers: McpRequestHeaders) -> tuple[tuple[str, str], ...]:
        """
        Extract configurables from headers and metadata.
        """
        prefix = "x-config-"
        cfg_dict: dict[str, str] = {}
        
        # 1. Extract from headers
        for key, value in mcp_headers.raw_headers.items():
            key_lower = str(key).lower()
            if key_lower.startswith(prefix):
                cfg_name_raw = key_lower[len(prefix):]
                cfg_name_normalized = u.normalize_name(cfg_name_raw)
                
                if cfg_name_normalized in cfg_dict:
                    raise InvalidInputError(
                        400, "duplicate_configurable",
                        f"Configurable '{cfg_name_normalized}' specified multiple times in headers."
                    )
                cfg_dict[cfg_name_normalized] = str(value)

        # 2. Extract from metadata
        metadata = self._get_request_metadata()
        for key, value in metadata.items():
            if key == "progressToken":
                continue
            
            cfg_name_normalized = u.normalize_name(key)
            if cfg_name_normalized in cfg_dict:
                raise InvalidInputError(
                    400, "duplicate_configurable",
                    f"Configurable '{cfg_name_normalized}' specified multiple times (header and metadata)."
                )
            cfg_dict[cfg_name_normalized] = str(value)

        return tuple(cfg_dict.items())

    def _get_validated_user_for_request(self, mcp_headers: McpRequestHeaders) -> tuple[AbstractUser, float | None]:
        """
        Return the validated user for the current HTTP request.

        If the MCP app runs with `enforce_oauth_bearer=True`, missing Bearer tokens
        must produce an HTTP 401 (not an MCP tool error), so we raise InvalidInputError.
        """
        # Prefer values set by the HTTP middleware to avoid double validation.
        try:
            request = self._server.request_context.request
            if request is not None and hasattr(request, "state"):
                state = request.state
                user = getattr(state, "sqrl_user", None)
                expiry = getattr(state, "access_token_expiry", None)
                if user is not None:
                    return user, expiry
        except (AttributeError, LookupError):
            pass

        if self.enforce_oauth_bearer and not mcp_headers.bearer_token:
            raise InvalidInputError(401, "user_required", "Authentication is required")

        return self._get_user_from_headers(api_key=mcp_headers.api_key, bearer_token=mcp_headers.bearer_token)

    async def _list_tools(self) -> list[types.Tool]:
        """Return the list of available MCP tools."""
        headers = self._get_request_headers()
        feature_flags = headers.feature_flags
        full_result_flag = "mcp-full-dataset-v1" in feature_flags
        
        dataset_results_extended_description = dedent("""
            The "offset" and "limit" arguments affect the "content" field, but not the "structuredContent" field, of this tool's result. Assume that you (the AI model) can only see the "content" field, but accessing this tool's result through code execution (if applicable) uses the "structuredContent" field. Note that the "sql_query" and "orientation" arguments still apply to both the "content" and "structuredContent" fields.
        """).strip() if full_result_flag else ""

        return [
            types.Tool(
                name=self.catalog_tool_name,
                title=f"Data Catalog For {self.project_label}",
                description=dedent(f"""
                    Use this tool to get the details of all datasets and parameters you can access in the Squirrels project '{self.project_name}'.
                    
                    Unless the data catalog for this project has already been provided, use this tool at the start of each conversation.
                """).strip(),
                annotations=self._get_tool_annotations(title=f"Data Catalog For {self.project_label}"),
                inputSchema={
                    "type": "object",
                    "properties": {},
                    "required": [],
                },
                # outputSchema=rm.CatalogModelForMcp.model_json_schema(),
            ),
            types.Tool(
                name=self.parameters_tool_name,
                title=f"Parameters Updates For {self.project_label}",
                description=dedent(f"""
                    Use this tool to get updates for dataset parameters in the Squirrels project "{self.project_name}" when a selection is to be made on a parameter with `"trigger_refresh": true`.

                    For example, suppose there are two parameters, "country" and "city", and the user selects "United States" for "country". If "country" has the "trigger_refresh" field as true, then this tool should be called to get the updates for other parameters such as "city".

                    Do not use this tool on parameters that do not have `"trigger_refresh": true`.
                """).strip(),
                annotations=self._get_tool_annotations(title=f"Parameters Updates For {self.project_label}"),
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
                title=f"Dataset Results For {self.project_label}",
                description=dedent(f"""
                    Use this tool to get the dataset results as a JSON object for a dataset in the Squirrels project "{self.project_name}".

                    {dataset_results_extended_description}
                """).strip(),
                annotations=self._get_tool_annotations(title=f"Dataset Results For {self.project_label}"),
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
            mcp_headers = self._get_request_headers()
            user, _ = self._get_validated_user_for_request(mcp_headers)
            
            feature_flags = mcp_headers.feature_flags
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
                configurables = self._get_configurables(mcp_headers)
                result_obj = await self._get_dataset_results_for_mcp(
                    dataset, parameters, sql_query, user, configurables
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
            # If auth is required, surface HTTP 401s as real HTTP responses.
            if e.status_code == 401:
                raise
            return types.CallToolResult(
                content=[types.TextContent(type="text", text=f"Error: {e.error_description}")],
                isError=True,
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
        mcp_headers = self._get_request_headers()
        
        if str(uri) == self.catalog_resource_uri:
            user, _ = self._get_validated_user_for_request(mcp_headers)
            result = await self._get_data_catalog_for_mcp(user)
            return result.model_dump_json(by_alias=True)
        else:
            raise ValueError(f"Unknown resource URI: {uri}")
    
    @asynccontextmanager
    async def lifespan(self, app: object | None = None) -> AsyncIterator[None]:
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
        async def _invalid_input_handler(request: Request, exc: InvalidInputError):
            # When mounted under `/mcp` (or a larger mount path ending in `/mcp`),
            # strip only that mount suffix so the resource_metadata URL points to
            # the top-level endpoint.
            return invalid_input_error_to_json_response(
                request,
                exc,
                oauth_resource_metadata_path=self.oauth_resource_metadata_path,
                strip_path_suffix=self.www_authenticate_strip_path_suffix,
                is_mcp=True,
            )

        app = Starlette(
            routes=[
                Mount("/", app=self._session_manager.handle_request),
            ],
            lifespan=self.lifespan,
            exception_handlers={InvalidInputError: _invalid_input_handler},
        )

        builder = self

        class _McpOAuthGateMiddleware(BaseHTTPMiddleware):
            async def dispatch(self, request: Request, call_next):
                try:
                    if builder.enforce_oauth_bearer:
                        auth_header = request.headers.get("authorization", "")
                        token = None
                        if auth_header.lower().startswith("bearer "):
                            token = auth_header[7:].strip()

                        if not token:
                            raise InvalidInputError(401, "user_required", "Authentication is required")

                        user, expiry = builder._get_user_from_headers(api_key=None, bearer_token=token)
                        request.state.sqrl_user = user
                        request.state.access_token_expiry = expiry

                    return await call_next(request)
                except InvalidInputError as exc:
                    # Starlette's BaseHTTPMiddleware may bypass exception handlers for
                    # exceptions raised within dispatch; handle explicitly here.
                    return invalid_input_error_to_json_response(
                        request,
                        exc,
                        oauth_resource_metadata_path=builder.oauth_resource_metadata_path,
                        strip_path_suffix=builder.www_authenticate_strip_path_suffix,
                        is_mcp=True,
                    )

        app.add_middleware(_McpOAuthGateMiddleware)
        return app
    