"""
MCP Server implementation using the official MCP Python SDK low-level APIs.

This module provides the MCP server for Squirrels projects, exposing:
- Tools: get_data_catalog, get_dataset_parameters, get_dataset_results
- Resources: sqrl://data-catalog
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from textwrap import dedent
from typing import Any, Callable, Coroutine

from pydantic import AnyUrl
from starlette.applications import Starlette
from starlette.routing import Mount
from starlette.types import ASGIApp

import mcp.types as types
from mcp.server.lowlevel import Server
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager

from ._exceptions import InvalidInputError
from ._schemas import response_models as rm


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
        get_data_catalog_func: Callable[[dict[str, str]], Coroutine[Any, Any, rm.CatalogModelForMcp]],
        get_dataset_parameters_func: Callable[[str, str, list[str], dict[str, str]], Coroutine[Any, Any, rm.ParametersModel]],
        get_dataset_results_func: Callable[[str, str, str | None, int, int, dict[str, str]], Coroutine[Any, Any, rm.DatasetResultModel]],
    ):
        """
        Initialize the MCP server builder.
        
        Args:
            project_name: The name of the Squirrels project
            project_label: The human-readable label of the project
            max_rows_for_ai: Maximum number of rows to return for AI tools
            get_data_catalog_func: Async function to get the data catalog
            get_dataset_parameters_func: Async function to get dataset parameters
            get_dataset_results_func: Async function to get dataset results
        """
        self.project_name = project_name
        self.project_label = project_label
        self.max_rows_for_ai = max_rows_for_ai
        self._get_data_catalog_func = get_data_catalog_func
        self._get_dataset_parameters_func = get_dataset_parameters_func
        self._get_dataset_results_func = get_dataset_results_func
        
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
    
    async def _list_tools(self) -> list[types.Tool]:
        """Return the list of available MCP tools."""
        default_for_limit = min(self.max_rows_for_ai, 10)

        return [
            types.Tool(
                name=self.catalog_tool_name,
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
                        "parameter_name": {
                            "type": "string",
                            "description": "The name of the parameter triggering the refresh",
                        },
                        "selected_ids": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "The ID(s) of the selected option(s) for the parameter",
                        },
                    },
                    "required": ["dataset", "parameter_name", "selected_ids"],
                },
                # outputSchema=rm.ParametersModel.model_json_schema(),
            ),
            types.Tool(
                name=self.results_tool_name,
                description=dedent(f"""
                    Use this tool to get the dataset results as a JSON object for a dataset in the Squirrels project "{self.project_name}".
                    - Use the "offset" and "limit" arguments to limit the number of rows you require
                    - The "limit" argument controls the number of rows returned. The maximum allowed value is {self.max_rows_for_ai}. If the 'total_num_rows' field in the response is greater than {self.max_rows_for_ai}, let the user know that only {self.max_rows_for_ai} rows are shown and clarify if they would like to see more.
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
                                - For single select, use a string for the ID of the selected value
                                - For multi select, use an array of strings for the IDs of the selected values
                                - For date, use a string like "YYYY-MM-DD"
                                - For date ranges, use array of strings like ["YYYY-MM-DD","YYYY-MM-DD"]
                                - For number, use a number like 1
                                - For number ranges, use array of numbers like [1,100]
                                - For text, use a string for the text value
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
                        },
                        "offset": {
                            "type": "integer",
                            "description": "The number of rows to skip from first row. Applied after final SQL. Default is 0.",
                            "default": 0,
                        },
                        "limit": {
                            "type": "integer",
                            "description": f"The maximum number of rows to return. Applied after final SQL. Default is {default_for_limit}. Maximum allowed value is {self.max_rows_for_ai}.",
                            "default": default_for_limit,
                        },
                    },
                    "required": ["dataset", "parameters"],
                },
                outputSchema=rm.DatasetResultModel.model_json_schema(),
            ),
        ]
    
    async def _call_tool(self, name: str, arguments: dict[str, Any] | None) -> dict[str, Any] | types.CallToolResult:
        """Handle tool calls by dispatching to the appropriate function.
        
        Returns structured data (dict) directly for successful calls, which the MCP
        framework will serialize to JSON. For errors, returns CallToolResult with isError=True.
        """
        arguments = arguments or {}
        headers = self._get_request_headers()
        
        try:
            if name == self.catalog_tool_name:
                result = await self._get_data_catalog_func(headers)
                return result.model_dump(mode="json")
            
            elif name == self.parameters_tool_name:
                dataset = arguments.get("dataset", "")
                parameter_name = arguments.get("parameter_name", "")
                selected_ids = arguments.get("selected_ids", [])
                
                result = await self._get_dataset_parameters_func(dataset, parameter_name, selected_ids, headers)
                return result.model_dump(mode="json")
            
            elif name == self.results_tool_name:
                dataset = arguments.get("dataset", "")
                parameters_json = arguments.get("parameters", "{}")
                sql_query = arguments.get("sql_query")
                offset = arguments.get("offset", 0)
                limit = arguments.get("limit", self.max_rows_for_ai)
                
                if limit > self.max_rows_for_ai:
                    raise ValueError(f"The maximum number of rows to return is {self.max_rows_for_ai}.")
                
                result = await self._get_dataset_results_func(dataset, parameters_json, sql_query, offset, limit, headers)
                return result.model_dump(mode="json")
            
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
            result = await self._get_data_catalog_func(headers)
            return result.model_dump_json()
        else:
            raise ValueError(f"Unknown resource URI: {uri}")
    
    def get_asgi_app(self) -> ASGIApp:
        """
        Get the ASGI app for the MCP server.
        """
        @asynccontextmanager
        async def lifespan(app: Starlette) -> AsyncIterator[None]:
            async with self._session_manager.run():
                yield
        
        app = Starlette(
            routes=[
                Mount("/", app=self._session_manager.handle_request),
            ],
            lifespan=lifespan,
        )
        return app
    
    @asynccontextmanager
    async def lifespan(self):
        """
        Async context manager for the MCP session manager lifecycle.
        
        Use this in the FastAPI app lifespan to ensure proper startup/shutdown.
        """
        async with self._session_manager.run():
            yield
