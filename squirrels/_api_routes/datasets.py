"""
Dataset routes for parameters and results
"""
from typing import Callable, Coroutine, Any
from pydantic import Field
from fastapi import FastAPI, Depends, Request
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer

from mcp.server.fastmcp import FastMCP, Context
from dataclasses import asdict
from cachetools import TTLCache
from textwrap import dedent

import time, json

from .. import _constants as c, _utils as u
from .._schemas import response_models as rm
from .._exceptions import ConfigurationError, InvalidInputError
from .._dataset_types import DatasetResult
from .._schemas.query_param_models import get_query_models_for_parameters, get_query_models_for_dataset
from .._schemas.auth_models import AbstractUser
from .base import RouteBase


class DatasetRoutes(RouteBase):
    """Dataset parameter and result routes"""
    
    def __init__(self, get_bearer_token: HTTPBearer, project, no_cache: bool = False):
        super().__init__(get_bearer_token, project, no_cache)
        
        # Setup caches
        dataset_results_cache_size = int(self.env_vars.get(c.SQRL_DATASETS_CACHE_SIZE, 128))
        dataset_results_cache_ttl = int(self.env_vars.get(c.SQRL_DATASETS_CACHE_TTL_MINUTES, 60))
        self.dataset_results_cache = TTLCache(maxsize=dataset_results_cache_size, ttl=dataset_results_cache_ttl*60)
        
        # Setup max rows for AI
        self.max_rows_for_ai = int(self.env_vars.get(c.SQRL_DATASETS_MAX_ROWS_FOR_AI, 100))
        
    async def _get_dataset_results_helper(
        self, dataset: str, user: AbstractUser, selections: tuple[tuple[str, Any], ...], configurables: tuple[tuple[str, str], ...]
    ) -> DatasetResult:
        """Helper to get dataset results"""
        # Only pass configurables that are defined in manifest
        cfg_filtered = {k: v for k, v in dict(configurables).items() if k in self.manifest_cfg.configurables}
        return await self.project.dataset(dataset, user=user, selections=dict(selections), configurables=cfg_filtered)

    async def _get_dataset_results_cachable(
        self, dataset: str, user: AbstractUser, selections: tuple[tuple[str, Any], ...], configurables: tuple[tuple[str, str], ...]
    ) -> DatasetResult:
        """Cachable version of dataset results helper"""
        return await self.do_cachable_action(self.dataset_results_cache, self._get_dataset_results_helper, dataset, user, selections, configurables)
    
    async def _get_dataset_results_definition(
        self, dataset_name: str, user: AbstractUser, all_request_params: dict, params: dict, headers: dict[str, str]
    ) -> rm.DatasetResultModel:
        """Get dataset results definition"""
        self._validate_request_params(all_request_params, params, headers)

        get_dataset_function = self._get_dataset_results_helper if self.no_cache else self._get_dataset_results_cachable
        uncached_keys = {"x_verify_params", "x_orientation", "x_sql_query", "x_limit", "x_offset"}
        selections = self.get_selections_as_immutable(params, uncached_keys)
        
        user_has_elevated_privileges = u.user_has_elevated_privileges(user.access_level, self.project._elevated_access_level)
        configurables = self.get_configurables_from_headers(headers) if user_has_elevated_privileges else tuple()
        result = await get_dataset_function(dataset_name, user, selections, configurables)
        
        # Apply optional final SQL transformation before select/limit/offset
        sql_query = params.get("x_sql_query")
        if sql_query:
            try:
                transformed = u.run_sql_on_dataframes(sql_query, {"result": result.df.lazy()})
            except Exception as e:
                raise InvalidInputError(400, "invalid_sql_query", "Failed to run provided SQL on the dataset result") from e
            
            transformed = transformed.drop("_row_num", strict=False).with_row_index("_row_num", offset=1)
            result = DatasetResult(target_model_config=result.target_model_config, df=transformed)
        
        orientation_header = headers.get("x-orientation")
        orientation = str(orientation_header).lower() if orientation_header is not None else params.get("x_orientation", "records")
        limit = params.get("x_limit", 1000)
        offset = params.get("x_offset", 0)
        return rm.DatasetResultModel(**result.to_json(orientation, limit, offset)) 
    
    def setup_routes(
        self, app: FastAPI, mcp: FastMCP, project_metadata_path: str, project_name: str, project_label: str, 
        param_fields: dict, get_parameters_definition: Callable[..., Coroutine[Any, Any, rm.ParametersModel]]
    ) -> None:
        """Setup dataset routes"""
        
        dataset_results_path = project_metadata_path + '/dataset/{dataset}'
        dataset_parameters_path = dataset_results_path + '/parameters'
        
        def validate_parameters_list(parameters: list[str] | None, entity_type: str, dataset_name: str) -> None:
            if parameters is None:
                return
            for param in parameters:
                if param not in param_fields:
                    all_params = list(param_fields.keys())
                    raise ConfigurationError(
                        f"{entity_type} '{dataset_name}' use parameter '{param}' which doesn't exist. Available parameters are:"
                        f"\n  {all_params}"
                    )
        
        async def get_dataset_parameters_updates(dataset_name: str, user: AbstractUser, all_request_params: dict, params: dict, headers: dict[str, str]):
            parameters_list = self.manifest_cfg.datasets[dataset_name].parameters
            scope = self.manifest_cfg.datasets[dataset_name].scope
            result = await get_parameters_definition(
                parameters_list, "dataset", dataset_name, scope, user, all_request_params, params, headers=headers
            )
            return result

        # Dataset parameters and results APIs
        for dataset_name, dataset_config in self.manifest_cfg.datasets.items():
            dataset_name_for_api = u.normalize_name_for_api(dataset_name)
            curr_parameters_path = dataset_parameters_path.format(dataset=dataset_name_for_api)
            curr_results_path = dataset_results_path.format(dataset=dataset_name_for_api)

            validate_parameters_list(dataset_config.parameters, "Dataset", dataset_name)

            QueryModelForGetParams, QueryModelForPostParams = get_query_models_for_parameters(dataset_config.parameters, param_fields)
            QueryModelForGetDataset, QueryModelForPostDataset = get_query_models_for_dataset(dataset_config.parameters, param_fields)

            @app.get(curr_parameters_path, tags=[f"Dataset '{dataset_name}'"], description=self._parameters_description, response_class=JSONResponse)
            async def get_dataset_parameters(
                request: Request, params: QueryModelForGetParams, user=Depends(self.get_current_user) # type: ignore
            ) -> rm.ParametersModel:
                start = time.time()
                curr_dataset_name = self.get_name_from_path_section(request, -2)
                result = await get_dataset_parameters_updates(curr_dataset_name, user, dict(request.query_params), asdict(params), dict(request.headers))
                self.log_activity_time("GET REQUEST for PARAMETERS", start, request)
                return result

            @app.post(curr_parameters_path, tags=[f"Dataset '{dataset_name}'"], description=self._parameters_description, response_class=JSONResponse)
            async def get_dataset_parameters_with_post(
                request: Request, params: QueryModelForPostParams, user=Depends(self.get_current_user) # type: ignore
            ) -> rm.ParametersModel:
                start = time.time()
                curr_dataset_name = self.get_name_from_path_section(request, -2)
                payload: dict = await request.json()
                result = await get_dataset_parameters_updates(curr_dataset_name, user, payload, params.model_dump(), dict(request.headers))
                self.log_activity_time("POST REQUEST for PARAMETERS", start, request)
                return result
            
            @app.get(curr_results_path, tags=[f"Dataset '{dataset_name}'"], description=dataset_config.description, response_class=JSONResponse)
            async def get_dataset_results(
                request: Request, params: QueryModelForGetDataset, user=Depends(self.get_current_user) # type: ignore
            ) -> rm.DatasetResultModel:
                start = time.time()
                curr_dataset_name = self.get_name_from_path_section(request, -1)
                result = await self._get_dataset_results_definition(
                    curr_dataset_name, user, dict(request.query_params), asdict(params), headers=dict(request.headers)
                )
                self.log_activity_time("GET REQUEST for DATASET RESULTS", start, request)
                return result
            
            @app.post(curr_results_path, tags=[f"Dataset '{dataset_name}'"], description=dataset_config.description, response_class=JSONResponse)
            async def get_dataset_results_with_post(
                request: Request, params: QueryModelForPostDataset, user=Depends(self.get_current_user) # type: ignore
            ) -> rm.DatasetResultModel:
                start = time.time()
                curr_dataset_name = self.get_name_from_path_section(request, -1)
                payload: dict = await request.json()
                result = await self._get_dataset_results_definition(
                    curr_dataset_name, user, payload, params.model_dump(), headers=dict(request.headers)
                )
                self.log_activity_time("POST REQUEST for DATASET RESULTS", start, request)
                return result
    
        # Setup MCP tools

        @mcp.tool(
            name=f"get_dataset_parameters_from_{project_name}",
            title=f"Get Dataset Parameters Updates (Project: {project_label})",
            description=dedent(f"""
            Use this tool to get updates for dataset parameters in the Squirrels project "{project_name}" when a selection is to be made on a parameter with "trigger_refresh" as true.

            For example, suppose there are two parameters, "country" and "city", and the user selects "United States" for "country". If "country" has the "trigger_refresh" field as true, then this tool should be called to get the updates for other parameters such as "city".

            Do not use this tool on parameters whose "trigger_refresh" field is false!
            """).strip()
        )
        async def get_dataset_parameters_tool(
            ctx: Context,
            dataset: str = Field(description="The name of the dataset whose parameters the trigger parameter will update"),
            parameter_name: str = Field(description="The name of the parameter triggering the refresh"),
            selected_ids: list[str] = Field(description="The ID(s) of the selected option(s) for the parameter"),
        ) -> rm.ParametersModel:
            headers = self.get_headers_from_tool_ctx(ctx)
            user = self.get_user_from_tool_headers(headers)
            dataset_name = u.normalize_name(dataset)
            payload = {
                "x_parent_param": parameter_name,
                parameter_name: selected_ids
            }
            return await get_dataset_parameters_updates(dataset_name, user, payload, payload, headers)
        
        @mcp.tool(
            name=f"get_dataset_results_from_{project_name}",
            title=f"Get Dataset Results (Project: {project_label})",
            description=dedent(f"""
            Use this tool to get the dataset results as a JSON object for a dataset in the Squirrels project "{project_name}".
            - Use the "offset" and "limit" arguments to limit the number of rows you require
            - The "limit" argument controls the number of rows returned. The maximum allowed value is {self.max_rows_for_ai}. If the 'total_num_rows' field in the response is greater than {self.max_rows_for_ai}, let the user know that only {self.max_rows_for_ai} rows are shown and clarify if they would like to see more.
            """).strip()
        )
        async def get_dataset_results_tool(
            ctx: Context,
            dataset: str = Field(description="The name of the dataset to get results for"),
            parameters: str = Field(description=dedent("""
            A JSON object (as string) containing key-value pairs for parameter name and selected value. The selected value to provide depends on the parameter widget type:
            - For single select, use a string for the ID of the selected value
            - For multi select, use an array of strings for the IDs of the selected values
            - For date, use a string like "YYYY-MM-DD"
            - For date ranges, use array of strings like ["YYYY-MM-DD","YYYY-MM-DD"]
            - For number, use a number like 1
            - For number ranges, use array of numbers like [1,100]
            - For text, use a string for the text value
            - Complex objects are NOT supported
            """).strip()),
            sql_query: str | None = Field(None, description=dedent("""
            A custom DuckDB SQL query to execute on the final dataset result. 
            - Use table name 'result' to reference the dataset result.
            - Use this to apply transformations to the dataset result if needed (such as filtering, sorting, or selecting columns).
            - If not provided, the dataset result is returned as is.
            """).strip()),
            offset: int = Field(0, description="The number of rows to skip from first row. Applied after final SQL. Default is 0."),
            limit: int = Field(self.max_rows_for_ai, description=f"The maximum number of rows to return. Applied after final SQL. Default is {self.max_rows_for_ai}. Maximum allowed value is {self.max_rows_for_ai}."),
        ) -> rm.DatasetResultModel:
            if limit > self.max_rows_for_ai:
                raise ValueError(f"The maximum number of rows to return is {self.max_rows_for_ai}.")

            headers = self.get_headers_from_tool_ctx(ctx)
            user = self.get_user_from_tool_headers(headers)
            dataset_name = u.normalize_name(dataset)
            
            try:
                params = json.loads(parameters)
            except json.JSONDecodeError:
                params = None # error handled below
            
            if not isinstance(params, dict):
                raise InvalidInputError(400, "invalid_parameters", f"The 'parameters' argument must be a JSON object.")
            
            params.update({
                "x_sql_query": sql_query,
                "x_offset": offset,
                "x_limit": limit
            })

            # Set default orientation as rows if not provided
            if "x-orientation" not in headers:
                headers["x-orientation"] = "rows"
            
            result = await self._get_dataset_results_definition(dataset_name, user, params, params, headers)
            return result
        