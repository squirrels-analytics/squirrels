"""
Dataset routes for parameters and results
"""
from typing import Callable, Coroutine, Any
from fastapi import FastAPI, Depends, Request
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer
from dataclasses import asdict
from cachetools import TTLCache

import time
import polars as pl

from .. import _constants as c, _utils as u
from .._schemas import response_models as rm
from .._exceptions import ConfigurationError, InvalidInputError
from .._dataset_types import DatasetResult
from .._schemas.query_param_models import get_query_models_for_parameters, get_query_models_for_dataset
from .._schemas.auth_models import AbstractUser
from .base import RouteBase, XApiKeyHeader


class DatasetRoutes(RouteBase):
    """Dataset parameter and result routes"""
    
    def __init__(self, get_bearer_token: HTTPBearer, project, no_cache: bool = False):
        super().__init__(get_bearer_token, project, no_cache)
        
        # Setup caches
        self.dataset_results_cache = TTLCache(
            maxsize=self.env_vars.datasets_cache_size, 
            ttl=self.env_vars.datasets_cache_ttl_minutes*60
        )
        
        # Setup max rows
        self.max_result_rows = self.env_vars.datasets_max_rows_output
        
        # Setup SQL query timeout
        self.sql_timeout_seconds = self.env_vars.datasets_sql_timeout_seconds
        
    async def _get_dataset_results_helper(
        self, dataset: str, user: AbstractUser, selections: tuple[tuple[str, Any], ...], configurables: tuple[tuple[str, str], ...]
    ) -> DatasetResult:
        """Helper to get dataset results"""
        # Only pass configurables that are defined in manifest
        cfg_filtered = {k: v for k, v in dict(configurables).items() if k in self.manifest_cfg.configurables}
        return await self.project._dataset_result(dataset, user=user, selections=dict(selections), configurables=cfg_filtered)

    async def _get_dataset_results_cachable(
        self, dataset: str, user: AbstractUser, selections: tuple[tuple[str, Any], ...], configurables: tuple[tuple[str, str], ...]
    ) -> DatasetResult:
        """Cachable version of dataset results helper"""
        return await self.do_cachable_action(
            self.dataset_results_cache, self._get_dataset_results_helper, dataset, user, selections, configurables
        )
    
    async def _get_dataset_result_object(
        self, dataset_name: str, user: AbstractUser, params: dict, headers: dict[str, str]
    ) -> DatasetResult:
        """Get dataset result object"""
        # self._validate_request_params(all_request_params, params, headers)

        get_dataset_function = self._get_dataset_results_helper if self.no_cache else self._get_dataset_results_cachable
        uncached_keys = {"x_sql_query", "x_orientation", "x_offset", "x_limit"}
        selections = self.get_selections_as_immutable(params, uncached_keys)
        
        user_has_elevated_privileges = u.user_has_elevated_privileges(user.access_level, self.env_vars.elevated_access_level)
        configurables = self.get_configurables_from_headers(headers) if user_has_elevated_privileges else tuple()
        result = await get_dataset_function(dataset_name, user, selections, configurables)
        
        # Apply optional final SQL transformation before select/limit/offset
        sql_query = params.get("x_sql_query")
        if sql_query:
            try:
                transformed = await u.run_polars_sql_on_dataframes(
                    sql_query, {"result": result.df.lazy()}, timeout_seconds=self.sql_timeout_seconds, max_rows=self.max_result_rows+1
                )
            except Exception as e:
                raise InvalidInputError(400, "invalid_sql_query", "Failed to run provided Polars SQL on the dataset result") from e
            
            # Enforce max result rows on transformed result
            row_count = transformed.select(pl.len()).item()
            if row_count > self.max_result_rows:
                raise InvalidInputError(
                    413,
                    "dataset_result_too_large",
                    f"The transformed dataset result exceeds the maximum allowed of {self.max_result_rows} rows."
                )
            
            transformed = transformed.drop("_row_num", strict=False).with_row_index("_row_num", offset=1)
            result = DatasetResult(target_model_config=result.target_model_config, df=transformed)
        
        return result

    async def _get_dataset_results_definition(
        self, dataset_name: str, user: AbstractUser, params: dict, headers: dict[str, str]
    ) -> rm.DatasetResultModel:
        """Get dataset results definition"""
        result = await self._get_dataset_result_object(dataset_name, user, params, headers)

        result_format = self.extract_orientation_offset_and_limit(params)
        return rm.DatasetResultModel(**result.to_json(result_format)) 
    
    def setup_routes(
        self, app: FastAPI, project_metadata_path: str, param_fields: dict, 
        get_parameters_definition: Callable[..., Coroutine[Any, Any, rm.ParametersModel]]
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
        
        async def get_dataset_parameters_updates(dataset_name: str, user: AbstractUser, params: dict):
            parameters_list = self.manifest_cfg.datasets[dataset_name].parameters
            scope = self.manifest_cfg.datasets[dataset_name].scope
            result = await get_parameters_definition(
                parameters_list, "dataset", dataset_name, scope, user, params
            )
            return result

        # Dataset parameters and results APIs
        for dataset_name, dataset_config in self.manifest_cfg.datasets.items():
            dataset_name_for_api = u.normalize_name_for_api(dataset_name)
            curr_parameters_path = dataset_parameters_path.format(dataset=dataset_name_for_api)
            curr_results_path = dataset_results_path.format(dataset=dataset_name_for_api)

            validate_parameters_list(dataset_config.parameters, "Dataset", dataset_name)

            QueryModelForGetParams, QueryModelForPostParams = get_query_models_for_parameters(param_fields, dataset_config.parameters)
            QueryModelForGetDataset, QueryModelForPostDataset = get_query_models_for_dataset(param_fields, dataset_config.parameters)

            @app.get(curr_parameters_path, tags=[f"Dataset '{dataset_name}'"], description=self._parameters_description, response_class=JSONResponse)
            async def get_dataset_parameters(
                request: Request, params: QueryModelForGetParams, user=Depends(self.get_current_user), # type: ignore
                x_api_key: str | None = XApiKeyHeader
            ) -> rm.ParametersModel:
                start = time.time()
                curr_dataset_name = self.get_name_from_path_section(request, -2)
                result = await get_dataset_parameters_updates(curr_dataset_name, user, asdict(params))
                self.logger.log_activity_time(
                    "GET REQUEST for PARAMETERS", start, additional_data={"dataset_name": curr_dataset_name}
                )
                return result

            @app.post(curr_parameters_path, tags=[f"Dataset '{dataset_name}'"], description=self._parameters_description, response_class=JSONResponse)
            async def get_dataset_parameters_with_post(
                request: Request, params: QueryModelForPostParams, user=Depends(self.get_current_user), # type: ignore
                x_api_key: str | None = XApiKeyHeader
            ) -> rm.ParametersModel:
                start = time.time()
                curr_dataset_name = self.get_name_from_path_section(request, -2)
                result = await get_dataset_parameters_updates(curr_dataset_name, user, params.model_dump())
                self.logger.log_activity_time(
                    "POST REQUEST for PARAMETERS", start, additional_data={"dataset_name": curr_dataset_name}
                )
                return result
            
            @app.get(curr_results_path, tags=[f"Dataset '{dataset_name}'"], description=dataset_config.description, response_class=JSONResponse)
            async def get_dataset_results(
                request: Request, params: QueryModelForGetDataset, user=Depends(self.get_current_user), # type: ignore
                x_api_key: str | None = XApiKeyHeader
            ) -> rm.DatasetResultModel:
                start = time.time()
                curr_dataset_name = self.get_name_from_path_section(request, -1)
                result = await self._get_dataset_results_definition(
                    curr_dataset_name, user, asdict(params), headers=dict(request.headers)
                )
                self.logger.log_activity_time(
                    "GET REQUEST for DATASET RESULTS", start, additional_data={"dataset_name": curr_dataset_name}
                )
                return result
            
            @app.post(curr_results_path, tags=[f"Dataset '{dataset_name}'"], description=dataset_config.description, response_class=JSONResponse)
            async def get_dataset_results_with_post(
                request: Request, params: QueryModelForPostDataset, user=Depends(self.get_current_user), # type: ignore
                x_api_key: str | None = XApiKeyHeader
            ) -> rm.DatasetResultModel:
                start = time.time()
                curr_dataset_name = self.get_name_from_path_section(request, -1)
                result = await self._get_dataset_results_definition(
                    curr_dataset_name, user, params.model_dump(), headers=dict(request.headers)
                )
                self.logger.log_activity_time(
                    "POST REQUEST for DATASET RESULTS", start, additional_data={"dataset_name": curr_dataset_name}
                )
                return result
    
        # MCP-callable methods (exposed as instance attributes for McpServerBuilder)
        
        async def get_dataset_parameters_for_mcp(
            dataset: str, parameter_name: str, selected_ids: str | list[str], user: AbstractUser
        ) -> rm.ParametersModel:
            """Get dataset parameter updates for MCP tools. Takes user and headers."""
            dataset_name = u.normalize_name(dataset)
            parameters = {
                "x_parent_param": parameter_name,
                parameter_name: selected_ids
            }
            return await get_dataset_parameters_updates(dataset_name, user, parameters)
        
        async def get_dataset_results_for_mcp(
            dataset: str, parameters: dict[str, Any], sql_query: str | None, user: AbstractUser, headers: dict[str, str]
        ) -> DatasetResult:
            """Get dataset results for MCP tools. Takes user and headers."""
            dataset_name = u.normalize_name(dataset)
            parameters.update({ "x_sql_query": sql_query })
            return await self._get_dataset_result_object(dataset_name, user, parameters, headers)
        
        # Store the MCP functions as instance attributes for access by McpServerBuilder
        self._get_dataset_parameters_for_mcp = get_dataset_parameters_for_mcp
        self._get_dataset_results_for_mcp = get_dataset_results_for_mcp
        