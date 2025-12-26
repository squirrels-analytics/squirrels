"""
Data management routes for build and query models
"""
from typing import Any
from fastapi import FastAPI, Depends, Request, Response, status
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer
from dataclasses import asdict
from cachetools import TTLCache
import time

from .. import _constants as c, _utils as u
from .._schemas import response_models as rm
from .._exceptions import InvalidInputError
from .._schemas.auth_models import AbstractUser
from .._dataset_types import DatasetResult
from .._schemas.query_param_models import get_query_models_for_querying_models, get_query_models_for_compiled_models
from .base import RouteBase, XApiKeyHeader


class DataManagementRoutes(RouteBase):
    """Data management routes for build and query operations"""
    
    def __init__(self, get_bearer_token: HTTPBearer, project, no_cache: bool = False):
        super().__init__(get_bearer_token, project, no_cache)
        
        # Setup cache (same settings as dataset results cache)
        self.query_models_cache = TTLCache(
            maxsize=self.env_vars.datasets_cache_size, 
            ttl=self.env_vars.datasets_cache_ttl_minutes*60
        )
        
    async def _query_models_helper(
        self, sql_query: str, user: AbstractUser, selections: tuple[tuple[str, Any], ...], configurables: tuple[tuple[str, str], ...]
    ) -> DatasetResult:
        """Helper to query models"""
        cfg_filtered = {k: v for k, v in dict(configurables).items() if k in self.manifest_cfg.configurables}
        return await self.project.query_models(sql_query, user=user, selections=dict(selections), configurables=cfg_filtered)

    async def _query_models_cachable(
        self, sql_query: str, user: AbstractUser, selections: tuple[tuple[str, Any], ...], configurables: tuple[tuple[str, str], ...]
    ) -> DatasetResult:
        """Cachable version of query models helper"""
        return await self.do_cachable_action(self.query_models_cache, self._query_models_helper, sql_query, user, selections, configurables)

    async def _query_models_definition(
        self, user: AbstractUser, params: dict, *, headers: dict[str, str]
    ) -> rm.DatasetResultModel:
        """Query models definition"""
        if not u.user_has_elevated_privileges(user.access_level, self.env_vars.elevated_access_level):
            raise InvalidInputError(403, "unauthorized_access_to_query_models", f"User '{user}' does not have permission to query data models")
        
        sql_query = params.get("x_sql_query")
        if sql_query is None:
            raise InvalidInputError(400, "sql_query_required", "SQL query must be provided")
        
        query_models_function = self._query_models_helper if self.no_cache else self._query_models_cachable
        uncached_keys = {"x_sql_query", "x_orientation", "x_offset", "x_limit"}
        selections = self.get_selections_as_immutable(params, uncached_keys)
        configurables = self.get_configurables_from_headers(headers)
        result = await query_models_function(sql_query, user, selections, configurables)
        
        result_format = self.extract_orientation_offset_and_limit(params)
        return rm.DatasetResultModel(**result.to_json(result_format)) 
    
    async def _get_compiled_model_definition(
        self, model_name: str, user: AbstractUser, params: dict, *, headers: dict[str, str]
    ) -> rm.CompiledQueryModel:
        """Get compiled model definition"""
        normalized_model_name = u.normalize_name(model_name)
        # self._validate_request_params(all_request_params, params, headers)

        # Internal users only
        if not u.user_has_elevated_privileges(user.access_level, self.env_vars.elevated_access_level):
            raise InvalidInputError(403, "unauthorized_access_to_compile_model", f"User '{user}' does not have permission to fetch compiled SQL")
        
        selections = self.get_selections_as_immutable(params, uncached_keys=set())
        configurables = self.get_configurables_from_headers(headers)
        cfg_filtered = {k: v for k, v in dict(configurables).items() if k in self.manifest_cfg.configurables}
        return await self.project.get_compiled_model_query(normalized_model_name, user=user, selections=dict(selections), configurables=cfg_filtered)
        
    def setup_routes(self, app: FastAPI, project_metadata_path: str, param_fields: dict) -> None:
        """Setup data management routes"""
        
        # Build project endpoint
        build_path = project_metadata_path + '/build'
        
        @app.post(build_path, tags=["Data Management"], summary="Build or update the Virtual Data Lake (VDL) for the project")
        async def build(
            user=Depends(self.get_current_user), # type: ignore
            x_api_key: str | None = XApiKeyHeader
        ):
            if not u.user_has_elevated_privileges(user.access_level, self.env_vars.elevated_access_level):
                raise InvalidInputError(403, "unauthorized_access_to_build_model", f"User '{user}' does not have permission to build the virtual data lake (VDL)")
            await self.project.build()
            return Response(status_code=status.HTTP_200_OK)
        
        # Query result endpoints
        query_models_path = project_metadata_path + '/query-result'
        QueryModelForQueryModels, QueryModelForPostQueryModels = get_query_models_for_querying_models(param_fields)

        @app.get(query_models_path, tags=["Data Management"], response_class=JSONResponse)
        async def query_models(
            request: Request, params: QueryModelForQueryModels, user=Depends(self.get_current_user), # type: ignore
            x_api_key: str | None = XApiKeyHeader
        ) -> rm.DatasetResultModel:
            start = time.time()
            result = await self._query_models_definition(user, asdict(params), headers=dict(request.headers))
            self.logger.log_activity_time("GET REQUEST for QUERY MODELS", start)
            return result
        
        @app.post(query_models_path, tags=["Data Management"], response_class=JSONResponse)
        async def query_models_with_post(
            request: Request, params: QueryModelForPostQueryModels, user=Depends(self.get_current_user), # type: ignore
            x_api_key: str | None = XApiKeyHeader
        ) -> rm.DatasetResultModel:
            start = time.time()
            result = await self._query_models_definition(user, params.model_dump(), headers=dict(request.headers))
            self.logger.log_activity_time("POST REQUEST for QUERY MODELS", start)
            return result

        # Compiled models endpoints - TODO: remove duplication
        compiled_models_path = project_metadata_path + '/compiled-models/{model_name}'
        QueryModelForGetCompiled, QueryModelForPostCompiled = get_query_models_for_compiled_models(param_fields)

        @app.get(compiled_models_path, tags=["Data Management"], response_class=JSONResponse, summary="Get compiled definition for a model")
        async def get_compiled_model(
            request: Request, model_name: str, params: QueryModelForGetCompiled, user=Depends(self.get_current_user), # type: ignore
            x_api_key: str | None = XApiKeyHeader
        ) -> rm.CompiledQueryModel:
            start = time.time()
            result = await self._get_compiled_model_definition(model_name, user, asdict(params), headers=dict(request.headers))
            self.logger.log_activity_time(
                "GET REQUEST for GET COMPILED MODEL", start, additional_data={"model_name": model_name}
            )
            return result

        @app.post(compiled_models_path, tags=["Data Management"], response_class=JSONResponse, summary="Get compiled definition for a model")
        async def get_compiled_model_with_post(
            request: Request, model_name: str, params: QueryModelForPostCompiled, user=Depends(self.get_current_user), # type: ignore
            x_api_key: str | None = XApiKeyHeader
        ) -> rm.CompiledQueryModel:
            start = time.time()
            result = await self._get_compiled_model_definition(model_name, user, params.model_dump(), headers=dict(request.headers))
            self.logger.log_activity_time(
                "POST REQUEST for GET COMPILED MODEL", start, additional_data={"model_name": model_name}
            )
            return result
    