"""
Data management routes for build and query models
"""
from typing import Callable, Any
from fastapi import FastAPI, Depends, Request, Response, status
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer
from dataclasses import asdict
from cachetools import TTLCache
import time

from .. import _constants as c
from .._schemas import response_models as rm
from .._exceptions import InvalidInputError
from .._auth import BaseUser
from .._manifest import PermissionScope
from .._dataset_types import DatasetResult
from .._schemas.query_param_models import get_query_models_for_querying_models
from .base import RouteBase


class DataManagementRoutes(RouteBase):
    """Data management routes for build and query operations"""
    
    def __init__(self, get_bearer_token: HTTPBearer, project, no_cache: bool = False):
        super().__init__(get_bearer_token, project, no_cache)
        
        # Setup cache (shared with dataset results cache)
        dataset_results_cache_size = int(self.env_vars.get(c.SQRL_DATASETS_CACHE_SIZE, 128))
        dataset_results_cache_ttl = int(self.env_vars.get(c.SQRL_DATASETS_CACHE_TTL_MINUTES, 60))
        self.query_models_cache = TTLCache(maxsize=dataset_results_cache_size, ttl=dataset_results_cache_ttl*60)
        
    async def _query_models_helper(
        self, sql_query: str, user: BaseUser | None, selections: tuple[tuple[str, Any], ...]
    ) -> DatasetResult:
        """Helper to query models"""
        return await self.project.query_models(sql_query, selections=dict(selections), user=user)

    async def _query_models_cachable(
        self, sql_query: str, user: BaseUser | None, selections: tuple[tuple[str, Any], ...]
    ) -> DatasetResult:
        """Cachable version of query models helper"""
        return await self.do_cachable_action(self.query_models_cache, self._query_models_helper, sql_query, user, selections)

    async def _query_models_definition(
        self, user: BaseUser | None, all_request_params: dict, params: dict
    ) -> rm.DatasetResultModel:
        """Query models definition"""
        self._validate_request_params(all_request_params, params)

        if not self.authenticator.can_user_access_scope(user, PermissionScope.PRIVATE):
            raise InvalidInputError(403, "Forbidden to query data models", f"User '{user}' does not have permission to query data models")
        
        sql_query = params.get("x_sql_query")
        if sql_query is None:
            raise InvalidInputError(400, "SQL query must be provided", "SQL query must be provided")
        
        query_models_function = self._query_models_helper if self.no_cache else self._query_models_cachable
        uncached_keys = {"x_verify_params", "x_sql_query", "x_orientation", "x_limit", "x_offset"}
        selections = self.get_selections_as_immutable(params, uncached_keys)
        result = await query_models_function(sql_query, user, selections)
        
        orientation = params.get("x_orientation", "records")
        limit = params.get("x_limit", 1000)
        offset = params.get("x_offset", 0)
        return rm.DatasetResultModel(**result.to_json(orientation, tuple(), limit, offset)) 
    
    def setup_routes(self, app: FastAPI, project_metadata_path: str, param_fields: dict) -> None:
        """Setup data management routes"""
        
        # Build project endpoint
        build_path = project_metadata_path + '/build'
        
        @app.post(build_path, tags=["Data Management"], summary="Build or update the virtual data environment for the project")
        async def build(user=Depends(self.get_current_user)): # type: ignore
            if not self.authenticator.can_user_access_scope(user, PermissionScope.PRIVATE):
                raise InvalidInputError(403, "Forbidden to build", f"User '{user}' does not have permission to build the virtual data environment")
            await self.project.build(stage_file=True)
            return Response(status_code=status.HTTP_200_OK)
        
        # Query models endpoints
        query_models_path = project_metadata_path + '/query-models'
        QueryModelForQueryModels, QueryModelForPostQueryModels = get_query_models_for_querying_models(param_fields)

        @app.get(query_models_path, tags=["Data Management"], response_class=JSONResponse)
        async def query_models(
            request: Request, params: QueryModelForQueryModels, user=Depends(self.get_current_user)  # type: ignore
        ) -> rm.DatasetResultModel:
            start = time.time()
            result = await self._query_models_definition(user, dict(request.query_params), asdict(params))
            self.log_activity_time("GET REQUEST for QUERY MODELS", start, request)
            return result
        
        @app.post(query_models_path, tags=["Data Management"], response_class=JSONResponse)
        async def query_models_with_post(
            request: Request, params: QueryModelForPostQueryModels, user=Depends(self.get_current_user)  # type: ignore
        ) -> rm.DatasetResultModel:
            start = time.time()
            payload: dict = await request.json()
            result = await self._query_models_definition(user, payload, params.model_dump())
            self.log_activity_time("POST REQUEST for QUERY MODELS", start, request)
            return result
    