"""
Dashboard routes for parameters and results
"""
from typing import Callable, Coroutine, Any
from fastapi import FastAPI, Depends, Request, Response
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.security import HTTPBearer
from dataclasses import asdict
from cachetools import TTLCache
import time

from .. import _constants as c, _utils as u
from .._schemas import response_models as rm
from .._exceptions import ConfigurationError
from .._dashboards import Dashboard
from .._schemas.query_param_models import get_query_models_for_parameters, get_query_models_for_dashboard
from .._auth import BaseUser
from .base import RouteBase


class DashboardRoutes(RouteBase):
    """Dashboard parameter and result routes"""
    
    def __init__(self, get_bearer_token: HTTPBearer, project, no_cache: bool = False):
        super().__init__(get_bearer_token, project, no_cache)
        
        # Setup caches
        dashboard_results_cache_size = int(self.env_vars.get(c.SQRL_DASHBOARDS_CACHE_SIZE, 128))
        dashboard_results_cache_ttl = int(self.env_vars.get(c.SQRL_DASHBOARDS_CACHE_TTL_MINUTES, 60))
        self.dashboard_results_cache = TTLCache(maxsize=dashboard_results_cache_size, ttl=dashboard_results_cache_ttl*60)
        
    async def _get_dashboard_results_helper(
        self, dashboard: str, user: BaseUser, selections: tuple[tuple[str, Any], ...], configurables: tuple[tuple[str, str], ...]
    ) -> Dashboard:
        """Helper to get dashboard results"""
        cfg_filtered = {k: v for k, v in dict(configurables).items() if k in self.manifest_cfg.configurables}
        return await self.project.dashboard(dashboard, user, selections=dict(selections), configurables=cfg_filtered)
    
    async def _get_dashboard_results_cachable(
        self, dashboard: str, user: BaseUser, selections: tuple[tuple[str, Any], ...], configurables: tuple[tuple[str, str], ...]
    ) -> Dashboard:
        """Cachable version of dashboard results helper"""
        return await self.do_cachable_action(self.dashboard_results_cache, self._get_dashboard_results_helper, dashboard, user, selections, configurables)
    
    async def _get_dashboard_results_definition(
        self, dashboard_name: str, user: BaseUser, all_request_params: dict, params: dict, headers: dict[str, str]
    ) -> Response:
        """Get dashboard results definition"""
        self._validate_request_params(all_request_params, params)
        
        get_dashboard_function = self._get_dashboard_results_helper if self.no_cache else self._get_dashboard_results_cachable
        selections = self.get_selections_as_immutable(params, uncached_keys={"x_verify_params"})
        configurables = self.get_configurables_from_headers(headers) if user.access_level == "admin" else tuple[tuple[str, str], ...]()
        dashboard_obj = await get_dashboard_function(dashboard_name, user, selections, configurables)
        
        if dashboard_obj._format == c.PNG:
            assert isinstance(dashboard_obj._content, bytes)
            result = Response(dashboard_obj._content, media_type="image/png")
        elif dashboard_obj._format == c.HTML:
            result = HTMLResponse(dashboard_obj._content)
        else:
            raise NotImplementedError()
        return result 
    
    def setup_routes(
        self, app: FastAPI, project_metadata_path: str, param_fields: dict, get_parameters_definition: Callable[..., Coroutine[Any, Any, rm.ParametersModel]]
    ) -> None:
        """Setup dashboard routes"""
        
        dashboard_results_path = project_metadata_path + '/dashboard/{dashboard}'
        dashboard_parameters_path = dashboard_results_path + '/parameters'
        
        def validate_parameters_list(parameters: list[str] | None, entity_type: str, dashboard_name: str) -> None:
            if parameters is None:
                return
            for param in parameters:
                if param not in param_fields:
                    all_params = list(param_fields.keys())
                    raise ConfigurationError(
                        f"{entity_type} '{dashboard_name}' use parameter '{param}' which doesn't exist. Available parameters are:"
                        f"\n  {all_params}"
                    )
        
        # Dashboard parameters and results APIs
        for dashboard_name, dashboard in self.project._dashboards.items():
            dashboard_normalized = u.normalize_name_for_api(dashboard_name)
            curr_parameters_path = dashboard_parameters_path.format(dashboard=dashboard_normalized)
            curr_results_path = dashboard_results_path.format(dashboard=dashboard_normalized)

            validate_parameters_list(dashboard.config.parameters, "Dashboard", dashboard_name)
            
            QueryModelForGetParams, QueryModelForPostParams = get_query_models_for_parameters(dashboard.config.parameters, param_fields)
            QueryModelForGetDash, QueryModelForPostDash = get_query_models_for_dashboard(dashboard.config.parameters, param_fields)

            @app.get(curr_parameters_path, tags=[f"Dashboard '{dashboard_name}'"], description=self._parameters_description, response_class=JSONResponse)
            async def get_dashboard_parameters(
                request: Request, params: QueryModelForGetParams, user=Depends(self.get_current_user) # type: ignore
            ) -> rm.ParametersModel:
                start = time.time()
                curr_dashboard_name = self.get_name_from_path_section(request, -2)
                parameters_list = self.project._dashboards[curr_dashboard_name].config.parameters    
                scope = self.project._dashboards[curr_dashboard_name].config.scope
                result = await get_parameters_definition(
                    parameters_list, "dashboard", curr_dashboard_name, scope, user, dict(request.query_params), asdict(params)
                )
                self.log_activity_time("GET REQUEST for PARAMETERS", start, request)
                return result

            @app.post(curr_parameters_path, tags=[f"Dashboard '{dashboard_name}'"], description=self._parameters_description, response_class=JSONResponse)
            async def get_dashboard_parameters_with_post(
                request: Request, params: QueryModelForPostParams, user=Depends(self.get_current_user) # type: ignore
            ) -> rm.ParametersModel:
                start = time.time()
                curr_dashboard_name = self.get_name_from_path_section(request, -2)
                parameters_list = self.project._dashboards[curr_dashboard_name].config.parameters
                scope = self.project._dashboards[curr_dashboard_name].config.scope
                payload: dict = await request.json()
                result = await get_parameters_definition(
                    parameters_list, "dashboard", curr_dashboard_name, scope, user, payload, params.model_dump()
                )
                self.log_activity_time("POST REQUEST for PARAMETERS", start, request)
                return result
            
            @app.get(curr_results_path, tags=[f"Dashboard '{dashboard_name}'"], description=dashboard.config.description, response_class=Response)
            async def get_dashboard_results(
                request: Request, params: QueryModelForGetDash, user=Depends(self.get_current_user) # type: ignore
            ) -> Response:
                start = time.time()
                curr_dashboard_name = self.get_name_from_path_section(request, -1)
                result = await self._get_dashboard_results_definition(
                    curr_dashboard_name, user, dict(request.query_params), asdict(params), headers=dict(request.headers)
                )
                self.log_activity_time("GET REQUEST for DASHBOARD RESULTS", start, request)
                return result

            @app.post(curr_results_path, tags=[f"Dashboard '{dashboard_name}'"], description=dashboard.config.description, response_class=Response)
            async def get_dashboard_results_with_post(
                request: Request, params: QueryModelForPostDash, user=Depends(self.get_current_user) # type: ignore
            ) -> Response:
                start = time.time()
                curr_dashboard_name = self.get_name_from_path_section(request, -1)
                payload: dict = await request.json()
                result = await self._get_dashboard_results_definition(
                    curr_dashboard_name, user, payload, params.model_dump(), headers=dict(request.headers)
                )
                self.log_activity_time("POST REQUEST for DASHBOARD RESULTS", start, request)
                return result
    