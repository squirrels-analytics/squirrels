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
from .._schemas.auth_models import AbstractUser
from .base import RouteBase, XApiKeyHeader


class DashboardRoutes(RouteBase):
    """Dashboard parameter and result routes"""
    
    def __init__(self, get_bearer_token: HTTPBearer, project, no_cache: bool = False):
        super().__init__(get_bearer_token, project, no_cache)
        
        # Setup caches
        self.dashboard_results_cache = TTLCache(
            maxsize=self.env_vars.dashboards_cache_size, 
            ttl=self.env_vars.dashboards_cache_ttl_minutes*60
        )
        
    async def _get_dashboard_results_helper(
        self, dashboard: str, user: AbstractUser, selections: tuple[tuple[str, Any], ...], configurables: tuple[tuple[str, str], ...]
    ) -> Dashboard:
        """Helper to get dashboard results"""
        cfg_filtered = {k: v for k, v in dict(configurables).items() if k in self.manifest_cfg.configurables}
        return await self.project.dashboard(dashboard, user=user, selections=dict(selections), configurables=cfg_filtered)
    
    async def _get_dashboard_results_cachable(
        self, dashboard: str, user: AbstractUser, selections: tuple[tuple[str, Any], ...], configurables: tuple[tuple[str, str], ...]
    ) -> Dashboard:
        """Cachable version of dashboard results helper"""
        return await self.do_cachable_action(self.dashboard_results_cache, self._get_dashboard_results_helper, dashboard, user, selections, configurables)
    
    async def _get_dashboard_results_definition(
        self, dashboard_name: str, user: AbstractUser, params: dict, headers: dict[str, str]
    ) -> Response:
        """Get dashboard results definition"""
        get_dashboard_function = self._get_dashboard_results_helper if self.no_cache else self._get_dashboard_results_cachable
        selections = self.get_selections_as_immutable(params, uncached_keys=set())
        
        user_has_elevated_privileges = u.user_has_elevated_privileges(user.access_level, self.env_vars.elevated_access_level)
        configurables = self.get_configurables_from_headers(headers) if user_has_elevated_privileges else tuple()
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
        self, app: FastAPI, project_metadata_path: str, param_fields: dict, 
        get_parameters_definition: Callable[..., Coroutine[Any, Any, rm.ParametersModel]]
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
            dashboard_name_for_api = u.normalize_name_for_api(dashboard_name)
            curr_parameters_path = dashboard_parameters_path.format(dashboard=dashboard_name_for_api)
            curr_results_path = dashboard_results_path.format(dashboard=dashboard_name_for_api)

            validate_parameters_list(dashboard.config.parameters, "Dashboard", dashboard_name)
            
            QueryModelForGetParams, QueryModelForPostParams = get_query_models_for_parameters(param_fields, dashboard.config.parameters)
            QueryModelForGetDash, QueryModelForPostDash = get_query_models_for_dashboard(param_fields, dashboard.config.parameters)

            @app.get(curr_parameters_path, tags=[f"Dashboard '{dashboard_name}'"], description=self._parameters_description, response_class=JSONResponse)
            async def get_dashboard_parameters(
                request: Request, params: QueryModelForGetParams, user=Depends(self.get_current_user), # type: ignore
                x_api_key: str | None = XApiKeyHeader
            ) -> rm.ParametersModel:
                start = time.time()
                curr_dashboard_name = self.get_name_from_path_section(request, -2)
                parameters_list = self.project._dashboards[curr_dashboard_name].config.parameters    
                scope = self.project._dashboards[curr_dashboard_name].config.scope
                result = await get_parameters_definition(
                    parameters_list, "dashboard", curr_dashboard_name, scope, user, asdict(params)
                )
                self.logger.log_activity_time(
                    "GET REQUEST for PARAMETERS", start, additional_data={"dashboard_name": curr_dashboard_name}
                )
                return result

            @app.post(curr_parameters_path, tags=[f"Dashboard '{dashboard_name}'"], description=self._parameters_description, response_class=JSONResponse)
            async def get_dashboard_parameters_with_post(
                request: Request, params: QueryModelForPostParams, user=Depends(self.get_current_user), # type: ignore
                x_api_key: str | None = XApiKeyHeader
            ) -> rm.ParametersModel:
                start = time.time()
                curr_dashboard_name = self.get_name_from_path_section(request, -2)
                parameters_list = self.project._dashboards[curr_dashboard_name].config.parameters
                scope = self.project._dashboards[curr_dashboard_name].config.scope
                result = await get_parameters_definition(
                    parameters_list, "dashboard", curr_dashboard_name, scope, user, params.model_dump()
                )
                self.logger.log_activity_time(
                    "POST REQUEST for PARAMETERS", start, additional_data={"dashboard_name": curr_dashboard_name}
                )
                return result
            
            @app.get(curr_results_path, tags=[f"Dashboard '{dashboard_name}'"], description=dashboard.config.description, response_class=Response)
            async def get_dashboard_results(
                request: Request, params: QueryModelForGetDash, user=Depends(self.get_current_user), # type: ignore
                x_api_key: str | None = XApiKeyHeader
            ) -> Response:
                start = time.time()
                curr_dashboard_name = self.get_name_from_path_section(request, -1)
                result = await self._get_dashboard_results_definition(
                    curr_dashboard_name, user, asdict(params), headers=dict(request.headers)
                )
                self.logger.log_activity_time(
                    "GET REQUEST for DASHBOARD RESULTS", start, additional_data={"dashboard_name": curr_dashboard_name}
                )
                return result

            @app.post(curr_results_path, tags=[f"Dashboard '{dashboard_name}'"], description=dashboard.config.description, response_class=Response)
            async def get_dashboard_results_with_post(
                request: Request, params: QueryModelForPostDash, user=Depends(self.get_current_user), # type: ignore
                x_api_key: str | None = XApiKeyHeader
            ) -> Response:
                start = time.time()
                curr_dashboard_name = self.get_name_from_path_section(request, -1)
                result = await self._get_dashboard_results_definition(
                    curr_dashboard_name, user, params.model_dump(), headers=dict(request.headers)
                )
                self.logger.log_activity_time(
                    "POST REQUEST for DASHBOARD RESULTS", start, additional_data={"dashboard_name": curr_dashboard_name}
                )
                return result
    