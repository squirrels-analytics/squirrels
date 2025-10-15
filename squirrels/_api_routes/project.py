"""
Project metadata routes
"""
from typing import Any
from fastapi import FastAPI, Depends, Request
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer
from mcp.server.fastmcp import FastMCP, Context
from dataclasses import asdict
from cachetools import TTLCache
from textwrap import dedent
import time

from .. import _utils as u, _constants as c
from .._schemas import response_models as rm
from .._parameter_sets import ParameterSet
from .._exceptions import ConfigurationError, InvalidInputError
from .._manifest import PermissionScope, AuthenticationEnforcement
from .._version import __version__
from .._schemas.query_param_models import get_query_models_for_parameters
from .._auth import BaseUser
from .base import RouteBase


class ProjectRoutes(RouteBase):
    """Project metadata and data catalog routes"""
    
    def __init__(self, get_bearer_token: HTTPBearer, project, no_cache: bool = False):
        super().__init__(get_bearer_token, project, no_cache)
        
        # Setup caches
        parameters_cache_size = int(self.env_vars.get(c.SQRL_PARAMETERS_CACHE_SIZE, 1024))
        parameters_cache_ttl = int(self.env_vars.get(c.SQRL_PARAMETERS_CACHE_TTL_MINUTES, 60))
        self.parameters_cache = TTLCache(maxsize=parameters_cache_size, ttl=parameters_cache_ttl*60)

    async def _get_parameters_helper(
        self, parameters_tuple: tuple[str, ...] | None, entity_type: str, entity_name: str, entity_scope: PermissionScope,
        user: BaseUser, selections: tuple[tuple[str, Any], ...]
    ) -> ParameterSet:
        """Helper for getting parameters"""
        selections_dict = dict(selections)
        if "x_parent_param" not in selections_dict:
            if len(selections_dict) > 1:
                raise InvalidInputError(400, "invalid_input_for_cascading_parameters", f"The parameters endpoint takes at most 1 widget parameter selection (unless x_parent_param is provided). Got {selections_dict}")
            elif len(selections_dict) == 1:
                parent_param = next(iter(selections_dict))
                selections_dict["x_parent_param"] = parent_param
        
        parent_param = selections_dict.get("x_parent_param")
        if parent_param is not None and parent_param not in selections_dict:
                # this condition is possible for multi-select parameters with empty selection
            selections_dict[parent_param] = list()
        
        if not self.authenticator.can_user_access_scope(user, entity_scope):
            raise self.project._permission_error(user, entity_type, entity_name, entity_scope.name)
        
        param_set = self.param_cfg_set.apply_selections(parameters_tuple, selections_dict, user, parent_param=parent_param)
        return param_set

    async def _get_parameters_cachable(
        self, parameters_tuple: tuple[str, ...] | None, entity_type: str, entity_name: str, entity_scope: PermissionScope,
        user: BaseUser, selections: tuple[tuple[str, Any], ...]
    ) -> ParameterSet:
        """Cachable version of parameters helper"""
        return await self.do_cachable_action(
            self.parameters_cache, self._get_parameters_helper, parameters_tuple, entity_type, entity_name, entity_scope, user, selections
        )
        
    def setup_routes(
        self, app: FastAPI, mcp: FastMCP, project_metadata_path: str, project_name: str, project_version: str, param_fields: dict
    ):
        """Setup project metadata routes"""
        
        # Project metadata endpoint
        @app.get(project_metadata_path, tags=["Project Metadata"], response_class=JSONResponse)
        async def get_project_metadata(request: Request) -> rm.ProjectModel:
            return rm.ProjectModel(
                name=project_name,
                version=project_version,
                label=self.manifest_cfg.project_variables.label,
                description=self.manifest_cfg.project_variables.description,
                squirrels_version=__version__
            )
        
        # Data catalog endpoint
        data_catalog_path = project_metadata_path + '/data-catalog'
        
        async def get_data_catalog0(user: BaseUser) -> rm.CatalogModel:
            parameters = self.param_cfg_set.apply_selections(None, {}, user)
            parameters_model = parameters.to_api_response_model0()
            full_parameters_list = [p.name for p in parameters_model.parameters]

            dataset_items: list[rm.DatasetItemModel] = []
            for name, config in self.manifest_cfg.datasets.items():
                if self.authenticator.can_user_access_scope(user, config.scope):
                    name_normalized = u.normalize_name_for_api(name)
                    metadata = self.project.dataset_metadata(name).to_json()
                    parameters = config.parameters if config.parameters is not None else full_parameters_list
                    
                    # Build dataset-specific configurables list
                    if user.access_level == "admin":
                        dataset_configurables_defaults = self.manifest_cfg.get_default_configurables(name)
                        dataset_configurables_list = [
                            rm.ConfigurableDefaultModel(name=name, default=default)
                            for name, default in dataset_configurables_defaults.items()
                        ]
                    else:
                        dataset_configurables_list = []
                    
                    dataset_items.append(rm.DatasetItemModel(
                        name=name, label=config.label, 
                        description=config.description,
                        schema=metadata["schema"], # type: ignore
                        configurables=dataset_configurables_list,
                        parameters=parameters,
                        parameters_path=f"{project_metadata_path}/dataset/{name_normalized}/parameters",
                        result_path=f"{project_metadata_path}/dataset/{name_normalized}"
                    ))
            
            dashboard_items: list[rm.DashboardItemModel] = []
            for name, dashboard in self.project._dashboards.items():
                config = dashboard.config
                if self.authenticator.can_user_access_scope(user, config.scope):
                    name_normalized = u.normalize_name_for_api(name)

                    try:
                        dashboard_format = self.project._dashboards[name].get_dashboard_format()
                    except KeyError:
                        raise ConfigurationError(f"No dashboard file found for: {name}")
                    
                    parameters = config.parameters if config.parameters is not None else full_parameters_list
                    dashboard_items.append(rm.DashboardItemModel(
                        name=name, label=config.label, 
                        description=config.description, 
                        result_format=dashboard_format,
                        parameters=parameters,
                        parameters_path=f"{project_metadata_path}/dashboard/{name_normalized}/parameters",
                        result_path=f"{project_metadata_path}/dashboard/{name_normalized}"
                    ))
            
            if user.access_level == "admin":
                compiled_dag = await self.project._get_compiled_dag(user)
                connections_items = self.project._get_all_connections()
                data_models = self.project._get_all_data_models(compiled_dag)
                lineage_items = self.project._get_all_data_lineage(compiled_dag)
                configurables_list = [
                    rm.ConfigurableItemModel(name=name, label=cfg.label, default=cfg.default, description=cfg.description)
                    for name, cfg in self.manifest_cfg.configurables.items()
                ]
            else:
                connections_items = []
                data_models = []
                lineage_items = []
                configurables_list = []

            return rm.CatalogModel(
                parameters=parameters_model.parameters, 
                datasets=dataset_items, 
                dashboards=dashboard_items,
                connections=connections_items,
                models=data_models,
                lineage=lineage_items,
                configurables=configurables_list,
            )
        
        @app.get(data_catalog_path, tags=["Project Metadata"], summary="Get catalog of datasets and dashboards available for user")
        async def get_data_catalog(request: Request, user: BaseUser = Depends(self.get_current_user)) -> rm.CatalogModel:
            """
            Get catalog of datasets and dashboards available for the authenticated user.
            
            For admin users, this endpoint will also return detailed information about all models and their lineage in the project.
            """
            start = time.time()

            # If authentication is required, require user to be authenticated to access catalog
            if self.manifest_cfg.authentication.enforcement == AuthenticationEnforcement.REQUIRED and user.access_level == "guest":
                raise InvalidInputError(401, "user_required", "Authentication is required to access the data catalog")
            data_catalog = await get_data_catalog0(user)
            
            self.log_activity_time("GET REQUEST for DATA CATALOG", start, request)
            return data_catalog
        
        @mcp.tool(
            name=f"get_data_catalog", 
            description=dedent(f"""
            Use this tool to get the details of all datasets and parameters you can access in the Squirrels project '{project_name}'.
            
            Unless the data catalog for this project has already been provided, use this tool at the start of each conversation.
            """).strip()
        )
        async def get_data_catalog_tool(ctx: Context) -> rm.CatalogModelForTool:
            headers = self.get_headers_from_tool_ctx(ctx)
            user = self.get_user_from_tool_headers(headers)
            data_catalog = await get_data_catalog0(user)
            return rm.CatalogModelForTool(parameters=data_catalog.parameters, datasets=data_catalog.datasets)
        
        # Project-level parameters endpoints
        project_level_parameters_path = project_metadata_path + '/parameters'
        parameters_description = "Selections of one parameter may cascade the available options in another parameter. " \
                "For example, if the dataset has parameters for 'country' and 'city', available options for 'city' would " \
                "depend on the selected option 'country'. If a parameter has 'trigger_refresh' as true, provide the parameter " \
                "selection to this endpoint whenever it changes to refresh the parameter options of children parameters."

        QueryModelForGetProjectParams, QueryModelForPostProjectParams = get_query_models_for_parameters(None, param_fields)

        async def get_parameters_definition(
            parameters_list: list[str] | None, entity_type: str, entity_name: str, entity_scope: PermissionScope,
            user: BaseUser, all_request_params: dict, params: dict
        ) -> rm.ParametersModel:
            self._validate_request_params(all_request_params, params)

            get_parameters_function = self._get_parameters_helper if self.no_cache else self._get_parameters_cachable
            selections = self.get_selections_as_immutable(params, uncached_keys={"x_verify_params"})
            parameters_tuple = tuple(parameters_list) if parameters_list is not None else None
            result = await get_parameters_function(parameters_tuple, entity_type, entity_name, entity_scope, user, selections)
            return result.to_api_response_model0()

        @app.get(project_level_parameters_path, tags=["Project Metadata"], description=parameters_description)
        async def get_project_parameters(
            request: Request, params: QueryModelForGetProjectParams, user=Depends(self.get_current_user) # type: ignore
        ) -> rm.ParametersModel:
            start = time.time()
            result = await get_parameters_definition(
                None, "project", "", PermissionScope.PUBLIC, user, dict(request.query_params), asdict(params)
            )
            self.log_activity_time("GET REQUEST for PROJECT PARAMETERS", start, request)
            return result

        @app.post(project_level_parameters_path, tags=["Project Metadata"], description=parameters_description)
        async def get_project_parameters_with_post(
            request: Request, params: QueryModelForPostProjectParams, user=Depends(self.get_current_user) # type: ignore
        ) -> rm.ParametersModel:
            start = time.time()
            payload: dict = await request.json()
            result = await get_parameters_definition(
                None, "project", "", PermissionScope.PUBLIC, user, payload, params.model_dump()
            )
            self.log_activity_time("POST REQUEST for PROJECT PARAMETERS", start, request)
            return result
        
        return get_parameters_definition
    