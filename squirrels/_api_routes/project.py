"""
Project metadata routes
"""
from typing import Any
from fastapi import FastAPI, Depends, Request
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer
from dataclasses import asdict
from cachetools import TTLCache
import time

from .. import _utils as u, _constants as c
from .._schemas import response_models as rm
from .._parameter_configs import APIParamFieldInfo
from .._parameter_sets import ParameterSet
from .._exceptions import ConfigurationError, InvalidInputError
from .._manifest import PermissionScope, AuthType
from .._version import __version__
from .._schemas.query_param_models import get_query_models_for_parameters
from .._schemas.auth_models import AbstractUser
from .base import RouteBase, XApiKeyHeader


class ProjectRoutes(RouteBase):
    """Project metadata and data catalog routes"""
    
    def __init__(self, get_bearer_token: HTTPBearer, project, no_cache: bool = False):
        super().__init__(get_bearer_token, project, no_cache)
        
        # Setup caches
        self.parameters_cache = TTLCache(
            maxsize=self.env_vars.parameters_cache_size, 
            ttl=self.env_vars.parameters_cache_ttl_minutes*60
        )

    async def _get_parameters_helper(
        self, parameters_tuple: tuple[str, ...] | None, entity_type: str, entity_name: str, entity_scope: PermissionScope,
        user: AbstractUser, selections: tuple[tuple[str, Any], ...]
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
        user: AbstractUser, selections: tuple[tuple[str, Any], ...]
    ) -> ParameterSet:
        """Cachable version of parameters helper"""
        return await self.do_cachable_action(
            self.parameters_cache, self._get_parameters_helper, parameters_tuple, entity_type, entity_name, entity_scope, user, selections
        )
        
    def setup_routes(
        self, app: FastAPI, project_metadata_path: str, project_name_version_path: str, 
        project_name: str, project_version: str, param_fields: dict[str, APIParamFieldInfo]
    ):
        """Setup project metadata routes"""

        elevated_access_level = self.env_vars.elevated_access_level
        if elevated_access_level != "admin":
            self.logger.warning(f"{c.SQRL_PERMISSIONS_ELEVATED_ACCESS_LEVEL} has been set to a non-admin access level. For security reasons, DO NOT expose the APIs for this app publicly!")
        
        # Project metadata endpoint
        @app.get(project_metadata_path, tags=["Project Metadata"], response_class=JSONResponse)
        async def get_project_metadata(request: Request) -> rm.ProjectModel:
            return rm.ProjectModel(
                name=project_name,
                version=project_version,
                label=self.manifest_cfg.project_variables.label,
                description=self.manifest_cfg.project_variables.description,
                elevated_access_level=elevated_access_level,
                redoc_path=project_name_version_path + "/redoc",
                swagger_path=project_name_version_path + "/docs",
                openapi_path=project_name_version_path + "/openapi.json",
                mcp_server_path=[project_name_version_path + "/mcp", "/mcp"],
                squirrels_version=__version__
            )
        
        # Data catalog endpoint
        data_catalog_path = project_metadata_path + '/data-catalog'
        
        async def get_data_catalog0(user: AbstractUser) -> rm.CatalogModel:
            parameters = self.param_cfg_set.apply_selections(None, {}, user)
            parameters_model = parameters.to_api_response_model0()
            full_parameters_list = [p.name for p in parameters_model.parameters]
            user_has_elevated_privileges = u.user_has_elevated_privileges(user.access_level, elevated_access_level)

            dataset_items: list[rm.DatasetItemModel] = []
            for name, config in self.manifest_cfg.datasets.items():
                if self.authenticator.can_user_access_scope(user, config.scope):
                    name_for_api = u.normalize_name_for_api(name)
                    metadata = self.project.dataset_metadata(name).to_json()
                    parameters = config.parameters if config.parameters is not None else full_parameters_list
                    
                    # Build dataset-specific configurables list
                    if user_has_elevated_privileges:
                        dataset_configurables_defaults = self.manifest_cfg.get_default_configurables(overrides=config.configurables)
                        dataset_configurables_list = [
                            rm.ConfigurableOverrideModel(name=name, default=default)
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
                        parameters_path=f"{project_metadata_path}/dataset/{name_for_api}/parameters",
                        result_path=f"{project_metadata_path}/dataset/{name_for_api}"
                    ))
            
            dashboard_items: list[rm.DashboardItemModel] = []
            for name, dashboard in self.project._dashboards.items():
                config = dashboard.config
                if self.authenticator.can_user_access_scope(user, config.scope):
                    name_for_api = u.normalize_name_for_api(name)

                    try:
                        dashboard_format = self.project._dashboards[name].get_dashboard_format()
                    except KeyError:
                        raise ConfigurationError(f"No dashboard file found for: {name}")

                    if user_has_elevated_privileges:
                        dashboard_configurables_defaults = self.manifest_cfg.get_default_configurables(overrides=config.configurables)
                        dashboard_configurables_list = [
                            rm.ConfigurableOverrideModel(name=name, default=default)
                            for name, default in dashboard_configurables_defaults.items()
                        ]
                    else:
                        dashboard_configurables_list = []
                    
                    parameters = config.parameters if config.parameters is not None else full_parameters_list
                    dashboard_items.append(rm.DashboardItemModel(
                        name=name, label=config.label, 
                        description=config.description, 
                        result_format=dashboard_format,
                        configurables=dashboard_configurables_list,
                        parameters=parameters,
                        parameters_path=f"{project_metadata_path}/dashboard/{name_for_api}/parameters",
                        result_path=f"{project_metadata_path}/dashboard/{name_for_api}"
                    ))
            
            if user_has_elevated_privileges:
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
        async def get_data_catalog(
            request: Request, user: AbstractUser = Depends(self.get_current_user),
            x_api_key: str | None = XApiKeyHeader
        ) -> rm.CatalogModel:
            """
            Get catalog of datasets and dashboards available for the authenticated user.
            
            For admin users, this endpoint will also return detailed information about all models and their lineage in the project.
            """
            start = time.time()

            # If authentication is required, require user to be authenticated to access catalog
            if self.manifest_cfg.project_variables.auth_type == AuthType.REQUIRED and user.access_level == "guest":
                raise InvalidInputError(401, "user_required", "Authentication is required to access the data catalog")
            data_catalog = await get_data_catalog0(user)
            
            self.logger.log_activity_time("GET REQUEST for DATA CATALOG", start)
            return data_catalog
        
        async def get_data_catalog_for_mcp(user: AbstractUser) -> rm.CatalogModelForMcp:
            """Get data catalog for MCP tools/resources. Takes user object."""
            data_catalog = await get_data_catalog0(user)
            return rm.CatalogModelForMcp(parameters=data_catalog.parameters, datasets=data_catalog.datasets)
        
        # Store the MCP function as an instance attribute for access by McpServerBuilder
        self._get_data_catalog_for_mcp = get_data_catalog_for_mcp
        
        # Project-level parameters endpoints
        project_level_parameters_path = project_metadata_path + '/parameters'
        parameters_description = "Selections of one parameter may cascade the available options in another parameter. " \
                "For example, if the dataset has parameters for 'country' and 'city', available options for 'city' would " \
                "depend on the selected option 'country'. If a parameter has 'trigger_refresh' as true, provide the parameter " \
                "selection to this endpoint whenever it changes to refresh the parameter options of children parameters."

        QueryModelForGetProjectParams, QueryModelForPostProjectParams = get_query_models_for_parameters(param_fields)

        async def get_parameters_definition(
            parameters_list: list[str] | None, entity_type: str, entity_name: str, entity_scope: PermissionScope,
            user: AbstractUser, params: dict
        ) -> rm.ParametersModel:
            # self._validate_request_params(all_request_params, params, headers)

            get_parameters_function = self._get_parameters_helper if self.no_cache else self._get_parameters_cachable
            selections = self.get_selections_as_immutable(params, uncached_keys=set())
            parameters_tuple = tuple(parameters_list) if parameters_list is not None else None
            result = await get_parameters_function(parameters_tuple, entity_type, entity_name, entity_scope, user, selections)
            return result.to_api_response_model0()

        @app.get(project_level_parameters_path, tags=["Project Metadata"], description=parameters_description)
        async def get_project_parameters(
            request: Request, params: QueryModelForGetProjectParams, user=Depends(self.get_current_user), # type: ignore
            x_api_key: str | None = XApiKeyHeader
        ) -> rm.ParametersModel:
            start = time.time()
            result = await get_parameters_definition(
                None, "project", "", PermissionScope.PUBLIC, user, asdict(params)
            )
            self.logger.log_activity_time("GET REQUEST for PROJECT PARAMETERS", start)
            return result

        @app.post(project_level_parameters_path, tags=["Project Metadata"], description=parameters_description)
        async def get_project_parameters_with_post(
            request: Request, params: QueryModelForPostProjectParams, user=Depends(self.get_current_user), # type: ignore
            x_api_key: str | None = XApiKeyHeader
        ) -> rm.ParametersModel:
            start = time.time()
            result = await get_parameters_definition(
                None, "project", "", PermissionScope.PUBLIC, user, params.model_dump()
            )
            self.logger.log_activity_time("POST REQUEST for PROJECT PARAMETERS", start)
            return result
        
        return get_parameters_definition
    