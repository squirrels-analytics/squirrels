from typing import Coroutine, Mapping, Callable, TypeVar, Annotated, Any
from dataclasses import make_dataclass, asdict
from fastapi import Depends, FastAPI, Request, Response, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from pydantic import create_model, BaseModel, Field
from contextlib import asynccontextmanager
from cachetools import TTLCache
from argparse import Namespace
from pathlib import Path
import io, time, mimetypes, traceback, uuid, asyncio, urllib.parse

from . import _constants as c, _utils as u, _api_response_models as arm
from ._exceptions import InvalidInputError, ConfigurationError, FileExecutionError
from ._version import __version__, sq_major_version
from ._manifest import PermissionScope
from ._auth import BaseUser, AccessToken, UserField
from ._parameter_sets import ParameterSet
from .dashboards import Dashboard
from ._project import SquirrelsProject
from .dataset_result import DatasetResult
from ._parameter_configs import APIParamFieldInfo

mimetypes.add_type('application/javascript', '.js')


class ApiServer:
    def __init__(self, no_cache: bool, project: SquirrelsProject) -> None:
        """
        Constructor for ApiServer

        Arguments:
            no_cache (bool): Whether to disable caching
        """
        self.no_cache = no_cache
        self.project = project
        self.logger = project._logger
        self.env_vars = project._env_vars
        self.j2_env = project._j2_env
        self.manifest_cfg = project._manifest_cfg
        self.seeds = project._seeds
        self.conn_args = project._conn_args
        self.conn_set = project._conn_set
        self.authenticator = project._auth
        self.param_args = project._param_args
        self.param_cfg_set = project._param_cfg_set
        self.context_func = project._context_func
        self.dashboards = project._dashboards
    
    
    async def _monitor_for_staging_file(self) -> None:
        """Background task that monitors for staging file and renames it when present"""
        duckdb_venv_path = self.project._duckdb_venv_path
        staging_file = Path(duckdb_venv_path + ".stg")
        target_file = Path(duckdb_venv_path)
                
        while True:
            try:
                if staging_file.exists():
                    try:
                        staging_file.replace(target_file)
                        self.logger.info("Successfully renamed staging database to virtual environment database")
                    except OSError:
                        # Silently continue if file cannot be renamed (will retry next iteration)
                        pass
                
            except Exception as e:
                # Log any unexpected errors but keep running
                self.logger.error(f"Error in monitoring {c.DUCKDB_VENV_FILE + '.stg'}: {str(e)}")
            
            await asyncio.sleep(1)  # Check every second
    
    @asynccontextmanager
    async def _run_background_tasks(self, app: FastAPI):
        task = asyncio.create_task(self._monitor_for_staging_file())
        yield
        task.cancel()


    def _validate_request_params(self, all_request_params: Mapping, params: Mapping) -> None:
        invalid_params = [param for param in all_request_params if param not in params]
        if params.get("x_verify_params", False) and invalid_params:
            raise InvalidInputError(201, f"Invalid query parameters: {', '.join(invalid_params)}")
        
    
    def run(self, uvicorn_args: Namespace) -> None:
        """
        Runs the API server with uvicorn for CLI "squirrels run"

        Arguments:
            uvicorn_args: List of arguments to pass to uvicorn.run. Currently only supports "host" and "port"
        """
        start = time.time()
        
        squirrels_version_path = f'/api/squirrels-v{sq_major_version}'
        project_name = u.normalize_name_for_api(self.manifest_cfg.project_variables.name)
        project_version = f"v{self.manifest_cfg.project_variables.major_version}"
        project_metadata_path = squirrels_version_path + f"/project/{project_name}/{project_version}"
        
        param_fields = self.param_cfg_set.get_all_api_field_info()

        tags_metadata = [
            {
                "name": "Authentication",
                "description": "Submit authentication credentials, and get token for authentication",
            },
            {
                "name": "User Management",
                "description": "Manage users and their attributes",
            },
            {
                "name": "Project Metadata",
                "description": "Get information on project such as name, version, and other API endpoints",
            },
            {
                "name": "Data Management",
                "description": "Actions to update the data components of the project",
            }
        ]

        for dataset_name in self.manifest_cfg.datasets:
            tags_metadata.append({
                "name": f"Dataset '{dataset_name}'",
                "description": f"Get parameters or results for dataset '{dataset_name}'",
            })
        
        for dashboard_name in self.dashboards:
            tags_metadata.append({
                "name": f"Dashboard '{dashboard_name}'",
                "description": f"Get parameters or results for dashboard '{dashboard_name}'",
            })
        
        app = FastAPI(
            title=f"Squirrels APIs for '{self.manifest_cfg.project_variables.label}'", openapi_tags=tags_metadata,
            description="For specifying parameter selections to dataset APIs, you can choose between using query parameters with the GET method or using request body with the POST method",
            lifespan=self._run_background_tasks,
            openapi_url=project_metadata_path+"/openapi.json",
            docs_url=project_metadata_path+"/docs",
            redoc_url=project_metadata_path+"/redoc"
        )

        async def _log_request_run(request: Request) -> None:
            headers = dict(request.scope["headers"])
            request_id = uuid.uuid4().hex
            headers[b"x-request-id"] = request_id.encode()
            request.scope["headers"] = list(headers.items())

            try:
                body = await request.json()
            except Exception:
                body = None
            
            headers_dict = dict(request.headers)
            path, params = request.url.path, dict(request.query_params)
            path_with_params = f"{path}?{request.query_params}" if len(params) > 0 else path
            data = {"request_method": request.method, "request_path": path, "request_params": params, "request_headers": headers_dict, "request_body": body}
            info = {"request_id": request_id}
            self.logger.info(f'Running request: {request.method} {path_with_params}', extra={"data": data, "info": info})
        
        def _get_request_id(request: Request) -> str:
            return request.headers.get("x-request-id", "")

        @app.middleware("http")
        async def catch_exceptions_middleware(request: Request, call_next):
            buffer = io.StringIO()
            try:
                await _log_request_run(request)
                return await call_next(request)
            except InvalidInputError as exc:
                traceback.print_exc(file=buffer)
                message = str(exc)
                if exc.error_code < 20:
                    status_code = status.HTTP_401_UNAUTHORIZED
                elif exc.error_code < 40:
                    status_code = status.HTTP_403_FORBIDDEN
                elif exc.error_code < 60:
                    status_code = status.HTTP_404_NOT_FOUND
                elif exc.error_code < 70:
                    if exc.error_code == 61:
                        message = "The dataset depends on static data models that cannot be found. You may need to build the virtual data environment first."
                    status_code = status.HTTP_409_CONFLICT
                else:
                    status_code = status.HTTP_400_BAD_REQUEST
                response = JSONResponse(
                    status_code=status_code, content={"message": message, "blame": "API client", "error_code": exc.error_code}
                )
            except FileExecutionError as exc:
                traceback.print_exception(exc.error, file=buffer)
                buffer.write(str(exc))
                response = JSONResponse(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content={"message": f"An unexpected error occurred", "blame": "Squirrels project"}
                )
            except ConfigurationError as exc:
                traceback.print_exc(file=buffer)
                response = JSONResponse(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content={"message": f"An unexpected error occurred", "blame": "Squirrels project"}
                )
            except Exception as exc:
                traceback.print_exc(file=buffer)
                response = JSONResponse(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content={"message": f"An unexpected error occurred", "blame": "Squirrels framework"}
                )
            
            err_msg = buffer.getvalue()
            self.logger.error(err_msg)
            print(err_msg)
            return response

        app.add_middleware(
            CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"], 
            expose_headers=["Applied-Username"]
        )

        # Helpers
        T = TypeVar('T')
        
        def get_selections_as_immutable(params: Mapping, uncached_keys: set[str]) -> tuple[tuple[str, Any], ...]:
            # Changing selections into a cachable "tuple of pairs" that will later be converted to dictionary
            selections = list()
            for key, val in params.items():
                if key in uncached_keys or val is None:
                    continue
                if isinstance(val, (list, tuple)):
                    if len(val) == 1: # for backward compatibility
                        val = val[0]
                    else:
                        val = tuple(val)
                selections.append((u.normalize_name(key), val))
            return tuple(selections)

        async def do_cachable_action(cache: TTLCache, action: Callable[..., Coroutine[Any, Any, T]], *args) -> T:
            cache_key = tuple(args)
            result = cache.get(cache_key)
            if result is None:
                result = await action(*args)
                cache[cache_key] = result
            return result

        def _get_query_models_helper(widget_parameters: list[str] | None, predefined_params: list[APIParamFieldInfo]):
            if widget_parameters is None:
                widget_parameters = list(param_fields.keys())
            
            QueryModelForGetRaw = make_dataclass("QueryParams", [
                param_fields[param].as_query_info() for param in widget_parameters
            ] + [param.as_query_info() for param in predefined_params])
            QueryModelForGet = Annotated[QueryModelForGetRaw, Depends()]

            field_definitions = {param: param_fields[param].as_body_info() for param in widget_parameters}
            for param in predefined_params:
                field_definitions[param.name] = param.as_body_info()
            QueryModelForPost = create_model("RequestBodyParams", **field_definitions) # type: ignore
            return QueryModelForGet, QueryModelForPost
        
        def get_query_models_for_parameters(widget_parameters: list[str] | None):
            predefined_params = [
                APIParamFieldInfo("x_verify_params", bool, default=False, description="If true, the query parameters are verified to be valid for the dataset"),
                APIParamFieldInfo("x_parent_param", str, description="The parameter name used for parameter updates. If not provided, then all parameters are retrieved"),
            ]
            return _get_query_models_helper(widget_parameters, predefined_params)
        
        def get_query_models_for_dataset(widget_parameters: list[str] | None):
            predefined_params = [
                APIParamFieldInfo("x_verify_params", bool, default=False, description="If true, the query parameters are verified to be valid for the dataset"),
                APIParamFieldInfo("x_orientation", str, default="records", description="The orientation of the data to return, one of: 'records', 'rows', or 'columns'"),
                APIParamFieldInfo("x_select", list[str], examples=[[]], description="The columns to select from the dataset. All are returned if not specified"), 
                APIParamFieldInfo("x_offset", int, default=0, description="The number of rows to skip before returning data (applied after data caching)"),
                APIParamFieldInfo("x_limit", int, default=1000, description="The maximum number of rows to return (applied after data caching and offset)"),
            ]
            return _get_query_models_helper(widget_parameters, predefined_params)
        
        def get_query_models_for_dashboard(widget_parameters: list[str] | None):
            predefined_params = [
                APIParamFieldInfo("x_verify_params", bool, default=False, description="If true, the query parameters are verified to be valid for the dashboard"),
            ]
            return _get_query_models_helper(widget_parameters, predefined_params)
        
        def get_query_models_for_querying_models():
            predefined_params = [
                APIParamFieldInfo("x_verify_params", bool, default=False, description="If true, the query parameters are verified to be valid"),
                APIParamFieldInfo("x_orientation", str, default="records", description="The orientation of the data to return, one of: 'records', 'rows', or 'columns'"),
                APIParamFieldInfo("x_offset", int, default=0, description="The number of rows to skip before returning data (applied after data caching)"),
                APIParamFieldInfo("x_limit", int, default=1000, description="The maximum number of rows to return (applied after data caching and offset)"),
                APIParamFieldInfo("x_sql_query", str, description="The SQL query to execute on the data models"),
            ]
            return _get_query_models_helper(None, predefined_params)
        
        def _get_section_from_request_path(request: Request, section: int) -> str:
            url_path: str = request.scope['route'].path
            return url_path.split('/')[section]

        def get_dataset_name(request: Request, section: int) -> str:
            dataset_raw = _get_section_from_request_path(request, section)
            return u.normalize_name(dataset_raw)

        def get_dashboard_name(request: Request, section: int) -> str:
            dashboard_raw = _get_section_from_request_path(request, section)
            return u.normalize_name(dashboard_raw)
        
        expiry_mins = self.env_vars.get(c.SQRL_AUTH_TOKEN_EXPIRE_MINUTES, 30)
        try:
            expiry_mins = int(expiry_mins)
        except ValueError:
            raise ConfigurationError(f"Value for environment variable {c.SQRL_AUTH_TOKEN_EXPIRE_MINUTES} is not an integer, got: {expiry_mins}")
        
        # Project Metadata API
        
        @app.get(project_metadata_path, tags=["Project Metadata"], response_class=JSONResponse)
        async def get_project_metadata(request: Request) -> arm.ProjectModel:
            return arm.ProjectModel(
                name=project_name,
                version=project_version,
                label=self.manifest_cfg.project_variables.label,
                description=self.manifest_cfg.project_variables.description,
                squirrels_version=__version__
            )
        
        # Authentication
        login_path = project_metadata_path + '/login'

        oauth2_scheme = OAuth2PasswordBearer(tokenUrl=login_path, auto_error=False)

        async def get_current_user(response: Response, token: str = Depends(oauth2_scheme)) -> BaseUser | None:
            user = self.authenticator.get_user_from_token(token)
            username = "" if user is None else user.username
            response.headers["Applied-Username"] = username
            return user

        ## Login API
        @app.post(login_path, tags=["Authentication"])
        async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()) -> arm.LoginReponse:
            user = self.authenticator.get_user(form_data.username, form_data.password)
            access_token, expiry = self.authenticator.create_access_token(user, expiry_minutes=expiry_mins)
            return arm.LoginReponse(access_token=access_token, token_type="bearer", username=user.username, is_admin=user.is_admin, expiry_time=expiry)
        
        ## Change Password API
        change_password_path = project_metadata_path + '/change-password'

        class ChangePasswordRequest(BaseModel):
            old_password: str
            new_password: str

        @app.put(change_password_path, description="Change the password for the current user", tags=["Authentication"])
        async def change_password(request: ChangePasswordRequest, user: BaseUser | None = Depends(get_current_user)) -> None:
            if user is None:
                raise InvalidInputError(1, "Invalid authorization token")
            self.authenticator.change_password(user.username, request.old_password, request.new_password)

        ## Token API
        tokens_path = project_metadata_path + '/tokens'

        class TokenRequestBody(BaseModel):
            title: str | None = Field(default=None, description=f"The title of the token. If not provided, a temporary token is created (expiring in {expiry_mins} minutes) and cannot be revoked")
            expiry_minutes: int | None = Field(
                default=None, 
                description=f"The number of minutes the token is valid for (or indefinitely if not provided). Ignored and set to {expiry_mins} minutes if title is not provided."
            )

        @app.post(tokens_path, description="Create a new token for the user", tags=["Authentication"])
        async def create_token(body: TokenRequestBody, user: BaseUser | None = Depends(get_current_user)) -> arm.LoginReponse:
            if user is None:
                raise InvalidInputError(1, "Invalid authorization token")
            
            if body.title is None:
                expiry_minutes = expiry_mins
            else:
                expiry_minutes = body.expiry_minutes
            
            access_token, expiry = self.authenticator.create_access_token(user, expiry_minutes=expiry_minutes, title=body.title)
            return arm.LoginReponse(access_token=access_token, token_type="bearer", username=user.username, is_admin=user.is_admin, expiry_time=expiry)
        
        ## Get All Tokens API
        @app.get(tokens_path, description="Get all tokens with title for the current user", tags=["Authentication"])
        async def get_all_tokens(user: BaseUser | None = Depends(get_current_user)) -> list[AccessToken]:
            if user is None:
                raise InvalidInputError(1, "Invalid authorization token")
            return self.authenticator.get_all_tokens(user.username)
        
        ## Revoke Token API
        revoke_token_path = project_metadata_path + '/tokens/{token_id}'

        @app.delete(revoke_token_path, description="Revoke a token", tags=["Authentication"])
        async def revoke_token(token_id: str, user: BaseUser | None = Depends(get_current_user)) -> None:
            if user is None:
                raise InvalidInputError(1, "Invalid authorization token")
            self.authenticator.revoke_token(user.username, token_id)
        
        ## Get Authenticated User Fields From Token API
        get_me_path = project_metadata_path + '/me'

        fields_without_username = {
            k: (v.annotation, v.default) 
            for k, v in self.authenticator.User.model_fields.items() 
            if k != "username"
        }
        UserModel = create_model("UserModel", __base__=BaseModel, **fields_without_username) # type: ignore

        class UserWithoutUsername(UserModel):
            pass

        class UserWithUsername(UserModel):
            username: str

        class AddUserRequestBody(UserWithUsername):
            password: str
        
        @app.get(get_me_path, description="Get the authenticated user's fields", tags=["Authentication"])
        async def get_me(user: BaseUser | None = Depends(get_current_user)) -> UserWithUsername:
            if user is None:
                raise InvalidInputError(1, "Invalid authorization token")
            return UserWithUsername(**user.model_dump(mode='json'))

        # User Management

        ## User Fields API
        user_fields_path = project_metadata_path + '/user-fields'

        @app.get(user_fields_path, description="Get details of the user fields", tags=["User Management"])
        async def get_user_fields() -> list[UserField]:
            return self.authenticator.user_fields
        
        ## Add User API
        add_user_path = project_metadata_path + '/users'

        @app.post(add_user_path, description="Add a new user by providing details for username, password, and user fields", tags=["User Management"])
        async def add_user(
            new_user: AddUserRequestBody, user: BaseUser | None = Depends(get_current_user)
        ) -> None:
            if user is None or not user.is_admin:
                raise InvalidInputError(20, "Authorized user is forbidden to add new users")
            self.authenticator.add_user(new_user.username, new_user.model_dump(mode='json', exclude={"username"}))

        ## Update User API
        update_user_path = project_metadata_path + '/users/{username}'

        @app.put(update_user_path, description="Update the user of the given username given the new user details", tags=["User Management"])
        async def update_user(
            username: str, updated_user: UserWithoutUsername, user: BaseUser | None = Depends(get_current_user)
        ) -> None:
            if user is None or not user.is_admin:
                raise InvalidInputError(20, "Authorized user is forbidden to update users")
            self.authenticator.add_user(username, updated_user.model_dump(mode='json'), update_user=True)

        ## List Users API
        list_users_path = project_metadata_path + '/users'

        @app.get(list_users_path, tags=["User Management"])
        async def list_all_users() -> list[UserWithUsername]:
            return self.authenticator.get_all_users()
        
        ## Delete User API
        delete_user_path = project_metadata_path + '/users/{username}'

        @app.delete(delete_user_path, tags=["User Management"])
        async def delete_user(username: str, user: BaseUser | None = Depends(get_current_user)) -> None:
            if user is None or not user.is_admin:
                raise InvalidInputError(21, "Authorized user is forbidden to delete users")
            if username == user.username:
                raise InvalidInputError(22, "Cannot delete your own user")
            self.authenticator.delete_user(username)
        
        # Data Catalog API
        data_catalog_path = project_metadata_path + '/data-catalog'

        dataset_results_path = project_metadata_path + '/dataset/{dataset}'
        dataset_parameters_path = dataset_results_path + '/parameters'

        dashboard_results_path = project_metadata_path + '/dashboard/{dashboard}'
        dashboard_parameters_path = dashboard_results_path + '/parameters'
        
        async def get_data_catalog0(user: BaseUser | None) -> arm.CatalogModel:
            parameters = self.param_cfg_set.apply_selections(None, {}, user)
            parameters_model = parameters.to_api_response_model0()
            full_parameters_list = [p.name for p in parameters_model.parameters]

            dataset_items: list[arm.DatasetItemModel] = []
            for name, config in self.manifest_cfg.datasets.items():
                if self.authenticator.can_user_access_scope(user, config.scope):
                    name_normalized = u.normalize_name_for_api(name)
                    metadata = self.project.dataset_metadata(name).to_json()
                    parameters = config.parameters if config.parameters is not None else full_parameters_list
                    dataset_items.append(arm.DatasetItemModel(
                        name=name_normalized, label=config.label, 
                        description=config.description,
                        schema=metadata["schema"], # type: ignore
                        parameters=parameters,
                        parameters_path=dataset_parameters_path.format(dataset=name_normalized),
                        result_path=dataset_results_path.format(dataset=name_normalized)
                    ))
            
            dashboard_items: list[arm.DashboardItemModel] = []
            for name, dashboard in self.dashboards.items():
                config = dashboard.config
                if self.authenticator.can_user_access_scope(user, config.scope):
                    name_normalized = u.normalize_name_for_api(name)

                    try:
                        dashboard_format = self.dashboards[name].get_dashboard_format()
                    except KeyError:
                        raise ConfigurationError(f"No dashboard file found for: {name}")
                    
                    parameters = config.parameters if config.parameters is not None else full_parameters_list
                    dashboard_items.append(arm.DashboardItemModel(
                        name=name, label=config.label, 
                        description=config.description, 
                        result_format=dashboard_format,
                        parameters=parameters,
                        parameters_path=dashboard_parameters_path.format(dashboard=name_normalized),
                        result_path=dashboard_results_path.format(dashboard=name_normalized)
                    ))
            
            if user and user.is_admin:
                compiled_dag = await self.project._get_compiled_dag(user=user)
                connections_items = self.project._get_all_connections()
                data_models = self.project._get_all_data_models(compiled_dag)
                lineage_items = self.project._get_all_data_lineage(compiled_dag)
            else:
                connections_items = []
                data_models = []
                lineage_items = []

            return arm.CatalogModel(
                parameters=parameters_model.parameters, 
                datasets=dataset_items, 
                dashboards=dashboard_items,
                connections=connections_items,
                models=data_models,
                lineage=lineage_items,
            )
        
        @app.get(data_catalog_path, tags=["Project Metadata"], summary="Get catalog of datasets and dashboards available for user")
        async def get_data_catalog(request: Request, user: BaseUser | None = Depends(get_current_user)) -> arm.CatalogModel:
            """
            Get catalog of datasets and dashboards available for the authenticated user.
            
            For admin users, this endpoint will also return detailed information about all models and their lineage in the project.
            """
            return await get_data_catalog0(user)
        
        # Parameters API Helpers
        parameters_description = "Selections of one parameter may cascade the available options in another parameter. " \
                "For example, if the dataset has parameters for 'country' and 'city', available options for 'city' would " \
                "depend on the selected option 'country'. If a parameter has 'trigger_refresh' as true, provide the parameter " \
                "selection to this endpoint whenever it changes to refresh the parameter options of children parameters."
        
        async def get_parameters_helper(
            parameters_tuple: tuple[str, ...] | None, entity_type: str, entity_name: str, entity_scope: PermissionScope,
            user: BaseUser | None, selections: tuple[tuple[str, Any], ...]
        ) -> ParameterSet:
            selections_dict = dict(selections)
            if "x_parent_param" not in selections_dict:
                if len(selections_dict) > 1:
                    raise InvalidInputError(202, f"The parameters endpoint takes at most 1 widget parameter selection (unless x_parent_param is provided). Got {selections_dict}")
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

        parameters_cache_size = int(self.env_vars.get(c.SQRL_PARAMETERS_CACHE_SIZE, 1024))
        parameters_cache_ttl = int(self.env_vars.get(c.SQRL_PARAMETERS_CACHE_TTL_MINUTES, 60))
        params_cache = TTLCache(maxsize=parameters_cache_size, ttl=parameters_cache_ttl*60)

        async def get_parameters_cachable(
            parameters_tuple: tuple[str, ...] | None, entity_type: str, entity_name: str, entity_scope: PermissionScope,
            user: BaseUser | None, selections: tuple[tuple[str, Any], ...]
        ) -> ParameterSet:
            return await do_cachable_action(params_cache, get_parameters_helper, parameters_tuple, entity_type, entity_name, entity_scope, user, selections)
        
        async def get_parameters_definition(
            parameters_list: list[str] | None, entity_type: str, entity_name: str, entity_scope: PermissionScope,
            user: BaseUser | None, all_request_params: dict, params: Mapping
        ) -> arm.ParametersModel:
            self._validate_request_params(all_request_params, params)

            get_parameters_function = get_parameters_helper if self.no_cache else get_parameters_cachable
            selections = get_selections_as_immutable(params, uncached_keys={"x_verify_params"})
            parameters_tuple = tuple(parameters_list) if parameters_list is not None else None
            result = await get_parameters_function(parameters_tuple, entity_type, entity_name, entity_scope, user, selections)
            return result.to_api_response_model0()

        def validate_parameters_list(parameters: list[str] | None, entity_type: str) -> None:
            if parameters is None:
                return
            for param in parameters:
                if param not in param_fields:
                    all_params = list(param_fields.keys())
                    raise ConfigurationError(
                        f"{entity_type} '{dataset_name}' use parameter '{param}' which doesn't exist. Available parameters are:"
                        f"\n  {all_params}"
                    )
        
        # Project-Level Parameters API
        project_level_parameters_path = project_metadata_path + '/parameters'

        QueryModelForGetProjectParams, QueryModelForPostProjectParams = get_query_models_for_parameters(None)

        @app.get(project_level_parameters_path, tags=["Project Metadata"], description=parameters_description)
        async def get_project_parameters(
            request: Request, params: QueryModelForGetProjectParams, user: BaseUser | None = Depends(get_current_user) # type: ignore
        ) -> arm.ParametersModel:
            start = time.time()
            result = await get_parameters_definition(
                None, "project", "", PermissionScope.PUBLIC, user, dict(request.query_params), asdict(params)
            )
            self.logger.log_activity_time("GET REQUEST for PROJECT PARAMETERS", start, request_id=_get_request_id(request))
            return result

        @app.post(project_level_parameters_path, tags=["Project Metadata"], description=parameters_description)
        async def get_project_parameters_with_post(
            request: Request, params: QueryModelForPostProjectParams, user: BaseUser | None = Depends(get_current_user) # type: ignore
        ) -> arm.ParametersModel:
            start = time.time()
            params_model: BaseModel = params
            payload: dict = await request.json()
            result = await get_parameters_definition(
                None, "project", "", PermissionScope.PUBLIC, user, payload, params_model.model_dump()
            )
            self.logger.log_activity_time("POST REQUEST for PROJECT PARAMETERS", start, request_id=_get_request_id(request))
            return result
        
        # Dataset Results API Helpers
        async def get_dataset_results_helper(
            dataset: str, user: BaseUser | None, selections: tuple[tuple[str, Any], ...]
        ) -> DatasetResult:
            return await self.project.dataset(dataset, selections=dict(selections), user=user)

        dataset_results_cache_size = int(self.env_vars.get(c.SQRL_DATASETS_CACHE_SIZE, 128))
        dataset_results_cache_ttl = int(self.env_vars.get(c.SQRL_DATASETS_CACHE_TTL_MINUTES, 60))
        dataset_results_cache = TTLCache(maxsize=dataset_results_cache_size, ttl=dataset_results_cache_ttl*60)

        async def get_dataset_results_cachable(
            dataset: str, user: BaseUser | None, selections: tuple[tuple[str, Any], ...]
        ) -> DatasetResult:
            return await do_cachable_action(dataset_results_cache, get_dataset_results_helper, dataset, user, selections)
        
        async def get_dataset_results_definition(
            dataset_name: str, user: BaseUser | None, all_request_params: dict, params: Mapping
        ) -> arm.DatasetResultModel:
            self._validate_request_params(all_request_params, params)

            get_dataset_function = get_dataset_results_helper if self.no_cache else get_dataset_results_cachable
            uncached_keys = {"x_verify_params", "x_orientation", "x_select", "x_limit", "x_offset"}
            selections = get_selections_as_immutable(params, uncached_keys)
            result = await get_dataset_function(dataset_name, user, selections)
            
            orientation = params.get("x_orientation", "records")
            raw_select = params.get("x_select")
            select = tuple(raw_select) if raw_select is not None else tuple()
            limit = params.get("x_limit", 1000)
            offset = params.get("x_offset", 0)
            return arm.DatasetResultModel(**result.to_json(orientation, select, limit, offset))
        
        # Dashboard Results API Helpers
        async def get_dashboard_results_helper(
            dashboard: str, user: BaseUser | None, selections: tuple[tuple[str, Any], ...]
        ) -> Dashboard:
            return await self.project.dashboard(dashboard, selections=dict(selections), user=user)
        
        dashboard_results_cache_size = int(self.env_vars.get(c.SQRL_DASHBOARDS_CACHE_SIZE, 128))
        dashboard_results_cache_ttl = int(self.env_vars.get(c.SQRL_DASHBOARDS_CACHE_TTL_MINUTES, 60))
        dashboard_results_cache = TTLCache(maxsize=dashboard_results_cache_size, ttl=dashboard_results_cache_ttl*60)

        async def get_dashboard_results_cachable(
            dashboard: str, user: BaseUser | None, selections: tuple[tuple[str, Any], ...]
        ) -> Dashboard:
            return await do_cachable_action(dashboard_results_cache, get_dashboard_results_helper, dashboard, user, selections)
        
        async def get_dashboard_results_definition(
            dashboard_name: str, user: BaseUser | None, all_request_params: dict, params: Mapping
        ) -> Response:
            self._validate_request_params(all_request_params, params)
            
            get_dashboard_function = get_dashboard_results_helper if self.no_cache else get_dashboard_results_cachable
            selections = get_selections_as_immutable(params, uncached_keys={"x_verify_params"})
            dashboard_obj = await get_dashboard_function(dashboard_name, user, selections)
            if dashboard_obj._format == c.PNG:
                assert isinstance(dashboard_obj._content, bytes)
                result = Response(dashboard_obj._content, media_type="image/png")
            elif dashboard_obj._format == c.HTML:
                result = HTMLResponse(dashboard_obj._content)
            else:
                raise NotImplementedError()
            return result
        
        # Dataset Parameters and Results APIs
        for dataset_name, dataset_config in self.manifest_cfg.datasets.items():
            dataset_normalized = u.normalize_name_for_api(dataset_name)
            curr_parameters_path = dataset_parameters_path.format(dataset=dataset_normalized)
            curr_results_path = dataset_results_path.format(dataset=dataset_normalized)

            validate_parameters_list(dataset_config.parameters, "Dataset")

            QueryModelForGetParams, QueryModelForPostParams = get_query_models_for_parameters(dataset_config.parameters)
            QueryModelForGetDataset, QueryModelForPostDataset = get_query_models_for_dataset(dataset_config.parameters)

            @app.get(curr_parameters_path, tags=[f"Dataset '{dataset_name}'"], description=parameters_description, response_class=JSONResponse)
            async def get_dataset_parameters(
                request: Request, params: QueryModelForGetParams, user: BaseUser | None = Depends(get_current_user) # type: ignore
            ) -> arm.ParametersModel:
                start = time.time()
                curr_dataset_name = get_dataset_name(request, -2)
                parameters_list = self.manifest_cfg.datasets[curr_dataset_name].parameters
                scope = self.manifest_cfg.datasets[curr_dataset_name].scope
                result = await get_parameters_definition(
                    parameters_list, "dataset", curr_dataset_name, scope, user, dict(request.query_params), asdict(params)
                )
                self.logger.log_activity_time("GET REQUEST for PARAMETERS", start, request_id=_get_request_id(request))
                return result

            @app.post(curr_parameters_path, tags=[f"Dataset '{dataset_name}'"], description=parameters_description, response_class=JSONResponse)
            async def get_dataset_parameters_with_post(
                request: Request, params: QueryModelForPostParams, user: BaseUser | None = Depends(get_current_user) # type: ignore
            ) -> arm.ParametersModel:
                start = time.time()
                curr_dataset_name = get_dataset_name(request, -2)
                parameters_list = self.manifest_cfg.datasets[curr_dataset_name].parameters
                scope = self.manifest_cfg.datasets[curr_dataset_name].scope
                params: BaseModel = params
                payload: dict = await request.json()
                result = await get_parameters_definition(
                    parameters_list, "dataset", curr_dataset_name, scope, user, payload, params.model_dump()
                )
                self.logger.log_activity_time("POST REQUEST for PARAMETERS", start, request_id=_get_request_id(request))
                return result
            
            @app.get(curr_results_path, tags=[f"Dataset '{dataset_name}'"], description=dataset_config.description, response_class=JSONResponse)
            async def get_dataset_results(
                request: Request, params: QueryModelForGetDataset, user: BaseUser | None = Depends(get_current_user) # type: ignore
            ) -> arm.DatasetResultModel:
                start = time.time()
                curr_dataset_name = get_dataset_name(request, -1)
                result = await get_dataset_results_definition(curr_dataset_name, user, dict(request.query_params), asdict(params))
                self.logger.log_activity_time("GET REQUEST for DATASET RESULTS", start, request_id=_get_request_id(request))
                return result
            
            @app.post(curr_results_path, tags=[f"Dataset '{dataset_name}'"], description=dataset_config.description, response_class=JSONResponse)
            async def get_dataset_results_with_post(
                request: Request, params: QueryModelForPostDataset, user: BaseUser | None = Depends(get_current_user) # type: ignore
            ) -> arm.DatasetResultModel:
                start = time.time()
                curr_dataset_name = get_dataset_name(request, -1)
                params: BaseModel = params
                payload: dict = await request.json()
                result = await get_dataset_results_definition(curr_dataset_name, user, payload, params.model_dump())
                self.logger.log_activity_time("POST REQUEST for DATASET RESULTS", start, request_id=_get_request_id(request))
                return result
        
        # Dashboard Parameters and Results APIs
        for dashboard_name, dashboard in self.dashboards.items():
            dashboard_normalized = u.normalize_name_for_api(dashboard_name)
            curr_parameters_path = dashboard_parameters_path.format(dashboard=dashboard_normalized)
            curr_results_path = dashboard_results_path.format(dashboard=dashboard_normalized)

            validate_parameters_list(dashboard.config.parameters, "Dashboard")
            
            QueryModelForGetParams, QueryModelForPostParams = get_query_models_for_parameters(dashboard.config.parameters)
            QueryModelForGetDash, QueryModelForPostDash = get_query_models_for_dashboard(dashboard.config.parameters)

            @app.get(curr_parameters_path, tags=[f"Dashboard '{dashboard_name}'"], description=parameters_description, response_class=JSONResponse)
            async def get_dashboard_parameters(
                request: Request, params: QueryModelForGetParams, user: BaseUser | None = Depends(get_current_user) # type: ignore
            ) -> arm.ParametersModel:
                start = time.time()
                curr_dashboard_name = get_dashboard_name(request, -2)
                parameters_list = self.dashboards[curr_dashboard_name].config.parameters    
                scope = self.dashboards[curr_dashboard_name].config.scope
                result = await get_parameters_definition(
                    parameters_list, "dashboard", curr_dashboard_name, scope, user, dict(request.query_params), asdict(params)
                )
                self.logger.log_activity_time("GET REQUEST for PARAMETERS", start, request_id=_get_request_id(request))
                return result

            @app.post(curr_parameters_path, tags=[f"Dashboard '{dashboard_name}'"], description=parameters_description, response_class=JSONResponse)
            async def get_dashboard_parameters_with_post(
                request: Request, params: QueryModelForPostParams, user: BaseUser | None = Depends(get_current_user) # type: ignore
            ) -> arm.ParametersModel:
                start = time.time()
                curr_dashboard_name = get_dashboard_name(request, -2)
                parameters_list = self.dashboards[curr_dashboard_name].config.parameters
                scope = self.dashboards[curr_dashboard_name].config.scope
                params: BaseModel = params
                payload: dict = await request.json()
                result = await get_parameters_definition(
                    parameters_list, "dashboard", curr_dashboard_name, scope, user, payload, params.model_dump()
                )
                self.logger.log_activity_time("POST REQUEST for PARAMETERS", start, request_id=_get_request_id(request))
                return result
            
            @app.get(curr_results_path, tags=[f"Dashboard '{dashboard_name}'"], description=dashboard.config.description, response_class=Response)
            async def get_dashboard_results(
                request: Request, params: QueryModelForGetDash, user: BaseUser | None = Depends(get_current_user) # type: ignore
            ) -> Response:
                start = time.time()
                curr_dashboard_name = get_dashboard_name(request, -1)
                result = await get_dashboard_results_definition(curr_dashboard_name, user, dict(request.query_params), asdict(params))
                self.logger.log_activity_time("GET REQUEST for DASHBOARD RESULTS", start, request_id=_get_request_id(request))
                return result

            @app.post(curr_results_path, tags=[f"Dashboard '{dashboard_name}'"], description=dashboard.config.description, response_class=Response)
            async def get_dashboard_results_with_post(
                request: Request, params: QueryModelForPostDash, user: BaseUser | None = Depends(get_current_user) # type: ignore
            ) -> Response:
                start = time.time()
                curr_dashboard_name = get_dashboard_name(request, -1)
                params: BaseModel = params
                payload: dict = await request.json()
                result = await get_dashboard_results_definition(curr_dashboard_name, user, payload, params.model_dump())
                self.logger.log_activity_time("POST REQUEST for DASHBOARD RESULTS", start, request_id=_get_request_id(request))
                return result

        # Build Project API
        @app.post(project_metadata_path + '/build', tags=["Data Management"], summary="Build or update the virtual data environment for the project")
        async def build(user: BaseUser | None = Depends(get_current_user)): # type: ignore
            if not self.authenticator.can_user_access_scope(user, PermissionScope.PRIVATE):
                raise InvalidInputError(26, f"User '{user}' does not have permission to build the virtual data environment")
            await self.project.build(stage_file=True)
            return Response(status_code=status.HTTP_200_OK)
        
        # Query Models API
        query_models_path = project_metadata_path + '/query-models'
        QueryModelForQueryModels, QueryModelForPostQueryModels = get_query_models_for_querying_models()

        async def query_models_helper(
            sql_query: str, user: BaseUser | None, selections: tuple[tuple[str, Any], ...]
        ) -> DatasetResult:
            return await self.project.query_models(sql_query, selections=dict(selections), user=user)

        async def query_models_cachable(
            sql_query: str, user: BaseUser | None, selections: tuple[tuple[str, Any], ...]
        ) -> DatasetResult:
            # Share the same cache for dataset results
            return await do_cachable_action(dataset_results_cache, query_models_helper, sql_query, user, selections)

        async def query_models_definition(
            user: BaseUser | None, all_request_params: dict, params: Mapping
        ) -> arm.DatasetResultModel:
            self._validate_request_params(all_request_params, params)

            if not self.authenticator.can_user_access_scope(user, PermissionScope.PRIVATE):
                raise InvalidInputError(27, f"User '{user}' does not have permission to query data models")
            sql_query = params.get("x_sql_query")
            if sql_query is None:
                raise InvalidInputError(203, "SQL query must be provided")
            
            query_models_function = query_models_helper if self.no_cache else query_models_cachable
            uncached_keys = {"x_verify_params", "x_sql_query", "x_orientation", "x_limit", "x_offset"}
            selections = get_selections_as_immutable(params, uncached_keys)
            result = await query_models_function(sql_query, user, selections)
            
            orientation = params.get("x_orientation", "records")
            limit = params.get("x_limit", 1000)
            offset = params.get("x_offset", 0)
            return arm.DatasetResultModel(**result.to_json(orientation, tuple(), limit, offset))

        @app.get(query_models_path, tags=["Data Management"], response_class=JSONResponse)
        async def query_models(
            request: Request, params: QueryModelForQueryModels, user: BaseUser | None = Depends(get_current_user)  # type: ignore
        ) -> arm.DatasetResultModel:
            start = time.time()
            result = await query_models_definition(user, dict(request.query_params), asdict(params))
            self.logger.log_activity_time("GET REQUEST for QUERY MODELS", start, request_id=_get_request_id(request))
            return result
        
        @app.post(query_models_path, tags=["Data Management"], response_class=JSONResponse)
        async def query_models_with_post(
            request: Request, params: QueryModelForPostQueryModels, user: BaseUser | None = Depends(get_current_user)  # type: ignore
        ) -> arm.DatasetResultModel:
            start = time.time()
            params: BaseModel = params
            payload: dict = await request.json()
            result = await query_models_definition(user, payload, params.model_dump())
            self.logger.log_activity_time("POST REQUEST for QUERY MODELS", start, request_id=_get_request_id(request))
            return result
        
        # Add Root Path Redirection to Squirrels Studio
        full_hostname = f"http://{uvicorn_args.host}:{uvicorn_args.port}"
        encoded_hostname = urllib.parse.quote(full_hostname, safe="")
        squirrels_studio_url = f"https://squirrels-analytics.github.io/squirrels-studio/#/login?host={encoded_hostname}&projectName={project_name}&projectVersion={project_version}"

        @app.get("/", include_in_schema=False)
        async def redirect_to_studio():
            return RedirectResponse(url=squirrels_studio_url)
        
        # Run the API Server
        import uvicorn
        
        print("\nWelcome to the Squirrels Data Application!\n")
        print(f"- Application UI: {squirrels_studio_url}")
        print(f"- API Docs (with ReDoc): {full_hostname}{project_metadata_path}/redoc")
        print(f"- API Docs (with Swagger UI): {full_hostname}{project_metadata_path}/docs")
        print()
        
        self.logger.log_activity_time("creating app server", start)
        uvicorn.run(app, host=uvicorn_args.host, port=uvicorn_args.port) 
