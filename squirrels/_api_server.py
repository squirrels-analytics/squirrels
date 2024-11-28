from typing import Coroutine, Mapping, Callable, TypeVar, Annotated, Any
from dataclasses import make_dataclass, asdict
from fastapi import Depends, FastAPI, Request, HTTPException, Response, status
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from pydantic import create_model, BaseModel
from cachetools import TTLCache
from argparse import Namespace
import os, io, time, mimetypes, traceback, uuid, polars as pl

from . import _constants as c, _utils as u, _api_response_models as arm
from ._version import sq_major_version
from ._authenticator import User
from ._parameter_sets import ParameterSet
from .dashboards import Dashboard
from .project import SquirrelsProject

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

        self.j2_env = project._j2_env
        self.env_cfg = project._env_cfg
        self.manifest_cfg = project._manifest_cfg
        self.seeds = project._seeds
        self.conn_args = project._conn_args
        self.conn_set = project._conn_set
        self.authenticator = project._authenticator
        self.param_args = project._param_args
        self.param_cfg_set = project._param_cfg_set
        self.context_func = project._context_func
        self.dashboards = project._dashboards
    
    def run(self, uvicorn_args: Namespace) -> None:
        """
        Runs the API server with uvicorn for CLI "squirrels run"

        Arguments:
            uvicorn_args: List of arguments to pass to uvicorn.run. Currently only supports "host" and "port"
        """
        start = time.time()
        
        tags_metadata = [
            {
                "name": "Project Metadata",
                "description": "Get information on project such as name, version, and other API endpoints",
            },
            {
                "name": "Login",
                "description": "Submit username and password, and get token for authentication",
            },
            {
                "name": "Catalogs",
                "description": "Get catalog of datasets and dashboards with endpoints for their parameters and results",
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
            description="For specifying parameter selections to dataset APIs, you can choose between using query parameters with the GET method or using request body with the POST method"
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
            except u.InvalidInputError as exc:
                traceback.print_exc(file=buffer)
                response = JSONResponse(
                    status_code=status.HTTP_400_BAD_REQUEST, content={"message": str(exc), "blame": "API client"}
                )
            except u.FileExecutionError as exc:
                traceback.print_exception(exc.error, file=buffer)
                buffer.write(str(exc))
                response = JSONResponse(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content={"message": f"An unexpected error occurred", "blame": "Squirrels project"}
                )
            except u.ConfigurationError as exc:
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

        squirrels_version_path = f'/squirrels-v{sq_major_version}'
        partial_base_path = f'/{self.manifest_cfg.project_variables.name}/v{self.manifest_cfg.project_variables.major_version}'
        base_path = squirrels_version_path + u.normalize_name_for_api(partial_base_path)
        
        # Helpers
        T = TypeVar('T')
        
        def get_versioning_request_header(headers: Mapping, header_key: str):
            header_value = headers.get(header_key)
            if header_value is None:
                return None
            
            try:
                result = int(header_value)
            except ValueError:
                raise u.InvalidInputError(f"Request header '{header_key}' must be an integer. Got '{header_value}'")
            
            if result < 0 or result > int(sq_major_version):
                raise u.InvalidInputError(f"Request header '{header_key}' not in valid range. Got '{result}'")
            
            return result

        def get_request_version_header(headers: Mapping):
            REQUEST_VERSION_REQUEST_HEADER = "squirrels-request-version"
            return get_versioning_request_header(headers, REQUEST_VERSION_REQUEST_HEADER)
        
        def process_based_on_response_version_header(headers: Mapping, processes: dict[int, Callable[[], T]]) -> T:
            RESPONSE_VERSION_REQUEST_HEADER = "squirrels-response-version"
            response_version = get_versioning_request_header(headers, RESPONSE_VERSION_REQUEST_HEADER)
            if response_version is None or response_version >= 0:
                return processes[0]()
            else:
                raise u.InvalidInputError(f'Invalid value for "{RESPONSE_VERSION_REQUEST_HEADER}" header: {response_version}')
        
        def get_selections_and_request_version(params: Mapping, headers: Mapping) -> tuple[frozenset[tuple[str, Any]], int | None]:
            # Changing selections into a cachable "frozenset" that will later be converted to dictionary
            selections = set()
            for key, val in params.items():
                if val is None:
                    continue
                if isinstance(val, (list, tuple)):
                    if len(val) == 1: # for backward compatibility
                        val = val[0]
                    else:
                        val = tuple(val)
                selections.add((u.normalize_name(key), val))
            selections = frozenset(selections)
            
            request_version = get_request_version_header(headers)
            return selections, request_version

        async def do_cachable_action(cache: TTLCache, action: Callable[..., Coroutine[Any, Any, T]], *args) -> T:
            cache_key = tuple(args)
            result = cache.get(cache_key)
            if result is None:
                result = await action(*args)
                cache[cache_key] = result
            return result
        
        def get_query_models_from_widget_params(parameters: list):
            QueryModelForGetRaw = make_dataclass("QueryParams", [
                param_fields[param].as_query_info() for param in parameters
            ])
            QueryModelForGet = Annotated[QueryModelForGetRaw, Depends()]

            QueryModelForPost = create_model("RequestBodyParams", **{
                param: param_fields[param].as_body_info() for param in parameters
            }) # type: ignore
            return QueryModelForGet, QueryModelForPost
        
        def _get_section_from_request_path(request: Request, section: int) -> str:
            url_path: str = request.scope['route'].path
            return url_path.split('/')[section]

        def get_dataset_name(request: Request, section: int) -> str:
            dataset_raw = _get_section_from_request_path(request, section)
            return u.normalize_name(dataset_raw)

        def get_dashboard_name(request: Request, section: int) -> str:
            dashboard_raw = _get_section_from_request_path(request, section)
            return u.normalize_name(dashboard_raw)
        
        # Login & Authorization
        token_path = base_path + '/token'

        oauth2_scheme = OAuth2PasswordBearer(tokenUrl=token_path, auto_error=False)

        @app.post(token_path, tags=["Login"])
        async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()) -> arm.LoginReponse:
            user: User | None = self.authenticator.authenticate_user(form_data.username, form_data.password)
            if not user:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED, detail="Incorrect username or password", headers={"WWW-Authenticate": "Bearer"}
                )
            access_token, expiry = self.authenticator.create_access_token(user)
            return arm.LoginReponse(access_token=access_token, token_type="bearer", username=user.username, expiry_time=expiry)
        
        async def get_current_user(response: Response, token: str = Depends(oauth2_scheme)) -> User | None:
            user = self.authenticator.get_user_from_token(token)
            username = "" if user is None else user.username
            response.headers["Applied-Username"] = username
            return user

        # Datasets / Dashboards Catalog API
        data_catalog_path = base_path + '/data-catalog'
        dataset_results_path = base_path + '/dataset/{dataset}'
        dataset_parameters_path = dataset_results_path + '/parameters'
        dashboard_results_path = base_path + '/dashboard/{dashboard}'
        dashboard_parameters_path = dashboard_results_path + '/parameters'
        
        def get_data_catalog0(user: User | None) -> arm.CatalogModel:
            dataset_items: list[arm.DatasetItemModel] = []
            for name, config in self.manifest_cfg.datasets.items():
                if self.authenticator.can_user_access_scope(user, config.scope):
                    name_normalized = u.normalize_name_for_api(name)
                    dataset_items.append(arm.DatasetItemModel(
                        name=name, label=config.label, description=config.description,
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
                        raise u.ConfigurationError(f"No dashboard file found for: {name}")
                    
                    dashboard_items.append(arm.DashboardItemModel(
                        name=name, label=config.label, description=config.description, result_format=dashboard_format,
                        parameters_path=dashboard_parameters_path.format(dashboard=name_normalized),
                        result_path=dashboard_results_path.format(dashboard=name_normalized)
                    ))
            
            return arm.CatalogModel(datasets=dataset_items, dashboards=dashboard_items)
        
        @app.get(data_catalog_path, tags=["Catalogs"], summary="Get list of datasets and dashboards available for user")
        def get_catalog_of_datasets_and_dashboards(request: Request, user: User | None = Depends(get_current_user)) -> arm.CatalogModel:
            return process_based_on_response_version_header(request.headers, {
                0: lambda: get_data_catalog0(user)
            })
        
        # Parameters API Helpers
        parameters_description = "Selections of one parameter may cascade the available options in another parameter. " \
                "For example, if the dataset has parameters for 'country' and 'city', available options for 'city' would " \
                "depend on the selected option 'country'. If a parameter has 'trigger_refresh' as true, provide the parameter " \
                "selection to this endpoint whenever it changes to refresh the parameter options of children parameters."
        
        async def get_parameters_helper(
            parameters_tuple: tuple[str, ...], user: User | None, selections: frozenset[tuple[str, Any]], request_version: int | None
        ) -> ParameterSet:
            if len(selections) > 1:
                raise u.InvalidInputError(f"The /parameters endpoint takes at most 1 query parameter. Got {dict(selections)}")
            
            param_set = self.param_cfg_set.apply_selections(
                parameters_tuple, dict(selections), user, updates_only=True, request_version=request_version
            )
            return param_set

        settings = self.manifest_cfg.settings
        parameters_cache_size = settings.get(c.PARAMETERS_CACHE_SIZE_SETTING, 1024)
        parameters_cache_ttl = settings.get(c.PARAMETERS_CACHE_TTL_SETTING, 60)
        params_cache = TTLCache(maxsize=parameters_cache_size, ttl=parameters_cache_ttl*60)

        async def get_parameters_cachable(
            parameters_tuple: tuple[str, ...], user: User | None, selections: frozenset[tuple[str, Any]], request_version: int | None
        ) -> ParameterSet:
            return await do_cachable_action(params_cache, get_parameters_helper, parameters_tuple, user, selections, request_version)
        
        async def get_parameters_definition(
            parameters_list: list[str], user: User | None, headers: Mapping, params: Mapping
        ) -> arm.ParametersModel:
            get_parameters_function = get_parameters_helper if self.no_cache else get_parameters_cachable
            selections, request_version = get_selections_and_request_version(params, headers)
            result = await get_parameters_function(tuple(parameters_list), user, selections, request_version)
            return process_based_on_response_version_header(headers, {
                0: result.to_api_response_model0
            })

        param_fields = self.param_cfg_set.get_all_api_field_info()

        def validate_parameters_list(parameters: list[str], entity_type: str) -> None:
            for param in parameters:
                if param not in param_fields:
                    all_params = list(param_fields.keys())
                    raise u.ConfigurationError(f"{entity_type} '{dataset_name}' use parameter '{param}' which doesn't exist. Available parameters are:"
                                               f"\n  {all_params}")
        
        # Dataset Results API Helpers
        async def get_dataset_results_helper(
            dataset: str, user: User | None, selections: frozenset[tuple[str, Any]], request_version: int | None
        ) -> pl.DataFrame:
            try:
                return await self.project.dataset(dataset, selections=dict(selections), user=user)
            except PermissionError as e:
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(e), headers={"WWW-Authenticate": "Bearer"}) from e

        settings = self.manifest_cfg.settings
        dataset_results_cache_size = settings.get(c.DATASETS_CACHE_SIZE_SETTING, 128)
        dataset_results_cache_ttl = settings.get(c.DATASETS_CACHE_TTL_SETTING, 60)
        dataset_results_cache = TTLCache(maxsize=dataset_results_cache_size, ttl=dataset_results_cache_ttl*60)

        async def get_dataset_results_cachable(
            dataset: str, user: User | None, selections: frozenset[tuple[str, Any]], request_version: int | None
        ) -> pl.DataFrame:
            return await do_cachable_action(dataset_results_cache, get_dataset_results_helper, dataset, user, selections, request_version)
        
        async def get_dataset_results_definition(
            dataset_name: str, user: User | None, headers: Mapping, params: Mapping
        ) -> arm.DatasetResultModel:
            get_dataset_function = get_dataset_results_helper if self.no_cache else get_dataset_results_cachable
            selections, request_version = get_selections_and_request_version(params, headers)
            result = await get_dataset_function(dataset_name, user, selections, request_version)
            return process_based_on_response_version_header(headers, {
                0: lambda: arm.DatasetResultModel(**u.df_to_json0(result))
            })
        
        # Dashboard Results API Helpers
        async def get_dashboard_results_helper(
            dashboard: str, user: User | None, selections: frozenset[tuple[str, Any]], request_version: int | None
        ) -> Dashboard:
            try:
                return await self.project.dashboard(dashboard, selections=dict(selections), user=user)
            except PermissionError as e:
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(e), headers={"WWW-Authenticate": "Bearer"}) from e

        settings = self.manifest_cfg.settings
        dashboard_results_cache_size = settings.get(c.DASHBOARDS_CACHE_SIZE_SETTING, 128)
        dashboard_results_cache_ttl = settings.get(c.DASHBOARDS_CACHE_TTL_SETTING, 60)
        dashboard_results_cache = TTLCache(maxsize=dashboard_results_cache_size, ttl=dashboard_results_cache_ttl*60)

        async def get_dashboard_results_cachable(
            dashboard: str, user: User | None, selections: frozenset[tuple[str, Any]], request_version: int | None
        ) -> Dashboard:
            return await do_cachable_action(dashboard_results_cache, get_dashboard_results_helper, dashboard, user, selections, request_version)
        
        async def get_dashboard_results_definition(
            dashboard_name: str, user: User | None, headers: Mapping, params: Mapping
        ) -> Response:
            get_dashboard_function = get_dashboard_results_helper if self.no_cache else get_dashboard_results_cachable
            selections, request_version = get_selections_and_request_version(params, headers)
            dashboard_obj = await get_dashboard_function(dashboard_name, user, selections, request_version)
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

            QueryModelForGet, QueryModelForPost = get_query_models_from_widget_params(dataset_config.parameters)

            @app.get(
                curr_parameters_path, tags=[f"Dataset '{dataset_name}'"], openapi_extra={"dataset": dataset_name},
                description=parameters_description, response_class=JSONResponse
            )
            async def get_dataset_parameters(
                request: Request, params: QueryModelForGet, user: User | None = Depends(get_current_user) # type: ignore
            ) -> arm.ParametersModel:
                start = time.time()
                curr_dataset_name = get_dataset_name(request, -2)
                parameters_list = self.manifest_cfg.datasets[curr_dataset_name].parameters
                result = await get_parameters_definition(parameters_list, user, request.headers, asdict(params))
                self.logger.log_activity_time("GET REQUEST for PARAMETERS", start, request_id=_get_request_id(request))
                return result

            @app.post(
                curr_parameters_path, tags=[f"Dataset '{dataset_name}'"], openapi_extra={"dataset": dataset_name},
                description=parameters_description, response_class=JSONResponse
            )
            async def get_dataset_parameters_with_post(
                request: Request, params: QueryModelForPost, user: User | None = Depends(get_current_user) # type: ignore
            ) -> arm.ParametersModel:
                start = time.time()
                curr_dataset_name = get_dataset_name(request, -2)
                parameters_list = self.manifest_cfg.datasets[curr_dataset_name].parameters
                params: BaseModel = params
                result = await get_parameters_definition(parameters_list, user, request.headers, params.model_dump())
                self.logger.log_activity_time("POST REQUEST for PARAMETERS", start, request_id=_get_request_id(request))
                return result
            
            @app.get(curr_results_path, tags=[f"Dataset '{dataset_name}'"], description=dataset_config.description, response_class=JSONResponse)
            async def get_dataset_results(
                request: Request, params: QueryModelForGet, user: User | None = Depends(get_current_user) # type: ignore
            ) -> arm.DatasetResultModel:
                start = time.time()
                curr_dataset_name = get_dataset_name(request, -1)
                result = await get_dataset_results_definition(curr_dataset_name, user, request.headers, asdict(params))
                self.logger.log_activity_time("GET REQUEST for DATASET RESULTS", start, request_id=_get_request_id(request))
                return result
            
            @app.post(curr_results_path, tags=[f"Dataset '{dataset_name}'"], description=dataset_config.description, response_class=JSONResponse)
            async def get_dataset_results_with_post(
                request: Request, params: QueryModelForPost, user: User | None = Depends(get_current_user) # type: ignore
            ) -> arm.DatasetResultModel:
                start = time.time()
                curr_dataset_name = get_dataset_name(request, -1)
                params: BaseModel = params
                result = await get_dataset_results_definition(curr_dataset_name, user, request.headers, params.model_dump())
                self.logger.log_activity_time("POST REQUEST for DATASET RESULTS", start, request_id=_get_request_id(request))
                return result
        
        # Dashboard Parameters and Results APIs
        for dashboard_name, dashboard in self.dashboards.items():
            dashboard_normalized = u.normalize_name_for_api(dashboard_name)
            curr_parameters_path = dashboard_parameters_path.format(dashboard=dashboard_normalized)
            curr_results_path = dashboard_results_path.format(dashboard=dashboard_normalized)

            validate_parameters_list(dashboard.config.parameters, "Dashboard")
            
            QueryModelForGet, QueryModelForPost = get_query_models_from_widget_params(dashboard.config.parameters)

            @app.get(curr_parameters_path, tags=[f"Dashboard '{dashboard_name}'"], description=parameters_description, response_class=JSONResponse)
            async def get_dashboard_parameters(
                request: Request, params: QueryModelForGet, user: User | None = Depends(get_current_user) # type: ignore
            ) -> arm.ParametersModel:
                start = time.time()
                curr_dashboard_name = get_dashboard_name(request, -2)
                parameters_list = self.dashboards[curr_dashboard_name].config.parameters
                result = await get_parameters_definition(parameters_list, user, request.headers, asdict(params))
                self.logger.log_activity_time("GET REQUEST for PARAMETERS", start, request_id=_get_request_id(request))
                return result

            @app.post(curr_parameters_path, tags=[f"Dashboard '{dashboard_name}'"], description=parameters_description, response_class=JSONResponse)
            async def get_dashboard_parameters_with_post(
                request: Request, params: QueryModelForPost, user: User | None = Depends(get_current_user) # type: ignore
            ) -> arm.ParametersModel:
                start = time.time()
                curr_dashboard_name = get_dashboard_name(request, -2)
                parameters_list = self.dashboards[curr_dashboard_name].config.parameters
                params: BaseModel = params
                result = await get_parameters_definition(parameters_list, user, request.headers, params.model_dump())
                self.logger.log_activity_time("POST REQUEST for PARAMETERS", start, request_id=_get_request_id(request))
                return result
            
            @app.get(curr_results_path, tags=[f"Dashboard '{dashboard_name}'"], description=dashboard.config.description, response_class=Response)
            async def get_dashboard_results(
                request: Request, params: QueryModelForGet, user: User | None = Depends(get_current_user) # type: ignore
            ) -> Response:
                start = time.time()
                curr_dashboard_name = get_dashboard_name(request, -1)
                result = await get_dashboard_results_definition(curr_dashboard_name, user, request.headers, asdict(params))
                self.logger.log_activity_time("GET REQUEST for DASHBOARD RESULTS", start, request_id=_get_request_id(request))
                return result

            @app.post(curr_results_path, tags=[f"Dashboard '{dashboard_name}'"], description=dashboard.config.description, response_class=Response)
            async def get_dashboard_results_with_post(
                request: Request, params: QueryModelForPost, user: User | None = Depends(get_current_user) # type: ignore
            ) -> Response:
                start = time.time()
                curr_dashboard_name = get_dashboard_name(request, -1)
                params: BaseModel = params
                result = await get_dashboard_results_definition(curr_dashboard_name, user, request.headers, params.model_dump())
                self.logger.log_activity_time("POST REQUEST for DASHBOARD RESULTS", start, request_id=_get_request_id(request))
                return result

        # Project Metadata API
        def get_project_metadata0() -> arm.ProjectModel:
            return arm.ProjectModel(
                name=self.manifest_cfg.project_variables.name,
                label=self.manifest_cfg.project_variables.label,
                versions=[arm.ProjectVersionModel(
                    major_version=self.manifest_cfg.project_variables.major_version,
                    minor_versions=[0],
                    token_path=token_path,
                    data_catalog_path=data_catalog_path
                )]
            )
        
        @app.get(squirrels_version_path, tags=["Project Metadata"], response_class=JSONResponse)
        async def get_project_metadata(request: Request) -> arm.ProjectModel:
            return process_based_on_response_version_header(request.headers, {
                0: lambda: get_project_metadata0()
            })
        
        # Squirrels Testing UI
        static_dir = u.Path(os.path.dirname(__file__), c.PACKAGE_DATA_FOLDER, c.ASSETS_FOLDER)
        app.mount('/'+c.ASSETS_FOLDER, StaticFiles(directory=static_dir), name=c.ASSETS_FOLDER)

        templates_dir = u.Path(os.path.dirname(__file__), c.PACKAGE_DATA_FOLDER, c.TEMPLATES_FOLDER)
        templates = Jinja2Templates(directory=templates_dir)

        @app.get('/', summary="Get the Squirrels Testing UI", response_class=HTMLResponse)
        async def get_testing_ui(request: Request):
            return templates.TemplateResponse('index.html', {
                'request': request, 'project_metadata_path': squirrels_version_path, 'token_path': token_path
            })
        
        # Run API server
        import uvicorn
        self.logger.log_activity_time("creating app server", start)
        uvicorn.run(app, host=uvicorn_args.host, port=uvicorn_args.port)
