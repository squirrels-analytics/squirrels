from typing import Iterable, Optional, Mapping, Callable, Coroutine, TypeVar, Annotated, Any
from dataclasses import make_dataclass, asdict
from fastapi import Depends, FastAPI, Request, HTTPException, Response, status
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from pydantic import create_model, BaseModel
from cachetools import TTLCache
from pandas.api import types as pd_types
import os, mimetypes, traceback, json, pandas as pd

from . import _constants as c, _utils as u, _api_response_models as arm
from ._version import sq_major_version
from ._manifest import ManifestIO
from ._parameter_sets import ParameterConfigsSetIO
from ._authenticator import User, Authenticator
from ._timer import timer, time
from ._parameter_sets import ParameterSet
from ._models import ModelsIO

mimetypes.add_type('application/javascript', '.js')


def df_to_api_response0(df: pd.DataFrame, dimensions: list[str] = None) -> arm.DatasetResultModel:
    """
    Convert a pandas DataFrame to the response format that the dataset result API of Squirrels outputs.

    Parameters:
        df: The dataframe to convert into an API response
        dimensions: The list of declared dimensions. If None, all non-numeric columns are assumed as dimensions

    Returns:
        The response of a Squirrels dataset result API
    """
    in_df_json = json.loads(df.to_json(orient='table', index=False))
    out_fields = []
    non_numeric_fields = []
    for in_column in in_df_json["schema"]["fields"]:
        col_name: str = in_column["name"]
        out_column = arm.ColumnModel(name=col_name, type=in_column["type"])
        out_fields.append(out_column)
        
        if not pd_types.is_numeric_dtype(df[col_name].dtype):
            non_numeric_fields.append(col_name)
    
    out_dimensions = non_numeric_fields if dimensions is None else dimensions
    out_schema = arm.SchemaModel(fields=out_fields, dimensions=out_dimensions)
    return arm.DatasetResultModel(schema=out_schema, data=in_df_json["data"])


class ApiServer:
    def __init__(self, no_cache: bool) -> None:
        """
        Constructor for ApiServer

        Parameters:
            no_cache (bool): Whether to disable caching
        """
        self.no_cache = no_cache
        self.dataset_configs = ManifestIO.obj.datasets
        
        token_expiry_minutes = ManifestIO.obj.settings.get(c.AUTH_TOKEN_EXPIRE_SETTING, 30)
        self.authenticator = Authenticator(token_expiry_minutes)
    
    def run(self, uvicorn_args: list[str]) -> None:
        """
        Runs the API server with uvicorn for CLI "squirrels run"

        Parameters:
            uvicorn_args: List of arguments to pass to uvicorn.run. Currently only supports "host" and "port"
        """
        start = time.time()
        app = FastAPI()

        @app.middleware("http")
        async def catch_exceptions_middleware(request: Request, call_next):
            try:
                return await call_next(request)
            except u.InvalidInputError as exc:
                traceback.print_exc()
                return JSONResponse(status_code=status.HTTP_400_BAD_REQUEST, 
                                    content={"message": str(exc), "blame": "API client"})
            except u.ConfigurationError as exc:
                traceback.print_exc()
                return JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                                    content={"message": f"An unexpected error occurred", "blame": "Squirrels project"})
            except Exception as exc:
                traceback.print_exc()
                return JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                                    content={"message": f"An unexpected error occurred", "blame": "Squirrels framework"})

        app.add_middleware(
            CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"], 
            expose_headers=["Applied-Username"]
        )

        squirrels_version_path = f'/squirrels-v{sq_major_version}'
        partial_base_path = f'/{ManifestIO.obj.project_variables.get_name()}/v{ManifestIO.obj.project_variables.get_major_version()}'
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

        REQUEST_VERSION_REQUEST_HEADER = "squirrels-request-version"
        def get_request_version_header(headers: Mapping):
            return get_versioning_request_header(headers, REQUEST_VERSION_REQUEST_HEADER)
        
        RESPONSE_VERSION_REQUEST_HEADER = "squirrels-response-version"
        def process_based_on_response_version_header(headers: Mapping, processes: dict[str, Callable[[], T]]) -> T:
            response_version = get_versioning_request_header(headers, RESPONSE_VERSION_REQUEST_HEADER)
            if response_version is None or response_version >= 0:
                return processes[0]()
            else:
                raise u.InvalidInputError(f'Invalid value for "{RESPONSE_VERSION_REQUEST_HEADER}" header: {response_version}')
        
        def can_user_access_dataset(user: Optional[User], dataset: str):
            try:
                dataset_scope = self.dataset_configs[dataset].scope
            except KeyError as e:
                raise u.InvalidInputError(f'Invalid dataset name: "{dataset}"')
            return self.authenticator.can_user_access_scope(user, dataset_scope)

        async def apply_dataset_api_function(
            api_function: Callable[..., Coroutine[Any, Any, T]], user: Optional[User], dataset: str, headers: Mapping, params: Mapping
        ) -> T:
            dataset_normalized = u.normalize_name(dataset)
            if not can_user_access_dataset(user, dataset_normalized):
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                                    detail="Could not validate credentials",
                                    headers={"WWW-Authenticate": "Bearer"})
            
            request_version = get_request_version_header(headers)
            
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
            
            return await api_function(user, dataset_normalized, selections, request_version)

        async def do_cachable_action(cache: TTLCache, action: Callable[..., Coroutine[Any, Any, T]], *args) -> T:
            cache_key = tuple(args)
            result = cache.get(cache_key)
            if result is None:
                result = await action(*args)
                cache[cache_key] = result
            return result
        
        def get_dataset_from_request_path(request: Request, section: int) -> str:
            url_path: str = request.scope['route'].path
            return url_path.split('/')[section]
        
        # Login
        token_path = base_path + '/token'

        oauth2_scheme = OAuth2PasswordBearer(tokenUrl=token_path, auto_error=False)

        @app.post(token_path)
        async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()) -> arm.LoginReponse:
            user: Optional[User] = self.authenticator.authenticate_user(form_data.username, form_data.password)
            if not user:
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, 
                                    detail="Incorrect username or password",
                                    headers={"WWW-Authenticate": "Bearer"})
            access_token, expiry = self.authenticator.create_access_token(user)
            return arm.LoginReponse(access_token=access_token, token_type="bearer", username=user.username, expiry_time=expiry)
        
        async def get_current_user(response: Response, token: str = Depends(oauth2_scheme)) -> Optional[User]:
            user = self.authenticator.get_user_from_token(token)
            username = "" if user is None else user.username
            response.headers["Applied-Username"] = username
            return user

        # Parameters API Helpers
        parameters_path = base_path + '/dataset/{dataset}/parameters'
        
        def get_dataset_for_parameters_request(request: Request) -> str:
            return get_dataset_from_request_path(request, -2)
        
        parameters_cache_size = ManifestIO.obj.settings.get(c.PARAMETERS_CACHE_SIZE_SETTING, 1024)
        parameters_cache_ttl = ManifestIO.obj.settings.get(c.PARAMETERS_CACHE_TTL_SETTING, 60)
    
        async def get_parameters_helper(
            user: Optional[User], dataset: str, selections: Iterable[tuple[str, str]], request_version: Optional[int]
        ) -> ParameterSet:
            if len(selections) > 1:
                raise u.InvalidInputError(f"The /parameters endpoint takes at most 1 query parameter. Got {dict(selections)}")
            dag = ModelsIO.GenerateDAG(dataset)
            dag.apply_selections(user, dict(selections), updates_only=True, request_version=request_version)
            return dag.parameter_set

        params_cache = TTLCache(maxsize=parameters_cache_size, ttl=parameters_cache_ttl*60)

        async def get_parameters_cachable(*args) -> T:
            return await do_cachable_action(params_cache, get_parameters_helper, *args)
        
        async def get_parameters_definition(dataset: str, user: Optional[User], headers: Mapping, params: Mapping) -> arm.ParametersModel:
            api_function = get_parameters_helper if self.no_cache else get_parameters_cachable
            result = await apply_dataset_api_function(api_function, user, dataset, headers, params)
            return process_based_on_response_version_header(headers, {
                0: result.to_api_response_model0
            })

        # Results API Helpers
        results_path = base_path + '/dataset/{dataset}'

        def get_dataset_for_results_request(request: Request) -> str:
            return get_dataset_from_request_path(request, -1)

        results_cache_size = ManifestIO.obj.settings.get(c.RESULTS_CACHE_SIZE_SETTING, 128)
        results_cache_ttl = ManifestIO.obj.settings.get(c.RESULTS_CACHE_TTL_SETTING, 60)
    
        async def get_results_helper(
            user: Optional[User], dataset: str, selections: Iterable[tuple[str, str]], request_version: Optional[int]
        ) -> pd.DataFrame:
            dag = ModelsIO.GenerateDAG(dataset)
            await dag.execute(ModelsIO.context_func, user, dict(selections), request_version=request_version)
            return dag.target_model.result

        results_cache = TTLCache(maxsize=results_cache_size, ttl=results_cache_ttl*60)

        async def get_results_cachable(*args) -> pd.DataFrame:
            return await do_cachable_action(results_cache, get_results_helper, *args)
        
        async def get_results_definition(dataset: str, user: Optional[User], headers: Mapping, params: Mapping) -> arm.DatasetResultModel:
            api_function = get_results_helper if self.no_cache else get_results_cachable
            result = await apply_dataset_api_function(api_function, user, dataset, headers, params)
            return process_based_on_response_version_header(headers, {
                0: lambda: df_to_api_response0(result)
            })
        
        param_fields = ParameterConfigsSetIO.obj.get_all_api_field_info()
        
        # Dataset Parameters and Results APIs
        for dataset_name, dataset_cfg in self.dataset_configs.items():
            dataset_normalized = u.normalize_name_for_api(dataset_name)
            curr_parameters_path = parameters_path.format(dataset=dataset_normalized)
            curr_results_path = results_path.format(dataset=dataset_normalized)

            for param in dataset_cfg.parameters:
                if param not in param_fields:
                    all_params = list(param_fields.keys())
                    raise u.ConfigurationError(f"Dataset '{dataset_name}' use parameter '{param}' which doesn't exist. Available parameters are:"
                                               f"\n  {all_params}")

            QueryModelGet = make_dataclass("QueryParams", [
                param_fields[param].as_query_info() for param in dataset_cfg.parameters
            ])
            AnnotatedQueryModel = Annotated[QueryModelGet, Depends()]

            QueryModelPost = create_model("RequestBodyParams", **{
                param: param_fields[param].as_body_info() for param in dataset_cfg.parameters
            })
        
            @app.get(curr_parameters_path, response_class=JSONResponse)
            async def get_parameters(
                request: Request, params: AnnotatedQueryModel, user: Optional[User] = Depends(get_current_user) # type: ignore
            ) -> arm.ParametersModel:
                start = time.time()
                dataset = get_dataset_for_parameters_request(request)
                result = await get_parameters_definition(dataset, user, request.headers, asdict(params))
                timer.add_activity_time("GET REQUEST total time for PARAMETERS endpoint", start)
                return result

            @app.post(curr_parameters_path, response_class=JSONResponse)
            async def get_parameters_with_post(
                request: Request, params: QueryModelPost, user: Optional[User] = Depends(get_current_user) # type: ignore
            ) -> arm.ParametersModel:
                start = time.time()
                dataset = get_dataset_for_parameters_request(request)
                params: BaseModel = params
                result = await get_parameters_definition(dataset, user, request.headers, params.model_dump())
                timer.add_activity_time("POST REQUEST total time for PARAMETERS endpoint", start)
                return result
            
            @app.get(curr_results_path, response_class=JSONResponse)
            async def get_results(
                request: Request, params: AnnotatedQueryModel, user: Optional[User] = Depends(get_current_user) # type: ignore
            ) -> arm.DatasetResultModel:
                start = time.time()
                dataset = get_dataset_for_results_request(request)
                result = await get_results_definition(dataset, user, request.headers, asdict(params))
                timer.add_activity_time("GET REQUEST total time for DATASET endpoint", start)
                return result
            
            @app.post(curr_results_path, response_class=JSONResponse)
            async def get_results_with_post(
                request: Request, params: QueryModelPost, user: Optional[User] = Depends(get_current_user) # type: ignore
            ) -> arm.DatasetResultModel:
                start = time.time()
                dataset = get_dataset_for_results_request(request)
                params: BaseModel = params
                result = await get_results_definition(dataset, user, request.headers, params.model_dump())
                timer.add_activity_time("POST REQUEST total time for DATASET endpoint", start)
                return result
        
        # Datasets Catalog API
        datasets_path = base_path + '/datasets'

        def get_datasets0(user: Optional[User]) -> arm.DatasetsCatalogModel:
            datasets_info = []
            for dataset_name, dataset_config in self.dataset_configs.items():
                if can_user_access_dataset(user, dataset_name):
                    dataset_normalized = u.normalize_name_for_api(dataset_name)
                    datasets_info.append(arm.DatasetInfoModel(
                        name=dataset_name, label=dataset_config.label,
                        parameters_path=parameters_path.format(dataset=dataset_normalized),
                        result_path=results_path.format(dataset=dataset_normalized)
                    ))
            return arm.DatasetsCatalogModel(datasets=datasets_info)
        
        @app.get(datasets_path)
        def get_datasets(request: Request, user: Optional[User] = Depends(get_current_user)) -> arm.DatasetsCatalogModel:
            return process_based_on_response_version_header(request.headers, {
                0: lambda: get_datasets0(user)
            })
        
        # Projects Catalog API
        def get_catalog0() -> arm.CatalogModel:
            return arm.CatalogModel(projects=[arm.ProjectModel(
                name=ManifestIO.obj.project_variables.get_name(),
                label=ManifestIO.obj.project_variables.get_label(),
                versions=[arm.ProjectVersionModel(
                    major_version=ManifestIO.obj.project_variables.get_major_version(),
                    minor_versions=[0],
                    token_path=token_path,
                    datasets_path=datasets_path
                )]
            )])
        
        @app.get(squirrels_version_path, response_class=JSONResponse)
        async def get_catalog(request: Request) -> arm.CatalogModel:
            return process_based_on_response_version_header(request.headers, {
                0: lambda: get_catalog0()
            })
        
        # Squirrels UI
        static_dir = u.join_paths(os.path.dirname(__file__), c.PACKAGE_DATA_FOLDER, c.ASSETS_FOLDER)
        app.mount('/'+c.ASSETS_FOLDER, StaticFiles(directory=static_dir), name=c.ASSETS_FOLDER)

        templates_dir = u.join_paths(os.path.dirname(__file__), c.PACKAGE_DATA_FOLDER, c.TEMPLATES_FOLDER)
        templates = Jinja2Templates(directory=templates_dir)

        @app.get('/', response_class=HTMLResponse)
        async def get_ui(request: Request):
            return templates.TemplateResponse('index.html', {
                'request': request, 'catalog_path': squirrels_version_path, 'token_path': token_path
            })
        
        # Run API server
        import uvicorn
        timer.add_activity_time("creating app for api server", start)
        uvicorn.run(app, host=uvicorn_args.host, port=uvicorn_args.port)
