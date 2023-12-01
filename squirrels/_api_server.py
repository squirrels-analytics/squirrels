from typing import Dict, List, Tuple, Iterable, Optional, Mapping, Callable, TypeVar
from fastapi import Depends, FastAPI, Request, HTTPException, status
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from cachetools.func import ttl_cache
import os, traceback, json, pandas as pd

from . import _constants as c, _utils as u
from ._version import sq_major_version
from ._manifest import ManifestIO
from ._renderer import RendererIOWrapper, Renderer
from ._authenticator import UserBase, Authenticator
from ._timer import timer, time
from ._parameter_sets import ParameterSet


class ApiServer:
    def __init__(self, no_cache: bool, debug: bool) -> None:
        """
        Constructor for ApiServer

        Parameters:
            no_cache (bool): Whether to disable caching
            debug (bool): Set to True to show "hidden" parameters in the /parameters endpoint response
        """
        self.no_cache = no_cache
        self.debug = debug
        
        self.datasets = ManifestIO.obj.get_all_dataset_names()
        self.renderers: Dict[str, Renderer] = {}
        for dataset in self.datasets:
            rendererIO = RendererIOWrapper(dataset)
            self.renderers[dataset] = rendererIO.renderer
        
        token_expiry_minutes = ManifestIO.obj.get_setting(c.AUTH_TOKEN_EXPIRE_SETTING, 30)
        self.authenticator = Authenticator(token_expiry_minutes)
    
    def run(self, uvicorn_args: List[str]) -> None:
        """
        Runs the API server with uvicorn for CLI "squirrels run"

        Parameters:
            uvicorn_args (List[str]): List of arguments to pass to uvicorn.run. Currently only supports "host" and "port"
        """
        start = time.time()
        app = FastAPI()

        app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

        squirrels_version_path = f'/squirrels-v{sq_major_version}'
        partial_base_path = f'/{ManifestIO.obj.get_product()}/v{ManifestIO.obj.get_major_version()}'
        base_path = squirrels_version_path + u.normalize_name_for_api(partial_base_path)

        static_dir = u.join_paths(os.path.dirname(__file__), c.PACKAGE_DATA_FOLDER, c.STATIC_FOLDER)
        app.mount('/static', StaticFiles(directory=static_dir), name='static')

        templates_dir = u.join_paths(os.path.dirname(__file__), c.PACKAGE_DATA_FOLDER, c.TEMPLATES_FOLDER)
        templates = Jinja2Templates(directory=templates_dir)

        # Exception handlers
        @app.exception_handler(u.InvalidInputError)
        async def invalid_input_error_handler(request: Request, exc: u.InvalidInputError):
            traceback.print_exc()
            return JSONResponse(status_code=status.HTTP_400_BAD_REQUEST, 
                                content={"message": f"Invalid user input: {str(exc)}"})

        @app.exception_handler(u.ConfigurationError)
        async def configuration_error_handler(request: Request, exc: u.InvalidInputError):
            traceback.print_exc()
            return JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                                content={"message": f"Squirrels configuration error: {str(exc)}"})
        
        @app.exception_handler(NotImplementedError)
        async def not_implemented_error_handler(request: Request, exc: u.InvalidInputError):
            traceback.print_exc()
            return JSONResponse(status_code=status.HTTP_501_NOT_IMPLEMENTED, 
                                content={"message": f"Not implemented error: {str(exc)}"})
        
        # Helpers
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
            return get_versioning_request_header(headers, "squirrels-request-version")
        
        def get_response_version_header(headers: Mapping):
            return get_versioning_request_header(headers, "squirrels-response-version")
        
        def can_user_access_dataset(user: Optional[UserBase], dataset: str):
            dataset_scope = ManifestIO.obj.get_dataset_scope(dataset)
            return self.authenticator.can_user_access_scope(user, dataset_scope)

        T = TypeVar('T')
        
        def apply_dataset_api_function(
            api_function: Callable[..., T], user: Optional[UserBase], dataset: str, headers: Mapping, params: Mapping
        ) -> T:
            dataset_normalized = u.normalize_name(dataset)
            if not can_user_access_dataset(user, dataset_normalized):
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                                    detail="Could not validate credentials",
                                    headers={"WWW-Authenticate": "Bearer"})
            
            request_version = get_request_version_header(headers)
            selections = set()
            for key, val in params.items():
                if not isinstance(val, str):
                    val = json.dumps(val)
                selections.add((u.normalize_name(key), val))
            selections = frozenset(selections)
            
            return api_function(user, dataset_normalized, selections, request_version)

        # Login
        token_path = base_path + '/token'

        oauth2_scheme = OAuth2PasswordBearer(tokenUrl=token_path, auto_error=False)

        @app.post(token_path)
        async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
            user: Optional[UserBase] = self.authenticator.authenticate_user(form_data.username, form_data.password)
            if not user:
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, 
                                    detail="Incorrect username or password",
                                    headers={"WWW-Authenticate": "Bearer"})
            access_token, expiry = self.authenticator.create_access_token(user)
            return {
                "access_token": access_token, 
                "token_type": "bearer", 
                "username": user.username,
                "expiry_time": expiry
            }
        
        async def get_current_user(token: str = Depends(oauth2_scheme)) -> Optional[UserBase]:
            user = self.authenticator.get_user_from_token(token)
            return user

        # Parameters API
        parameters_path = base_path + '/{dataset}/parameters'
        
        parameters_cache_size = ManifestIO.obj.get_setting(c.PARAMETERS_CACHE_SIZE_SETTING, 1024)
        parameters_cache_ttl = ManifestIO.obj.get_setting(c.PARAMETERS_CACHE_TTL_SETTING, 0)
    
        def get_parameters_helper(
            user: Optional[UserBase], dataset: str, selections: Iterable[Tuple[str, str]], request_version: Optional[int]
        ) -> ParameterSet:
            if len(selections) > 1:
                raise u.InvalidInputError(f"The /parameters endpoint takes at most 1 query parameter. Got {selections}")
            renderer = self.renderers[dataset]
            parameters = renderer.apply_selections(user, dict(selections), request_version=request_version, updates_only = True)
            return parameters

        @ttl_cache(maxsize=parameters_cache_size, ttl=parameters_cache_ttl*60)
        def get_parameters_cachable(*args):
            return get_parameters_helper(*args)
        
        def get_parameters_definition(dataset: str, user: Optional[UserBase], headers: Mapping, params: Mapping):
            api_function = get_parameters_helper if self.no_cache else get_parameters_cachable
            result = apply_dataset_api_function(api_function, user, dataset, headers, params)
            response_version = get_response_version_header(headers)
            return result.to_json_dict0()
        
        @app.get(parameters_path, response_class=JSONResponse)
        async def get_parameters(dataset: str, request: Request, user: Optional[UserBase] = Depends(get_current_user)):
            start = time.time()
            result = get_parameters_definition(dataset, user, request.headers, request.query_params)
            timer.add_activity_time("GET REQUEST total time for PARAMETERS", start)
            return result

        @app.post(parameters_path, response_class=JSONResponse)
        async def get_parameters_with_post(dataset: str, request: Request, user: Optional[UserBase] = Depends(get_current_user)):
            start = time.time()
            request_body = await request.json()
            result = get_parameters_definition(dataset, user, request.headers, request_body)
            timer.add_activity_time("POST REQUEST total time for PARAMETERS", start)
            return result

        # Results API
        results_path = base_path + '/{dataset}'

        results_cache_size = ManifestIO.obj.get_setting(c.RESULTS_CACHE_SIZE_SETTING, 128)
        results_cache_ttl = ManifestIO.obj.get_setting(c.RESULTS_CACHE_TTL_SETTING, 0)
    
        def get_results_helper(
            user: Optional[UserBase], dataset: str, selections: Iterable[Tuple[str, str]], request_version: Optional[int]
        ) -> pd.DataFrame:
            renderer = self.renderers[dataset]
            _, _, _, _, df = renderer.load_results(user, dict(selections), request_version=request_version)
            return df

        @ttl_cache(maxsize=results_cache_size, ttl=results_cache_ttl*60)
        def get_results_cachable(*args):
            return get_results_helper(*args)
        
        def get_results_definition(dataset: str, user: Optional[UserBase], headers: Mapping, params: Mapping):
            api_function = get_results_helper if self.no_cache else get_results_cachable
            result = apply_dataset_api_function(api_function, user, dataset, headers, params)
            response_version = get_response_version_header(headers)
            return u.df_to_json0(result)
        
        @app.get(results_path, response_class=JSONResponse)
        async def get_results(dataset: str, request: Request, user: Optional[UserBase] = Depends(get_current_user)):
            start = time.time()
            result = get_results_definition(dataset, user, request.headers, request.query_params)
            timer.add_activity_time("GET REQUEST total time for DATASET", start)
            return result
        
        @app.post(results_path, response_class=JSONResponse)
        async def get_results_with_post(dataset: str, request: Request, user: Optional[UserBase] = Depends(get_current_user)):
            start = time.time()
            request_body = await request.json()
            result = get_results_definition(dataset, user, request.headers, request_body)
            timer.add_activity_time("POST REQUEST total time for DATASET", start)
            return result
        
        # Catalog API
        def get_catalog0(user: Optional[UserBase]):
            datasets_info = []
            for dataset in ManifestIO.obj.get_all_dataset_names():
                if can_user_access_dataset(user, dataset):
                    dataset_normalized = u.normalize_name_for_api(dataset)
                    datasets_info.append({
                        'name': dataset,
                        'label': ManifestIO.obj.get_dataset_label(dataset),
                        'parameters_path': parameters_path.format(dataset=dataset_normalized),
                        'result_path': results_path.format(dataset=dataset_normalized),
                        'first_minor_version': 0
                    })
            
            project_vars = ManifestIO.obj.get_proj_vars()
            product_name = project_vars[c.PRODUCT_KEY]
            product_label = project_vars.get(c.PRODUCT_LABEL_KEY, product_name)
            return {
                'products': [{
                    'name': product_name,
                    'label': product_label,
                    'versions': [{
                        'major_version': project_vars[c.MAJOR_VERSION_KEY],
                        'latest_minor_version': project_vars[c.MINOR_VERSION_KEY],
                        'datasets': datasets_info
                    }]
                }]
            }
        
        @app.get(squirrels_version_path, response_class=JSONResponse)
        async def get_catalog(request: Request, user: Optional[UserBase] = Depends(get_current_user)):
            response_version = get_response_version_header(request.headers)
            return get_catalog0(user)
        
        # Squirrels UI
        @app.get('/', response_class=HTMLResponse)
        async def get_ui(request: Request):
            return templates.TemplateResponse('index.html', {
                'request': request, 'catalog_path': squirrels_version_path, 'token_path': token_path
            })
        
        # Run API server
        import uvicorn
        timer.add_activity_time("starting api server", start)
        uvicorn.run(app, host=uvicorn_args.host, port=uvicorn_args.port)
