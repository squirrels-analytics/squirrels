from typing import Dict, List, Tuple, Set, Optional
from fastapi import Depends, FastAPI, Request, HTTPException, status
from fastapi.datastructures import QueryParams
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from cachetools.func import ttl_cache
import os, traceback

from squirrels import _constants as c, _utils
from squirrels._version import sq_major_version
from squirrels._manifest import Manifest
from squirrels.connection_set import ConnectionSet
from squirrels._renderer import RendererIOWrapper, Renderer
from squirrels._auth import UserBase, Authenticator


class ApiServer:
    def __init__(self, manifest: Manifest, conn_set: ConnectionSet, no_cache: bool, debug: bool) -> None:
        """
        Constructor for ApiServer

        Parameters:
            manifest (Manifest): Manifest object produced from squirrels.yaml
            conn_set (ConnectionSet): Set of all connection pools defined in connections.py
            no_cache (bool): Whether to disable caching
            debug (bool): Set to True to show "hidden" parameters in the /parameters endpoint response
        """
        self.manifest = manifest
        self.conn_set = conn_set
        self.no_cache = no_cache
        self.debug = debug
        
        self.datasets = manifest.get_all_dataset_names()
        self.renderers: Dict[str, Renderer] = {}
        for dataset in self.datasets:
            rendererIO = RendererIOWrapper(dataset, manifest, conn_set)
            self.renderers[dataset] = rendererIO.renderer
        
        token_expiry_minutes = self.manifest.get_setting(c.AUTH_TOKEN_EXPIRE_SETTING, 30)
        self.authenticator = Authenticator(token_expiry_minutes)
        
    def _get_parameters_helper(self, user: Optional[UserBase], dataset: str, query_params: Set[Tuple[str, str]]) -> Dict:
        if len(query_params) > 1:
            raise _utils.InvalidInputError("The /parameters endpoint takes at most 1 query parameter")
        renderer = self.renderers[dataset]
        parameters = renderer.apply_selections(dict(query_params), updates_only = True)
        return parameters.to_json_dict(self.debug)
    
    def _get_results_helper(self, user: Optional[UserBase], dataset: str, query_params: Set[Tuple[str, str]]) -> Dict:
        renderer = self.renderers[dataset]
        _, _, _, _, df = renderer.load_results(user, dict(query_params))
        return _utils.df_to_json(df)
    
    def _can_user_access_dataset(self, user: Optional[UserBase], dataset: str):
        dataset_scope = self.manifest.get_dataset_scope(dataset)
        return self.authenticator.can_user_access_scope(user, dataset_scope)

    def _apply_api_function(self, api_function):
        try:
            return api_function()
        except _utils.InvalidInputError as e:
            traceback.print_exc()
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, 
                                detail="Invalid User Input: "+str(e)) from e
        except _utils.ConfigurationError as e:
            traceback.print_exc()
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                                detail="Squirrels Configuration Error: "+str(e)) from e
        except Exception as e:
            traceback.print_exc()
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                                detail="Squirrels Framework Error: "+str(e)) from e
    
    def _apply_dataset_api_function(self, api_function, user: Optional[UserBase], dataset: str, raw_query_params: QueryParams):
        def dataset_api_function():
            dataset_normalized = _utils.normalize_name(dataset)
            if not self._can_user_access_dataset(user, dataset_normalized):
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                                    detail="Could not validate credentials",
                                    headers={"WWW-Authenticate": "Bearer"})
            query_params = set()
            for key, val in raw_query_params.items():
                query_params.add((_utils.normalize_name(key), val))
            query_params = frozenset(query_params)
            
            return api_function(user, dataset_normalized, query_params)
        
        return self._apply_api_function(dataset_api_function)
    
    def run(self, uvicorn_args: List[str]) -> None:
        """
        Runs the API server with uvicorn for CLI "squirrels run"

        Parameters:
            uvicorn_args (List[str]): List of arguments to pass to uvicorn.run. Currently only supports "host" and "port"
        """
        app = FastAPI()

        squirrels_version_path = f'/squirrels-v{sq_major_version}'
        partial_base_path = f'/{self.manifest.get_product()}/v{self.manifest.get_major_version()}'
        base_path = squirrels_version_path + _utils.normalize_name_for_api(partial_base_path)

        static_dir = _utils.join_paths(os.path.dirname(__file__), 'package_data', 'static')
        app.mount('/static', StaticFiles(directory=static_dir), name='static')

        templates_dir = _utils.join_paths(os.path.dirname(__file__), 'package_data', 'templates')
        templates = Jinja2Templates(directory=templates_dir)

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
        
        parameters_cache_size = self.manifest.get_setting(c.PARAMETERS_CACHE_SIZE_SETTING, 1024)
        parameters_cache_ttl = self.manifest.get_setting(c.PARAMETERS_CACHE_TTL_SETTING, 0)

        @ttl_cache(maxsize=parameters_cache_size, ttl=parameters_cache_ttl)
        def get_parameters_cachable(*args):
            return self._get_parameters_helper(*args)
        
        @app.get(parameters_path, response_class=JSONResponse)
        async def get_parameters(dataset: str, request: Request, user: Optional[UserBase] = Depends(get_current_user)):
            api_function = self._get_parameters_helper if self.no_cache else get_parameters_cachable
            return self._apply_dataset_api_function(api_function, user, dataset, request.query_params)

        # Results API
        results_path = base_path + '/{dataset}'

        results_cache_size = self.manifest.get_setting(c.RESULTS_CACHE_SIZE_SETTING, 128)
        results_cache_ttl = self.manifest.get_setting(c.RESULTS_CACHE_TTL_SETTING, 0)

        @ttl_cache(maxsize=results_cache_size, ttl=results_cache_ttl)
        def get_results_cachable(*args):
            return self._get_results_helper(*args)
        
        @app.get(results_path, response_class=JSONResponse)
        async def get_results(dataset: str, request: Request, user: Optional[UserBase] = Depends(get_current_user)):
            api_function = self._get_results_helper if self.no_cache else get_results_cachable
            return self._apply_dataset_api_function(api_function, user, dataset, request.query_params)
        
        # Catalog API
        @app.get(squirrels_version_path, response_class=JSONResponse)
        async def get_catalog(user: Optional[UserBase] = Depends(get_current_user)):
            def api_function():
                datasets_info = []
                for dataset in self.manifest.get_all_dataset_names():
                    if self._can_user_access_dataset(user, dataset):
                        dataset_normalized = _utils.normalize_name_for_api(dataset)
                        datasets_info.append({
                            'name': dataset,
                            'label': self.manifest.get_dataset_label(dataset),
                            'parameters_path': parameters_path.format(dataset=dataset_normalized),
                            'result_path': results_path.format(dataset=dataset_normalized),
                            'first_minor_version': 0
                        })
                
                project_vars = self.manifest.get_proj_vars()
                return {
                    'response_version': 0,
                    'products': [{
                        'name': project_vars[c.PRODUCT_KEY],
                        'label': project_vars[c.PRODUCT_LABEL_KEY],
                        'versions': [{
                            'major_version': project_vars[c.MAJOR_VERSION_KEY],
                            'latest_minor_version': project_vars[c.MINOR_VERSION_KEY],
                            'datasets': datasets_info
                        }]
                    }]
                }
            return self._apply_api_function(api_function)
        
        # Squirrels UI
        @app.get('/', response_class=HTMLResponse)
        async def get_ui(request: Request):
            return templates.TemplateResponse('index.html', {
                'request': request, 'catalog_path': squirrels_version_path, 'token_path': token_path
            })
        
        # Run API server
        import uvicorn
        uvicorn.run(app, host=uvicorn_args.host, port=uvicorn_args.port)
