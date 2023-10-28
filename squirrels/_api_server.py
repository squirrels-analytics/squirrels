from typing import Dict, List, Tuple, Set
from fastapi import Depends, FastAPI, Request, HTTPException, status
from fastapi.datastructures import QueryParams
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from cachetools.func import ttl_cache
import os, traceback

from squirrels import _constants as c, _utils
from squirrels._version import major_version
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
        
    def _get_parameters_helper(self, dataset: str, query_params: Set[Tuple[str, str]]) -> Dict:
        if len(query_params) > 1:
            raise _utils.InvalidInputError("The /parameters endpoint takes at most 1 query parameter")
        renderer = self.renderers[dataset]
        parameters = renderer.apply_selections(dict(query_params), updates_only = True)
        return parameters.to_json_dict(self.debug)
    
    def _get_results_helper(self, dataset: str, query_params: Set[Tuple[str, str]]) -> Dict:
        renderer = self.renderers[dataset]
        _, _, _, _, df = renderer.load_results(dict(query_params))
        return _utils.df_to_json(df)

    def _apply_api_function(self, api_function):
        try:
            return api_function()
        except _utils.InvalidInputError as e:
            traceback.print_exc()
            raise HTTPException(status_code=400, detail="Invalid User Input: "+str(e)) from e
        except _utils.ConfigurationError as e:
            traceback.print_exc()
            raise HTTPException(status_code=500, detail="Squirrels Configuration Error: "+str(e)) from e
        except Exception as e:
            traceback.print_exc()
            raise HTTPException(status_code=500, detail="Squirrels Framework Error: "+str(e)) from e
    
    def _apply_dataset_api_function(self, api_function, dataset: str, raw_query_params: QueryParams):
        dataset = _utils.normalize_name(dataset)
        query_params = set()
        for key, val in raw_query_params.items():
            query_params.add((_utils.normalize_name(key), val))
        query_params = frozenset(query_params)
        return self._apply_api_function(lambda: api_function(dataset, query_params))
    
    def run(self, uvicorn_args: List[str]) -> None:
        """
        Runs the API server with uvicorn for CLI "squirrels run"

        Parameters:
            uvicorn_args (List[str]): List of arguments to pass to uvicorn.run. Currently only supports "host" and "port"
        """
        app = FastAPI()

        squirrels_version_path = f'/squirrels{major_version}'
        catalog_path_raw = f'{squirrels_version_path}/{self.manifest.get_product()}'
        catalog_path = _utils.normalize_name_for_api(catalog_path_raw)
        base_path = f'{catalog_path}/v{self.manifest.get_major_version()}'

        static_dir = _utils.join_paths(os.path.dirname(__file__), 'package_data', 'static')
        app.mount('/static', StaticFiles(directory=static_dir), name='static')

        templates_dir = _utils.join_paths(os.path.dirname(__file__), 'package_data', 'templates')
        templates = Jinja2Templates(directory=templates_dir)

        # Login
        auth_enabled = self.manifest.get_setting(c.AUTH_ENABLED_SETTING, False)
        if auth_enabled:
            token_expiry_minutes = self.manifest.get_setting(c.AUTH_TOKEN_EXPIRE_SETTING, 30)
            authenticator = Authenticator(token_expiry_minutes)

            @app.post('/token')
            async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
                user: UserBase = authenticator.authenticate_user(form_data.username, form_data.password)
                if not user:
                    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, 
                                        detail="Incorrect username or password",
                                        headers={"WWW-Authenticate": "Bearer"})
                access_token = authenticator.create_access_token(user)
                return {"access_token": access_token, "token_type": "bearer"}
            
            oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

            async def get_current_user(token: str = Depends(oauth2_scheme)):
                user = authenticator.get_user_from_token(token)
                if user is None:
                    raise HTTPException(
                        status_code=status.HTTP_401_UNAUTHORIZED,
                        detail="Could not validate credentials",
                        headers={"WWW-Authenticate": "Bearer"},
                    )
                return user
        else:
            async def get_current_user():
                return None

        # Parameters API
        parameters_path = base_path + '/{dataset}/parameters'
        
        parameters_cache_size = self.manifest.get_setting(c.PARAMETERS_CACHE_SIZE_SETTING, 1024)
        parameters_cache_ttl = self.manifest.get_setting(c.PARAMETERS_CACHE_TTL_SETTING, 0)

        @ttl_cache(maxsize=parameters_cache_size, ttl=parameters_cache_ttl)
        def get_parameters_cachable(*args):
            return self._get_parameters_helper(*args)
        
        @app.get(parameters_path, response_class=JSONResponse)
        async def get_parameters(dataset: str, request: Request, user: UserBase = Depends(get_current_user)):
            api_function = self._get_parameters_helper if self.no_cache else get_parameters_cachable
            return self._apply_dataset_api_function(api_function, dataset, request.query_params)

        # Results API
        results_path = base_path + '/{dataset}'

        results_cache_size = self.manifest.get_setting(c.RESULTS_CACHE_SIZE_SETTING, 128)
        results_cache_ttl = self.manifest.get_setting(c.RESULTS_CACHE_TTL_SETTING, 0)

        @ttl_cache(maxsize=results_cache_size, ttl=results_cache_ttl)
        def get_results_cachable(*args):
            return self._get_results_helper(*args)
        
        @app.get(results_path, response_class=JSONResponse)
        async def get_results(dataset: str, request: Request, user: UserBase = Depends(get_current_user)):
            api_function = self._get_results_helper if self.no_cache else get_results_cachable
            return self._apply_dataset_api_function(api_function, dataset, request.query_params)
        
        # Catalog API
        @app.get(catalog_path, response_class=JSONResponse)
        async def get_catalog(user: UserBase = Depends(get_current_user)):
            api_function = lambda: self.manifest.get_catalog(parameters_path, results_path)
            return self._apply_api_function(api_function)
        
        # Squirrels UI
        @app.get('/', response_class=HTMLResponse)
        async def get_ui(request: Request):
            return templates.TemplateResponse('index.html', {'request': request, 'catalog_path': catalog_path})
        
        # Run API server
        import uvicorn
        uvicorn.run(app, host=uvicorn_args.host, port=uvicorn_args.port)
