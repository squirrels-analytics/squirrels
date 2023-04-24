from typing import Dict, List, Tuple, Set
from fastapi import FastAPI, Request
from fastapi.datastructures import QueryParams
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from cachetools.func import ttl_cache
import os, json

from squirrels import major_version, constants as c, utils
from squirrels.manifest import Manifest
from squirrels.connection_set import ConnectionSet
from squirrels.renderer import RendererIOWrapper, Renderer


class ApiServer:
    def __init__(self, manifest: Manifest, conn_set: ConnectionSet, no_cache: bool, debug: bool) -> None:
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
            raise utils.InvalidInputError("The /parameters endpoint takes at most 1 query parameter")
        renderer = self.renderers[dataset]
        parameters = renderer.apply_selections(dict(query_params), updates_only = True)
        return parameters.to_dict(self.debug)
    
    def _get_results_helper(self, dataset: str, query_params: Set[Tuple[str, str]]) -> Dict:
        renderer = self.renderers[dataset]
        _, _, _, _, df = renderer.load_results(dict(query_params))
        return json.loads(df.to_json(orient='table', index=False))
    
    def _apply_dataset_api_function(self, api_function, dataset: str, raw_query_params: QueryParams):
        dataset = utils.normalize_name(dataset)
        query_params = set()
        for key, val in raw_query_params.items():
            query_params.add((utils.normalize_name(key), val))
        query_params = frozenset(query_params)
        return api_function(dataset, query_params)
    
    def run(self, uvicorn_args: List[str]) -> None:
        app = FastAPI()

        squirrels_version_path = f'/squirrels{major_version}'
        config_base_path = utils.normalize_name_for_api(self.manifest.get_base_path())
        base_path = squirrels_version_path + config_base_path

        static_dir = utils.join_paths(os.path.dirname(__file__), 'package_data', 'static')
        app.mount('/static', StaticFiles(directory=static_dir), name='static')

        templates_dir = utils.join_paths(os.path.dirname(__file__), 'package_data', 'templates')
        templates = Jinja2Templates(directory=templates_dir)

        # Parameters API
        parameters_path = base_path + '/{dataset}/parameters'
        
        parameters_cache_size = self.manifest.get_setting(c.PARAMETERS_CACHE_SIZE_SETTING, 1024)
        parameters_cache_ttl = self.manifest.get_setting(c.PARAMETERS_CACHE_TTL_SETTING, 24*60*60)

        @ttl_cache(maxsize=parameters_cache_size, ttl=parameters_cache_ttl)
        def get_parameters_cachable(*args):
            return self._get_parameters_helper(*args)
        
        @app.get(parameters_path, response_class=JSONResponse)
        async def get_parameters(dataset: str, request: Request):
            api_function = self._get_parameters_helper if self.no_cache else get_parameters_cachable
            return self._apply_dataset_api_function(api_function, dataset, request.query_params)

        # Results API
        results_path = base_path + '/{dataset}'

        results_cache_size = self.manifest.get_setting(c.RESULTS_CACHE_SIZE_SETTING, 128)
        results_cache_ttl = self.manifest.get_setting(c.RESULTS_CACHE_TTL_SETTING, 60*60)

        @ttl_cache(maxsize=results_cache_size, ttl=results_cache_ttl)
        def get_results_cachable(*args):
            return self._get_results_helper(*args)
        
        @app.get(results_path, response_class=JSONResponse)
        async def get_results(dataset: str, request: Request):
            api_function = self._get_results_helper if self.no_cache else get_results_cachable
            return self._apply_dataset_api_function(api_function, dataset, request.query_params)
        
        # Catalog API
        @app.get(base_path, response_class=JSONResponse)
        async def get_catalog():
            datasets_info = []
            for dataset in self.datasets:
                dataset_normalized = utils.normalize_name_for_api(dataset)
                datasets_info.append({
                    'dataset': dataset,
                    'label': self.manifest.get_dataset_label(dataset),
                    'parameters_path': parameters_path.format(dataset=dataset_normalized),
                    'result_path': results_path.format(dataset=dataset_normalized)
                })
            return {'project_variables': self.manifest.get_proj_vars(), 'resource_paths': datasets_info}
        
        # Squirrels UI
        @app.get('/', response_class=HTMLResponse)
        async def get_ui(request: Request):
            return templates.TemplateResponse('index.html', {'request': request, 'base_path': base_path})
        
        # Run API server
        import uvicorn
        uvicorn.run(app, host=uvicorn_args.host, port=uvicorn_args.port)
