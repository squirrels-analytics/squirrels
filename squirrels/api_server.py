from typing import Dict, List
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from cachetools.func import ttl_cache
import os

from squirrels import major_version, constants as c, manifest as mf


class ApiServer:
    def __init__(self, manifest: mf.Manifest, no_cache: bool, debug: bool) -> None:
        self.manifest = manifest
        self.debug = debug
        self.no_cache = no_cache
    
    def run(self, uvicorn_args: List[str]) -> None:
        app = FastAPI()

        squirrels_version_path = f'/squirrels{major_version}'
        config_base_path = normalize_name_for_api(mf.parms[c.BASE_PATH_KEY])
        base_path = squirrels_version_path + config_base_path

        static_dir = os.path.join(os.path.dirname(__file__), 'static')
        app.mount('/static', StaticFiles(directory=static_dir), name='static')

        templates_dir = os.path.join(os.path.dirname(__file__), 'templates')
        templates = Jinja2Templates(directory=templates_dir)

        # Parameters API
        parameters_path = '/{dataset}/parameters'
        
        parameters_cache_size = mf.get_setting(c.PARAMETERS_CACHE_SIZE_SETTING, 1024)
        parameters_cache_ttl = mf.get_setting(c.PARAMETERS_CACHE_TTL_SETTING, 24*60*60)

        @ttl_cache(maxsize=parameters_cache_size, ttl=parameters_cache_ttl)
        def get_parameters_cachable(*args):
            return get_parameters_helper(*args)
        
        @app.get(base_path + parameters_path, response_class=JSONResponse)
        async def get_parameters(dataset: str, request: Request):
            helper_func = get_parameters_helper if self.no_cache else get_parameters_cachable
            return template_function(dataset, request, helper_func)

        # Results API
        results_path = '/{dataset}'

        results_cache_size = mf.get_setting(c.RESULTS_CACHE_SIZE_SETTING, 128)
        results_cache_ttl = mf.get_setting(c.RESULTS_CACHE_TTL_SETTING, 60*60)

        @ttl_cache(maxsize=results_cache_size, ttl=results_cache_ttl)
        def get_results_cachable(*args):
            return get_results_helper(*args)
        
        @app.get(base_path + results_path, response_class=JSONResponse)
        async def get_results(dataset: str, request: Request):
            helper_func = get_results_helper if self.no_cache else get_results_cachable
            return template_function(dataset, request, helper_func)
        
        # Catalog API
        @app.get(base_path, response_class=JSONResponse)
        async def get_catalog():
            all_dataset_contexts: Dict = mf.parms[c.DATASETS_KEY]
            datasets = []
            for dataset, dataset_context in all_dataset_contexts.items():
                dataset_normalized = normalize_name_for_api(dataset)
                datasets.append({
                    'dataset': dataset,
                    'label': dataset_context[c.DATASET_LABEL_KEY],
                    'parameters_path': base_path + parameters_path.format(dataset=dataset_normalized),
                    'result_path': base_path + results_path.format(dataset=dataset_normalized)
                })
            return {'project_variables': mf.parms[c.PROJ_VARS_KEY], 'resource_paths': datasets}
        
        # Squirrels UI
        @app.get('/', response_class=HTMLResponse)
        async def get_ui(request: Request):
            return templates.TemplateResponse('index.html', {'request': request, 'base_path': base_path})
        
        # Run API server
        import uvicorn
        uvicorn.run(app, host=uvicorn_args.host, port=uvicorn_args.port)
