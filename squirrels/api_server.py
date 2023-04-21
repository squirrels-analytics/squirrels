import json, os
from typing import Dict, List, FrozenSet, Tuple
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from cachetools.func import ttl_cache

from squirrels import major_version, constants as c, manifest as mf
from squirrels.renderer import Renderer

debug = False


def normalize_name(name: str):
    return name.replace('-', '_')

def normalize_name_for_api(name: str):
    return name.replace('_', '-')


def load_selected_parameters(renderer: Renderer, query_params: FrozenSet[Tuple[str, str]]): # -> ParameterSet:
    parameters = renderer.parameters
    query_params_dict = dict(query_params)
    for name, parameter in parameters._parameters_dict.items():
        parameter.refresh(parameters)
        selected_value = query_params_dict.get(name)
        if selected_value is not None:
            parameter.set_selection(selected_value)
    return parameters


def load_dataframe(renderer: Renderer):
    renderer.set_job_context()
    sql_by_view_name = renderer.get_rendered_sql_by_view()
    
    dataset_context = mf.get_dataset_parms(renderer.dataset)
    final_view_name = dataset_context[c.FINAL_VIEW_KEY]
    final_view_sql_str = renderer.get_final_view_sql_str(final_view_name, sql_by_view_name)
    
    _, df = renderer.get_all_results(sql_by_view_name, final_view_name, final_view_sql_str)
    return df


def template_function(dataset: str, request: Request, helper_func):
    dataset = normalize_name(dataset)
    query_params = frozenset((normalize_name(key), val) for key, val in request.query_params.items())
    return helper_func(dataset, query_params)


# Helper functions for "parameters" api
def get_parameters_helper(dataset: str, query_params: FrozenSet[Tuple[str, str]]):
    renderer = Renderer(dataset)
    parameters = load_selected_parameters(renderer, query_params)
    return parameters._to_dict(debug)
    

# Helper functions for "get_results" api
def get_results_helper(dataset: str, query_params: FrozenSet[Tuple[str, str]]):
    renderer = Renderer(dataset)
    load_selected_parameters(renderer, query_params)
    df = load_dataframe(renderer)
    return json.loads(df.to_json(orient='table', index=False))
        
        
def run(no_cache: bool, debug_value: bool, uvicorn_args: List[str]):
    global debug
    debug = debug_value

    app = FastAPI()

    mf.initialize(c.MANIFEST_FILE)
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
        helper_func = get_parameters_helper if no_cache else get_parameters_cachable
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
        helper_func = get_results_helper if no_cache else get_results_cachable
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
