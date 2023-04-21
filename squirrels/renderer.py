from typing import Dict, Optional, Callable, Any
from functools import partial
import concurrent.futures

from squirrels import manifest as mf
from squirrels.connection_set import ConnectionSet
from squirrels.configs.data_sources import DataSource
from squirrels.configs.parameter_set import ParameterSet
from squirrels.utils import ConfigurationError
from squirrels.timed_imports import pandas as pd, sqldf, jinja2 as j2

ContextFunc = Optional[Callable[[ParameterSet], Dict]]
DatabaseViews = Optional[Dict[str, pd.DataFrame]]

# TODO: add unit tests
class Renderer:
    def __init__(self, dataset: str, manifest: mf.Manifest, conn_set: ConnectionSet, raw_param_set: ParameterSet,
                 context_func: ContextFunc = None, excel_file: Optional[pd.ExcelFile] = None):
        self.dataset = dataset
        self.manifest = manifest
        self.conn_set = conn_set
        self.param_set: ParameterSet = self._render_param_set(raw_param_set, excel_file)
        self.context: Dict[str, Any] = self._render_context(context_func)
    
    def _render_param_set(self, param_set: ParameterSet, excel_file: Optional[pd.ExcelFile] = None) -> ParameterSet:
        datasources = param_set.get_datasources()
        if excel_file is not None:
            df_dict = pd.read_excel(excel_file, None)
            for key in datasources:
                if key not in df_dict:
                    raise ConfigurationError('No sheet found for parameter "{key}" in the Excel workbook')
        else:
            default_db_conn = self.manifest.get_default_db_connection()
            if default_db_conn is None and len(datasources) > 0:
                raise ConfigurationError("A default db_connection must be provided when using datasource parameters")

            def get_dataframe_from_query(key: str, datasource: DataSource) -> pd.DataFrame:
                return key, self.conn_set.get_dataframe_from_query(default_db_conn, datasource.get_query())
            
            with concurrent.futures.ThreadPoolExecutor() as executor:
                df_dict = dict(executor.map(get_dataframe_from_query, datasources.items()))
        
        param_set.convert_datasource_params(df_dict)
        return param_set
    
    def _render_context(self, context_func: ContextFunc):
        return context_func(self.param_set) if context_func is not None else {}
    
    def _get_args(self):
        return {
            'prms': self.param_set,
            'ctx':  self.context,
            'proj': self.manifest.get_proj_vars()
        }
    
    def render_sql_template(self, sql_template: str) -> str:
        env = j2.Environment(loader=j2.FileSystemLoader('.'))
        template = env.from_string(sql_template)
        args = self._get_args()
        return template.render(args)
    
    def render_dataframe_from_sql(self, db_view_name: str, sql_str: str, 
                                  database_views: DatabaseViews = None) -> pd.DataFrame:
        if database_views is not None:
            return sqldf(sql_str, env=database_views)
        else:
            conn_name = self.manifest.get_database_view_db_connection(self.dataset, db_view_name)
            return self.conn_set.get_dataframe_from_query(conn_name, sql_str)

    def render_dataframe_from_py_func(self, py_func: Callable[[Any], pd.DataFrame], 
                                      database_views: DatabaseViews = None) -> pd.DataFrame:
        args = self._get_args()
        partial_func = partial(py_func, **args)
        if database_views is None:
            return partial_func()
        else:
            return partial_func(database_views)


class RendererIOWrapper:
    pass