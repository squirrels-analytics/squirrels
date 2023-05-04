from typing import Dict, Tuple, Optional, Union, Callable, Any
from functools import partial
from configparser import ConfigParser
import concurrent.futures, os, json

from squirrels import manifest as mf, utils, constants as c
from squirrels.connection_set import ConnectionSet, sqldf
from squirrels.param_configs.data_sources import DataSource
from squirrels.param_configs.parameter_set import ParameterSet
from squirrels.utils import ConfigurationError
from squirrels.timed_imports import pandas as pd

ContextFunc = Optional[Callable[..., Dict[str, Any]]]
DatabaseViews = Optional[Dict[str, pd.DataFrame]]
Query = Union[Callable[..., pd.DataFrame], str]


class Renderer:
    def __init__(self, dataset: str, manifest: mf.Manifest, conn_set: ConnectionSet, raw_param_set: ParameterSet, 
                 context_func: Callable[..., Dict[str, Any]], raw_query_by_db_view: Dict[str, Query], 
                 raw_final_view_query: Query, excel_file: Optional[pd.ExcelFile] = None):
        self.dataset = dataset
        self.manifest = manifest
        self.conn_set = conn_set
        self.param_set: ParameterSet = self._convert_param_set_datasources(raw_param_set, excel_file)
        self.context_func = context_func
        self.raw_query_by_db_view = raw_query_by_db_view
        self.raw_final_view_query = raw_final_view_query
    
    def _convert_param_set_datasources(self, param_set: ParameterSet, excel_file: Optional[pd.ExcelFile] = None) -> ParameterSet:
        datasources = param_set.get_datasources()
        if excel_file is not None:
            df_dict = pd.read_excel(excel_file, None)
            for key in datasources:
                if key not in df_dict:
                    raise ConfigurationError('No sheet found for parameter "{key}" in the Excel workbook')
        else:
            def get_dataframe_from_query(item: Tuple[str, DataSource]) -> pd.DataFrame:
                key, datasource = item
                df = self.conn_set.get_dataframe_from_query(datasource.connection_name, datasource.get_query())
                return key, df
            
            with concurrent.futures.ThreadPoolExecutor() as executor:
                df_dict = dict(executor.map(get_dataframe_from_query, datasources.items()))
        
        param_set.convert_datasource_params(df_dict)
        return param_set
    
    def apply_selections(self, selections: Dict[str, str], updates_only: bool = False) -> ParameterSet:
        parameter_set = self.param_set
        parameters_dict = parameter_set.get_parameters_as_ordered_dict()
        
        # iterating through parameters dict instead of query_params since order matters for cascading parameters
        for param_name, parameter in parameters_dict.items():
            if param_name in selections:
                value = selections[param_name]
                parameter = parameter_set[param_name].with_selection(value)
                updates = parameter.get_all_dependent_params()
                if updates_only:
                    return updates
                parameter_set = parameter_set.merge(updates)
        return parameter_set

    def _render_context(self, context_func: ContextFunc, param_set: ParameterSet) -> Dict[str, Any]:
        try:
            return context_func(param_set, self.manifest.get_proj_vars()) if context_func is not None else {}
        except Exception as e:
            raise ConfigurationError(f'Error in the {c.CONTEXT_FILE} function for dataset "{self.dataset}"') from e
    
    def _get_args(self, param_set: ParameterSet, context: Dict[str, Any]) -> Dict:
        return {
            'prms': param_set,
            'ctx':  context,
            'proj': self.manifest.get_proj_vars()
        }
    
    def _render_query_from_raw(self, raw_query: Query, args: Dict) -> Query:
        if isinstance(raw_query, str):
            template = utils.j2_env.from_string(raw_query)
            return template.render(args)
        else:
            return partial(raw_query, **args)
    
    def _render_dataframe_from_sql(self, db_view_name: str, sql_str: str, 
                                   database_views: DatabaseViews = None) -> pd.DataFrame:
        if database_views is not None:
            return sqldf(sql_str, database_views)
        else:
            conn_name = self.manifest.get_database_view_db_connection(self.dataset, db_view_name)
            return self.conn_set.get_dataframe_from_query(conn_name, sql_str)

    def _render_dataframe_from_py_func(self, db_view_name: str, py_func: Callable[[Any], pd.DataFrame], 
                                       database_views: DatabaseViews = None) -> pd.DataFrame:
        if database_views is not None:
            try:
                return py_func(database_views=database_views)
            except Exception as e:
                raise ConfigurationError(f'Error in the final view python function for dataset "{self.dataset}"') from e
        else:
            conn_name = self.manifest.get_database_view_db_connection(self.dataset, db_view_name)
            connection_pool = self.conn_set.get_connection_pool(conn_name)
            try:
                return py_func(connection_pool=connection_pool, connection_set=self.conn_set)
            except Exception as e:
                raise ConfigurationError(f'Error in the python function for database view "{db_view_name}" in dataset "{self.dataset}"') from e
    
    def _render_db_view_dataframes(self, query_by_db_view: Dict[str, Query]) -> Dict[str, pd.DataFrame]:
        def run_single_query(item: Tuple[str, Query]) -> Tuple[str, pd.DataFrame]:
            view_name, query = item
            if isinstance(query, str):
                return view_name, self._render_dataframe_from_sql(view_name, query)
            else:
                return view_name, self._render_dataframe_from_py_func(view_name, query)
        
        with concurrent.futures.ThreadPoolExecutor() as executor:
            df_by_view_name = executor.map(run_single_query, query_by_db_view.items())
        
        return dict(df_by_view_name)
    
    def _render_final_view_dataframe(self, df_by_db_views: Dict[str, pd.DataFrame], 
                                    final_view_query: Optional[Query]) -> pd.DataFrame:
        if final_view_query in df_by_db_views:
            return df_by_db_views[final_view_query]
        elif isinstance(final_view_query, str):
            return self._render_dataframe_from_sql("final_view", final_view_query, df_by_db_views)
        else:
            return self._render_dataframe_from_py_func("final_view", final_view_query, df_by_db_views)

    def load_results(self, selections: Dict[str, str], run_query: bool = True) \
        -> Tuple[ParameterSet, Dict[str, Query], Query, Dict[str, pd.DataFrame], Optional[pd.DataFrame]]:
        
        # apply selections and render context
        param_set = self.apply_selections(selections)
        context = self._render_context(self.context_func, param_set)

        # render database view queries
        query_by_db_view = {}
        args = self._get_args(param_set, context)
        for db_view, raw_query in self.raw_query_by_db_view.items():
            query_by_db_view[db_view] = self._render_query_from_raw(raw_query, args)
        
        # render final view query
        final_view_query = self._render_query_from_raw(self.raw_final_view_query, args)

        # render all dataframes if "run_query" is enabled
        df_by_db_views = {}
        final_view_df = None
        if run_query:
            df_by_db_views = self._render_db_view_dataframes(query_by_db_view)
            final_view_df = self._render_final_view_dataframe(df_by_db_views, final_view_query)
        
        return param_set, query_by_db_view, final_view_query, df_by_db_views, final_view_df


class RendererIOWrapper:
    def __init__(self, dataset: str, manifest: mf.Manifest, conn_set: ConnectionSet, excel_file_name: Optional[str] = None):
        dataset_folder = manifest.get_dataset_folder(dataset)
        parameters_path = utils.join_paths(dataset_folder, c.PARAMETERS_FILE)
        parameters_module = utils.import_file_as_module(parameters_path)
        try:
            parameter_set = parameters_module.main()
        except Exception as e:
            raise ConfigurationError(f'Error in the {c.PARAMETERS_FILE} function for dataset "{dataset}"') from e

        context_path = utils.join_paths(dataset_folder, c.CONTEXT_FILE)
        context_func = utils.import_file_as_module(context_path).main
        
        excel_file = None
        if excel_file_name is not None:
            excel_file_path = utils.join_paths(dataset_folder, excel_file_name)
            excel_file = pd.ExcelFile(excel_file_path)
        
        db_views = manifest.get_all_database_view_names(dataset)
        raw_query_by_db_view = {}
        for db_view in db_views:
            db_view_template_path = str(manifest.get_database_view_file(dataset, db_view))
            raw_query_by_db_view[db_view] = self._get_raw_query(db_view_template_path)
        
        final_view_path = str(manifest.get_dataset_final_view(dataset))
        if final_view_path in db_views:
            raw_final_view_query = final_view_path
        else:
            raw_final_view_query = self._get_raw_query(final_view_path)
        
        self.dataset_folder = dataset_folder
        self.output_folder = utils.join_paths(c.OUTPUTS_FOLDER, dataset)
        self.renderer = Renderer(dataset, manifest, conn_set, parameter_set, context_func,
                                 raw_query_by_db_view, raw_final_view_query, excel_file)
    
    def _get_raw_query(self, template_path: str) -> Dict[str, Query]:
        if template_path.endswith(".py"):
            return utils.import_file_as_module(template_path).main
        else:
            with open(template_path, 'r') as f:
                sql_template = f.read()
            return sql_template

    def _get_selections(self, selection_cfg_file: Optional[str]) -> Dict[str, str]:
        if selection_cfg_file is not None:
            selection_cfg_path = utils.join_paths(self.dataset_folder, selection_cfg_file)
            config = ConfigParser()
            config.read(selection_cfg_path)
            if config.has_section(c.PARAMETERS_SECTION):
                config_section = config[c.PARAMETERS_SECTION]
                return dict(config_section.items())
        return {}

    def _write_sql_file(self, view_name: str, query: Any):
        if isinstance(query, str):
            db_view_sql_output_path = utils.join_paths(self.output_folder, view_name+'.sql')
            with open(db_view_sql_output_path, 'w') as f:
                f.write(query)
    
    def write_outputs(self, selection_cfg_file: Optional[str], run_query: bool) -> None:
        # create output folder if it doesn't exist
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)
        
        # clear everything in output folder
        files = os.listdir(self.output_folder)
        for file in files:
            file_path = utils.join_paths(self.output_folder, file)
            os.remove(file_path)
        
        # apply selections and render outputs
        selections = self._get_selections(selection_cfg_file)
        result = self.renderer.load_results(selections, run_query)
        param_set, query_by_db_view, final_view_query, df_by_db_views, final_view_df = result
        
        # write the parameters response
        param_set_dict = param_set.to_dict()
        parameter_json_output_path = utils.join_paths(self.output_folder, c.PARAMETERS_OUTPUT)
        with open(parameter_json_output_path, 'w') as f:
            json.dump(param_set_dict, f, indent=4)
        
        # write the rendered sql queries for database views
        for db_view, query in query_by_db_view.items():
            self._write_sql_file(db_view, query)

        # write the rendered sql query for final view
        if final_view_query not in query_by_db_view:
            self._write_sql_file(c.FINAL_VIEW_OUT_STEM, final_view_query)
        
        # Run the sql queries and write output
        if run_query:
            for db_view, df in df_by_db_views.items():
                csv_file = utils.join_paths(self.output_folder, db_view+'.csv')
                df.to_csv(csv_file, index=False)
            
            final_csv_path = utils.join_paths(self.output_folder, c.FINAL_VIEW_OUT_STEM+'.csv')
            final_view_df.to_csv(final_csv_path, index=False)

            final_json_path = utils.join_paths(self.output_folder, c.FINAL_VIEW_OUT_STEM+'.json')
            final_view_df.to_json(final_json_path, orient='table', index=False, indent=4)
