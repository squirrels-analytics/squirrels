from typing import Dict, Tuple, Optional, Union, Callable, Any
from functools import partial
from configparser import ConfigParser
import concurrent.futures, os, json, pandas as pd

from . import _constants as c, _utils as u, Parameter, sqldf
from ._manifest import ManifestIO
from ._connection_set import ConnectionSetIO
from ._parameter_sets import ParameterConfigsSetIO
from ._parameter_sets import ParameterSet
from ._timer import timer, time
from ._authenticator import UserBase, Authenticator

ContextFunc = Optional[Callable[..., Dict[str, Any]]]
DatabaseViews = Optional[Dict[str, pd.DataFrame]]
Query = Union[Callable[..., pd.DataFrame], str]


class Renderer:
    def __init__(
        self, dataset: str, context_func: Callable[..., Dict[str, Any]], raw_query_by_db_view: Dict[str, Query], raw_final_view_query: Query
    ) -> None:
        self.dataset = dataset
        self.context_func = context_func
        self.raw_query_by_db_view = raw_query_by_db_view
        self.raw_final_view_query = raw_final_view_query
    
    def apply_selections(
        self, user: Optional[UserBase], selections: Dict[str, str], *, updates_only: bool = False, request_version: Optional[int] = None
    ) -> ParameterSet:
        start = time.time()
        dataset_params = ManifestIO.obj.get_dataset_parameters(self.dataset)
        parameter_set = ParameterConfigsSetIO.obj.apply_selections(dataset_params, selections, user, updates_only=updates_only, 
                                                                   request_version=request_version)
        timer.add_activity_time(f"applying selections - dataset {self.dataset}", start)
        return parameter_set

    def _render_context(self, context_func: ContextFunc, user: Optional[UserBase], prms: Dict[str, Parameter]) -> Dict[str, Any]:
        context = {}
        try:
            context_func(ctx=context, user=user, prms=prms)
        except Exception as e:
            raise u.ConfigurationError(f'Error in the {c.CONTEXT_FILE} function for dataset "{self.dataset}"') from e
        return context
    
    def _render_query_from_raw(self, raw_query: Query, kwargs: Dict) -> Query:
        if isinstance(raw_query, str):
            return u.render_string(raw_query, kwargs)
        else:
            return partial(raw_query, **kwargs)
    
    def _render_dataframe_from_sql(
        self, db_view_name: str, sql_str: str, database_views: DatabaseViews = None
    ) -> pd.DataFrame:
        if database_views is not None:
            return sqldf(sql_str, database_views)
        else:
            conn_name = ManifestIO.obj.get_database_view_db_connection(self.dataset, db_view_name)
            return ConnectionSetIO.obj.run_sql_query_from_conn_name(sql_str, conn_name)

    def _render_dataframe_from_py_func(
        self, db_view_name: str, py_func: Callable[[Any], pd.DataFrame], database_views: DatabaseViews = None
    ) -> pd.DataFrame:
        if database_views is not None:
            try:
                return py_func(database_views=database_views)
            except Exception as e:
                raise u.ConfigurationError(f'Error in the final view python function for dataset "{self.dataset}"') from e
        else:
            conn_name = ManifestIO.obj.get_database_view_db_connection(self.dataset, db_view_name)
            connection_pool = ConnectionSetIO.obj.get_connection_pool(conn_name)
            try:
                return py_func(connection_pool=connection_pool)
            except Exception as e:
                raise u.ConfigurationError(f'Error in the python function for database view "{db_view_name}" in dataset "{self.dataset}"') from e
    
    def _create_db_view_dataframes(self, query_by_db_view: Dict[str, Query]) -> Dict[str, pd.DataFrame]:
        def run_single_query(item: Tuple[str, Query]) -> Tuple[str, pd.DataFrame]:
            view_name, query = item
            if isinstance(query, str):
                return view_name, self._render_dataframe_from_sql(view_name, query)
            else:
                return view_name, self._render_dataframe_from_py_func(view_name, query)
        
        with concurrent.futures.ThreadPoolExecutor() as executor:
            df_by_view_name = executor.map(run_single_query, query_by_db_view.items())
        
        return dict(df_by_view_name)
    
    def _create_final_view_dataframe(self, df_by_db_views: Dict[str, pd.DataFrame], 
                                    final_view_query: Optional[Query]) -> pd.DataFrame:
        if final_view_query in df_by_db_views:
            return df_by_db_views[final_view_query]
        elif isinstance(final_view_query, str):
            return self._render_dataframe_from_sql("final_view", final_view_query, df_by_db_views)
        else:
            return self._render_dataframe_from_py_func("final_view", final_view_query, df_by_db_views)

    def load_results(
        self, user: Optional[UserBase], selections: Dict[str, str], *, run_query: bool = True, request_version: Optional[int] = None
    ) -> Tuple[ParameterSet, Dict[str, Query], Query, Dict[str, pd.DataFrame], Optional[pd.DataFrame]]:
        
        # apply selections
        param_set = self.apply_selections(user, selections, request_version=request_version)

        # render context
        start = time.time()
        prms = param_set.get_parameters_as_dict()
        context = self._render_context(self.context_func, user, prms)
        timer.add_activity_time(f"rendering context - dataset {self.dataset}", start)

        # render database view queries
        start = time.time()
        query_by_db_view = {}
        kwargs = {"user": user, "prms": prms, "ctx": context, "args": None}
        for db_view, raw_query in self.raw_query_by_db_view.items():
            kwargs["args"] = ManifestIO.obj.get_view_args(self.dataset, database_view=db_view)
            query_by_db_view[db_view] = self._render_query_from_raw(raw_query, kwargs)
        timer.add_activity_time(f"rendering database view queries - dataset {self.dataset}", start)

        # render final view query
        start = time.time()
        kwargs["args"] = ManifestIO.obj.get_view_args(self.dataset)
        final_view_query = self._render_query_from_raw(self.raw_final_view_query, kwargs)
        timer.add_activity_time(f"rendering final view query - dataset {self.dataset}", start)

        # create all dataframes if "run_query" is enabled
        df_by_db_views = {}
        final_view_df = None
        if run_query:
            start = time.time()
            df_by_db_views = self._create_db_view_dataframes(query_by_db_view)
            timer.add_activity_time(f"executing dataview view queries - dataset {self.dataset}", start)

            start = time.time()
            final_view_df = self._create_final_view_dataframe(df_by_db_views, final_view_query)
            timer.add_activity_time(f"executing final view query - dataset {self.dataset}", start)
        
        return param_set, query_by_db_view, final_view_query, df_by_db_views, final_view_df


class RendererIOWrapper:
    def __init__(self, dataset: str):
        context_path = u.join_paths(c.PYCONFIG_FOLDER, c.CONTEXT_FILE)
        args = ManifestIO.obj.get_dataset_args(dataset)
        try:
            context_module = u.import_file_as_module(context_path)
            context_func = partial(context_module.main, args=args)
        except FileNotFoundError:
            def no_op(*args, **kwargs):
                pass
            context_func = no_op
        
        db_views = ManifestIO.obj.get_all_database_view_names(dataset)
        raw_query_by_db_view = {}
        for db_view in db_views:
            db_view_template_path = str(ManifestIO.obj.get_database_view_file(dataset, db_view))
            raw_query_by_db_view[db_view] = self._get_raw_query(db_view_template_path)
        
        final_view_path = str(ManifestIO.obj.get_dataset_final_view_file(dataset))
        if final_view_path in db_views:
            raw_final_view_query = final_view_path
        else:
            raw_final_view_query = self._get_raw_query(final_view_path)
        
        self.dataset_folder = ManifestIO.obj.get_dataset_folder(dataset)
        self.output_folder = u.join_paths(c.OUTPUTS_FOLDER, dataset)
        self.renderer = Renderer(dataset, context_func, raw_query_by_db_view, raw_final_view_query)
    
    def _get_raw_query(self, template_path: str) -> Dict[str, Query]:
        if template_path.endswith(".py"):
            return u.get_py_main(template_path, is_required=True)
        else:
            return u.read_file(template_path)

    def _get_selections(self, selection_cfg_file: Optional[str]) -> Dict[str, str]:
        user_attributes, parameter_selections = {}, {}
        if selection_cfg_file is not None:
            selection_cfg_path = u.join_paths(self.dataset_folder, selection_cfg_file)
            config = ConfigParser()
            config.read(selection_cfg_path)
            if config.has_section(c.USER_ATTRIBUTES_SECTION):
                config_section = config[c.USER_ATTRIBUTES_SECTION]
                user_attributes = dict(config_section.items())
            if config.has_section(c.PARAMETERS_SECTION):
                config_section = config[c.PARAMETERS_SECTION]
                parameter_selections = dict(config_section.items())
        return user_attributes, parameter_selections

    def _write_sql_file(self, view_name: str, query: Any):
        if isinstance(query, str):
            db_view_sql_output_path = u.join_paths(self.output_folder, view_name+'.sql')
            with open(db_view_sql_output_path, 'w') as f:
                f.write(query)
    
    def write_outputs(self, selection_cfg_file: Optional[str], run_query: bool) -> None:
        # create output folder if it doesn't exist
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)
        
        # clear everything in output folder
        files = os.listdir(self.output_folder)
        for file in files:
            file_path = u.join_paths(self.output_folder, file)
            os.remove(file_path)
        
        # apply selections and render outputs
        user_attributes, parameter_selections = self._get_selections(selection_cfg_file)
        auth_helper = Authenticator.get_auth_helper()
        user = auth_helper.User._FromDict(user_attributes) if auth_helper is not None else None
        result = self.renderer.load_results(user, parameter_selections, run_query=run_query)
        param_set, query_by_db_view, final_view_query, df_by_db_views, final_view_df = result
        
        # write the parameters response
        param_set_dict = param_set.to_json_dict0()
        parameter_json_output_path = u.join_paths(self.output_folder, c.PARAMETERS_OUTPUT)
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
                csv_file = u.join_paths(self.output_folder, db_view+'.csv')
                df.to_csv(csv_file, index=False)
            
            final_csv_path = u.join_paths(self.output_folder, c.FINAL_VIEW_OUT_STEM+'.csv')
            final_view_df.to_csv(final_csv_path, index=False)

            final_json_path = u.join_paths(self.output_folder, c.FINAL_VIEW_OUT_STEM+'.json')
            final_view_df.to_json(final_json_path, orient='table', index=False, indent=4)
