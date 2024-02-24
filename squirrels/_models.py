from __future__ import annotations
from typing import Union, Optional, Callable, Iterable, Any
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
import sqlite3, pandas as pd, asyncio, os, shutil

from . import _constants as c, _utils as u, _py_module as pm
from .arguments.run_time_args import ContextArgs, ModelDepsArgs, ModelArgs
from ._authenticator import User, Authenticator
from ._connection_set import ConnectionSetIO
from ._manifest import ManifestIO, DatasetsConfig
from ._parameter_sets import ParameterConfigsSetIO, ParameterSet
from ._timer import timer, time

class ModelType(Enum):
    DBVIEW = 1
    FEDERATE = 2

class QueryType(Enum):
    SQL = 0
    PYTHON = 1

class Materialization(Enum):
    TABLE = 0
    VIEW = 1


@dataclass
class SqlModelConfig:
    ## Applicable for dbview models
    connection_name: str

    ## Applicable for federated models
    materialized: Materialization

    def get_sql_for_create(self, model_name: str, select_query: str) -> str:
        if self.materialized == Materialization.TABLE:
            create_prefix = f"CREATE TABLE {model_name} AS\n"
        elif self.materialized == Materialization.VIEW:
            create_prefix = f"CREATE VIEW {model_name} AS\n"
        else:
            raise NotImplementedError(f"Materialization option not supported: {self.materialized}")
        
        return create_prefix + select_query
    
    def set_attribute(self, **kwargs) -> str:
        connection_name = kwargs.get(c.DBVIEW_CONN_KEY)
        materialized = kwargs.get(c.MATERIALIZED_KEY)
        if isinstance(connection_name, str):
            self.connection_name = connection_name
        if isinstance(materialized, str):
            self.materialized = Materialization[materialized.upper()]
        return ""


ContextFunc = Callable[[dict[str, Any], ContextArgs], None]


@dataclass(frozen=True)
class RawQuery:
    pass

@dataclass(frozen=True)
class RawSqlQuery(RawQuery):
    query: str

@dataclass(frozen=True)
class RawPyQuery(RawQuery):
    query: Callable[[Any], pd.DataFrame]
    dependencies_func: Callable[[Any], Iterable]


@dataclass
class Query:
    query: Any

@dataclass
class WorkInProgress:
    query: None = field(default=None, init=False)

@dataclass
class SqlModelQuery(Query):
    query: str
    config: SqlModelConfig

@dataclass
class PyModelQuery(Query):
    query: Callable[[], pd.DataFrame]


@dataclass(frozen=True)
class QueryFile:
    filepath: str
    model_type: ModelType
    query_type: QueryType
    raw_query: RawQuery


@dataclass
class Model:
    name: str
    query_file: QueryFile
    is_target: bool = field(default=False, init=False)
    compiled_query: Optional[Query] = field(default=None, init=False)

    needs_sql_table: bool = field(default=False, init=False)
    needs_pandas: bool = False
    result: Optional[pd.DataFrame] = field(default=None, init=False, repr=False)
    
    wait_count: int = field(default=0, init=False, repr=False)
    upstreams: dict[str, Model] = field(default_factory=dict, init=False, repr=False)
    downstreams: dict[str, Model] = field(default_factory=dict, init=False, repr=False)

    confirmed_no_cycles: bool = field(default=False, init=False)

    def _add_upstream(self, other: Model) -> None:
        self.upstreams[other.name] = other
        other.downstreams[self.name] = self
        
        if self.query_file.query_type == QueryType.PYTHON:
            other.needs_pandas = True
        elif self.query_file.query_type == QueryType.SQL:
            other.needs_sql_table = True

    def _get_dbview_conn_name(self) -> str:
        dbview_config = ManifestIO.obj.dbviews.get(self.name)
        if dbview_config is None or dbview_config.connection_name is None:
            return ManifestIO.obj.settings.get(c.DB_CONN_DEFAULT_USED_SETTING, c.DEFAULT_DB_CONN)
        return dbview_config.connection_name

    def _get_materialized(self) -> str:
        federate_config = ManifestIO.obj.federates.get(self.name)
        if federate_config is None or federate_config.materialized is None:
            materialized = ManifestIO.obj.settings.get(c.DEFAULT_MATERIALIZE_SETTING, c.DEFAULT_TABLE_MATERIALIZE)
        else:
            materialized = federate_config.materialized
        return Materialization[materialized.upper()]
    
    async def _compile_sql_model(self, ctx: dict[str, Any], ctx_args: ContextArgs) -> tuple[SqlModelQuery, set]:
        assert(isinstance(self.query_file.raw_query, RawSqlQuery))
        raw_query = self.query_file.raw_query.query
        
        connection_name = self._get_dbview_conn_name()
        materialized = self._get_materialized()
        configuration = SqlModelConfig(connection_name, materialized)
        kwargs = {
            "proj_vars": ctx_args.proj_vars, "env_vars": ctx_args.env_vars,
            "user": ctx_args.user, "prms": ctx_args.prms, "traits": ctx_args.traits,
            "ctx": ctx, "config": configuration.set_attribute
        }
        dependencies = set()
        if self.query_file.model_type == ModelType.FEDERATE:
            def ref(name):
                dependencies.add(name)
                return name
            kwargs["ref"] = ref

        try:
            query = await asyncio.to_thread(u.render_string, raw_query, kwargs)
        except Exception as e:
            raise u.FileExecutionError(f'Failed to compile sql model "{self.name}"', e)
        
        compiled_query = SqlModelQuery(query, configuration)
        return compiled_query, dependencies
    
    async def _compile_python_model(self, ctx: dict[str, Any], ctx_args: ContextArgs) -> tuple[PyModelQuery, set]:
        assert(isinstance(self.query_file.raw_query, RawPyQuery))
        sqrl_args = ModelDepsArgs(ctx_args.proj_vars, ctx_args.env_vars, ctx_args.user, ctx_args.prms, ctx_args.traits, ctx)
        try:
            dependencies = await asyncio.to_thread(self.query_file.raw_query.dependencies_func, sqrl_args)
        except Exception as e:
            raise u.FileExecutionError(f'Failed to run "{c.DEP_FUNC}" function for python model "{self.name}"', e)
        
        dbview_conn_name = self._get_dbview_conn_name()
        connections = ConnectionSetIO.obj.get_engines_as_dict()
        ref = lambda x: self.upstreams[x].result
        sqrl_args = ModelArgs(ctx_args.proj_vars, ctx_args.env_vars, ctx_args.user, ctx_args.prms, ctx_args.traits, 
                              ctx, dbview_conn_name, connections, ref, set(dependencies))
            
        def compiled_query():
            try:
                return self.query_file.raw_query.query(sqrl=sqrl_args)
            except Exception as e:
                raise u.FileExecutionError(f'Failed to run "{c.MAIN_FUNC}" function for python model "{self.name}"', e)
        
        return PyModelQuery(compiled_query), dependencies

    async def compile(self, ctx: dict[str, Any], ctx_args: ContextArgs, models_dict: dict[str, Model], recurse: bool) -> None:
        if self.compiled_query is not None:
            return
        else:
            self.compiled_query = WorkInProgress()
        
        start = time.time()
        if self.query_file.query_type == QueryType.SQL:
            compiled_query, dependencies = await self._compile_sql_model(ctx, ctx_args)
        elif self.query_file.query_type == QueryType.PYTHON:
            compiled_query, dependencies = await self._compile_python_model(ctx, ctx_args)
        else:
            raise NotImplementedError(f"Query type not supported: {self.query_file.query_type}")
        
        self.compiled_query = compiled_query
        self.wait_count = len(dependencies)
        timer.add_activity_time(f"compiling model '{self.name}'", start)
        
        if not recurse:
            return 
        
        dep_models = [models_dict[x] for x in dependencies]
        coroutines = []
        for dep_model in dep_models:
            self._add_upstream(dep_model)
            coro = dep_model.compile(ctx, ctx_args, models_dict, recurse)
            coroutines.append(coro)
        await asyncio.gather(*coroutines)
    
    def validate_no_cycles(self, depencency_path: set[str]) -> set[str]:
        if self.confirmed_no_cycles:
            return
        
        if self.name in depencency_path:
            raise u.ConfigurationError(f'Cycle found in model dependency graph')

        terminal_nodes = set()
        if len(self.upstreams) == 0:
            terminal_nodes.add(self.name)
        else:
            new_path = set(depencency_path)
            new_path.add(self.name)
            for dep_model in self.upstreams.values():
                terminal_nodes_under_dep = dep_model.validate_no_cycles(new_path)
                terminal_nodes = terminal_nodes.union(terminal_nodes_under_dep)
        
        self.confirmed_no_cycles = True
        return terminal_nodes
        
    def _load_pandas_to_table(self, df: pd.DataFrame, conn: sqlite3.Connection) -> None:
        if u.use_duckdb():
            conn.execute(f"CREATE TABLE {self.name} AS FROM df")
        else:
            df.to_sql(self.name, conn, index=False)
            
    def _load_table_to_pandas(self, conn: sqlite3.Connection) -> pd.DataFrame:
        if u.use_duckdb():
            return conn.execute(f"FROM {self.name}").df()
        else:
            query = f"SELECT * FROM {self.name}"
            return pd.read_sql(query, conn)

    async def _run_sql_model(self, conn: sqlite3.Connection) -> None:
        assert(isinstance(self.compiled_query, SqlModelQuery))
        config = self.compiled_query.config
        query = self.compiled_query.query

        if self.query_file.model_type == ModelType.DBVIEW:
            def run_sql_query():
                try:
                    return ConnectionSetIO.obj.run_sql_query_from_conn_name(query, config.connection_name)
                except RuntimeError as e:
                    raise u.FileExecutionError(f'Failed to run dbview sql model "{self.name}"', e)
            
            df = await asyncio.to_thread(run_sql_query)
            await asyncio.to_thread(self._load_pandas_to_table, df, conn)
            if self.needs_pandas or self.is_target:
                self.result = df
        elif self.query_file.model_type == ModelType.FEDERATE:
            def create_table():
                create_query = config.get_sql_for_create(self.name, query)
                try:
                    return conn.execute(create_query)
                except Exception as e:
                    raise u.FileExecutionError(f'Failed to run federate sql model "{self.name}"', e)
            
            await asyncio.to_thread(create_table)
            if self.needs_pandas or self.is_target:
                self.result = await asyncio.to_thread(self._load_table_to_pandas, conn)
    
    async def _run_python_model(self, conn: sqlite3.Connection) -> None:
        assert(isinstance(self.compiled_query, PyModelQuery))

        df = await asyncio.to_thread(self.compiled_query.query)
        if self.needs_sql_table:
            await asyncio.to_thread(self._load_pandas_to_table, df, conn)
        if self.needs_pandas or self.is_target:
            self.result = df
    
    async def run_model(self, conn: sqlite3.Connection) -> None:
        start = time.time()
        if self.query_file.query_type == QueryType.SQL:
            await self._run_sql_model(conn)
        elif self.query_file.query_type == QueryType.PYTHON:
            await self._run_python_model(conn)
        timer.add_activity_time(f"running model '{self.name}'", start)
        
        coroutines = []
        for model in self.downstreams.values():
            coroutines.append(model.trigger(conn))
        await asyncio.gather(*coroutines)
    
    async def trigger(self, conn: sqlite3.Connection) -> None:
        self.wait_count -= 1
        if (self.wait_count == 0):
            await self.run_model(conn)
    
    def fill_dependent_model_names(self, dependent_model_names: set[str]) -> None:
        if self.name not in dependent_model_names:
            dependent_model_names.add(self.name)
            for dep_model in self.upstreams.values():
                dep_model.fill_dependent_model_names(dependent_model_names)


@dataclass
class DAG:
    dataset: DatasetsConfig
    target_model: Model
    models_dict: dict[str, Model]
    parameter_set: Optional[ParameterSet] = field(default=None, init=False)

    def apply_selections(
        self, user: Optional[User], selections: dict[str, str], *, updates_only: bool = False, request_version: Optional[int] = None
    ) -> None:
        start = time.time()
        dataset_params = self.dataset.parameters
        parameter_set = ParameterConfigsSetIO.obj.apply_selections(dataset_params, selections, user, updates_only=updates_only, 
                                                                   request_version=request_version)
        self.parameter_set = parameter_set
        timer.add_activity_time(f"applying selections for dataset", start)
    
    def _compile_context(self, context_func: ContextFunc, user: Optional[User]) -> tuple[dict[str, Any], ContextArgs]:
        start = time.time()
        context = {}
        param_args = ParameterConfigsSetIO.args
        prms = self.parameter_set.get_parameters_as_dict()
        args = ContextArgs(param_args.proj_vars, param_args.env_vars, user, prms, self.dataset.traits)
        try:
            context_func(ctx=context, sqrl=args)
        except Exception as e:
            raise u.FileExecutionError(f'Failed to run {c.CONTEXT_FILE} for dataset "{self.dataset}"', e)
        timer.add_activity_time(f"running context.py for dataset", start)
        return context, args
    
    async def _compile_models(self, context: dict[str, Any], ctx_args: ContextArgs, recurse: bool) -> None:
        await self.target_model.compile(context, ctx_args, self.models_dict, recurse)
    
    def _validate_no_cycles(self) -> set[str]:
        start = time.time()
        terminal_nodes = self.target_model.validate_no_cycles(set())
        timer.add_activity_time(f"validating no cycles in models dependencies", start)
        return terminal_nodes

    async def _run_models(self, terminal_nodes: set[str]) -> None:
        if u.use_duckdb():
            import duckdb
            conn = duckdb.connect()
        else:
            conn = sqlite3.connect(":memory:", check_same_thread=False)
        
        try:
            coroutines = []
            for model_name in terminal_nodes:
                model = self.models_dict[model_name]
                coroutines.append(model.run_model(conn))
            await asyncio.gather(*coroutines)
        finally:
            conn.close()
    
    async def execute(
        self, context_func: ContextFunc, user: Optional[User], selections: dict[str, str], *, request_version: Optional[int] = None,
        runquery: bool = True, recurse: bool = True
    ) -> None:
        recurse = (recurse or runquery)

        self.apply_selections(user, selections, request_version=request_version)

        context, ctx_args = self._compile_context(context_func, user)

        await self._compile_models(context, ctx_args, recurse)
        
        terminal_nodes = self._validate_no_cycles()

        if runquery:
            await self._run_models(terminal_nodes)
    
    def get_all_model_names(self) -> set[str]:
        all_model_names = set()
        self.target_model.fill_dependent_model_names(all_model_names)
        return all_model_names


class ModelsIO:
    raw_queries_by_model: dict[str, QueryFile] 
    context_func: ContextFunc

    @classmethod
    def LoadFiles(cls) -> None:
        start = time.time()
        cls.raw_queries_by_model = {}

        def populate_raw_queries_for_type(folder_path: Path, model_type: ModelType):

            def populate_from_file(dp, file):
                query_type = None
                filepath = os.path.join(dp, file)
                file_stem, extension = os.path.splitext(file)
                if extension == '.py':
                    query_type = QueryType.PYTHON
                    module = pm.PyModule(filepath)
                    dependencies_func = module.get_func_or_class(c.DEP_FUNC, default_attr=lambda x: [])
                    raw_query = RawPyQuery(module.get_func_or_class(c.MAIN_FUNC), dependencies_func)
                elif extension == '.sql':
                    query_type = QueryType.SQL
                    raw_query = RawSqlQuery(u.read_file(filepath))
                
                if query_type is not None:
                    query_file = QueryFile(filepath, model_type, query_type, raw_query)
                    if file_stem in cls.raw_queries_by_model:
                        conflicts = [cls.raw_queries_by_model[file_stem].filepath, filepath]
                        raise u.ConfigurationError(f"Multiple models found for '{file_stem}': {conflicts}")
                    cls.raw_queries_by_model[file_stem] = query_file
            
            for dp, _, filenames in os.walk(folder_path):
                for file in filenames:
                    populate_from_file(dp, file)
            
        dbviews_path = u.join_paths(c.MODELS_FOLDER, c.DBVIEWS_FOLDER)
        populate_raw_queries_for_type(dbviews_path, ModelType.DBVIEW)

        federates_path = u.join_paths(c.MODELS_FOLDER, c.FEDERATES_FOLDER)
        populate_raw_queries_for_type(federates_path, ModelType.FEDERATE)

        context_path = u.join_paths(c.PYCONFIG_FOLDER, c.CONTEXT_FILE)
        cls.context_func = pm.PyModule(context_path).get_func_or_class(c.MAIN_FUNC, default_attr=lambda x, y: None)
        
        timer.add_activity_time("loading models and/or context.py", start)

    @classmethod
    def GenerateDAG(cls, dataset: str, *, target_model_name: Optional[str] = None, always_pandas: bool = False) -> DAG:
        models_dict = {key: Model(key, val, needs_pandas=always_pandas) for key, val in cls.raw_queries_by_model.items()}
        
        dataset_config = ManifestIO.obj.datasets[dataset]
        target_model_name = dataset_config.model if target_model_name is None else target_model_name
        target_model = models_dict[target_model_name]
        target_model.is_target = True
        
        return DAG(dataset_config, target_model, models_dict)
    
    @classmethod
    async def WriteDatasetOutputsGivenTestSet(cls, dataset: str, select: str, test_set: str, runquery: bool, recurse: bool) -> Any:
        test_set_conf = ManifestIO.obj.selection_test_sets[test_set]
        user_attributes = test_set_conf.user_attributes
        selections = test_set_conf.parameters
        
        username, is_internal = user_attributes.get("username", ""), user_attributes.get("is_internal", False)
        user_cls: type[User] = Authenticator.get_auth_helper().get_func_or_class("User", default_attr=User)
        user = user_cls.Create(username, test_set_conf.user_attributes, is_internal=is_internal)
        
        dag = cls.GenerateDAG(dataset, target_model_name=select, always_pandas=True)
        await dag.execute(cls.context_func, user, selections, runquery=runquery, recurse=recurse)
        
        output_folder = u.join_paths(c.TARGET_FOLDER, c.COMPILE_FOLDER, test_set, dataset)
        if os.path.exists(output_folder):
            shutil.rmtree(output_folder)
        
        def write_model_outputs(model: Model) -> None:
            subfolder = c.DBVIEWS_FOLDER if model.query_file.model_type == ModelType.DBVIEW else c.FEDERATES_FOLDER
            subpath = u.join_paths(output_folder, subfolder)
            os.makedirs(subpath, exist_ok=True)
            if isinstance(model.compiled_query, SqlModelQuery):
                output_filepath = u.join_paths(subpath, model.name+'.sql')
                query = model.compiled_query.query
                with open(output_filepath, 'w') as f:
                    f.write(query)
            if runquery and isinstance(model.result, pd.DataFrame):
                output_filepath = u.join_paths(subpath, model.name+'.csv')
                model.result.to_csv(output_filepath, index=False)

        all_model_names = dag.get_all_model_names()
        coroutines = [asyncio.to_thread(write_model_outputs, dag.models_dict[name]) for name in all_model_names]
        await asyncio.gather(*coroutines)
        return dag.target_model.compiled_query.query

    @classmethod
    async def WriteOutputs(
        cls, dataset: Optional[str], select: Optional[str], all_test_sets: bool, test_set: Optional[str], runquery: bool
    ) -> None:
        if test_set is None:
            test_set = ManifestIO.obj.settings.get(c.TEST_SET_DEFAULT_USED_SETTING, c.DEFAULT_TEST_SET_NAME)

        if all_test_sets:
            test_sets = ManifestIO.obj.selection_test_sets.keys()
        else:
            test_sets = [test_set]
        
        recurse = True
        dataset_configs = ManifestIO.obj.datasets
        if dataset is None:
            selected_models = [(dataset.name, dataset.model) for dataset in dataset_configs.values()]
        else:
            if select is None:
                select = dataset_configs[dataset].model
            else:
                recurse = False
            selected_models = [(dataset, select)]
        
        coroutines = []
        for test_set in test_sets:
            for dataset, select in selected_models:
                coroutine = cls.WriteDatasetOutputsGivenTestSet(dataset, select, test_set, runquery, recurse)
                coroutines.append(coroutine)
        
        queries = await asyncio.gather(*coroutines)
        if not recurse and len(queries) == 1 and isinstance(queries[0], str):
            print()
            print(queries[0])
            print()
    