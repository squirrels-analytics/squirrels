from __future__ import annotations
from typing import Optional, Callable, Iterable, Any
from dataclasses import dataclass, field
from enum import Enum
from functools import partial
from pathlib import Path
import sqlite3, pandas as pd, asyncio, os, shutil

from . import _constants as c, _utils as u, _py_module as pm
from .arguments.run_time_args import ContextArgs, DbviewModelArgs, FederateModelArgs
from ._authenticator import User
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
    materialization: Materialization

    def get_sql_for_create(self, model_name: str, select_query: str) -> str:
        if self.materialization == Materialization.TABLE:
            create_prefix = f"CREATE TABLE {model_name} AS\n"
        elif self.materialization == Materialization.VIEW:
            create_prefix = f"CREATE VIEW {model_name} AS\n"
        else:
            raise NotImplementedError(f"Materialization option not supported: {self.materialization}")
        
        return create_prefix + select_query
    
    def set_attribute(self, **kwargs) -> str:
        connection_name = kwargs.get("connection_name")
        materialization = kwargs.get("materialization")
        if isinstance(connection_name, str):
            self.connection_name = connection_name
        if isinstance(materialization, str):
            self.materialization = Materialization[materialization.upper()]
        return ""


ContextFunc = Callable[[dict[str, Any], ContextArgs], None]


@dataclass
class RawQuery:
    pass

@dataclass
class RawSqlQuery(RawQuery):
    query: str

@dataclass
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
    compiled_query_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False, repr=False)
    compiled_query: Optional[Query] = field(default=None, init=False)

    needs_sql_table: bool = field(default=False, init=False)
    needs_pandas: bool = False
    result: Optional[pd.DataFrame] = field(default=None, init=False, repr=False)
    
    wait_count: int = field(default=0, init=False, repr=False)
    wait_count_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False, repr=False)
    upstreams: dict[str, Model] = field(default_factory=dict, init=False, repr=False)
    downstreams: dict[str, Model] = field(default_factory=dict, init=False, repr=False)

    confirmed_no_cycles: bool = field(default=False, init=False)

    def add_upstream(self, other: Model) -> None:
        self.upstreams[other.name] = other
        other.downstreams[self.name] = self
        
        if self.query_file.query_type == QueryType.PYTHON:
            other.needs_pandas = True
        elif self.query_file.query_type == QueryType.SQL:
            other.needs_sql_table = True

    def _get_dbview_conn_name(self) -> str:
        dbview_config = ManifestIO.obj.dbviews.get(self.name)
        return dbview_config.connection_name if dbview_config is not None else c.DEFAULT_DB_CONN

    def _get_materialization(self) -> str:
        federate_config = ManifestIO.obj.federates.get(self.name)
        materialization = federate_config.materialization if federate_config is not None else c.DEFAULT_TABLE_MATERIALIZE
        return Materialization[materialization.upper()]
    
    def _compile_sql_model(self, ctx: dict[str, Any], ctx_args: ContextArgs) -> tuple[SqlModelQuery, set]:
        assert(isinstance(self.query_file.raw_query, RawSqlQuery))
        raw_query = self.query_file.raw_query.query
        
        connection_name = self._get_dbview_conn_name()
        materialization = self._get_materialization()
        configuration = SqlModelConfig(connection_name, materialization)
        kwargs = {
            "proj_vars": ctx_args.proj_vars, "env_vars": ctx_args.env_vars,
            "user": ctx_args.user, "prms": ctx_args.prms, "args": ctx_args.args,
            "ctx": ctx, "conf": configuration.set_attribute
        }
        dependencies = set()
        if self.query_file.model_type == ModelType.FEDERATE:
            def ref(name):
                dependencies.add(name)
                return name
            kwargs["ref"] = ref

        compiled_query = SqlModelQuery(u.render_string(raw_query, kwargs), configuration)
        return compiled_query, dependencies
    
    def _compile_python_model(self, ctx: dict[str, Any], ctx_args: ContextArgs) -> tuple[PyModelQuery, set]:
        assert(isinstance(self.query_file.raw_query, RawPyQuery))
        dependencies = set()
        if self.query_file.model_type == ModelType.DBVIEW:
            dbview_conn_name = self._get_dbview_conn_name()
            connections = ConnectionSetIO.obj.get_connections_as_dict()
            sqrl_args = DbviewModelArgs(ctx_args.proj_vars, ctx_args.env_vars, ctx_args.user, ctx_args.prms, ctx_args.args,
                                        ctx, dbview_conn_name, connections)
        elif self.query_file.model_type == ModelType.FEDERATE:
            sqrl_args = FederateModelArgs(ctx_args.proj_vars, ctx_args.env_vars, ctx_args.user, ctx_args.prms, ctx_args.args, 
                                          ctx, None, None)
            dependencies = set(self.query_file.raw_query.dependencies_func(sqrl_args))
            sqrl_args.dependencies = dependencies
            sqrl_args.ref = lambda x: self.upstreams[x].result
            
        compiled_query = PyModelQuery(partial(self.query_file.raw_query.query, sqrl=sqrl_args))
        return compiled_query, dependencies

    async def compile(self, ctx: dict[str, Any], ctx_args: ContextArgs, models_dict: dict[str, Model], recurse: bool) -> None:
        async with self.compiled_query_lock:
            is_compiled = (self.compiled_query is not None)
            if not is_compiled:
                self.compiled_query = WorkInProgress()
        
        if is_compiled:
            return
        
        if self.query_file.query_type == QueryType.SQL:
            compiled_query, dependencies = self._compile_sql_model(ctx, ctx_args)
        elif self.query_file.query_type == QueryType.PYTHON:
            compiled_query, dependencies = self._compile_python_model(ctx, ctx_args)
        else:
            raise NotImplementedError(f"Query type not supported: {self.query_file.query_type}")
        
        self.compiled_query = compiled_query
        self.wait_count = len(dependencies)
        
        if not recurse:
            return 
        
        dep_models = [models_dict[x] for x in dependencies]
        tasks = []
        for dep_model in dep_models:
            self.add_upstream(dep_model)
            task = dep_model.compile(ctx, ctx_args, models_dict, recurse)
            tasks.append(task)
        await asyncio.gather(*tasks)
    
    def validate_no_cycles(self, depencency_path: set[str], terminal_nodes: set[str]) -> None:
        if self.confirmed_no_cycles:
            return
        
        if self.name in depencency_path:
            raise u.ConfigurationError(f'Cycle found in model dependency graph')

        if len(self.upstreams) == 0:
            terminal_nodes.add(self.name)
            self.wait_count = 1
        else:
            new_path = set(depencency_path)
            new_path.add(self.name)
            for dep_model in self.upstreams.values():
                dep_model.validate_no_cycles(new_path, terminal_nodes)
        
        self.confirmed_no_cycles = True

    def _run_sql_model(self, conn: sqlite3.Connection) -> None:
        assert(isinstance(self.compiled_query, SqlModelQuery))
        config = self.compiled_query.config
        query = self.compiled_query.query

        if self.query_file.model_type == ModelType.DBVIEW:
            df = ConnectionSetIO.obj.run_sql_query_from_conn_name(query, config.connection_name)
            df.to_sql(self.name, conn, index=False)
            if self.needs_pandas:
                self.result = df
        elif self.query_file.model_type == ModelType.FEDERATE:
            create_query = config.get_sql_for_create(self.name, query)
            conn.execute(create_query)
            if self.needs_pandas:
                self.result = pd.read_sql(f"SELECT * FROM {self.name}", conn)
    
    def _run_python_model(self, conn: sqlite3.Connection) -> None:
        assert(isinstance(self.compiled_query, PyModelQuery))
        df = self.compiled_query.query()
        if self.needs_sql_table:
            df.to_sql(self.name, conn, index=False)
        if self.needs_pandas:
            self.result = df
    
    def run_model(self, conn: sqlite3.Connection, event: asyncio.Event) -> None:
        if self.query_file.query_type == QueryType.SQL:
            self._run_sql_model(conn)
        elif self.query_file.query_type == QueryType.PYTHON:
            self._run_python_model(conn)

        if self.is_target:
            event.set()
            return
        
        for model in self.downstreams.values():
            asyncio.create_task(model.trigger(conn, event))
    
    async def trigger(self, conn: sqlite3.Connection, event: asyncio.Event) -> None:
        async with self.wait_count_lock:
            self.wait_count -= 1
            is_wait_over = (self.wait_count == 0)
        
        if is_wait_over:
            self.run_model(conn, event)


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
        timer.add_activity_time(f"applying selections - dataset {self.dataset}", start)
    
    def _compile_context(self, context_func: ContextFunc, user: Optional[User]) -> tuple[dict[str, Any], ContextArgs]:
        context = {}
        param_args = ParameterConfigsSetIO.args
        prms = self.parameter_set.get_parameters_as_dict()
        args = ContextArgs(param_args.proj_vars, param_args.env_vars, user, prms, self.dataset.args)
        try:
            context_func(ctx=context, sqrl=args)
        except Exception as e:
            raise u.ConfigurationError(f'Error in the {c.CONTEXT_FILE} function for dataset "{self.dataset}"') from e
        return context, args
    
    async def _compile_models(self, context: dict[str, Any], ctx_args: ContextArgs, recurse: bool) -> None:
        await self.target_model.compile(context, ctx_args, self.models_dict, recurse)
    
    def _validate_no_cycles(self) -> set[str]:
        terminal_nodes = set()
        self.target_model.validate_no_cycles(set(), terminal_nodes)
        return terminal_nodes

    async def _run_models(self, terminal_nodes: set[str]) -> None:
        event = asyncio.Event()
        conn = sqlite3.connect(":memory:")
        try:
            for model_name in terminal_nodes:
                model = self.models_dict[model_name]
                asyncio.create_task(model.trigger(conn, event))
            await event.wait()
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


class ModelsIO:
    raw_queries_by_model: dict[str, QueryFile] 
    context_func: ContextFunc

    @classmethod
    def LoadFiles(cls) -> None:
        cls.raw_queries_by_model = {}

        def populate_raw_queries_for_type(folder_path: Path, model_type: ModelType):

            def populate_from_file(dp, file):
                query_type = None
                filepath = os.path.join(dp, file)
                file_stem, extension = os.path.splitext(file)
                if extension == '.py':
                    query_type = QueryType.PYTHON
                    module = pm.PyModule(filepath)
                    dependencies_func = module.get_func_or_class("dependencies", default_attr=lambda x: [])
                    raw_query = RawPyQuery(module.get_func_or_class("main"), dependencies_func)
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
        cls.context_func = pm.PyModule(context_path).get_func_or_class("main", default_attr=lambda x, y: None)

    @classmethod
    def GenerateDAG(cls, dataset: str, *, target_model_name: Optional[str] = None, always_pandas: bool = False) -> DAG:
        dataset_config = ManifestIO.obj.datasets[dataset]
        target_model_name = dataset_config.model if target_model_name is None else target_model_name
        models_dict = {key: Model(key, val, needs_pandas=always_pandas) for key, val in cls.raw_queries_by_model.items()}
        target_model = models_dict[target_model_name]
        target_model.is_target = True
        target_model.needs_pandas = True
        return DAG(dataset_config, target_model, models_dict)
    
    @classmethod
    async def WriteDatasetOutputsGivenTestSet(cls, dataset: str, select: str, test_set: str, runquery: bool, recurse: bool) -> Any:
        test_set_conf = ManifestIO.obj.selection_test_sets[test_set]
        user = User("")
        for key, val in test_set_conf.user_attributes.items():
            setattr(user, key, val)
        selections = test_set_conf.parameters
        dag = cls.GenerateDAG(dataset, target_model_name=select, always_pandas=True)
        await dag.execute(cls.context_func, user, selections, runquery=runquery, recurse=recurse)
        
        output_folder = u.join_paths(c.TARGET_FOLDER, c.COMPILE_FOLDER, test_set, dataset)
        if os.path.exists(output_folder):
            shutil.rmtree(output_folder)
        
        target_model = dag.models_dict[select]
        for name, model in dag.models_dict.items(): # TODO: iterate only dependent models instead of all models
            subfolder = c.DBVIEWS_FOLDER if model.query_file.model_type == ModelType.DBVIEW else c.FEDERATES_FOLDER
            subpath = u.join_paths(output_folder, subfolder)
            os.makedirs(subpath)
            if isinstance(model.compiled_query, SqlModelQuery):
                output_filepath = u.join_paths(subpath, name+'.sql')
                query = model.compiled_query.query
                with open(output_filepath, 'w') as f:
                    f.write(query)
            if runquery and isinstance(model.result, pd.DataFrame):
                output_filepath = u.join_paths(subpath, name+'.csv')
                model.result.to_csv(output_filepath, index=False)
        
        return target_model.compiled_query.query

    @classmethod
    async def WriteOutputs(
        cls, dataset: Optional[str], select: Optional[str], all_test_sets: bool, test_set: str, runquery: bool
    ) -> None:
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
    