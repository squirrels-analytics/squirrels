from __future__ import annotations
from typing import Optional, Callable, Iterable, Any
from dataclasses import dataclass, field
from abc import ABCMeta, abstractmethod
from enum import Enum
from pathlib import Path
import sqlite3, asyncio, os, shutil, pandas as pd, json
import matplotlib.pyplot as plt, networkx as nx

from . import _constants as c, _utils as u, _py_module as pm
from .arguments.run_time_args import ContextArgs, ModelDepsArgs, ModelArgs
from ._authenticator import User, Authenticator
from ._connection_set import ConnectionSetIO
from ._manifest import ManifestIO, DatasetsConfig, DatasetScope
from ._parameter_sets import ParameterConfigsSetIO, ParameterSet
from ._seeds import SeedsIO
from ._timer import timer, time

class ModelType(Enum):
    DBVIEW = 1
    FEDERATE = 2
    SEED = 3

class QueryType(Enum):
    SQL = 0
    PYTHON = 1

class Materialization(Enum):
    TABLE = 0
    VIEW = 1


@dataclass
class _SqlModelConfig:
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
class _RawQuery(metaclass=ABCMeta):
    pass

@dataclass(frozen=True)
class _RawSqlQuery(_RawQuery):
    query: str

@dataclass(frozen=True)
class _RawPyQuery(_RawQuery):
    query: Callable[[Any], pd.DataFrame]
    dependencies_func: Callable[[Any], Iterable]


@dataclass
class _Query(metaclass=ABCMeta):
    query: Any

@dataclass
class _WorkInProgress(_Query):
    query: None = field(default=None, init=False)

@dataclass
class _SqlModelQuery(_Query):
    query: str
    config: _SqlModelConfig

@dataclass
class _PyModelQuery(_Query):
    query: Callable[[], pd.DataFrame]


@dataclass(frozen=True)
class _QueryFile:
    filepath: str
    model_type: ModelType
    query_type: QueryType
    raw_query: _RawQuery


@dataclass
class _Referable(metaclass=ABCMeta):
    name: str
    is_target: bool = field(default=False, init=False)

    needs_sql_table: bool = field(default=False, init=False)
    needs_pandas: bool = field(default=False, init=False)
    result: Optional[pd.DataFrame] = field(default=None, init=False, repr=False)

    wait_count: int = field(default=0, init=False, repr=False)
    confirmed_no_cycles: bool = field(default=False, init=False)
    upstreams: dict[str, _Referable] = field(default_factory=dict, init=False, repr=False)
    downstreams: dict[str, _Referable] = field(default_factory=dict, init=False, repr=False)

    @abstractmethod
    def get_model_type(self) -> ModelType:
        pass

    async def compile(
        self, ctx: dict[str, Any], ctx_args: ContextArgs, placeholders: dict[str, Any], models_dict: dict[str, _Referable], recurse: bool
    ) -> None:
        pass

    @abstractmethod
    def get_terminal_nodes(self, depencency_path: set[str]) -> set[str]:
        pass
        
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
    
    async def _trigger(self, conn: sqlite3.Connection, placeholders: dict = {}) -> None:
        self.wait_count -= 1
        if (self.wait_count == 0):
            await self.run_model(conn, placeholders)
    
    @abstractmethod
    async def run_model(self, conn: sqlite3.Connection, placeholders: dict = {}) -> None:
        coroutines = []
        for model in self.downstreams.values():
            coroutines.append(model._trigger(conn, placeholders))
        await asyncio.gather(*coroutines)
    
    def retrieve_dependent_query_models(self, dependent_model_names: set[str]) -> None:
        pass
    
    def get_max_path_length_to_target(self) -> int:
        if not hasattr(self, "max_path_len_to_target"):
            path_lengths = []
            for child_model in self.downstreams.values():
                path_lengths.append(child_model.get_max_path_length_to_target()+1)
            if len(path_lengths) > 0:
                self.max_path_len_to_target = max(path_lengths)
            else:
                self.max_path_len_to_target = 0 if self.is_target else None
        return self.max_path_len_to_target


@dataclass
class _Seed(_Referable):
    result: pd.DataFrame

    def get_model_type(self) -> ModelType:
        return ModelType.SEED

    def get_terminal_nodes(self, depencency_path: set[str]) -> set[str]:
        return {self.name}
    
    async def run_model(self, conn: sqlite3.Connection, placeholders: dict = {}) -> None:
        if self.needs_sql_table:
            await asyncio.to_thread(self._load_pandas_to_table, self.result, conn)
        await super().run_model(conn, placeholders)


@dataclass
class _Model(_Referable):
    query_file: _QueryFile

    compiled_query: Optional[_Query] = field(default=None, init=False)

    def get_model_type(self) -> ModelType:
        return self.query_file.model_type

    def _add_upstream(self, other: _Referable) -> None:
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
    
    async def _compile_sql_model(
        self, ctx: dict[str, Any], ctx_args: ContextArgs, placeholders: dict[str, Any]
    ) -> tuple[_SqlModelQuery, set]:
        assert(isinstance(self.query_file.raw_query, _RawSqlQuery))

        raw_query = self.query_file.raw_query.query
        connection_name = self._get_dbview_conn_name()
        materialized = self._get_materialized()
        configuration = _SqlModelConfig(connection_name, materialized)
        is_placeholder = lambda x: x in placeholders
        kwargs = {
            "proj_vars": ctx_args.proj_vars, "env_vars": ctx_args.env_vars, "user": ctx_args.user, "prms": ctx_args.prms, 
            "traits": ctx_args.traits, "ctx": ctx, "is_placeholder": is_placeholder, "set_placeholder": ctx_args.set_placeholder,
            "config": configuration.set_attribute, "is_param_enabled": ctx_args.param_exists
        }
        dependencies = set()
        if self.query_file.model_type == ModelType.FEDERATE:
            def ref(name):
                dependencies.add(name)
                return name
            kwargs["ref"] = ref

        try:
            query = await asyncio.to_thread(u.render_string, raw_query, **kwargs)
        except Exception as e:
            raise u.FileExecutionError(f'Failed to compile sql model "{self.name}"', e)
        
        compiled_query = _SqlModelQuery(query, configuration)
        return compiled_query, dependencies
    
    async def _compile_python_model(
        self, ctx: dict[str, Any], ctx_args: ContextArgs, placeholders: dict[str, Any]
    ) -> tuple[_PyModelQuery, set]:
        assert(isinstance(self.query_file.raw_query, _RawPyQuery))
        
        sqrl_args = ModelDepsArgs(
            ctx_args.proj_vars, ctx_args.env_vars, ctx_args.user, ctx_args.prms, ctx_args.traits, placeholders, ctx
        )
        try:
            dependencies = await asyncio.to_thread(self.query_file.raw_query.dependencies_func, sqrl_args)
        except Exception as e:
            raise u.FileExecutionError(f'Failed to run "{c.DEP_FUNC}" function for python model "{self.name}"', e)
        
        dbview_conn_name = self._get_dbview_conn_name()
        connections = ConnectionSetIO.obj.get_engines_as_dict()
        ref = lambda x: self.upstreams[x].result
        sqrl_args = ModelArgs(
            ctx_args.proj_vars, ctx_args.env_vars, ctx_args.user, ctx_args.prms, ctx_args.traits, placeholders, ctx, 
            dbview_conn_name, connections, dependencies, ref
        )
            
        def compiled_query():
            try:
                raw_query: _RawPyQuery = self.query_file.raw_query
                return raw_query.query(sqrl=sqrl_args)
            except Exception as e:
                raise u.FileExecutionError(f'Failed to run "{c.MAIN_FUNC}" function for python model "{self.name}"', e)
        
        return _PyModelQuery(compiled_query), dependencies

    async def compile(
        self, ctx: dict[str, Any], ctx_args: ContextArgs, placeholders: dict[str, Any], models_dict: dict[str, _Referable], recurse: bool
    ) -> None:
        if self.compiled_query is not None:
            return
        else:
            self.compiled_query = _WorkInProgress()
        
        start = time.time()

        if self.query_file.query_type == QueryType.SQL:
            compiled_query, dependencies = await self._compile_sql_model(ctx, ctx_args, placeholders)
        elif self.query_file.query_type == QueryType.PYTHON:
            compiled_query, dependencies = await self._compile_python_model(ctx, ctx_args, placeholders)
        else:
            raise NotImplementedError(f"Query type not supported: {self.query_file.query_type}")
        
        self.compiled_query = compiled_query
        self.wait_count = len(dependencies)

        model_type = self.get_model_type().name.lower()
        timer.add_activity_time(f"compiling {model_type} model '{self.name}'", start)
        
        if not recurse:
            return 
        
        dep_models = [models_dict[x] for x in dependencies]
        coroutines = []
        for dep_model in dep_models:
            self._add_upstream(dep_model)
            coro = dep_model.compile(ctx, ctx_args, placeholders, models_dict, recurse)
            coroutines.append(coro)
        await asyncio.gather(*coroutines)
    
    def get_terminal_nodes(self, depencency_path: set[str]) -> set[str]:
        if self.confirmed_no_cycles:
            return set()
        
        if self.name in depencency_path:
            raise u.ConfigurationError(f'Cycle found in model dependency graph')

        terminal_nodes = set()
        if len(self.upstreams) == 0:
            terminal_nodes.add(self.name)
        else:
            new_path = set(depencency_path)
            new_path.add(self.name)
            for dep_model in self.upstreams.values():
                terminal_nodes_under_dep = dep_model.get_terminal_nodes(new_path)
                terminal_nodes = terminal_nodes.union(terminal_nodes_under_dep)
        
        self.confirmed_no_cycles = True
        return terminal_nodes

    async def _run_sql_model(self, conn: sqlite3.Connection, placeholders: dict = {}) -> None:
        assert(isinstance(self.compiled_query, _SqlModelQuery))
        config = self.compiled_query.config
        query = self.compiled_query.query

        if self.query_file.model_type == ModelType.DBVIEW:
            def run_sql_query():
                try:
                    return ConnectionSetIO.obj.run_sql_query_from_conn_name(query, config.connection_name, placeholders)
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
                    return conn.execute(create_query, placeholders)
                except Exception as e:
                    raise u.FileExecutionError(f'Failed to run federate sql model "{self.name}"', e)
            
            await asyncio.to_thread(create_table)
            if self.needs_pandas or self.is_target:
                self.result = await asyncio.to_thread(self._load_table_to_pandas, conn)
    
    async def _run_python_model(self, conn: sqlite3.Connection) -> None:
        assert(isinstance(self.compiled_query, _PyModelQuery))

        df = await asyncio.to_thread(self.compiled_query.query)
        if self.needs_sql_table:
            await asyncio.to_thread(self._load_pandas_to_table, df, conn)
        if self.needs_pandas or self.is_target:
            self.result = df
    
    async def run_model(self, conn: sqlite3.Connection, placeholders: dict = {}) -> None:
        start = time.time()
        
        if self.query_file.query_type == QueryType.SQL:
            await self._run_sql_model(conn, placeholders)
        elif self.query_file.query_type == QueryType.PYTHON:
            await self._run_python_model(conn)
        
        model_type = self.get_model_type().name.lower()
        timer.add_activity_time(f"running {model_type} model '{self.name}'", start)
        
        await super().run_model(conn, placeholders)
    
    def retrieve_dependent_query_models(self, dependent_model_names: set[str]) -> None:
        if self.name not in dependent_model_names:
            dependent_model_names.add(self.name)
            for dep_model in self.upstreams.values():
                dep_model.retrieve_dependent_query_models(dependent_model_names)


@dataclass
class _DAG:
    dataset: DatasetsConfig
    target_model: _Referable
    models_dict: dict[str, _Referable]
    parameter_set: Optional[ParameterSet] = field(default=None, init=False)
    placeholders: dict[str, Any] = field(init=False, default_factory=dict)

    def apply_selections(
        self, user: Optional[User], selections: dict[str, str], *, updates_only: bool = False, request_version: Optional[int] = None
    ) -> None:
        start = time.time()
        dataset_params = self.dataset.parameters
        parameter_set = ParameterConfigsSetIO.obj.apply_selections(
            dataset_params, selections, user, updates_only=updates_only, request_version=request_version
        )
        self.parameter_set = parameter_set
        timer.add_activity_time(f"applying selections for dataset '{self.dataset.name}'", start)
    
    def _compile_context(self, context_func: ContextFunc, user: Optional[User]) -> tuple[dict[str, Any], ContextArgs]:
        start = time.time()
        context = {}
        param_args = ParameterConfigsSetIO.args
        prms = self.parameter_set.get_parameters_as_dict()
        args = ContextArgs(param_args.proj_vars, param_args.env_vars, user, prms, self.dataset.traits, self.placeholders)
        try:
            context_func(ctx=context, sqrl=args)
        except Exception as e:
            raise u.FileExecutionError(f'Failed to run {c.CONTEXT_FILE} for dataset "{self.dataset.name}"', e)
        timer.add_activity_time(f"running context.py for dataset '{self.dataset.name}'", start)
        return context, args
    
    async def _compile_models(self, context: dict[str, Any], ctx_args: ContextArgs, recurse: bool) -> None:
        await self.target_model.compile(context, ctx_args, self.placeholders, self.models_dict, recurse)
    
    def _get_terminal_nodes(self) -> set[str]:
        start = time.time()
        terminal_nodes = self.target_model.get_terminal_nodes(set())
        for model in self.models_dict.values():
            model.confirmed_no_cycles = False
        timer.add_activity_time(f"validating no cycles in model dependencies", start)
        return terminal_nodes

    async def _run_models(self, terminal_nodes: set[str], placeholders: dict = {}) -> None:
        if u.use_duckdb():
            import duckdb
            conn = duckdb.connect()
        else:
            conn = sqlite3.connect(":memory:", check_same_thread=False)
        
        try:
            coroutines = []
            for model_name in terminal_nodes:
                model = self.models_dict[model_name]
                coroutines.append(model.run_model(conn, placeholders))
            await asyncio.gather(*coroutines)
        finally:
            conn.close()
    
    async def execute(
        self, context_func: ContextFunc, user: Optional[User], selections: dict[str, str], *, request_version: Optional[int] = None,
        runquery: bool = True, recurse: bool = True
    ) -> dict[str, Any]:
        recurse = (recurse or runquery)

        self.apply_selections(user, selections, request_version=request_version)

        context, ctx_args = self._compile_context(context_func, user)

        await self._compile_models(context, ctx_args, recurse)
        
        terminal_nodes = self._get_terminal_nodes()

        placeholders = ctx_args._placeholders.copy()
        if runquery:
            await self._run_models(terminal_nodes, placeholders)

        return placeholders
    
    def get_all_query_models(self) -> set[str]:
        all_model_names = set()
        self.target_model.retrieve_dependent_query_models(all_model_names)
        return all_model_names
    
    def to_networkx_graph(self) -> nx.DiGraph:
        G = nx.DiGraph()

        for model_name, model in self.models_dict.items():
            model_type = model.get_model_type()
            level = model.get_max_path_length_to_target()
            if level is not None:
                G.add_node(model_name, layer=-level, model_type=model_type)
        
        for model_name in G.nodes:
            model = self.models_dict[model_name]
            for dep_model_name in model.downstreams:
                G.add_edge(model_name, dep_model_name)
        
        return G

class ModelsIO:
    raw_queries_by_model: dict[str, _QueryFile] 
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
                    raw_query = _RawPyQuery(module.get_func_or_class(c.MAIN_FUNC), dependencies_func)
                elif extension == '.sql':
                    query_type = QueryType.SQL
                    raw_query = _RawSqlQuery(u.read_file(filepath))
                
                if query_type is not None:
                    query_file = _QueryFile(filepath, model_type, query_type, raw_query)
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

        context_path = u.join_paths(c.PYCONFIGS_FOLDER, c.CONTEXT_FILE)
        cls.context_func = pm.PyModule(context_path).get_func_or_class(c.MAIN_FUNC, default_attr=lambda x, y: None)
        
        timer.add_activity_time("loading files for models and context.py", start)

    @classmethod
    def GenerateDAG(cls, dataset: str, *, target_model_name: Optional[str] = None, always_pandas: bool = False) -> _DAG:
        seeds_dict = SeedsIO.obj.get_dataframes()

        models_dict: dict[str, _Referable] = {key: _Seed(key, df) for key, df in seeds_dict.items()}
        for key, val in cls.raw_queries_by_model.items():
            models_dict[key] = _Model(key, val)
            models_dict[key].needs_pandas = always_pandas
        
        dataset_config = ManifestIO.obj.datasets[dataset]
        target_model_name = dataset_config.model if target_model_name is None else target_model_name
        target_model = models_dict[target_model_name]
        target_model.is_target = True
        
        return _DAG(dataset_config, target_model, models_dict)
    
    @classmethod
    def draw_dag(cls, dag: _DAG, output_folder: Path) -> None:
        color_map = {ModelType.SEED: "green", ModelType.DBVIEW: "red", ModelType.FEDERATE: "skyblue"}

        G = dag.to_networkx_graph()
        
        fig, _ = plt.subplots()
        pos = nx.multipartite_layout(G, subset_key="layer")
        colors = [color_map[node[1]] for node in G.nodes(data="model_type")]
        nx.draw(G, pos=pos, node_shape='^', node_size=1000, node_color=colors, arrowsize=20)
        
        y_values = [val[1] for val in pos.values()]
        scale = max(y_values) - min(y_values) if len(y_values) > 0 else 0
        label_pos = {key: (val[0], val[1]-0.002-0.1*scale) for key, val in pos.items()}
        nx.draw_networkx_labels(G, pos=label_pos, font_size=8)
        
        fig.tight_layout()
        plt.margins(x=0.1, y=0.1)
        plt.savefig(u.join_paths(output_folder, "dag.png"))
        plt.close(fig)

    @classmethod
    async def WriteDatasetOutputsGivenTestSet(
        cls, dataset_conf: DatasetsConfig, select: str, test_set: Optional[str], runquery: bool, recurse: bool
    ) -> Any:
        dataset = dataset_conf.name
        if test_set is None:
            test_set = ManifestIO.obj.get_default_test_set(dataset)
        error_msg_intro = f"Cannot compile dataset '{dataset}' with test set '{test_set}'."

        test_set_conf = ManifestIO.obj.selection_test_sets[test_set]
        user_attributes = test_set_conf.user_attributes.copy()
        selections = test_set_conf.parameters.copy()
        if test_set_conf.datasets is not None and dataset not in test_set_conf.datasets:
            raise u.InvalidInputError(f"{error_msg_intro}\n Applicable datasets for test set '{test_set}' does not include dataset '{dataset}'.")
        
        username, is_internal = user_attributes.pop("username", ""), user_attributes.pop("is_internal", False)
        if test_set_conf.is_authenticated:
            user_cls: type[User] = Authenticator.get_auth_helper().get_func_or_class("User", default_attr=User)
            user = user_cls.Create(username, is_internal=is_internal, **user_attributes)
        elif dataset_conf.scope == DatasetScope.PUBLIC:
            user = None
        else:
            raise u.ConfigurationError(f"{error_msg_intro}\n Non-public datasets require a test set with 'user_attributes' section defined")
        
        if dataset_conf.scope == DatasetScope.PRIVATE and not is_internal:
            raise u.ConfigurationError(f"{error_msg_intro}\n Private datasets require a test set with user_attribute 'is_internal' set to true")

        # always_pandas is set to True for creating CSV files from results (when runquery is True)
        dag = cls.GenerateDAG(dataset, target_model_name=select, always_pandas=True)
        placeholders = await dag.execute(cls.context_func, user, selections, runquery=runquery, recurse=recurse)
        
        output_folder = u.join_paths(c.TARGET_FOLDER, c.COMPILE_FOLDER, dataset, test_set)
        if os.path.exists(output_folder):
            shutil.rmtree(output_folder)
        os.makedirs(output_folder, exist_ok=True)
        
        def write_placeholders() -> None:
            output_filepath = u.join_paths(output_folder, "placeholders.json")
            with open(output_filepath, 'w') as f:
                json.dump(placeholders, f, indent=4)
        
        def write_model_outputs(model: _Model) -> None:
            subfolder = c.DBVIEWS_FOLDER if model.query_file.model_type == ModelType.DBVIEW else c.FEDERATES_FOLDER
            subpath = u.join_paths(output_folder, subfolder)
            os.makedirs(subpath, exist_ok=True)
            if isinstance(model.compiled_query, _SqlModelQuery):
                output_filepath = u.join_paths(subpath, model.name+'.sql')
                query = model.compiled_query.query
                with open(output_filepath, 'w') as f:
                    f.write(query)
            if runquery and isinstance(model.result, pd.DataFrame):
                output_filepath = u.join_paths(subpath, model.name+'.csv')
                model.result.to_csv(output_filepath, index=False)

        write_placeholders()
        all_model_names = dag.get_all_query_models()
        coroutines = [asyncio.to_thread(write_model_outputs, dag.models_dict[name]) for name in all_model_names]
        await asyncio.gather(*coroutines)

        if recurse:
            cls.draw_dag(dag, output_folder)

    @classmethod
    async def WriteOutputs(
        cls, dataset: Optional[str], all_datasets: bool, select: Optional[str], test_set: Optional[str], all_test_sets: bool, 
        runquery: bool
    ) -> None:
        if all_test_sets:
            test_sets = ManifestIO.obj.selection_test_sets.keys()
        else:
            test_sets = [test_set]
        
        recurse = True
        dataset_configs = ManifestIO.obj.datasets
        if all_datasets:
            selected_models = [(dataset, dataset.model) for dataset in dataset_configs.values()]
        else:
            if select is None:
                select = dataset_configs[dataset].model
            else:
                recurse = False
            selected_models = [(dataset_configs[dataset], select)]
        
        coroutines = []
        for dataset_conf, select in selected_models:
            for test_set in test_sets:
                coroutine = cls.WriteDatasetOutputsGivenTestSet(dataset_conf, select, test_set, runquery, recurse)
                coroutines.append(coroutine)
        
        queries = await asyncio.gather(*coroutines)
        if not recurse and len(queries) == 1 and isinstance(queries[0], str):
            print()
            print(queries[0])
            print()
    