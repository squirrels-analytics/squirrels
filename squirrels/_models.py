from __future__ import annotations
from typing import Iterable, Callable, Any
from dataclasses import dataclass, field
from abc import ABCMeta, abstractmethod
from enum import Enum
from pathlib import Path
from sqlalchemy import create_engine, text, Connection
import asyncio, os, time, pandas as pd, networkx as nx

from . import _constants as c, _utils as u, _py_module as pm
from .arguments.run_time_args import ContextArgs, ModelDepsArgs, ModelArgs
from ._authenticator import User
from ._connection_set import ConnectionSet
from ._manifest import ManifestConfig, DatasetConfig
from ._parameter_sets import ParameterConfigsSet, ParametersArgs, ParameterSet

ContextFunc = Callable[[dict[str, Any], ContextArgs], None]


class ModelType(Enum):
    DBVIEW = 1
    FEDERATE = 2
    SEED = 3

class _Materialization(Enum):
    TABLE = 0
    VIEW = 1


@dataclass
class _SqlModelConfig:
    ## Applicable for dbview models
    connection_name: str

    ## Applicable for federated models
    materialized: _Materialization
    
    def set_attribute(self, *, connection_name: str | None = None, materialized: str | None = None, **kwargs) -> str:
        if connection_name is not None: 
            if not isinstance(connection_name, str):
                raise u.ConfigurationError("The 'connection_name' argument of 'config' macro must be a string")
            self.connection_name = connection_name
        
        if materialized is not None:
            if not isinstance(materialized, str):
                raise u.ConfigurationError("The 'materialized' argument of 'config' macro must be a string")
            try:
                self.materialized = _Materialization[materialized.upper()]
            except KeyError as e:
                valid_options = [x.name for x in _Materialization]
                raise u.ConfigurationError(f"The 'materialized' argument value '{materialized}' is not valid. Must be one of: {valid_options}") from e
        return ""

    def get_sql_for_create(self, model_name: str, select_query: str) -> str:
        create_prefix = f"CREATE {self.materialized.name} {model_name} AS\n"
        return create_prefix + select_query


@dataclass(frozen=True)
class QueryFile:
    filepath: str
    model_type: ModelType

@dataclass(frozen=True)
class SqlQueryFile(QueryFile):
    raw_query: str

@dataclass(frozen=True)
class _RawPyQuery:
    query: Callable[[ModelArgs], pd.DataFrame]
    dependencies_func: Callable[[ModelDepsArgs], Iterable[str]]

@dataclass(frozen=True)
class PyQueryFile(QueryFile):
    raw_query: _RawPyQuery


@dataclass
class _Query(metaclass=ABCMeta):
    query: Any

@dataclass
class _WorkInProgress(_Query):
    query: None = field(default=None, init=False)

@dataclass
class SqlModelQuery(_Query):
    query: str
    config: _SqlModelConfig

@dataclass
class PyModelQuery(_Query):
    query: Callable[[], pd.DataFrame]


@dataclass
class Referable(metaclass=ABCMeta):
    name: str
    is_target: bool = field(default=False, init=False)

    needs_sql_table: bool = field(default=False, init=False)
    needs_pandas: bool = field(default=False, init=False)
    result: pd.DataFrame | None = field(default=None, init=False, repr=False)

    wait_count: int = field(default=0, init=False, repr=False)
    confirmed_no_cycles: bool = field(default=False, init=False)
    upstreams: dict[str, Referable] = field(default_factory=dict, init=False, repr=False)
    downstreams: dict[str, Referable] = field(default_factory=dict, init=False, repr=False)

    @abstractmethod
    def get_model_type(self) -> ModelType:
        pass

    async def compile(
        self, ctx: dict[str, Any], ctx_args: ContextArgs, placeholders: dict[str, Any], models_dict: dict[str, Referable], recurse: bool
    ) -> None:
        pass

    @abstractmethod
    def get_terminal_nodes(self, depencency_path: set[str]) -> set[str]:
        pass
        
    def _load_pandas_to_table(self, df: pd.DataFrame, conn: Connection) -> None:
        df.to_sql(self.name, conn, index=False)
            
    def _load_table_to_pandas(self, conn: Connection) -> pd.DataFrame:
        query = f"SELECT * FROM {self.name}"
        return pd.read_sql(query, conn)
    
    async def _trigger(self, conn: Connection, placeholders: dict = {}) -> None:
        self.wait_count -= 1
        if (self.wait_count == 0):
            await self.run_model(conn, placeholders)
    
    @abstractmethod
    async def run_model(self, conn: Connection, placeholders: dict = {}) -> None:
        coroutines = []
        for model in self.downstreams.values():
            coroutines.append(model._trigger(conn, placeholders))
        await asyncio.gather(*coroutines)
    
    def retrieve_dependent_query_models(self, dependent_model_names: set[str]) -> None:
        pass
    
    def get_max_path_length_to_target(self) -> int | None:
        if not hasattr(self, "max_path_len_to_target"):
            path_lengths = []
            for child_model in self.downstreams.values():
                assert isinstance(child_model_path_length := child_model.get_max_path_length_to_target(), int)
                path_lengths.append(child_model_path_length+1)
            if len(path_lengths) > 0:
                self.max_path_len_to_target = max(path_lengths)
            else:
                self.max_path_len_to_target = 0 if self.is_target else None
        return self.max_path_len_to_target


@dataclass
class Seed(Referable):
    result: pd.DataFrame

    def get_model_type(self) -> ModelType:
        return ModelType.SEED

    def get_terminal_nodes(self, depencency_path: set[str]) -> set[str]:
        return {self.name}
    
    async def run_model(self, conn: Connection, placeholders: dict = {}) -> None:
        if self.needs_sql_table:
            await asyncio.to_thread(self._load_pandas_to_table, self.result, conn)
        await super().run_model(conn, placeholders)


@dataclass
class Model(Referable):
    query_file: QueryFile
    manifest_cfg: ManifestConfig
    conn_set: ConnectionSet
    logger: u.Logger = field(default_factory=lambda: u.Logger(""))
    j2_env: u.j2.Environment = field(default_factory=lambda: u.j2.Environment(loader=u.j2.FileSystemLoader(".")))
    compiled_query: _Query | None = field(default=None, init=False)

    def get_model_type(self) -> ModelType:
        return self.query_file.model_type

    def _add_upstream(self, other: Referable) -> None:
        self.upstreams[other.name] = other
        other.downstreams[self.name] = self
        
        if isinstance(self.query_file, SqlQueryFile):
            other.needs_sql_table = True
        elif isinstance(self.query_file, PyQueryFile):
            other.needs_pandas = True

    def _get_dbview_conn_name(self) -> str:
        dbview_config = self.manifest_cfg.dbviews.get(self.name)
        if dbview_config is None or dbview_config.connection_name is None:
            return self.manifest_cfg.settings.get(c.DB_CONN_DEFAULT_USED_SETTING, c.DEFAULT_DB_CONN)
        return dbview_config.connection_name

    def _get_materialized(self) -> _Materialization:
        federate_config = self.manifest_cfg.federates.get(self.name)
        if federate_config is None or federate_config.materialized is None:
            materialized = self.manifest_cfg.settings.get(c.DEFAULT_MATERIALIZE_SETTING, c.DEFAULT_MATERIALIZE)
        else:
            materialized = federate_config.materialized
        return _Materialization[materialized.upper()]
    
    async def _compile_sql_model(
        self, ctx: dict[str, Any], ctx_args: ContextArgs, placeholders: dict[str, Any], models_dict: dict[str, Referable]
    ) -> tuple[SqlModelQuery, set]:
        assert isinstance(self.query_file, SqlQueryFile)
        
        connection_name = self._get_dbview_conn_name()
        materialized = self._get_materialized()
        configuration = _SqlModelConfig(connection_name, materialized)
        is_placeholder = lambda placeholder: placeholder in placeholders
        kwargs = {
            "proj_vars": ctx_args.proj_vars, "env_vars": ctx_args.env_vars, "user": ctx_args.user, "prms": ctx_args.prms, 
            "traits": ctx_args.traits, "ctx": ctx, "is_placeholder": is_placeholder, "set_placeholder": ctx_args.set_placeholder,
            "config": configuration.set_attribute, "param_exists": ctx_args.param_exists
        }
        dependencies = set()
        if self.query_file.model_type == ModelType.FEDERATE:
            def ref(dependent_model_name):
                if dependent_model_name not in models_dict:
                    raise u.ConfigurationError(f'Model "{self.name}" references unknown model "{dependent_model_name}"')
                dependencies.add(dependent_model_name)
                return dependent_model_name
            kwargs["ref"] = ref

        try:
            template = self.j2_env.from_string(self.query_file.raw_query)
            query = await asyncio.to_thread(template.render, kwargs)
        except Exception as e:
            raise u.FileExecutionError(f'Failed to compile sql model "{self.name}"', e) from e
        
        compiled_query = SqlModelQuery(query, configuration)
        return compiled_query, dependencies
    
    async def _compile_python_model(
        self, ctx: dict[str, Any], ctx_args: ContextArgs, placeholders: dict[str, Any], models_dict: dict[str, Referable]
    ) -> tuple[PyModelQuery, Iterable]:
        assert isinstance(self.query_file, PyQueryFile)
        
        sqrl_args = ModelDepsArgs(
            ctx_args.proj_vars, ctx_args.env_vars, ctx_args.user, ctx_args.prms, ctx_args.traits, placeholders, ctx
        )
        try:
            dependencies = await asyncio.to_thread(self.query_file.raw_query.dependencies_func, sqrl_args)
            for dependent_model_name in dependencies:
                if dependent_model_name not in models_dict:
                    raise u.ConfigurationError(f'Model "{self.name}" references unknown model "{dependent_model_name}"')
        except Exception as e:
            raise u.FileExecutionError(f'Failed to run "{c.DEP_FUNC}" function for python model "{self.name}"', e) from e
        
        dbview_conn_name = self._get_dbview_conn_name()
        connections = self.conn_set.get_engines_as_dict()

        def ref(dependent_model_name):
            if dependent_model_name not in self.upstreams:
                raise u.ConfigurationError(f'Model "{self.name}" must include model "{dependent_model_name}" as a dependency to use')
            return pd.DataFrame(self.upstreams[dependent_model_name].result)
        
        def run_external_sql(sql_query: str, connection_name: str | None):
            connection_name = dbview_conn_name if connection_name is None else connection_name
            return self.conn_set.run_sql_query_from_conn_name(sql_query, connection_name, placeholders)
        
        sqrl_args = ModelArgs(
            ctx_args.proj_vars, ctx_args.env_vars, ctx_args.user, ctx_args.prms, ctx_args.traits, placeholders, ctx, 
            dbview_conn_name, connections, dependencies, ref, run_external_sql
        )
            
        def compiled_query():
            try:
                assert isinstance(self.query_file, PyQueryFile)
                raw_query: _RawPyQuery = self.query_file.raw_query
                return raw_query.query(sqrl_args)
            except Exception as e:
                raise u.FileExecutionError(f'Failed to run "{c.MAIN_FUNC}" function for python model "{self.name}"', e) from e
        
        return PyModelQuery(compiled_query), dependencies

    async def compile(
        self, ctx: dict[str, Any], ctx_args: ContextArgs, placeholders: dict[str, Any], models_dict: dict[str, Referable], recurse: bool
    ) -> None:
        if self.compiled_query is not None:
            return
        else:
            self.compiled_query = _WorkInProgress()
        
        start = time.time()

        if isinstance(self.query_file, SqlQueryFile):
            compiled_query, dependencies = await self._compile_sql_model(ctx, ctx_args, placeholders, models_dict)
        elif isinstance(self.query_file, PyQueryFile):
            compiled_query, dependencies = await self._compile_python_model(ctx, ctx_args, placeholders, models_dict)
        else:
            raise NotImplementedError(f"Query type not supported: {self.query_file.__class__.__name__}")
        
        self.compiled_query = compiled_query
        self.wait_count = len(set(dependencies))

        model_type = self.get_model_type().name.lower()
        self.logger.log_activity_time(f"compiling {model_type} model '{self.name}'", start)
        
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

    async def _run_sql_model(self, conn: Connection, placeholders: dict = {}) -> None:
        assert(isinstance(self.compiled_query, SqlModelQuery))
        config = self.compiled_query.config
        query = self.compiled_query.query

        if self.query_file.model_type == ModelType.DBVIEW:
            def run_sql_query():
                try:
                    return self.conn_set.run_sql_query_from_conn_name(query, config.connection_name, placeholders)
                except RuntimeError as e:
                    raise u.FileExecutionError(f'Failed to run dbview sql model "{self.name}"', e) from e
            
            df = await asyncio.to_thread(run_sql_query)
            await asyncio.to_thread(self._load_pandas_to_table, df, conn)
            if self.needs_pandas or self.is_target:
                self.result = df
        elif self.query_file.model_type == ModelType.FEDERATE:
            def create_table():
                create_query = config.get_sql_for_create(self.name, query)
                try:
                    return conn.execute(text(create_query), placeholders)
                except Exception as e:
                    raise u.FileExecutionError(f'Failed to run federate sql model "{self.name}"', e) from e
            
            await asyncio.to_thread(create_table)
            if self.needs_pandas or self.is_target:
                self.result = await asyncio.to_thread(self._load_table_to_pandas, conn)
    
    async def _run_python_model(self, conn: Connection) -> None:
        assert(isinstance(self.compiled_query, PyModelQuery))

        df = await asyncio.to_thread(self.compiled_query.query)
        if self.needs_sql_table:
            await asyncio.to_thread(self._load_pandas_to_table, df, conn)
        if self.needs_pandas or self.is_target:
            self.result = df
    
    async def run_model(self, conn: Connection, placeholders: dict = {}) -> None:
        start = time.time()
        
        if isinstance(self.query_file, SqlQueryFile):
            await self._run_sql_model(conn, placeholders)
        elif isinstance(self.query_file, PyQueryFile):
            await self._run_python_model(conn)
        else:
            raise NotImplementedError(f"Query type not supported: {self.query_file.__class__.__name__}")
        
        model_type = self.get_model_type().name.lower()
        self.logger.log_activity_time(f"running {model_type} model '{self.name}'", start)
        
        await super().run_model(conn, placeholders)
    
    def retrieve_dependent_query_models(self, dependent_model_names: set[str]) -> None:
        if self.name not in dependent_model_names:
            dependent_model_names.add(self.name)
            for dep_model in self.upstreams.values():
                dep_model.retrieve_dependent_query_models(dependent_model_names)


@dataclass
class DAG:
    manifest_cfg: ManifestConfig
    dataset: DatasetConfig
    target_model: Referable
    models_dict: dict[str, Referable]
    logger: u.Logger = field(default_factory=lambda: u.Logger(""))
    parameter_set: ParameterSet | None = field(default=None, init=False) # set in apply_selections
    placeholders: dict[str, Any] = field(init=False, default_factory=dict)

    def apply_selections(
        self, param_cfg_set: ParameterConfigsSet, user: User | None, selections: dict[str, str], *, updates_only: bool = False, request_version: int | None = None
    ) -> None:
        start = time.time()
        dataset_params = self.dataset.parameters
        parameter_set = param_cfg_set.apply_selections(
            dataset_params, selections, user, updates_only=updates_only, request_version=request_version
        )
        self.parameter_set = parameter_set
        self.logger.log_activity_time(f"applying selections for dataset '{self.dataset.name}'", start)
    
    def _compile_context(self, param_args: ParametersArgs, context_func: ContextFunc, user: User | None) -> tuple[dict[str, Any], ContextArgs]:
        start = time.time()
        context = {}
        assert isinstance(self.parameter_set, ParameterSet)
        prms = self.parameter_set.get_parameters_as_dict()
        args = ContextArgs(param_args.proj_vars, param_args.env_vars, user, prms, self.dataset.traits, self.placeholders)
        try:
            context_func(context, args)
        except Exception as e:
            raise u.FileExecutionError(f'Failed to run {c.CONTEXT_FILE} for dataset "{self.dataset.name}"', e) from e
        self.logger.log_activity_time(f"running context.py for dataset '{self.dataset.name}'", start)
        return context, args
    
    async def _compile_models(self, context: dict[str, Any], ctx_args: ContextArgs, recurse: bool) -> None:
        await self.target_model.compile(context, ctx_args, self.placeholders, self.models_dict, recurse)
    
    def _get_terminal_nodes(self) -> set[str]:
        start = time.time()
        terminal_nodes = self.target_model.get_terminal_nodes(set())
        for model in self.models_dict.values():
            model.confirmed_no_cycles = False
        self.logger.log_activity_time(f"validating no cycles in model dependencies", start)
        return terminal_nodes

    async def _run_models(self, terminal_nodes: set[str], placeholders: dict = {}) -> None:
        use_duckdb = self.manifest_cfg.settings_obj.do_use_duckdb()
        conn_url = "duckdb:///" if use_duckdb else "sqlite:///?check_same_thread=False"
        engine = create_engine(conn_url)
        
        with engine.connect() as conn:
            coroutines = []
            for model_name in terminal_nodes:
                model = self.models_dict[model_name]
                coroutines.append(model.run_model(conn, placeholders))
            await asyncio.gather(*coroutines)
        
        engine.dispose()
    
    async def execute(
        self, param_args: ParametersArgs, param_cfg_set: ParameterConfigsSet, context_func: ContextFunc, user: User | None, selections: dict[str, str], 
        *, request_version: int | None = None, runquery: bool = True, recurse: bool = True
    ) -> dict[str, Any]:
        recurse = (recurse or runquery)

        self.apply_selections(param_cfg_set, user, selections, request_version=request_version)

        context, ctx_args = self._compile_context(param_args, context_func, user)

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

    @classmethod
    def load_files(cls, logger: u.Logger, base_path: str) -> dict[str, QueryFile]:
        start = time.time()
        raw_queries_by_model: dict[str, QueryFile] = {}

        def populate_from_file(dp: str, file: str, model_type: ModelType) -> None:
            filepath = Path(dp, file)
            file_stem, extension = os.path.splitext(file)
            if extension == '.py':
                module = pm.PyModule(filepath)
                dependencies_func = module.get_func_or_class(c.DEP_FUNC, default_attr=lambda sqrl: [])
                raw_query = _RawPyQuery(module.get_func_or_class(c.MAIN_FUNC), dependencies_func)
                query_file = PyQueryFile(filepath.as_posix(), model_type, raw_query)
            elif extension == '.sql':
                query_file = SqlQueryFile(filepath.as_posix(), model_type, filepath.read_text())
            else:
                query_file = None
            
            if query_file is not None:
                if file_stem in raw_queries_by_model:
                    conflicts = [raw_queries_by_model[file_stem].filepath, filepath]
                    raise u.ConfigurationError(f"Multiple models found for '{file_stem}': {conflicts}")
                raw_queries_by_model[file_stem] = query_file

        def populate_raw_queries_for_type(folder_path: Path, model_type: ModelType) -> None:
            for dp, _, filenames in os.walk(folder_path):
                for file in filenames:
                    populate_from_file(dp, file, model_type)
            
        dbviews_path = u.Path(base_path, c.MODELS_FOLDER, c.DBVIEWS_FOLDER)
        populate_raw_queries_for_type(dbviews_path, ModelType.DBVIEW)

        federates_path = u.Path(base_path, c.MODELS_FOLDER, c.FEDERATES_FOLDER)
        populate_raw_queries_for_type(federates_path, ModelType.FEDERATE)

        logger.log_activity_time("loading files for models", start)
        return raw_queries_by_model

    @classmethod
    def load_context_func(cls, logger: u.Logger, base_path: str) -> ContextFunc:
        start = time.time()

        context_path = u.Path(base_path, c.PYCONFIGS_FOLDER, c.CONTEXT_FILE)
        context_func: ContextFunc = pm.PyModule(context_path).get_func_or_class(c.MAIN_FUNC, default_attr=lambda ctx, sqrl: None)

        logger.log_activity_time("loading file for context.py", start)
        return context_func
    