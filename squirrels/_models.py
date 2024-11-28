from __future__ import annotations
from typing import Callable, Any
from dataclasses import dataclass, field, KW_ONLY
from abc import ABCMeta, abstractmethod
from enum import Enum
from pathlib import Path
import asyncio, os, re, time, yaml, duckdb
import polars as pl, pandas as pd, networkx as nx

from . import _constants as c, _utils as u, _py_module as pm, _model_queries as mq, _model_configs as mc
from .arguments.run_time_args import ContextArgs, ModelArgs
from ._authenticator import User
from ._connection_set import ConnectionSet
from ._manifest import ManifestConfig, DatasetConfig
from ._parameter_sets import ParameterConfigsSet, ParametersArgs, ParameterSet

ContextFunc = Callable[[dict[str, Any], ContextArgs], None]


class ModelType(Enum):
    DBVIEW = "dbview"
    FEDERATE = "federate"
    SEED = "seed"


@dataclass
class Referable(metaclass=ABCMeta):
    name: str
    model_config: mc.ModelConfig
    is_target: bool = field(default=False, init=False)

    result: pl.LazyFrame | None = field(default=None, init=False, repr=False)
    needs_python_df: bool = field(default=False, init=False)

    wait_count: int = field(default=0, init=False, repr=False)
    confirmed_no_cycles: bool = field(default=False, init=False)
    upstreams: dict[str, Referable] = field(default_factory=dict, init=False, repr=False)
    downstreams: dict[str, Referable] = field(default_factory=dict, init=False, repr=False)

    @property
    @abstractmethod
    def model_type(self) -> ModelType:
        pass

    async def compile(
        self, ctx: dict[str, Any], ctx_args: ContextArgs, placeholders: dict[str, Any], models_dict: dict[str, Referable], recurse: bool
    ) -> None:
        pass

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

    def _register_python_df_to_duckdb(self, df: pl.LazyFrame, conn: duckdb.DuckDBPyConnection) -> None:
        conn.register(self.name, df)
            
    def _load_duckdb_view_to_python_df(self, conn: duckdb.DuckDBPyConnection) -> pl.LazyFrame:
        query = f"SELECT * FROM {self.name}"
        return conn.sql(query).pl().lazy()
    
    async def _trigger(self, conn: duckdb.DuckDBPyConnection, placeholders: dict = {}) -> None:
        self.wait_count -= 1
        if (self.wait_count == 0):
            await self.run_model(conn, placeholders)
    
    @abstractmethod
    async def run_model(self, conn: duckdb.DuckDBPyConnection, placeholders: dict = {}) -> None:
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
    model_config: mc.SeedConfig
    result: pl.LazyFrame

    @property
    def model_type(self) -> ModelType:
        return ModelType.SEED

    async def run_model(self, conn: duckdb.DuckDBPyConnection, placeholders: dict = {}) -> None:
        self._register_python_df_to_duckdb(self.result, conn)
        await super().run_model(conn, placeholders)


@dataclass
class QueryModel(Referable):
    query_file: mq.QueryFile
    compiled_query: mq.Query | None = field(default=None, init=False)

    manifest_cfg: ManifestConfig
    conn_set: ConnectionSet
    _: KW_ONLY
    logger: u.Logger = field(default_factory=lambda: u.Logger(""))
    j2_env: u.j2.Environment = field(default_factory=lambda: u.j2.Environment(loader=u.j2.FileSystemLoader(".")))

    def _get_compile_sql_model_args(
        self, ctx: dict[str, Any], ctx_args: ContextArgs, placeholders: dict[str, Any], models_dict: dict[str, Referable]
    ) -> dict[str, Any]:
        is_placeholder = lambda placeholder: placeholder in placeholders
        kwargs = {
            "proj_vars": ctx_args.proj_vars, "env_vars": ctx_args.env_vars, "user": ctx_args.user, "prms": ctx_args.prms, 
            "traits": ctx_args.traits, "ctx": ctx, "is_placeholder": is_placeholder, "set_placeholder": ctx_args.set_placeholder,
            "param_exists": ctx_args.param_exists
        }
        return kwargs
    
    def _get_compiled_sql_query_str(self, raw_query: str, kwargs: dict[str, Any]) -> str:
        try:
            template = self.j2_env.from_string(raw_query)
            query = template.render(kwargs)
        except Exception as e:
            raise u.FileExecutionError(f'Failed to compile sql model "{self.name}"', e) from e
        return query

    def retrieve_dependent_query_models(self, dependent_model_names: set[str]) -> None:
        if self.name not in dependent_model_names:
            dependent_model_names.add(self.name)
            for dep_model in self.upstreams.values():
                dep_model.retrieve_dependent_query_models(dependent_model_names)


@dataclass
class DbviewModel(QueryModel):
    model_config: mc.DbviewModelConfig
    query_file: mq.SqlQueryFile
    compiled_query: mq.SqlModelQuery | None = field(default=None, init=False)

    @property
    def model_type(self) -> ModelType:
        return ModelType.DBVIEW

    def _compile_sql_model(
        self, ctx: dict[str, Any], ctx_args: ContextArgs, placeholders: dict[str, Any], models_dict: dict[str, Referable]
    ) -> mq.SqlModelQuery:
        kwargs = self._get_compile_sql_model_args(ctx, ctx_args, placeholders, models_dict)
        compiled_query_str = self._get_compiled_sql_query_str(self.query_file.raw_query, kwargs)
        compiled_query = mq.SqlModelQuery(compiled_query_str)
        return compiled_query
    
    async def compile(
        self, ctx: dict[str, Any], ctx_args: ContextArgs, placeholders: dict[str, Any], models_dict: dict[str, Referable], recurse: bool
    ) -> None:
        if self.compiled_query is not None:
            return
        
        start = time.time()

        self.compiled_query = mq.WorkInProgress()
        self.compiled_query = self._compile_sql_model(ctx, ctx_args, placeholders, models_dict)
        
        self.logger.log_activity_time(f"compiling dbview model '{self.name}'", start)
    
    async def _run_sql_model(self, conn: duckdb.DuckDBPyConnection, placeholders: dict = {}) -> None:
        query = self.compiled_query.query
        default_connection_name = self.manifest_cfg.settings_obj.get_default_connection_name()
        connection_name = self.model_config.connection if self.model_config.connection else default_connection_name

        def run_sql_query():
            try:
                return self.conn_set.run_sql_query_from_conn_name(query, connection_name, placeholders)
            except RuntimeError as e:
                raise u.FileExecutionError(f'Failed to run dbview sql model "{self.name}"', e) from e
        
        self.result = await asyncio.to_thread(run_sql_query)
        self._register_python_df_to_duckdb(self.result, conn)

    async def run_model(self, conn: duckdb.DuckDBPyConnection, placeholders: dict = {}) -> None:
        start = time.time()
        
        await self._run_sql_model(conn, placeholders)
        
        self.logger.log_activity_time(f"running dbview model '{self.name}'", start)
        
        await super().run_model(conn, placeholders)
    

@dataclass
class FederateModel(QueryModel):
    model_config: mc.FederateModelConfig
    query_file: mq.SqlQueryFile | mq.PyQueryFile
    compiled_query: mq.SqlModelQuery | mq.PyModelQuery | None = field(default=None, init=False)

    @property
    def model_type(self) -> ModelType:
        return ModelType.FEDERATE

    def _add_upstream(self, other: Referable) -> None:
        self.upstreams[other.name] = other
        other.downstreams[self.name] = self
        
        if isinstance(self.query_file, mq.PyQueryFile):
            other.needs_python_df = True

    def _get_compile_sql_model_args(
        self, ctx: dict[str, Any], ctx_args: ContextArgs, placeholders: dict[str, Any], models_dict: dict[str, Referable]
    ) -> dict[str, Any]:
        kwargs = super()._get_compile_sql_model_args(ctx, ctx_args, placeholders, models_dict)
        
        def ref(dependent_model_name):
            if dependent_model_name not in models_dict:
                raise u.ConfigurationError(f'Model "{self.name}" references unknown model "{dependent_model_name}"')
            self.model_config.depends_on.add(dependent_model_name)
            return dependent_model_name
        
        kwargs["ref"] = ref
        return kwargs

    def _compile_sql_model(
        self, ctx: dict[str, Any], ctx_args: ContextArgs, placeholders: dict[str, Any], models_dict: dict[str, Referable]
    ) -> mq.SqlModelQuery:
        kwargs = self._get_compile_sql_model_args(ctx, ctx_args, placeholders, models_dict)
        compiled_query_str = self._get_compiled_sql_query_str(self.query_file.raw_query, kwargs)
        compiled_query = mq.SqlModelQuery(compiled_query_str)
        return compiled_query
    
    def _get_python_model_args(self, ctx: dict[str, Any], ctx_args: ContextArgs, placeholders: dict[str, Any]) -> dict[str, Any]:
        dependencies = self.model_config.depends_on
        connections = self.conn_set.get_connections_as_dict()

        def ref(dependent_model_name: str) -> pl.LazyFrame:
            if dependent_model_name not in self.upstreams:
                raise u.ConfigurationError(f'Model "{self.name}" must include model "{dependent_model_name}" as a dependency to use')
            df = self.upstreams[dependent_model_name].result
            assert df is not None
            return df
        
        def run_external_sql(sql_query: str, connection_name: str | None):
            return self.conn_set.run_sql_query_from_conn_name(sql_query, connection_name, placeholders)
        
        return ModelArgs(
            ctx_args.proj_vars, ctx_args.env_vars, ctx_args.user, ctx_args.prms, ctx_args.traits, placeholders, ctx, 
            connections, dependencies, ref, run_external_sql
        )

    def _compile_python_model(
        self, ctx: dict[str, Any], ctx_args: ContextArgs, placeholders: dict[str, Any], models_dict: dict[str, Referable]
    ) -> mq.PyModelQuery:
        sqrl_args = self._get_python_model_args(ctx, ctx_args, placeholders)
            
        def compiled_query() -> pl.LazyFrame | pd.DataFrame:
            try:
                raw_query: Callable[[ModelArgs], pl.LazyFrame | pd.DataFrame] = self.query_file.raw_query
                return raw_query(sqrl_args)
            except Exception as e:
                raise u.FileExecutionError(f'Failed to run "{c.MAIN_FUNC}" function for python model "{self.name}"', e) from e
        
        return mq.PyModelQuery(compiled_query)
    
    async def compile(
        self, ctx: dict[str, Any], ctx_args: ContextArgs, placeholders: dict[str, Any], models_dict: dict[str, Referable], recurse: bool
    ) -> None:
        if self.compiled_query is not None:
            return
        
        start = time.time()
        self.compiled_query = mq.WorkInProgress()

        if isinstance(self.query_file, mq.SqlQueryFile):
            self.compiled_query = self._compile_sql_model(ctx, ctx_args, placeholders, models_dict)
        elif isinstance(self.query_file, mq.PyQueryFile):
            self.compiled_query = self._compile_python_model(ctx, ctx_args, placeholders, models_dict)
        else:
            raise NotImplementedError(f"Query type not supported: {self.query_file.__class__.__name__}")
        
        self.logger.log_activity_time(f"compiling federate model '{self.name}'", start)
        
        if not recurse:
            return 
        
        dependencies = self.model_config.depends_on
        self.wait_count = len(dependencies)

        dep_models = [models_dict[x] for x in dependencies]
        coroutines = []
        for dep_model in dep_models:
            self._add_upstream(dep_model)
            coro = dep_model.compile(ctx, ctx_args, placeholders, models_dict, recurse)
            coroutines.append(coro)
        await asyncio.gather(*coroutines)

    async def _run_sql_model(self, conn: duckdb.DuckDBPyConnection, placeholders: dict = {}) -> None:
        query = self.compiled_query.query

        def create_table():
            placeholer_exists = lambda key: re.search(r"\$" + key + r"(?!\w)", query)
            existing_placeholders = {key: value for key, value in placeholders.items() if placeholer_exists(key)}

            create_query = self.model_config.get_sql_for_create(self.name, query)
            try:
                return conn.execute(create_query, existing_placeholders)
            except Exception as e:
                raise u.FileExecutionError(f'Failed to run federate sql model "{self.name}"', e) from e
        
        await asyncio.to_thread(create_table)
        if self.needs_python_df or self.is_target:
            self.result = await asyncio.to_thread(self._load_duckdb_view_to_python_df, conn)

    async def _run_python_model(self, conn: duckdb.DuckDBPyConnection) -> None:
        query_result = await asyncio.to_thread(self.compiled_query.query)
        if isinstance(query_result, pd.DataFrame):
            query_result = pl.from_pandas(query_result)
        if isinstance(query_result, pl.DataFrame):
            query_result = query_result.lazy()
        
        self.result = query_result
        self._register_python_df_to_duckdb(query_result, conn)

    async def run_model(self, conn: duckdb.DuckDBPyConnection, placeholders: dict = {}) -> None:
        start = time.time()
        
        if isinstance(self.compiled_query, mq.SqlModelQuery):
            await self._run_sql_model(conn, placeholders)
        elif isinstance(self.compiled_query, mq.PyModelQuery):
            await self._run_python_model(conn)
        else:
            raise NotImplementedError(f"Query type not supported: {self.query_file.__class__.__name__}")
        
        self.logger.log_activity_time(f"running federate model '{self.name}'", start)
        
        await super().run_model(conn, placeholders)
    

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
        conn = duckdb.connect()
        try:
            coroutines = []
            for model_name in terminal_nodes:
                model = self.models_dict[model_name]
                coroutines.append(model.run_model(conn, placeholders))
            await asyncio.gather(*coroutines)
        finally:
            conn.close()
    
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
            level = model.get_max_path_length_to_target()
            if level is not None:
                G.add_node(model_name, layer=-level, model_type=model.model_type)
        
        for model_name in G.nodes:
            model = self.models_dict[model_name]
            for dep_model_name in model.downstreams:
                G.add_edge(model_name, dep_model_name)
        
        return G


class ModelsIO:

    @classmethod
    def load_files(cls, logger: u.Logger, base_path: str) -> dict[ModelType, dict[str, mq.QueryFileWithConfig]]:
        start = time.time()
        raw_queries_by_model_type: dict[ModelType, dict[str, mq.QueryFileWithConfig]] = {}

        def load_model_config(filepath: Path, model_type: ModelType) -> mc.ModelConfig:
            yaml_path = filepath.with_suffix('.yml')
            config_dict = u.load_yaml_config(yaml_path) if yaml_path.exists() else {}
            
            if model_type == ModelType.DBVIEW:
                return mc.DbviewModelConfig(**config_dict)
            elif model_type == ModelType.FEDERATE:
                return mc.FederateModelConfig(**config_dict)
            else:
                return mc.ModelConfig(**config_dict)

        def populate_from_file(raw_queries_by_model: dict[str, mq.QueryFileWithConfig], dp: str, file: str, model_type: ModelType) -> None:
            filepath = Path(dp, file)
            file_stem, extension = os.path.splitext(file)
            
            # Skip files that are not query files
            if extension not in ['.py', '.sql']:
                return
            
            if extension == '.py':
                module = pm.PyModule(filepath)
                raw_query = module.get_func_or_class(c.MAIN_FUNC)
                query_file = mq.PyQueryFile(filepath.as_posix(), raw_query)
            elif extension == '.sql':
                query_file = mq.SqlQueryFile(filepath.as_posix(), filepath.read_text())
                
            if file_stem in raw_queries_by_model:
                conflicts = [raw_queries_by_model[file_stem].filepath, filepath]
                raise u.ConfigurationError(f"Multiple models found for '{file_stem}': {conflicts}")
            
            model_config = load_model_config(filepath, model_type)
            raw_queries_by_model[file_stem] = mq.QueryFileWithConfig(query_file, model_config)

        def populate_raw_queries_for_type(folder_path: Path, model_type: ModelType) -> dict[str, mq.QueryFileWithConfig]:
            raw_queries_by_model: dict[str, mq.QueryFileWithConfig] = {}
            for dp, _, filenames in os.walk(folder_path):
                for file in filenames:
                    populate_from_file(raw_queries_by_model, dp, file, model_type)
            return raw_queries_by_model
            
        dbviews_path = u.Path(base_path, c.MODELS_FOLDER, c.DBVIEWS_FOLDER)
        raw_queries_by_model_type[ModelType.DBVIEW] = populate_raw_queries_for_type(dbviews_path, ModelType.DBVIEW)

        federates_path = u.Path(base_path, c.MODELS_FOLDER, c.FEDERATES_FOLDER)
        raw_queries_by_model_type[ModelType.FEDERATE] = populate_raw_queries_for_type(federates_path, ModelType.FEDERATE)

        logger.log_activity_time("loading files for models", start)
        return raw_queries_by_model_type

    @classmethod
    def load_context_func(cls, logger: u.Logger, base_path: str) -> ContextFunc:
        start = time.time()

        context_path = u.Path(base_path, c.PYCONFIGS_FOLDER, c.CONTEXT_FILE)
        context_func: ContextFunc = pm.PyModule(context_path).get_func_or_class(c.MAIN_FUNC, default_attr=lambda ctx, sqrl: None)

        logger.log_activity_time("loading file for context.py", start)
        return context_func
    