from __future__ import annotations
from typing import Callable, Any
from dataclasses import dataclass, field, KW_ONLY
from abc import ABCMeta, abstractmethod
from enum import Enum
from pathlib import Path
import asyncio, os, re, time, duckdb, sqlglot
import polars as pl, pandas as pd, networkx as nx

from . import _constants as c, _utils as u, _py_module as pm, _model_queries as mq, _model_configs as mc, _sources as src, _api_response_models as arm
from ._exceptions import FileExecutionError, InvalidInputError
from .arguments.run_time_args import ContextArgs, ModelArgs, BuildModelArgs
from ._auth import BaseUser
from ._connection_set import ConnectionsArgs, ConnectionSet, ConnectionProperties
from ._manifest import DatasetConfig
from ._parameter_sets import ParameterConfigsSet, ParametersArgs, ParameterSet

ContextFunc = Callable[[dict[str, Any], ContextArgs], None]


class ModelType(Enum):
    SOURCE = "source"
    DBVIEW = "dbview"
    FEDERATE = "federate"
    SEED = "seed"
    BUILD = "build"


@dataclass
class DataModel(metaclass=ABCMeta):
    name: str
    model_config: mc.ModelConfig
    is_target: bool = field(default=False, init=False)

    result: pl.LazyFrame | None = field(default=None, init=False, repr=False)
    needs_python_df: bool = field(default=False, init=False)

    wait_count: int = field(default=0, init=False, repr=False)
    confirmed_no_cycles: bool = field(default=False, init=False)
    upstreams: dict[str, DataModel] = field(default_factory=dict, init=False, repr=False)
    downstreams: dict[str, DataModel] = field(default_factory=dict, init=False, repr=False)

    _: KW_ONLY
    logger: u.Logger = field(default_factory=lambda: u.Logger(""))
    env_vars: dict[str, str] = field(default_factory=dict)
    conn_set: ConnectionSet = field(default_factory=ConnectionSet)

    @property
    @abstractmethod
    def model_type(self) -> ModelType:
        pass

    @property
    def is_queryable(self) -> bool:
        return True

    def compile(
        self, ctx: dict[str, Any], ctx_args: ContextArgs, models_dict: dict[str, DataModel], recurse: bool
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
                terminal_nodes.update(dep_model.get_terminal_nodes(new_path))
        
        self.confirmed_no_cycles = True
        return terminal_nodes
    
    def _load_duckdb_view_to_python_df(self, conn: duckdb.DuckDBPyConnection, *, use_venv: bool = False) -> pl.LazyFrame:
        table_name = ("venv." if use_venv else "") + self.name
        try:
            return conn.sql(f"FROM {table_name}").pl().lazy()
        except duckdb.CatalogException as e:
            raise u.ConfigurationError(f'Failed to load duckdb table or view "{self.name}" to python dataframe') from e
    
    def _run_sql_query_on_connection(self, connection_name: str, query: str, placeholders: dict = {}) -> pl.DataFrame:
        self.logger.info(f"Running sql query on connection '{connection_name}': {query}")
        return self.conn_set.run_sql_query_from_conn_name(query, connection_name, placeholders)
    
    async def _trigger(self, conn: duckdb.DuckDBPyConnection, placeholders: dict = {}) -> None:
        self.wait_count -= 1
        if (self.wait_count == 0):
            await self.run_model(conn, placeholders)
    
    async def run_model(self, conn: duckdb.DuckDBPyConnection, placeholders: dict = {}) -> None:
        coroutines = []
        for model in self.downstreams.values():
            coroutines.append(model._trigger(conn, placeholders))
        await u.asyncio_gather(coroutines)
    
    def retrieve_dependent_query_models(self, dependent_model_names: set[str]) -> None:
        pass

    def _register_all_upstream_python_df_helper(self, conn: duckdb.DuckDBPyConnection, tables_set: set[str]) -> None:
        if self.result is not None and self.name not in tables_set:
            conn.register(self.name, self.result)
        for dep_model in self.upstreams.values():
            dep_model._register_all_upstream_python_df_helper(conn, tables_set)

    def register_all_upstream_python_df(self, conn: duckdb.DuckDBPyConnection) -> None:
        show_tables_query = f"SHOW TABLES"
        tables_df = conn.sql(show_tables_query).pl()
        tables_set = set(tables_df["name"])
        self._register_all_upstream_python_df_helper(conn, tables_set)

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

    async def _trigger_build(self, conn: duckdb.DuckDBPyConnection, full_refresh: bool) -> None:
        pass
    
    def _create_table_from_df(self, conn: duckdb.DuckDBPyConnection, query_result: pl.LazyFrame | pd.DataFrame):
        local_conn = conn.cursor()
        try:
            local_conn.register("df", query_result)
            local_conn.execute(f"CREATE OR REPLACE TABLE {self.name} AS SELECT * FROM df")
        finally:
            local_conn.close()
        
    def process_pass_through_columns(self, models_dict: dict[str, DataModel]) -> None:
        pass


@dataclass
class StaticModel(DataModel):
    needs_python_df_for_build: bool = field(default=False, init=False)
    wait_count_for_build: int = field(default=0, init=False, repr=False)
    upstreams_for_build: dict[str, StaticModel] = field(default_factory=dict, init=False, repr=False)
    downstreams_for_build: dict[str, StaticModel] = field(default_factory=dict, init=False, repr=False)
    
    def get_terminal_nodes_for_build(self, depencency_path: set[str]) -> set[str]:
        if self.confirmed_no_cycles:
            return set()
        
        if self.name in depencency_path:
            raise u.ConfigurationError(f'Cycle found in model dependency graph')

        terminal_nodes = set()
        if len(self.upstreams_for_build) == 0:
            terminal_nodes.add(self.name)
        else:
            new_path = set(depencency_path)
            new_path.add(self.name)
            for dep_model in self.upstreams_for_build.values():
                terminal_nodes.update(dep_model.get_terminal_nodes_for_build(new_path))
        
        self.confirmed_no_cycles = True
        return terminal_nodes
    
    def _get_result(self, conn: duckdb.DuckDBPyConnection) -> pl.LazyFrame:
        local_conn = conn.cursor()
        try:
            return self._load_duckdb_view_to_python_df(local_conn, use_venv=True)
        except Exception as e:
            raise InvalidInputError(61, f'Model "{self.name}" depends on static data models that cannot be found.')
        finally:
            local_conn.close()
    
    async def run_model(self, conn: duckdb.DuckDBPyConnection, placeholders: dict = {}) -> None:
        start = time.time()

        if (self.needs_python_df or self.is_target) and self.result is None:
            self.result = await asyncio.to_thread(self._get_result, conn)
        
        self.logger.log_activity_time(f"loading static model '{self.name}'", start)

        await super().run_model(conn, placeholders)

    def compile_for_build(
        self, conn_args: ConnectionsArgs, models_dict: dict[str, StaticModel]
    ) -> None:
        pass
    
    async def _trigger_build(self, conn: duckdb.DuckDBPyConnection, full_refresh: bool) -> None:
        self.wait_count_for_build -= 1
        if (self.wait_count_for_build == 0):
            await self.build_model(conn, full_refresh)
    
    async def build_model(self, conn: duckdb.DuckDBPyConnection, full_refresh: bool) -> None:
        if self.needs_python_df and self.result is None:
            local_conn = conn.cursor()
            try:
                self.result = await asyncio.to_thread(self._load_duckdb_view_to_python_df, local_conn)
            finally:
                local_conn.close()
        
        coroutines = []
        for model in self.downstreams_for_build.values():
            coroutines.append(model._trigger_build(conn, full_refresh))
        await u.asyncio_gather(coroutines)


@dataclass
class Seed(StaticModel):
    model_config: mc.SeedConfig
    result: pl.LazyFrame

    @property
    def model_type(self) -> ModelType:
        return ModelType.SEED
    
    async def build_model(self, conn: duckdb.DuckDBPyConnection, full_refresh: bool) -> None:
        start = time.time()

        print(f"[{u.get_current_time()}] 🔨 BUILDING: seed model '{self.name}'")
        await asyncio.to_thread(self._create_table_from_df, conn, self.result)

        print(f"[{u.get_current_time()}] ✅ FINISHED: seed model '{self.name}'")
        self.logger.log_activity_time(f"building seed model '{self.name}' to venv", start)

        await super().build_model(conn, full_refresh)


@dataclass
class SourceModel(StaticModel):
    model_config: src.Source

    @property
    def model_type(self) -> ModelType:
        return ModelType.SOURCE
    
    @property
    def is_queryable(self) -> bool:
        return self.model_config.load_to_duckdb
    
    def _build_source_model(self, conn: duckdb.DuckDBPyConnection, full_refresh: bool) -> None:
        local_conn = conn.cursor()
        try:
            source = self.model_config
            conn_name = source.get_connection()
            
            connection_props = self.conn_set.get_connection(conn_name)
            if isinstance(connection_props, ConnectionProperties):
                dialect = connection_props.dialect
            else:
                raise u.ConfigurationError(f'Unable to use connection "{conn_name}" for source "{self.name}"')
            
            result = u.run_duckdb_stmt(self.logger, local_conn, f"FROM (SHOW DATABASES) WHERE database_name = 'db_{conn_name}'").fetchone()
            if result is None:
                return # skip this source if connection is not attached
            
            table_name = source.get_table()
            new_table_name = self.name

            if len(source.columns) == 0:
                stmt = f"CREATE OR REPLACE TABLE {new_table_name} AS SELECT * FROM db_{conn_name}.{table_name}"
                u.run_duckdb_stmt(self.logger, local_conn, stmt)
                return
            
            increasing_column = source.update_hints.increasing_column
            recreate_table = full_refresh or increasing_column is None
            if recreate_table:
                u.run_duckdb_stmt(self.logger, local_conn, f"DROP TABLE IF EXISTS {new_table_name}")

            create_table_cols_clause = source.get_cols_for_create_table_stmt()
            stmt = f"CREATE TABLE IF NOT EXISTS {new_table_name} ({create_table_cols_clause})"
            u.run_duckdb_stmt(self.logger, local_conn, stmt)
        
            if not recreate_table:
                if source.update_hints.selective_overwrite_value is not None:
                    stmt = f"DELETE FROM {new_table_name} WHERE {increasing_column} >= $value"
                    u.run_duckdb_stmt(self.logger, local_conn, stmt, params={"value": source.update_hints.selective_overwrite_value})
                elif not source.update_hints.strictly_increasing:
                    stmt = f"DELETE FROM {new_table_name} WHERE {increasing_column} = ({source.get_max_incr_col_query(new_table_name)})"
                    u.run_duckdb_stmt(self.logger, local_conn, stmt)
            
            max_val_of_incr_col = None
            if increasing_column is not None:
                max_val_of_incr_col_tuple = u.run_duckdb_stmt(self.logger, local_conn, source.get_max_incr_col_query(new_table_name)).fetchone()
                max_val_of_incr_col = max_val_of_incr_col_tuple[0] if isinstance(max_val_of_incr_col_tuple, tuple) else None
                if max_val_of_incr_col is None:
                    recreate_table = True

            insert_cols_clause = source.get_cols_for_insert_stmt()
            insert_replace_clause = source.get_insert_replace_clause()
            query = source.get_query_for_insert(dialect, conn_name, table_name, max_val_of_incr_col, full_refresh=recreate_table)
            stmt = f"INSERT {insert_replace_clause} INTO {new_table_name} ({insert_cols_clause}) {query}"
            u.run_duckdb_stmt(self.logger, local_conn, stmt)
        finally:
            local_conn.close()

    async def build_model(self, conn: duckdb.DuckDBPyConnection, full_refresh: bool) -> None:
        if self.model_config.load_to_duckdb:
            start = time.time()
            print(f"[{u.get_current_time()}] 🔨 BUILDING: source model '{self.name}'")

            await asyncio.to_thread(self._build_source_model, conn, full_refresh)
            
            print(f"[{u.get_current_time()}] ✅ FINISHED: source model '{self.name}'")
            self.logger.log_activity_time(f"building source model '{self.name}' to venv", start)

            await super().build_model(conn, full_refresh)
        

@dataclass
class QueryModel(DataModel):
    model_config: mc.QueryModelConfig
    query_file: mq.QueryFile
    compiled_query: mq.Query | None = field(default=None, init=False)
    _: KW_ONLY
    j2_env: u.j2.Environment = field(default_factory=lambda: u.j2.Environment(loader=u.j2.FileSystemLoader(".")))

    def _add_upstream(self, other: DataModel) -> None:
        self.upstreams[other.name] = other
        other.downstreams[self.name] = self
        
        if isinstance(self.query_file, mq.PyQueryFile):
            other.needs_python_df = True

    def _ref_for_sql(self, dependent_model_name: str, models_dict: dict[str, DataModel]) -> str:
        if dependent_model_name not in models_dict:
            raise u.ConfigurationError(f'Model "{self.name}" references unknown model "{dependent_model_name}"')
        
        dep_model = models_dict[dependent_model_name]
        if isinstance(dep_model, SourceModel) and not dep_model.model_config.load_to_duckdb:
            raise u.ConfigurationError(
                f'Model "{self.name}" cannot reference source model "{dependent_model_name}" which has load_to_duckdb=False'
            )
        
        self.model_config.depends_on.add(dependent_model_name)
        return dependent_model_name
    
    def _ref_for_python(self, dependent_model_name: str) -> pl.LazyFrame:
        if dependent_model_name not in self.upstreams:
            raise u.ConfigurationError(f'Model "{self.name}" must include model "{dependent_model_name}" as a dependency to use')
        df = self.upstreams[dependent_model_name].result
        assert df is not None
        return df

    def _get_compile_sql_model_args_from_ctx_args(
        self, ctx: dict[str, Any], ctx_args: ContextArgs
    ) -> dict[str, Any]:
        is_placeholder = lambda placeholder: placeholder in ctx_args.placeholders
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
            raise FileExecutionError(f'Failed to compile sql model "{self.name}"', e) from e
        return query
    
    def process_pass_through_columns(self, models_dict: dict[str, DataModel]) -> None:
        if getattr(self, "processed_pass_through_columns", False):
            return
        
        for col in self.model_config.columns:
            if col.pass_through:
                # Validate pass-through column has exactly one dependency
                if len(col.depends_on) != 1:
                    raise u.ConfigurationError(
                        f'Column "{self.name}.{col.name}" has pass_through=true, which must have exactly one depends_on value'
                    )
                
                # Get the upstream column reference
                upstream_col_ref = next(iter(col.depends_on))
                table_name, col_name = upstream_col_ref.split('.')
                self.model_config.depends_on.add(table_name)
                
                # Get the upstream model
                if table_name not in models_dict:
                    raise u.ConfigurationError(
                        f'Column "{self.name}.{col.name}" depends on unknown model "{table_name}"'
                    )
        
        # Do not rely on self.upstreams here, as it may not be fully populated for metadata passthrough purposes
        for dep_model_name in self.model_config.depends_on:
            dep_model = models_dict[dep_model_name]
            dep_model.process_pass_through_columns(models_dict)
        
        for col in self.model_config.columns:
            if col.pass_through:
                upstream_col_ref = next(iter(col.depends_on))
                table_name, col_name = upstream_col_ref.split('.')
                upstream_model = models_dict[table_name]
                
                # Find the upstream column config
                upstream_col = next(
                    (c for c in upstream_model.model_config.columns if c.name == col_name),
                    None
                )
                if upstream_col is None:
                    raise u.ConfigurationError(
                        f'Column "{self.name}.{col.name}" depends on unknown column "{upstream_col_ref}"'
                    )
                
                # Copy metadata from upstream column
                col.type = upstream_col.type if col.type == "" else col.type
                col.condition = upstream_col.condition if col.condition == "" else col.condition
                col.description = upstream_col.description if col.description == "" else col.description
                col.category = upstream_col.category if col.category == mc.ColumnCategory.MISC else col.category

        self.processed_pass_through_columns = True
        
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
    sources: dict[str, src.Source] = field(default_factory=dict, init=False)

    @property
    def model_type(self) -> ModelType:
        return ModelType.DBVIEW

    def _get_compile_sql_model_args(
        self, ctx: dict[str, Any], ctx_args: ContextArgs, models_dict: dict[str, DataModel]
    ) -> dict[str, Any]:
        kwargs = self._get_compile_sql_model_args_from_ctx_args(ctx, ctx_args)
        
        def source(source_name: str) -> str:
            if source_name not in models_dict or not isinstance(source_model := models_dict[source_name], SourceModel):
                raise u.ConfigurationError(f'Dbview "{self.name}" references unknown source "{source_name}"')
            if source_model.model_config.get_connection() != self.model_config.get_connection():
                raise u.ConfigurationError(f'Dbview "{self.name}" references source "{source_name}" with different connection')
            
            # Check if the source model has load_to_duckdb=False but this dbview has translate_to_duckdb=True
            if not source_model.model_config.load_to_duckdb and self.model_config.translate_to_duckdb:
                raise u.ConfigurationError(
                    f'Dbview "{self.name}" with translate_to_duckdb=True cannot reference source "{source_name}" '
                    f'which has load_to_duckdb=False'
                )
                
            self.model_config.depends_on.add(source_name)
            self.sources[source_name] = source_model.model_config
            return "{{ source(\"" + source_name + "\") }}"
        
        kwargs["source"] = source
        return kwargs

    def _get_duckdb_query(self, read_dialect: str, query: str) -> str:
        kwargs = {
            "source": lambda source_name: "venv." + source_name
        }
        compiled_query = self._get_compiled_sql_query_str(query, kwargs)
        return sqlglot.transpile(compiled_query, read=read_dialect, write="duckdb")[0]
    
    def _compile_sql_model(self, kwargs: dict[str, Any]) -> mq.SqlModelQuery:
        compiled_query_str = self._get_compiled_sql_query_str(self.query_file.raw_query, kwargs)

        connection_name = self.model_config.get_connection()
        connection_props = self.conn_set.get_connection(connection_name)
        
        if self.model_config.translate_to_duckdb and isinstance(connection_props, ConnectionProperties):
            macros = {
                "source": lambda source_name: "venv." + source_name
            }
            compiled_query2 = self._get_compiled_sql_query_str(compiled_query_str, macros)
            compiled_query_str = self._get_duckdb_query(connection_props.dialect, compiled_query2)
            is_duckdb = True
        else:
            macros = {
                "source": lambda source_name: self.sources[source_name].get_table()
            }
            compiled_query_str = self._get_compiled_sql_query_str(compiled_query_str, macros)
            is_duckdb = False
        
        compiled_query = mq.SqlModelQuery(compiled_query_str, is_duckdb)
        return compiled_query
    
    def compile(
        self, ctx: dict[str, Any], ctx_args: ContextArgs, models_dict: dict[str, DataModel], recurse: bool
    ) -> None:
        if self.compiled_query is not None:
            return
        else:
            self.compiled_query = mq.WorkInProgress() # type: ignore
        
        start = time.time()

        kwargs = self._get_compile_sql_model_args(ctx, ctx_args, models_dict)
        self.compiled_query = self._compile_sql_model(kwargs)
        
        self.logger.log_activity_time(f"compiling dbview model '{self.name}'", start)
    
    async def _run_sql_model(self, conn: duckdb.DuckDBPyConnection, placeholders: dict = {}) -> None:
        assert self.compiled_query is not None
        is_duckdb = self.compiled_query.is_duckdb
        query = self.compiled_query.query
        connection_name = self.model_config.get_connection()
        
        def run_sql_query_on_connection(is_duckdb: bool, query: str, placeholders: dict) -> pl.DataFrame:
            try:
                if is_duckdb:
                    local_conn = conn.cursor()
                    try:
                        self.logger.info(f"Running duckdb query: {query}")
                        return local_conn.sql(query, params=placeholders).pl()
                    except duckdb.CatalogException as e:
                        raise InvalidInputError(61, f'Model "{self.name}" depends on static data models that cannot be found.')
                    except Exception as e:
                        raise RuntimeError(e)
                    finally:
                        local_conn.close()
                else:
                    return self._run_sql_query_on_connection(connection_name, query, placeholders)
            except RuntimeError as e:
                raise FileExecutionError(f'Failed to run dbview sql model "{self.name}"', e)
        
        result = await asyncio.to_thread(run_sql_query_on_connection, is_duckdb, query, placeholders)
        self.result = result.lazy()

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

    def _get_compile_sql_model_args(
        self, ctx: dict[str, Any], ctx_args: ContextArgs, models_dict: dict[str, DataModel]
    ) -> dict[str, Any]:
        kwargs = self._get_compile_sql_model_args_from_ctx_args(ctx, ctx_args)
        
        def ref(dependent_model_name: str) -> str:
            dependent_model = self._ref_for_sql(dependent_model_name, models_dict)
            prefix = "venv." if isinstance(models_dict[dependent_model], (SourceModel, BuildModel)) else ""
            return prefix + dependent_model
        
        kwargs["ref"] = ref
        return kwargs

    def _compile_sql_model(
        self, query_file: mq.SqlQueryFile, ctx: dict[str, Any], ctx_args: ContextArgs, models_dict: dict[str, DataModel]
    ) -> mq.SqlModelQuery:
        kwargs = self._get_compile_sql_model_args(ctx, ctx_args, models_dict)
        compiled_query_str = self._get_compiled_sql_query_str(query_file.raw_query, kwargs)
        compiled_query = mq.SqlModelQuery(compiled_query_str, is_duckdb=True)
        return compiled_query
    
    def _get_python_model_args(self, ctx: dict[str, Any], ctx_args: ContextArgs) -> ModelArgs:
        dependencies = self.model_config.depends_on
        connections = self.conn_set.get_connections_as_dict()
        
        def run_external_sql(connection_name: str, sql_query: str) -> pl.DataFrame:
            return self._run_sql_query_on_connection(connection_name, sql_query, ctx_args.placeholders)
        
        conn_args = ConnectionsArgs(ctx_args.project_path, ctx_args.proj_vars, ctx_args.env_vars)
        build_model_args = BuildModelArgs(conn_args, connections, dependencies, self._ref_for_python, run_external_sql)
        return ModelArgs(ctx_args, build_model_args, ctx)

    def _compile_python_model(
        self, query_file: mq.PyQueryFile, ctx: dict[str, Any], ctx_args: ContextArgs
    ) -> mq.PyModelQuery:
        sqrl_args = self._get_python_model_args(ctx, ctx_args)
            
        def compiled_query() -> pl.LazyFrame | pd.DataFrame:
            try:
                return query_file.raw_query(sqrl_args)
            except Exception as e:
                raise FileExecutionError(f'Failed to run "{c.MAIN_FUNC}" function for python model "{self.name}"', e) from e
        
        return mq.PyModelQuery(compiled_query)
    
    def compile(
        self, ctx: dict[str, Any], ctx_args: ContextArgs, models_dict: dict[str, DataModel], recurse: bool
    ) -> None:
        if self.compiled_query is not None:
            return
        else:
            self.compiled_query = mq.WorkInProgress() # type: ignore
        
        start = time.time()

        if isinstance(self.query_file, mq.SqlQueryFile):
            self.compiled_query = self._compile_sql_model(self.query_file, ctx, ctx_args, models_dict)
        elif isinstance(self.query_file, mq.PyQueryFile):
            self.compiled_query = self._compile_python_model(self.query_file, ctx, ctx_args)
        else:
            raise NotImplementedError(f"Query type not supported: {self.query_file.__class__.__name__}")
        
        self.logger.log_activity_time(f"compiling federate model '{self.name}'", start)
        
        if not recurse:
            return 
        
        dependencies = self.model_config.depends_on
        self.wait_count = len(dependencies)

        for name in dependencies:
            dep_model = models_dict[name]
            self._add_upstream(dep_model)
            dep_model.compile(ctx, ctx_args, models_dict, recurse)

    async def _run_sql_model(self, compiled_query: mq.SqlModelQuery, conn: duckdb.DuckDBPyConnection, placeholders: dict = {}) -> None:
        local_conn = conn.cursor()
        try:
            self.register_all_upstream_python_df(local_conn)
            query = compiled_query.query

            def create_table(local_conn: duckdb.DuckDBPyConnection):
                placeholer_exists = lambda key: re.search(r"\$" + key + r"(?!\w)", query)
                existing_placeholders = {key: value for key, value in placeholders.items() if placeholer_exists(key)}

                create_query = self.model_config.get_sql_for_create(self.name, query)
                try:
                    return local_conn.execute(create_query, existing_placeholders)
                except duckdb.CatalogException as e:
                    raise InvalidInputError(61, f'Model "{self.name}" depends on static data models that cannot be found.')
                except Exception as e:
                    if self.name == "__fake_target":
                        raise InvalidInputError(204, f"Failed to run provided SQL query")
                    else:
                        raise FileExecutionError(f'Failed to run federate sql model "{self.name}"', e) from e
            
            await asyncio.to_thread(create_table, local_conn)
            if self.needs_python_df or self.is_target:
                self.result = await asyncio.to_thread(self._load_duckdb_view_to_python_df, local_conn)
        finally:
            local_conn.close()

    async def _run_python_model(self, compiled_query: mq.PyModelQuery) -> None:
        query_result = await asyncio.to_thread(compiled_query.query)
        if isinstance(query_result, pd.DataFrame):
            query_result = pl.from_pandas(query_result)
        
        self.result = query_result.lazy()

    async def run_model(self, conn: duckdb.DuckDBPyConnection, placeholders: dict = {}) -> None:
        start = time.time()
        
        if isinstance(self.compiled_query, mq.SqlModelQuery):
            await self._run_sql_model(self.compiled_query, conn, placeholders)
        elif isinstance(self.compiled_query, mq.PyModelQuery):
            await self._run_python_model(self.compiled_query)
        else:
            raise NotImplementedError(f"Query type not supported: {self.query_file.__class__.__name__}")
        
        self.logger.log_activity_time(f"running federate model '{self.name}'", start)
        
        await super().run_model(conn, placeholders)
    

@dataclass
class BuildModel(StaticModel, QueryModel):
    model_config: mc.BuildModelConfig
    query_file: mq.SqlQueryFile | mq.PyQueryFile
    compiled_query: mq.SqlModelQuery | mq.PyModelQuery | None = field(default=None, init=False)

    @property
    def model_type(self) -> ModelType:
        return ModelType.BUILD
    
    def _add_upstream_for_build(self, other: StaticModel) -> None:
        self.upstreams_for_build[other.name] = other
        other.downstreams_for_build[self.name] = self
        
        if isinstance(self.query_file, mq.PyQueryFile):
            other.needs_python_df_for_build = True
    
    def _get_compile_sql_model_args(
        self, conn_args: ConnectionsArgs, models_dict: dict[str, StaticModel]
    ) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "proj_vars": conn_args.proj_vars, "env_vars": conn_args.env_vars
        }
        
        def ref_for_build(dependent_model_name: str) -> str:
            dependent_model = self._ref_for_sql(dependent_model_name, dict(models_dict))
            return dependent_model
        
        kwargs["ref"] = ref_for_build
        return kwargs

    def _compile_sql_model(
        self, query_file: mq.SqlQueryFile, conn_args: ConnectionsArgs, models_dict: dict[str, StaticModel]
    ) -> mq.SqlModelQuery:
        kwargs = self._get_compile_sql_model_args(conn_args, models_dict)
        compiled_query_str = self._get_compiled_sql_query_str(query_file.raw_query, kwargs)
        compiled_query = mq.SqlModelQuery(compiled_query_str, is_duckdb=True)
        return compiled_query
    
    def _ref_for_python(self, dependent_model_name: str) -> pl.LazyFrame:
        if dependent_model_name not in self.upstreams_for_build:
            raise u.ConfigurationError(f'Model "{self.name}" must include model "{dependent_model_name}" as a dependency to use')
        df = self.upstreams_for_build[dependent_model_name].result
        assert df is not None
        return df
    
    def _get_compile_python_model_args(self, conn_args: ConnectionsArgs) -> BuildModelArgs:
        
        def run_external_sql(connection_name: str, sql_query: str):
            return self._run_sql_query_on_connection(connection_name, sql_query)
        
        return BuildModelArgs(
            conn_args, self.conn_set.get_connections_as_dict(), self.model_config.depends_on, self._ref_for_python, run_external_sql
        )

    def _compile_python_model(
        self, query_file: mq.PyQueryFile, conn_args: ConnectionsArgs
    ) -> mq.PyModelQuery:
        sqrl_args = self._get_compile_python_model_args(conn_args)
            
        def compiled_query() -> pl.LazyFrame | pd.DataFrame:
            try:
                return query_file.raw_query(sqrl_args)
            except Exception as e:
                raise FileExecutionError(f'Failed to run "{c.MAIN_FUNC}" function for build model "{self.name}"', e)
        
        return mq.PyModelQuery(compiled_query)
    
    def compile_for_build(self, conn_args: ConnectionsArgs, models_dict: dict[str, StaticModel]) -> None:
        start = time.time()

        if isinstance(self.query_file, mq.SqlQueryFile):
            self.compiled_query = self._compile_sql_model(self.query_file, conn_args, models_dict)
        elif isinstance(self.query_file, mq.PyQueryFile):
            self.compiled_query = self._compile_python_model(self.query_file, conn_args)
        else:
            raise NotImplementedError(f"Query type not supported: {self.query_file.__class__.__name__}")
        
        self.logger.log_activity_time(f"compiling build model '{self.name}'", start)
        
        dependencies = self.model_config.depends_on
        self.wait_count_for_build = len(dependencies)

        for name in dependencies:
            dep_model = models_dict[name]
            self._add_upstream_for_build(dep_model)

    async def _build_sql_model(self, compiled_query: mq.SqlModelQuery, conn: duckdb.DuckDBPyConnection) -> None:
        query = compiled_query.query

        def create_table():
            create_query = self.model_config.get_sql_for_build(self.name, query)
            local_conn = conn.cursor()
            try:
                return u.run_duckdb_stmt(self.logger, local_conn, create_query)
            except Exception as e:
                raise FileExecutionError(f'Failed to build static sql model "{self.name}"', e) from e
            finally:
                local_conn.close()
        
        await asyncio.to_thread(create_table)

    async def _build_python_model(self, compiled_query: mq.PyModelQuery, conn: duckdb.DuckDBPyConnection) -> None:
        query_result = await asyncio.to_thread(compiled_query.query)
        if isinstance(query_result, pd.DataFrame):
            query_result = pl.from_pandas(query_result).lazy()
        if self.needs_python_df_for_build:
            self.result = query_result.lazy()
        await asyncio.to_thread(self._create_table_from_df, conn, query_result)

    async def build_model(self, conn: duckdb.DuckDBPyConnection, full_refresh: bool) -> None:
        start = time.time()
        print(f"[{u.get_current_time()}] 🔨 BUILDING: build model '{self.name}'")
        
        if isinstance(self.compiled_query, mq.SqlModelQuery):
            await self._build_sql_model(self.compiled_query, conn)
        elif isinstance(self.compiled_query, mq.PyModelQuery):
            # First ensure all upstream models have an associated Python dataframe
            def load_df(conn: duckdb.DuckDBPyConnection, dep_model: DataModel):
                if dep_model.result is None:
                    local_conn = conn.cursor()
                    try:
                        dep_model.result = dep_model._load_duckdb_view_to_python_df(local_conn)
                    finally:
                        local_conn.close()
                
            coroutines = []
            for dep_model in self.upstreams_for_build.values():
                coro = asyncio.to_thread(load_df, conn, dep_model)
                coroutines.append(coro)
            await u.asyncio_gather(coroutines)
            
            # Then run the model's Python function to build the model
            await self._build_python_model(self.compiled_query, conn)
        else:
            raise NotImplementedError(f"Query type not supported: {self.query_file.__class__.__name__}")
        
        print(f"[{u.get_current_time()}] ✅ FINISHED: build model '{self.name}'")
        self.logger.log_activity_time(f"building static build model '{self.name}'", start)
        
        await super().build_model(conn, full_refresh)


@dataclass
class DAG:
    dataset: DatasetConfig | None
    target_model: DataModel
    models_dict: dict[str, DataModel]
    duckdb_filepath: str = field(default="")
    logger: u.Logger = field(default_factory=lambda: u.Logger(""))
    parameter_set: ParameterSet | None = field(default=None, init=False) # set in apply_selections
    placeholders: dict[str, Any] = field(init=False, default_factory=dict)

    def _get_msg_extension(self) -> str:
        return f" for dataset '{self.dataset.name}'" if self.dataset else ""
    
    def compile_build_models(self, conn_args: ConnectionsArgs) -> None:
        static_models: dict[str, StaticModel] = {k: v for k, v in self.models_dict.items() if isinstance(v, StaticModel)}
        for model in static_models.values():
            if isinstance(model, BuildModel):
                model.compile_for_build(conn_args, static_models)

    def apply_selections(
        self, param_cfg_set: ParameterConfigsSet, user: BaseUser | None, selections: dict[str, str]
    ) -> None:
        start = time.time()
        dataset_params = self.dataset.parameters if self.dataset else None
        parameter_set = param_cfg_set.apply_selections(dataset_params, selections, user)
        self.parameter_set = parameter_set
        msg_extension = self._get_msg_extension()
        self.logger.log_activity_time("applying selections" + msg_extension, start)
    
    def _compile_context(
        self, param_args: ParametersArgs, context_func: ContextFunc, user: BaseUser | None, default_traits: dict[str, Any]
    ) -> tuple[dict[str, Any], ContextArgs]:
        start = time.time()
        context = {}
        assert isinstance(self.parameter_set, ParameterSet)
        prms = self.parameter_set.get_parameters_as_dict()
        traits = self.dataset.traits if self.dataset else default_traits
        args = ContextArgs(param_args, user, prms, traits)
        msg_extension = self._get_msg_extension()
        try:
            context_func(context, args)
        except Exception as e:
            raise FileExecutionError(f'Failed to run {c.CONTEXT_FILE}' + msg_extension, e) from e
        self.logger.log_activity_time("running context.py" + msg_extension, start)
        return context, args
    
    def _compile_models(self, context: dict[str, Any], ctx_args: ContextArgs, recurse: bool) -> None:
        self.target_model.compile(context, ctx_args, self.models_dict, recurse)
    
    def _get_terminal_nodes(self) -> set[str]:
        start = time.time()
        terminal_nodes = self.target_model.get_terminal_nodes(set())
        for model in self.models_dict.values():
            model.confirmed_no_cycles = False
        self.logger.log_activity_time(f"validating no cycles in model dependencies", start)
        return terminal_nodes

    async def _run_models(self) -> None:
        terminal_nodes = self._get_terminal_nodes()

        # create an empty duckdb venv file if it does not exist
        try:
            conn = duckdb.connect(self.duckdb_filepath)
            conn.close()
        except duckdb.IOException as e:
            # unable to create duckdb venv file means it's in use and already exists
            # do not throw error here since attaching in read-only mode later may still work
            pass
        
        conn = u.create_duckdb_connection()
        try:
            read_only = "(READ_ONLY)" if self.duckdb_filepath else ""
            try:
                conn.execute(f"ATTACH '{self.duckdb_filepath}' AS venv {read_only}")
            except duckdb.IOException as e:
                self.logger.warn(f"Unable to attach to duckdb venv file: {self.duckdb_filepath}")
                raise e
            
            coroutines = []
            for model_name in terminal_nodes:
                model = self.models_dict[model_name] if model_name != "__fake_target" else self.target_model
                coroutines.append(model.run_model(conn, self.placeholders))
            await u.asyncio_gather(coroutines)
            
        finally:
            conn.close()
    
    async def execute(
        self, param_args: ParametersArgs, param_cfg_set: ParameterConfigsSet, context_func: ContextFunc, user: BaseUser | None, selections: dict[str, str], 
        *, runquery: bool = True, recurse: bool = True, default_traits: dict[str, Any] = {}
    ) -> None:
        recurse = (recurse or runquery)

        self.apply_selections(param_cfg_set, user, selections)

        context, ctx_args = self._compile_context(param_args, context_func, user, default_traits)

        self._compile_models(context, ctx_args, recurse)
        
        self.placeholders = ctx_args.placeholders
        if runquery:
            await self._run_models()
        
        self.target_model.process_pass_through_columns(self.models_dict)
    
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
    
    def get_all_data_models(self) -> list[arm.DataModelItem]:
        data_models = []
        for model_name, model in self.models_dict.items():
            is_queryable = model.is_queryable
            data_model = arm.DataModelItem(name=model_name, model_type=model.model_type.value, config=model.model_config, is_queryable=is_queryable)
            data_models.append(data_model)
        return data_models
    
    def get_all_model_lineage(self) -> list[arm.LineageRelation]:
        model_lineage = []
        for model_name, model in self.models_dict.items():
            if not isinstance(model, QueryModel):
                continue
            for dep_model_name in model.model_config.depends_on:
                edge_type = "buildtime" if isinstance(model, BuildModel) else "runtime"
                source_model = arm.LineageNode(name=dep_model_name, type="model")
                target_model = arm.LineageNode(name=model_name, type="model")
                model_lineage.append(arm.LineageRelation(type=edge_type, source=source_model, target=target_model))
        return model_lineage


class ModelsIO:

    @classmethod
    def _load_model_config(cls, filepath: Path, model_type: ModelType, env_vars: dict[str, str]) -> mc.ModelConfig:
        yaml_path = filepath.with_suffix('.yml')
        config_dict = u.load_yaml_config(yaml_path) if yaml_path.exists() else {}
        
        if model_type == ModelType.DBVIEW:
            config = mc.DbviewModelConfig(**config_dict).finalize_connection(env_vars)
            return config
        elif model_type == ModelType.FEDERATE:
            return mc.FederateModelConfig(**config_dict)
        elif model_type == ModelType.BUILD:
            return mc.BuildModelConfig(**config_dict)
        else:
            return mc.ModelConfig(**config_dict)

    @classmethod
    def _populate_from_file(
        cls, raw_queries_by_model: dict[str, mq.QueryFileWithConfig], dp: str, file: str, model_type: ModelType, env_vars: dict[str, str]
    ) -> None:
        filepath = Path(dp, file)
        file_stem, extension = os.path.splitext(file)
        
        if extension == '.py':
            module = pm.PyModule(filepath)
            raw_query = module.get_func_or_class(c.MAIN_FUNC)
            query_file = mq.PyQueryFile(filepath.as_posix(), raw_query)
        elif extension == '.sql':
            query_file = mq.SqlQueryFile(filepath.as_posix(), filepath.read_text())
        else:
            return # Skip files that are not query files
            
        if file_stem in raw_queries_by_model:
            assert isinstance(prior_query_file := raw_queries_by_model[file_stem].query_file, mq.QueryFile)
            conflicts = [prior_query_file.filepath, query_file.filepath]
            raise u.ConfigurationError(f"Multiple models found for '{file_stem}': {conflicts}")
        
        model_config = cls._load_model_config(filepath, model_type, env_vars)
        raw_queries_by_model[file_stem] = mq.QueryFileWithConfig(query_file, model_config)

    @classmethod
    def _populate_raw_queries_for_type(
        cls, folder_path: Path, model_type: ModelType, *, env_vars: dict[str, str] = {}
    ) -> dict[str, mq.QueryFileWithConfig]:
        raw_queries_by_model: dict[str, mq.QueryFileWithConfig] = {}
        for dp, _, filenames in os.walk(folder_path):
            for file in filenames:
                cls._populate_from_file(raw_queries_by_model, dp, file, model_type, env_vars)
        return raw_queries_by_model

    @classmethod
    def load_build_files(cls, logger: u.Logger, base_path: str) -> dict[str, mq.QueryFileWithConfig]:
        start = time.time()
        builds_path = u.Path(base_path, c.MODELS_FOLDER, c.BUILDS_FOLDER)
        raw_queries_by_model = cls._populate_raw_queries_for_type(builds_path, ModelType.BUILD)
        logger.log_activity_time("loading build files", start)
        return raw_queries_by_model

    @classmethod
    def load_dbview_files(cls, logger: u.Logger, base_path: str, env_vars: dict[str, str]) -> dict[str, mq.QueryFileWithConfig]:
        start = time.time()
        dbviews_path = u.Path(base_path, c.MODELS_FOLDER, c.DBVIEWS_FOLDER)
        raw_queries_by_model = cls._populate_raw_queries_for_type(dbviews_path, ModelType.DBVIEW, env_vars=env_vars)
        logger.log_activity_time("loading dbview files", start)
        return raw_queries_by_model

    @classmethod
    def load_federate_files(cls, logger: u.Logger, base_path: str) -> dict[str, mq.QueryFileWithConfig]:
        start = time.time()
        federates_path = u.Path(base_path, c.MODELS_FOLDER, c.FEDERATES_FOLDER)
        raw_queries_by_model = cls._populate_raw_queries_for_type(federates_path, ModelType.FEDERATE)
        logger.log_activity_time("loading federate files", start)
        return raw_queries_by_model

    @classmethod
    def load_context_func(cls, logger: u.Logger, base_path: str) -> ContextFunc:
        start = time.time()

        context_path = u.Path(base_path, c.PYCONFIGS_FOLDER, c.CONTEXT_FILE)
        context_func: ContextFunc = pm.PyModule(context_path).get_func_or_class(c.MAIN_FUNC, default_attr=lambda ctx, sqrl: None)

        logger.log_activity_time("loading file for context.py", start)
        return context_func
    