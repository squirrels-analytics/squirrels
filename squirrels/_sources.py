from typing import Any
from pydantic import BaseModel, Field, model_validator
import time, sqlglot

from . import _utils as u, _constants as c, _model_configs as mc


class UpdateHints(BaseModel):
    increasing_column: str | None = Field(default=None)
    strictly_increasing: bool = Field(default=True, description="Delete the max value of the increasing column, ignored if value is set")
    selective_overwrite_value: Any = Field(default=None)


class Source(mc.ConnectionInterface, mc.ModelConfig):
    table: str | None = Field(default=None)
    load_to_duckdb: bool = Field(default=False, description="Whether to load the data to DuckDB")
    primary_key: list[str] = Field(default_factory=list)
    update_hints: UpdateHints = Field(default_factory=UpdateHints)

    def finalize_table(self, source_name: str):
        if self.table is None:
            self.table = source_name
        return self
    
    def get_table(self) -> str:
        assert self.table is not None, "Table must be set"
        return self.table
    
    def get_cols_for_create_table_stmt(self) -> str:
        cols_clause = ", ".join([f"{col.name} {col.type}" for col in self.columns])
        primary_key_clause = f", PRIMARY KEY ({', '.join(self.primary_key)})" if self.primary_key else ""
        return f"{cols_clause}{primary_key_clause}"
    
    def get_cols_for_insert_stmt(self) -> str:
        return ", ".join([col.name for col in self.columns])
    
    def get_max_incr_col_query(self, source_name: str) -> str:
        return f"SELECT max({self.update_hints.increasing_column}) FROM {source_name}"
    
    def get_query_for_insert(self, dialect: str, conn_name: str, table_name: str, max_value_of_increasing_col: Any | None, *, full_refresh: bool = True) -> str:
        select_cols = self.get_cols_for_insert_stmt()
        if full_refresh or max_value_of_increasing_col is None:
            return f"SELECT {select_cols} FROM db_{conn_name}.{table_name}"
        
        increasing_col = self.update_hints.increasing_column
        increasing_col_type = next(col.type for col in self.columns if col.name == increasing_col)
        where_cond = f"{increasing_col}::{increasing_col_type} > '{max_value_of_increasing_col}'::{increasing_col_type}"
        pushdown_query = f"SELECT {select_cols} FROM {table_name} WHERE {where_cond}"
        
        if dialect in ['postgres', 'mysql']:
            transpiled_query = sqlglot.transpile(pushdown_query, read='duckdb', write=dialect)[0].replace("'", "''")
            return f"FROM {dialect}_query('db_{conn_name}', '{transpiled_query}')"
        
        return f"SELECT {select_cols} FROM db_{conn_name}.{table_name} WHERE {where_cond}"
    
    def get_insert_replace_clause(self) -> str:
        return "" if len(self.primary_key) == 0 else "OR REPLACE"


class Sources(BaseModel):
    sources: dict[str, Source] = Field(default_factory=dict)
    
    @model_validator(mode="before")
    @classmethod
    def convert_sources_list_to_dict(cls, data: dict[str, Any]) -> dict[str, Any]:
        if "sources" in data and isinstance(data["sources"], list):
            # Convert list of sources to dictionary
            sources_dict = {}
            for source in data["sources"]:
                if isinstance(source, dict) and "name" in source:
                    name = source.pop("name")  # Remove name from source config
                    if name in sources_dict:
                        raise u.ConfigurationError(f"Duplicate source name found: {name}")
                    sources_dict[name] = source
                else:
                    raise u.ConfigurationError(f"All sources must have a name field in sources file")
            data["sources"] = sources_dict
        return data
    
    @model_validator(mode="after")
    def validate_column_types(self):
        for source_name, source in self.sources.items():
            for col in source.columns:
                if not col.type:
                    raise u.ConfigurationError(f"Column '{col.name}' in source '{source_name}' must have a type specified")
        return self
    
    def finalize_null_fields(self, env_vars: dict[str, str]):
        for source_name, source in self.sources.items():
            source.finalize_connection(env_vars)
            source.finalize_table(source_name)
        return self


class SourcesIO:
    @classmethod
    def load_file(cls, logger: u.Logger, base_path: str, env_vars: dict[str, str]) -> Sources:
        start = time.time()
        
        sources_path = u.Path(base_path, c.MODELS_FOLDER, c.SOURCES_FILE)
        sources_data = u.load_yaml_config(sources_path) if sources_path.exists() else {}
        
        sources = Sources(**sources_data).finalize_null_fields(env_vars)
        
        logger.log_activity_time("loading sources", start)
        return sources
