from typing import Any
from pydantic import BaseModel, Field
import time, sqlglot

from . import _utils as u, _constants as c, _model_configs as mc


class UpdateHints(BaseModel):
    increasing_column: str | None = Field(default=None)
    strictly_increasing: bool = Field(default=True, description="Delete the max value of the increasing column, ignored if value is set")
    selective_overwrite_value: Any = Field(default=None)


class Source(mc.ConnectionInterface, mc.ModelConfig):
    name: str
    table: str | None = Field(default=None)
    primary_key: list[str] = Field(default_factory=list)
    update_hints: UpdateHints = Field(default_factory=UpdateHints)

    def model_post_init(self, __context: Any) -> None:
        # Ensure all columns have a type specified
        for col in self.columns:
            if not col.type:
                raise u.ConfigurationError(f"Column '{col.name}' in source '{self.name}' must have a type specified")

    def get_table(self) -> str:
        if self.table is None:
            return self.name
        return self.table
    
    def get_cols_for_create_table_stmt(self) -> str:
        cols_clause = ", ".join([f"{col.name} {col.type}" for col in self.columns])
        primary_key_clause = f", PRIMARY KEY ({', '.join(self.primary_key)})" if self.primary_key else ""
        return f"{cols_clause}{primary_key_clause}"
    
    def get_cols_for_insert_stmt(self) -> str:
        return ", ".join([col.name for col in self.columns])
    
    def get_max_incr_col_query(self) -> str:
        return f"SELECT max({self.update_hints.increasing_column}) FROM {self.name}"
    
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
    sources: list[Source] = Field(default_factory=list)

    def model_post_init(self, __context: Any) -> None:
        source_names = {source.name for source in self.sources}
        if len(self.sources) != len(source_names):
            duplicate_names = [name for name in source_names if sum(1 for s in self.sources if s.name == name) > 1]
            raise u.ConfigurationError(f"Duplicate source names found: {duplicate_names}")


class SourcesIO:
    @classmethod
    def load_file(cls, logger: u.Logger, base_path: str) -> Sources:
        start = time.time()
        
        sources_path = u.Path(base_path, c.MODELS_FOLDER, c.SOURCES_FILE)
        sources_data = u.load_yaml_config(sources_path) if sources_path.exists() else {}
        sources = Sources(**sources_data)
        
        logger.log_activity_time("loading sources", start)
        return sources
