from typing import Any
from pydantic import BaseModel, Field
import time

from . import _utils as u, _constants as c, _model_configs as mc


class UpdateHints(BaseModel):
    increasing_column: str | None = Field(default=None)
    strictly_increasing: bool = Field(default=True, description="Delete the max value of the increasing column, ignored if value is set")
    selective_overwrite_value: Any = Field(default=None)


class Source(mc.DbviewModelConfig):
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

    def get_insert_where_cond(self, *, full_refresh: bool = True) -> str:
        increasing_col = self.update_hints.increasing_column
        if full_refresh:
            return "true"
        increasing_col_type = next(col.type for col in self.columns if col.name == increasing_col)
        return f"{increasing_col}::{increasing_col_type} > ({self.get_max_incr_col_query()})"
    
    def get_insert_on_conflict_clause(self) -> str:
        if len(self.primary_key) == 0:
            return ""
        set_clause = ", ".join([f'{col.name} = EXCLUDED.{col.name}' for col in self.columns if col.name not in self.primary_key])
        return f"ON CONFLICT DO UPDATE SET {set_clause}"


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
        
        sources_data = u.load_yaml_config(u.Path(base_path, c.MODELS_FOLDER, "sources.yml"))
        sources = Sources(**sources_data)
        
        logger.log_activity_time("loading sources", start)
        return sources
