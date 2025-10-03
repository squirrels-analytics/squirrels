from typing import Any
from pydantic import BaseModel, Field, model_validator
import time, sqlglot, yaml

from . import _utils as u, _constants as c, _model_configs as mc


class UpdateHints(BaseModel):
    increasing_column: str | None = Field(default=None)
    strictly_increasing: bool = Field(default=True, description="Delete the max value of the increasing column, ignored if selective_overwrite_value is set")
    selective_overwrite_value: Any = Field(default=None, description="Delete all values of the increasing column greater than or equal to this value")


class Source(mc.ConnectionInterface, mc.ModelConfig):
    table: str | None = Field(default=None)
    load_to_vdl: bool = Field(default=False, description="Whether to load the data to the 'virtual data lake' (VDL)")
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
        return cols_clause
    
    def get_max_incr_col_query(self, source_name: str) -> str:
        return f"SELECT max({self.update_hints.increasing_column}) FROM {source_name}"
    
    def get_query_for_upsert(self, dialect: str, conn_name: str, table_name: str, max_value_of_increasing_col: Any | None, *, full_refresh: bool = True) -> str:
        select_cols = ", ".join([col.name for col in self.columns])
        if full_refresh or max_value_of_increasing_col is None:
            return f"SELECT {select_cols} FROM db_{conn_name}.{table_name}"
        
        increasing_col = self.update_hints.increasing_column
        increasing_col_type = next(col.type for col in self.columns if col.name == increasing_col)
        where_cond = f"{increasing_col}::{increasing_col_type} > '{max_value_of_increasing_col}'::{increasing_col_type}"
        
        # TODO: figure out if using pushdown query is worth it
        # if dialect in ['postgres', 'mysql']:
        #     pushdown_query = f"SELECT {select_cols} FROM {table_name} WHERE {where_cond}"
        #     transpiled_query = sqlglot.transpile(pushdown_query, read='duckdb', write=dialect)[0].replace("'", "''")
        #     return f"FROM {dialect}_query('db_{conn_name}', '{transpiled_query}')"
        
        return f"SELECT {select_cols} FROM db_{conn_name}.{table_name} WHERE {where_cond}"
    

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
        if sources_path.exists():
            raw_content = u.read_file(sources_path)
            rendered = u.render_string(raw_content, base_path=base_path, env_vars=env_vars)
            sources_data = yaml.safe_load(rendered) or {}
        else:
            sources_data = {}
        
        if not isinstance(sources_data, dict):
            raise u.ConfigurationError(
                f"Parsed content from YAML file must be a dictionary. Got: {sources_data}"
            )
        
        sources = Sources(**sources_data).finalize_null_fields(env_vars)
        
        logger.log_activity_time("loading sources", start)
        return sources
