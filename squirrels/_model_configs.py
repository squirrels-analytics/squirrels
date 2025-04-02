from enum import Enum
from pydantic import BaseModel, Field

from . import _constants as c


class ColumnCategory(Enum):
    DIMENSION = "dimension"
    MEASURE = "measure"
    MISC = "misc"


class ColumnConfig(BaseModel):
    name: str = Field(description="The name of the column")
    type: str = Field(default="", description="The type of the column such as 'string', 'integer', 'float', 'boolean', 'datetime', etc.")
    condition: str = Field(default="", description="The condition of when the column is included")
    description: str = Field(default="", description="The description of the column")
    category: ColumnCategory = Field(default=ColumnCategory.MISC, description="The category of the column, either 'dimension', 'measure', or 'misc'")
    depends_on: set[str] = Field(default_factory=set, description="List of dependent columns")
    pass_through: bool = Field(default=False, description="Whether the column should be passed through to the federate")


class ModelConfig(BaseModel):
    description: str = Field(default="", description="The description of the model")
    columns: list[ColumnConfig] = Field(default_factory=list, description="The columns of the model")


class SeedConfig(ModelConfig):
    cast_column_types: bool = Field(default=False, description="Whether the column types should be cast to the appropriate type")


class ConnectionInterface(BaseModel):
    connection: str | None = Field(default=None, description="The connection name of the source model / database view")

    def finalize_connection(self, env_vars: dict[str, str]):
        if self.connection is None:
            self.connection = env_vars.get(c.SQRL_CONNECTIONS_DEFAULT_NAME_USED, "default")
        return self

    def get_connection(self) -> str:
        assert self.connection is not None, "Connection must be set"
        return self.connection
    

class QueryModelConfig(ModelConfig):
    depends_on: set[str] = Field(default_factory=set, description="The dependencies of the model")


class BuildModelConfig(QueryModelConfig):
    materialization: str = Field(default="TABLE", description="The materialization of the model (ignored if Python model which is always a table)")

    def get_sql_for_build(self, model_name: str, select_query: str) -> str:
        if self.materialization.upper() == "TABLE":
            materialization = "TABLE"
        elif self.materialization.upper() == "VIEW":
            materialization = "VIEW"
        else:
            raise ValueError(f"Invalid materialization: {self.materialization}")
        
        create_prefix = f"CREATE OR REPLACE {materialization} {model_name} AS\n"
        return create_prefix + select_query


class DbviewModelConfig(ConnectionInterface, QueryModelConfig):
    translate_to_duckdb: bool = Field(default=False, description="Whether to translate the query to DuckDB and use DuckDB tables at runtime")


class FederateModelConfig(QueryModelConfig):
    eager: bool = Field(default=False, description="Whether the model should be materialized for SQL models")

    def get_sql_for_create(self, model_name: str, select_query: str) -> str:
        materialization = "TABLE" if self.eager else "VIEW"
        create_prefix = f"CREATE {materialization} {model_name} AS\n"
        return create_prefix + select_query
