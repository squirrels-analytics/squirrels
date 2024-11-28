from enum import Enum
from pydantic import BaseModel, Field


class ColumnCategory(Enum):
    DIMENSION = "dimension"
    MEASURE = "measure"
    MISC = "misc"


class ColumnConfig(BaseModel):
    name: str = Field(description="The name of the column")
    type: str | None = Field(default=None, description="The type of the column such as 'string', 'integer', 'float', 'boolean', 'datetime', etc.")
    condition: str = Field(default="", description="The condition of when the column is included")
    description: str = Field(default="", description="The description of the column")
    category: ColumnCategory = Field(default=ColumnCategory.MISC, description="The category of the column, either 'dimension', 'measure', or 'misc'")
    depends_on: set[str] = Field(default_factory=set, description="List of dependent columns")
    pass_through: bool = Field(default=False, description="Whether the column should be passed through to the federate")


class ModelConfig(BaseModel):
    description: str = Field(default="", description="The description of the model")
    columns: list[ColumnConfig] = Field(default_factory=list,description="The columns of the model")


class SeedConfig(ModelConfig):
    cast_column_types: bool = Field(default=False, description="Whether the column types should be cast to the appropriate type")


class QueryModelConfig(ModelConfig):
    depends_on: set[str] = Field(default_factory=set, description="The dependencies of the model")

class DbviewModelConfig(QueryModelConfig):
    connection: str | None = Field(default=None, description="The connection name of the database view")

class FederateModelConfig(QueryModelConfig):
    eager: bool = Field(default=False, description="Whether the model should be materialized for SQL models")

    def get_sql_for_create(self, model_name: str, select_query: str) -> str:
        materialization = "TABLE" if self.eager else "VIEW"
        create_prefix = f"CREATE {materialization} {model_name} AS\n"
        return create_prefix + select_query
