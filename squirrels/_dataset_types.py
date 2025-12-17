from typing import Callable, Literal
from dataclasses import dataclass, field
from functools import cached_property, lru_cache
import polars as pl

from ._model_configs import ModelConfig


@dataclass
class DatasetMetadata:
    target_model_config: ModelConfig

    @cached_property
    def _json_repr(self) -> dict:
        fields = []
        for col in self.target_model_config.columns:
            fields.append({
                "name": col.name,
                "type": col.type,
                "condition": col.condition,
                "description": col.description,
                "category": col.category.value
            })
        
        return {
            "schema": {
                "fields": fields
            },
        }

    def to_json(self) -> dict:
        return self._json_repr


@dataclass(frozen=True)
class DatasetResultFormat:
    orientation: Literal["records", "rows", "columns"]
    offset: int
    limit: int | None


@dataclass
class DatasetResult(DatasetMetadata):
    df: pl.DataFrame
    to_json: Callable[[DatasetResultFormat], dict] = field(init=False)

    def __post_init__(self):
        self.to_json = lru_cache()(self._to_json)
    
    def _to_json(self, result_format: DatasetResultFormat) -> dict:
        df = self.df.lazy()
        if result_format.offset > 0:
            df = df.filter(pl.col("_row_num") > result_format.offset)
        if result_format.limit is not None:
            df = df.limit(result_format.limit)
        df = df.collect()
        
        if result_format.orientation == "columns":
            data = df.to_dict(as_series=False)
        else:
            data = df.to_dicts()
            if result_format.orientation == "rows":
                data = [[row[col] for col in df.columns] for row in data]

        column_details_by_name = {col.name: col for col in self.target_model_config.columns}
        fields = []
        for col in df.columns:
            if col == "_row_num":
                fields.append({"name": "_row_num", "type": "integer", "description": "The row number of the dataset (starts at 1)", "category": "misc"})
            elif col in column_details_by_name:
                column_details = column_details_by_name[col]
                fields.append({
                    "name": col,
                    "type": column_details.type,
                    "description": column_details.description,
                    "category": column_details.category.value
                })
            else:
                fields.append({"name": col, "type": "unknown", "description": "", "category": "misc"})
        
        return {
            "schema": {
                "fields": fields
            },
            "total_num_rows": self.df.select(pl.len()).item(),
            "data_details": {
                "num_rows": df.select(pl.len()).item(),
                "orientation": result_format.orientation
            },
            "data": data
        }
