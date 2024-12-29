from dataclasses import dataclass, field
from functools import cached_property
import polars as pl

from ._model_configs import ModelConfig


@dataclass
class DatasetMetadata:
    description: str
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
            "description": self.description,
            "schema": {
                "fields": fields
            },
        }

    def to_json(self) -> dict:
        return self._json_repr


@dataclass
class DatasetResult(DatasetMetadata):
    df: pl.DataFrame

    @cached_property
    def _json_repr(self) -> dict:
        column_details_by_name = {col.name: col for col in self.target_model_config.columns}
        fields = []
        for col in self.df.columns:
            column_details = column_details_by_name[col]
            fields.append({
                "name": col,
                "type": column_details.type,
                "description": column_details.description,
                "category": column_details.category.value
            })
        
        return {
            "description": self.description,
            "schema": {
                "fields": fields
            },
            "data": self.df.to_dicts()
        }
