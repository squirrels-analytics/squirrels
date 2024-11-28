from abc import ABCMeta
from dataclasses import dataclass, field
from typing import Callable, Any
import polars as pl, pandas as pd

from .arguments.run_time_args import ModelArgs
from ._model_configs import ModelConfig, DbviewModelConfig, FederateModelConfig


# Input query file classes

@dataclass(frozen=True)
class QueryFile(metaclass=ABCMeta):
    filepath: str
    raw_query: Any

@dataclass(frozen=True)
class SqlQueryFile(QueryFile):
    raw_query: str

@dataclass(frozen=True)
class PyQueryFile(QueryFile):
    raw_query: Callable[[ModelArgs], pl.LazyFrame | pd.DataFrame]


@dataclass(frozen=True)
class QueryFileWithConfig:
    query_file: QueryFile
    config: ModelConfig


# Compiled query classes

@dataclass
class Query(metaclass=ABCMeta):
    query: Any

@dataclass
class WorkInProgress(Query):
    query: None = field(default=None, init=False)

@dataclass
class SqlModelQuery(Query):
    query: str

@dataclass
class PyModelQuery(Query):
    query: Callable[[], pl.LazyFrame | pd.DataFrame]
