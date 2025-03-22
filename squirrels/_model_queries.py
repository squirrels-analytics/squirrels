from abc import ABCMeta
from dataclasses import dataclass, field
from typing import Callable, Generic, TypeVar, Any
import polars as pl, pandas as pd

from .arguments.run_time_args import BuildModelArgs
from ._model_configs import ModelConfig


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
    raw_query: Callable[[BuildModelArgs], pl.LazyFrame | pd.DataFrame]


Q = TypeVar('Q', bound=QueryFile)
M = TypeVar('M', bound=ModelConfig)

@dataclass(frozen=True)
class QueryFileWithConfig(Generic[Q, M]):
    query_file: Q
    config: M


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
    is_duckdb: bool

@dataclass
class PyModelQuery(Query):
    query: Callable[[], pl.LazyFrame | pd.DataFrame]
