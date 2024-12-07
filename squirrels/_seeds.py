from dataclasses import dataclass
from typing import Any
import os, time, glob, polars as pl

from . import _utils as u, _constants as c, _model_configs as mc


@dataclass
class Seed:
    config: mc.SeedConfig
    df: pl.LazyFrame

    def __post_init__(self):
        if not self.config.cast_column_types:
            return
        
        exprs = []
        for col_config in self.config.columns:
            polars_dtype = u.sqrl_dtypes_to_polars_dtypes.get(col_config.type, pl.String)
            exprs.append(pl.col(col_config.name).cast(polars_dtype))

        self.df = self.df.with_columns(*exprs)


@dataclass
class Seeds:
    _data: dict[str, Seed]
    
    def run_query(self, sql_query: str) -> pl.DataFrame:
        dataframes = {key: seed.df for key, seed in self._data.items()}
        return u.run_sql_on_dataframes(sql_query, dataframes)
    
    def get_dataframes(self) -> dict[str, Seed]:
        return self._data.copy()


class SeedsIO:

    @classmethod
    def load_files(cls, logger: u.Logger, base_path: str, *, settings: dict[str, Any] = {}) -> Seeds:
        start = time.time()
        infer_schema: bool = settings.get(c.SEEDS_INFER_SCHEMA_SETTING, True)
        na_values: list[str] = settings.get(c.SEEDS_NA_VALUES_SETTING, [])
        
        seeds_dict = {}
        csv_files = glob.glob(os.path.join(base_path, c.SEEDS_FOLDER, '**/*.csv'), recursive=True)
        for csv_file in csv_files:
            file_stem = os.path.splitext(os.path.basename(csv_file))[0]
            df = pl.read_csv(csv_file, try_parse_dates=True, infer_schema=infer_schema, null_values=na_values).lazy()
            
            config_file = os.path.splitext(csv_file)[0] + '.yml'
            config_dict = u.load_yaml_config(config_file) if os.path.exists(config_file) else {}
            config = mc.SeedConfig(**config_dict)
            seeds_dict[file_stem] = Seed(config, df)
        
        seeds = Seeds(seeds_dict)
        logger.log_activity_time("loading seed files", start)
        return seeds
