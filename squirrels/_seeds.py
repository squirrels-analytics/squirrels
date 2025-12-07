from dataclasses import dataclass
import os
import re
import time
import glob
import json

import polars as pl

from ._exceptions import ConfigurationError
from . import _utils as u, _constants as c, _model_configs as mc


@dataclass
class Seed:
    config: mc.SeedConfig
    df: pl.LazyFrame

    def __post_init__(self):
        if self.config.cast_column_types:
            exprs = []
            for col_config in self.config.columns:
                col_type = col_config.type.lower()
                if col_type.startswith("decimal"):
                    polars_dtype = self._parse_decimal_type(col_type)
                else:
                    try:
                        polars_dtype = u.sqrl_dtypes_to_polars_dtypes[col_type]
                    except KeyError as e:
                        raise ConfigurationError(f"Unknown column type: '{col_type}'") from e
                
                exprs.append(pl.col(col_config.name).cast(polars_dtype))

            self.df = self.df.with_columns(*exprs)

    @staticmethod
    def _parse_decimal_type(col_type: str) -> pl.Decimal:
        """Parse a decimal type string and return the appropriate polars Decimal type.
        
        Supports formats: "decimal" or "decimal(precision, scale)"
        """

        # Match decimal(precision, scale) pattern
        match = re.match(r"decimal\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)", col_type)
        if match:
            precision = int(match.group(1))
            scale = int(match.group(2))
            return pl.Decimal(precision=precision, scale=scale)
        
        if col_type == "decimal":
            return pl.Decimal(precision=18, scale=2)
        
        raise ConfigurationError(f"Unknown column type: '{col_type}'")


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
    def load_files(cls, logger: u.Logger, base_path: str, env_vars: dict[str, str]) -> Seeds:
        start = time.time()
        infer_schema_setting: bool = u.to_bool(env_vars.get(c.SQRL_SEEDS_INFER_SCHEMA, "true"))
        na_values_setting: list[str] = json.loads(env_vars.get(c.SQRL_SEEDS_NA_VALUES, "[]"))

        seeds_dict = {}
        csv_files = glob.glob(os.path.join(base_path, c.SEEDS_FOLDER, '**/*.csv'), recursive=True)
        for csv_file in csv_files:
            config_file = os.path.splitext(csv_file)[0] + '.yml'
            config_dict = u.load_yaml_config(config_file) if os.path.exists(config_file) else {}
            config = mc.SeedConfig(**config_dict)

            file_stem = os.path.splitext(os.path.basename(csv_file))[0]
            infer_schema = not config.cast_column_types and infer_schema_setting
            df = pl.read_csv(
                csv_file, try_parse_dates=True,
                infer_schema=infer_schema,
                null_values=na_values_setting
            ).lazy()

            seeds_dict[file_stem] = Seed(config, df)

        seeds = Seeds(seeds_dict)
        logger.log_activity_time("loading seed files", start)
        return seeds
