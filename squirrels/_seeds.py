from dataclasses import dataclass
import os, glob, pandas as pd

from ._timer import timer, time
from ._manifest import ManifestConfig
from . import _utils as _u, _constants as c


@dataclass
class Seeds:
    _data: dict[str, pd.DataFrame]
    _manifest_cfg: ManifestConfig
    
    def run_query(self, sql_query: str) -> pd.DataFrame:
        use_duckdb = self._manifest_cfg.settings_obj.do_use_duckdb()
        return _u.run_sql_on_dataframes(sql_query, self._data, use_duckdb)
    
    def get_dataframes(self) -> dict[str, pd.DataFrame]:
        return self._data.copy()


class SeedsIO:

    @classmethod
    def load_files(cls, base_path: str, manifest_cfg: ManifestConfig) -> Seeds:
        start = time.time()
        infer_schema: bool = manifest_cfg.settings.get(c.SEEDS_INFER_SCHEMA_SETTING, True)
        na_values: list[str] = manifest_cfg.settings.get(c.SEEDS_NA_VALUES_SETTING, ["NA"])
        csv_dtype = None if infer_schema else str
        
        seeds_dict = {}
        csv_files = glob.glob(os.path.join(base_path, c.SEEDS_FOLDER, '**/*.csv'), recursive=True)
        for csv_file in csv_files:
            file_stem = os.path.splitext(os.path.basename(csv_file))[0]
            df = pd.read_csv(csv_file, dtype=csv_dtype, keep_default_na=False, na_values=na_values)
            seeds_dict[file_stem] = df
        
        seeds = Seeds(seeds_dict, manifest_cfg)
        timer.add_activity_time("loading seed files", start)
        return seeds
