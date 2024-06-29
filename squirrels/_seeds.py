from dataclasses import dataclass
import os, glob, pandas as pd

from ._timer import timer, time
from ._manifest import ManifestIO
from . import _utils as u, _constants as c


@dataclass
class Seeds:
    _data: dict[str, pd.DataFrame]
    
    def run_query(self, sql_query: str) -> pd.DataFrame:
        return u.run_sql_on_dataframes(sql_query, self._data)
    
    def get_dataframes(self) -> dict[str, pd.DataFrame]:
        return self._data.copy()


class SeedsIO:
    obj: Seeds

    @classmethod
    def LoadFiles(cls) -> None:
        start = time.time()
        na_values: list[str] = ManifestIO.obj.settings.get(c.SEEDS_NA_VALUES_SETTING, ["NA"])
        infer_schema: bool = ManifestIO.obj.settings.get(c.SEEDS_INFER_SCHEMA_SETTING, True)
        csv_dtype = None if infer_schema else str
        
        seeds_dict = {}
        csv_files = glob.glob(os.path.join(c.SEEDS_FOLDER, '**/*.csv'), recursive=True)
        for csv_file in csv_files:
            file_stem = os.path.splitext(os.path.basename(csv_file))[0]
            df = pd.read_csv(csv_file, dtype=csv_dtype, keep_default_na=False, na_values=na_values)
            seeds_dict[file_stem] = df
        
        cls.obj = Seeds(seeds_dict)
        timer.add_activity_time("loading seed files", start)
