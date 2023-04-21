from typing import Iterable, Dict
from collections import OrderedDict

from squirrels.configs import data_sources as d, parameters as p
from squirrels.timed_imports import pandas as pd


class ParameterSet(p.ParameterSetBase):
    def __init__(self, parameters: Iterable[p.Parameter]):
        super().__init__()
        self._data_source_params: OrderedDict[str, d.DataSourceParameter] = OrderedDict()
        for param in parameters:
            self._parameters_dict[param.name] = param
            if isinstance(param, d.DataSourceParameter):
                self._data_source_params[param.name] = param

    def get_datasources(self) -> Dict[str, d.DataSource]:
        new_dict = {}
        for param_name, ds_param in self._data_source_params.items():
            new_dict[param_name] = ds_param.data_source
        return new_dict

    def convert_datasource_params(self, df_dict: Dict[str, pd.DataFrame]):
        # Done sequentially since parents must be converted first before children
        for key, ds_param in self._data_source_params.items():
            ds_param.parent = self.get_parameter(ds_param.parent.name) if ds_param.parent is not None else None
            self._parameters_dict[key] = ds_param.convert(df_dict[key])
        self._data_source_params.clear()
