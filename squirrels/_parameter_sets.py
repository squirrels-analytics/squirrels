from __future__ import annotations
from typing import Dict, Optional, Sequence
from dataclasses import dataclass, field
from collections import OrderedDict
import concurrent.futures, pandas as pd

from . import _parameter_configs as pc, _utils as u, _constants as c, parameters as p
from ._manifest import ManifestIO
from ._connection_set import ConnectionSetIO
from ._authenticator import UserBase
from ._timer import timer, time


@dataclass
class ParameterSet:
    """
    A wrapper class for a sequence of parameters with the selections applied as well
    """
    _parameters_dict: OrderedDict[str, p.Parameter]

    def get_parameters_as_dict(self) -> Dict[str, p.Parameter]:
        return self._parameters_dict.copy()

    def to_json_dict(self, *, debug: bool = False) -> Dict:
        parameters = []
        for x in self._parameters_dict.values():
            if not x._config.is_hidden or debug:
                parameters.append(x.to_json_dict())
        
        output = {
            "response_version": 0, 
            "parameters": parameters
        }
        return output


@dataclass
class _ParameterConfigsSet:
    """
    Pool of parameter configs, can create multiple for unit testing purposes
    """
    _data: Dict[str, pc.ParameterConfig] = field(default_factory=OrderedDict)
    _data_source_params: Dict[str, pc.DataSourceParameterConfig] = field(default_factory=dict)
        
    def get(self, name: Optional[str]) -> Optional[pc.ParameterConfig]:
        try:
            return self._data[name] if name is not None else None
        except KeyError as e:
            raise u.ConfigurationError(f'Unable to find parameter named "{name}"') from e

    def add(self, param_config: pc.ParameterConfigBase) -> None:
        self._data[param_config.name] = param_config
        if isinstance(param_config, pc.DataSourceParameterConfig):
            self._data_source_params[param_config.name] = param_config
    
    def _get_all_ds_param_configs(self) -> Sequence[pc.DataSourceParameterConfig]:
        return list(self._data_source_params.values())

    def __convert_datasource_params(self, df_dict: Dict[str, pd.DataFrame]) -> None:
        done = set()
        for curr_name in self._data_source_params:
            stack = [curr_name] # Note: parents must be converted first before children
            while stack:
                name = stack[-1]
                if name not in done:
                    param = self._data_source_params.get(name, self.get(name))
                    parent_name = param.parent_name
                    if parent_name is not None and parent_name not in done:
                        stack.append(parent_name)
                        continue
                    if isinstance(param, pc.DataSourceParameterConfig):
                        if name not in df_dict:
                            raise u.ConfigurationError(f'No reference data found for parameter "{name}"')
                        self._data[name] = param.convert(df_dict[name])
                    done.add(name)
                stack.pop()
    
    def __validate_param_relationships(self) -> None:
        for param_config in self._data.values():
            assert isinstance(param_config, pc.ParameterConfig)
            parent_name = param_config.parent_name
            parent = self.get(parent_name)
            if parent:
                if not isinstance(param_config, pc.SelectionParameterConfig):
                    if not isinstance(parent, pc.SingleSelectParameterConfig):
                        raise u.ConfigurationError(f'Only single-select parameters can be parents of non-select parameters. ' +
                                                   f'Parameter "{parent_name}" is the parent of non-select parameter ' +
                                                   f'"{param_config.name}" but "{parent_name}" is not a single-select parameter.')
                    seen = set()
                    for option in param_config.all_options:
                        lookup_keys = option._parent_option_ids
                        if len(option._user_groups) > 0:
                            lookup_keys = set((x, y) for x in option._parent_option_ids for y in option._user_groups)
                        if not seen.isdisjoint(lookup_keys):
                            raise u.ConfigurationError(f'Each distinct value of "parent option id" can only appear once (per user group)' +
                                                       f'among the options of non-select parameter "{param_config.name}".')
                        seen.update(lookup_keys)
                
                if not isinstance(parent, pc.SelectionParameterConfig):
                    raise u.ConfigurationError(f'Only selection parameters can be parents. Parameter "{parent_name}" is the parent of ' +
                                               f'"{param_config.name}" but "{parent_name}" is not a selection parameter.')
                
                parent._add_child_mutate(param_config)
    
    def _post_process_params(self, df_dict: Dict[str, pd.DataFrame]) -> None:
        self.__convert_datasource_params(df_dict)
        self.__validate_param_relationships()
    
    def apply_selections(
        self, dataset_params: Optional[Sequence[str]], selections: Dict[str, str], user: Optional[UserBase], *, updates_only: bool = False
    ) -> ParameterSet:
        if dataset_params is None:
            dataset_params = self._data.keys()
        
        parameters_by_name: Dict[str, p.Parameter] = {}
        params_to_process = selections.keys() if selections and updates_only else dataset_params
        params_to_process_set = set(params_to_process)
        for some_name in params_to_process:
            stack = [some_name] # Note: process parent selections first (if applicable) before children
            while stack:
                curr_name = stack[-1]
                children = []
                if curr_name not in parameters_by_name:
                    param_conf = self.get(curr_name)
                    parent_name = param_conf.parent_name
                    if parent_name is None:
                        parent = None
                    elif parent_name in params_to_process_set and parent_name not in parameters_by_name:
                        stack.append(parent_name)
                        continue
                    else:
                        parent = parameters_by_name.get(parent_name)
                    param = param_conf.with_selection(selections.get(curr_name), user, parent)
                    parameters_by_name[curr_name] = param
                    if isinstance(param_conf, pc.SelectionParameterConfig):
                        children = list(param_conf.children.keys())
                stack.pop()
                stack.extend(children)
        
        ordered_parameters = OrderedDict((key, parameters_by_name[key]) for key in dataset_params if key in parameters_by_name)
        return ParameterSet(ordered_parameters)


class ParameterConfigsSetIO:
    """
    Static class for the singleton object of __ParameterConfigsPoolData
    """
    obj = _ParameterConfigsSet()
    
    @classmethod
    def _GetDfDict(cls, excel_file_name: Optional[str]) -> Dict[str, pd.DataFrame]:
        if excel_file_name:
            excel_file = pd.ExcelFile(excel_file_name)
            df_dict = pd.read_excel(excel_file, None)
        else:
            def get_dataframe_from_query(ds_param_config: pc.DataSourceParameterConfig) -> pd.DataFrame:
                key, datasource = ds_param_config.name, ds_param_config.data_source
                df = ConnectionSetIO.obj.run_sql_query_from_conn_name(datasource._get_query(), datasource._connection_name)
                return key, df
            
            ds_param_configs = cls.obj._get_all_ds_param_configs()
            with concurrent.futures.ThreadPoolExecutor() as executor:
                df_dict = dict(executor.map(get_dataframe_from_query, ds_param_configs))
        
        return df_dict
    
    @classmethod
    def LoadFromFile(cls, *, excel_file_name: Optional[str] = None) -> None:
        start = time.time()
        proj_vars = ManifestIO.obj.get_proj_vars()
        u.run_module_main(c.PARAMETERS_FILE, {"proj": proj_vars})
        df_dict = cls._GetDfDict(excel_file_name)
        cls.obj._post_process_params(df_dict)
        timer.add_activity_time("loading parameters", start)