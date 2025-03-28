from __future__ import annotations
from typing import Optional, Sequence, Any
from dataclasses import dataclass, field
from collections import OrderedDict
import time, concurrent.futures, polars as pl

from . import _utils as u, _constants as c, parameters as p, _parameter_configs as _pc, _py_module as pm, _api_response_models as arm
from .arguments.init_time_args import ParametersArgs
from ._manifest import ParametersConfig, ManifestConfig
from ._connection_set import ConnectionSet, ConnectionsArgs
from ._seeds import Seeds
from ._auth import BaseUser


@dataclass
class ParameterSet:
    """
    A wrapper class for a sequence of parameters with the selections applied as well
    """
    _parameters_dict: OrderedDict[str, p.Parameter]

    def get_parameters_as_dict(self) -> dict[str, p.Parameter]:
        return self._parameters_dict.copy()

    def to_api_response_model0(self) -> arm.ParametersModel:
        parameters = []
        for x in self._parameters_dict.values():
            parameters.append(x._to_api_response_model0())
        return arm.ParametersModel(parameters=parameters)


@dataclass
class ParameterConfigsSet:
    """
    Pool of parameter configs, can create multiple for unit testing purposes
    """
    _data: dict[str, _pc.ParameterConfigBase] = field(default_factory=OrderedDict)
    _data_source_params: dict[str, _pc.DataSourceParameterConfig] = field(default_factory=dict)
        
    def get(self, name: Optional[str]) -> Optional[_pc.ParameterConfigBase]:
        try:
            return self._data[name] if name is not None else None
        except KeyError as e:
            raise u.ConfigurationError(f'Unable to find parameter named "{name}"') from e

    def add(self, param_config: _pc.ParameterConfigBase) -> None:
        self._data[param_config.name] = param_config
        if isinstance(param_config, _pc.DataSourceParameterConfig):
            self._data_source_params[param_config.name] = param_config
    
    def _get_all_ds_param_configs(self) -> Sequence[_pc.DataSourceParameterConfig]:
        return list(self._data_source_params.values())

    def __convert_datasource_params(self, df_dict: dict[str, pl.DataFrame]) -> None:
        done = set()
        for curr_name in self._data_source_params:
            stack = [curr_name] # Note: parents must be converted first before children
            while stack:
                name = stack[-1]
                if name not in done:
                    param = self._data_source_params.get(name, self.get(name))
                    assert param is not None
                    parent_name = param.parent_name
                    if parent_name is not None and parent_name not in done:
                        stack.append(parent_name)
                        continue
                    if isinstance(param, _pc.DataSourceParameterConfig):
                        if name not in df_dict:
                            raise u.ConfigurationError(f'No reference data found for parameter "{name}"')
                        self._data[name] = param.convert(df_dict[name])
                    done.add(name)
                stack.pop()
    
    def __validate_param_relationships(self) -> None:
        for param_config in self._data.values():
            assert isinstance(param_config, _pc.ParameterConfig)
            parent_name = param_config.parent_name
            parent = self.get(parent_name)
            if parent:
                if not isinstance(param_config, _pc.SelectionParameterConfig):
                    if not isinstance(parent, _pc.SingleSelectParameterConfig):
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
                
                if not isinstance(parent, _pc.SelectionParameterConfig):
                    raise u.ConfigurationError(f'Only selection parameters can be parents. Parameter "{parent_name}" is the parent of ' +
                                               f'"{param_config.name}" but "{parent_name}" is not a selection parameter.')
                
                parent._add_child_mutate(param_config)
    
    def _post_process_params(self, df_dict: dict[str, pl.DataFrame]) -> None:
        self.__convert_datasource_params(df_dict)
        self.__validate_param_relationships()
    
    def apply_selections(
        self, dataset_params: Optional[Sequence[str]], selections: dict[str, Any], user: BaseUser | None, *, parent_param: str | None = None
    ) -> ParameterSet:
        if dataset_params is None:
            if user is None:
                dataset_params = [k for k, v in self._data.items() if v.user_attribute is None]
            else:
                dataset_params = list(self._data.keys())
        
        parameters_by_name: dict[str, p.Parameter] = {}
        params_to_process = [parent_param] if parent_param else dataset_params
        params_to_process_set = set(params_to_process)
        for some_name in params_to_process:
            stack = [some_name] # Note: process parent selections first (if applicable) before children
            while stack:
                curr_name = stack[-1]
                children = []
                if curr_name not in parameters_by_name:
                    param_conf = self.get(curr_name)
                    assert isinstance(param_conf, _pc.ParameterConfig)
                    parent_name = param_conf.parent_name
                    if parent_name is None:
                        parent = None
                    elif parent_name in params_to_process_set and parent_name not in parameters_by_name:
                        stack.append(parent_name)
                        continue
                    else:
                        parent = parameters_by_name.get(parent_name)
                    assert isinstance(parent, p._SelectionParameter) or parent is None
                    param = param_conf.with_selection(selections.get(curr_name), user, parent)
                    parameters_by_name[curr_name] = param
                    if isinstance(param_conf, _pc.SelectionParameterConfig):
                        children = list(x for x in param_conf.children.keys() if x in dataset_params)
                stack.pop()
                stack.extend(children)
        
        ordered_parameters = OrderedDict((key, parameters_by_name[key]) for key in dataset_params if key in parameters_by_name)
        return ParameterSet(ordered_parameters)
    
    def get_all_api_field_info(self) -> dict[str, _pc.APIParamFieldInfo]:
        api_field_infos = {}
        for param, config in self._data.items():
            assert isinstance(config, _pc.ParameterConfig)
            api_field_infos[param] = config.get_api_field_info()
        return api_field_infos


class ParameterConfigsSetIO:
    """
    Static class for the singleton object of ParameterConfigsSet
    """
    obj: ParameterConfigsSet # this is static (set in load_from_file) to simplify development experience for pyconfigs/parameters.py
    
    @classmethod
    def _get_df_dict_from_data_sources(cls, default_conn_name: str, seeds: Seeds, conn_set: ConnectionSet) -> dict[str, pl.DataFrame]:
        def get_dataframe(ds_param_config: _pc.DataSourceParameterConfig) -> tuple[str, pl.DataFrame]:
            return ds_param_config.name, ds_param_config.get_dataframe(default_conn_name, conn_set, seeds)
        
        ds_param_configs = cls.obj._get_all_ds_param_configs()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            df_dict = dict(executor.map(get_dataframe, ds_param_configs))
        
        return df_dict
    
    @classmethod
    def _add_from_dict(cls, param_config: ParametersConfig) -> None:
        ptype = getattr(p, param_config.type)
        factory = getattr(ptype, param_config.factory)
        factory(**param_config.arguments)
    
    @classmethod
    def get_param_args(cls, conn_args: ConnectionsArgs) -> ParametersArgs:
        return ParametersArgs(conn_args.project_path, conn_args.proj_vars, conn_args.env_vars)
    
    @classmethod
    def load_from_file(
        cls, logger: u.Logger, base_path: str, manifest_cfg: ManifestConfig, seeds: Seeds, conn_set: ConnectionSet, param_args: ParametersArgs
    ) -> ParameterConfigsSet:
        start = time.time()
        cls.obj = ParameterConfigsSet()

        for param_as_dict in manifest_cfg.parameters:
            cls._add_from_dict(param_as_dict)
        
        pm.run_pyconfig_main(base_path, c.PARAMETERS_FILE, {"sqrl": param_args})
        
        default_conn_name = manifest_cfg.env_vars.get(c.SQRL_CONNECTIONS_DEFAULT_NAME_USED, "default")
        df_dict = cls._get_df_dict_from_data_sources(default_conn_name, seeds, conn_set)
        cls.obj._post_process_params(df_dict)
        
        logger.log_activity_time("loading parameters", start)
        return cls.obj
