from __future__ import annotations
from typing import Sequence, Dict
from collections import OrderedDict

from squirrels import data_sources as d, parameters as p
from squirrels._timed_imports import pandas as pd


class _ParameterSetBase:
    def __init__(self) -> None:
        self._parameters_dict: OrderedDict[str, p.Parameter] = OrderedDict()
    
    def add_parameter(self, parameter: p.Parameter) -> None:
        self._parameters_dict[parameter.name] = parameter

    def get_parameter(self, param_name: str) -> p.Parameter:
        if param_name in self._parameters_dict:
            return self._parameters_dict[param_name]
        else:
            raise KeyError(f'No such parameter exists called "{param_name}"')
    
    def get_parameters_as_ordered_dict(self) -> OrderedDict:
        return OrderedDict(self._parameters_dict)
    
    def merge(self, other: _ParameterSetBase) -> _ParameterSetBase:
        new_param_set = _ParameterSetBase()
        new_param_set._parameters_dict = OrderedDict(self._parameters_dict)
        new_param_set._parameters_dict.update(other._parameters_dict)
        return new_param_set

    def __getitem__(self, param_name: str) -> p.Parameter:
        return self.get_parameter(param_name)

    def to_dict(self, debug: bool = False):
        parameters = []
        for x in self._parameters_dict.values():
            if not x.is_hidden or debug:
                parameters.append(x.to_dict())
        
        output = {
            "response_version": 0, 
            "parameters": parameters
        }
        return output


class ParameterSet(_ParameterSetBase):
    def __init__(self, parameters: Sequence[p.Parameter]):
        """
        Constructor for ParameterSet, a wrapper class for a sequence of parameters.

        Parameters:
            parameters: A sequence of parameters
        """
        super().__init__()
        self._data_source_params: OrderedDict[str, p.DataSourceParameter] = OrderedDict()
        for param in parameters:
            self._parameters_dict[param.name] = param
            if isinstance(param, p.DataSourceParameter):
                self._data_source_params[param.name] = param
    
    def merge(self, other: _ParameterSetBase) -> ParameterSet:
        """
        Merges this object with another ParameterSet (by combining the parameters) to create a new ParameterSet.

        The _parameters_dict are merged (with the other ParameterSet taking precedence when a name exist in both dict),
        while the _data_source_params are only taken from this object. This object and the other ParameterSet remain
        unchanged.

        Parameters:
            other: The other parameter set
        
        Returns:
            A new ParameterSet that contains all the parameters from this and the other parameter set.
        """
        new_param_set_base = super().merge(other)
        new_param_set = ParameterSet(())
        new_param_set._parameters_dict = new_param_set_base._parameters_dict
        new_param_set._data_source_params = self._data_source_params
        return new_param_set

    def get_datasources(self) -> Dict[str, d.DataSource]:
        """
        Gets all the DataSource objects as values to a dictionary where keys are the DataSource parameter names.

        Each DataSource object represents a lookup table with table name, connection name, corresponding columns to ID, label, etc.

        Returns:
            A dictionary where keys are the names of DataSourceParameter's and values are the corresponding DataSource.
        """
        new_dict = {}
        for param_name, ds_param in self._data_source_params.items():
            new_dict[param_name] = ds_param.data_source
        return new_dict

    def convert_datasource_params(self, df_dict: Dict[str, pd.DataFrame]) -> None:
        """
        Changes all the DataSourceParameters into other Parameter types. The _data_source_params field gets cleared.

        Parameters:
            df_dict: A dictionary of DataSourceParameter name to the pandas DataFrame of the lookup table data.
        """
        # Done sequentially since parents must be converted first before children
        for key, ds_param in self._data_source_params.items():
            ds_param.parent = self.get_parameter(ds_param.parent.name) if ds_param.parent is not None else None
            self._parameters_dict[key] = ds_param.convert(df_dict[key])
        self._data_source_params.clear()
