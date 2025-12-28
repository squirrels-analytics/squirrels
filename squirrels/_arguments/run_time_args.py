from typing import Callable, Any, Coroutine
from dataclasses import dataclass, field, KW_ONLY
import polars as pl

from .init_time_args import ConnectionsArgs, ParametersArgs, BuildModelArgs
from .._schemas.auth_models import AbstractUser
from .._parameters import Parameter, TextValue


@dataclass
class ContextArgs(ParametersArgs):
    user: AbstractUser
    prms: dict[str, Parameter]
    configurables: dict[str, str]
    _conn_args: ConnectionsArgs
    _: KW_ONLY
    _placeholders: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        super().__post_init__()
        self.prms = self.prms.copy()
        self.configurables = self.configurables.copy()
        self._conn_args = ConnectionsArgs(**self._conn_args.__dict__)
        self._placeholders = self._placeholders.copy()

    def set_placeholder(self, placeholder: str, value: TextValue | Any) -> str:
        """
        Method to set a placeholder value.

        Arguments:
            placeholder: A string for the name of the placeholder
            value: The value of the placeholder. Can be of any type
        """
        if isinstance(value, TextValue):
            value = value._value_do_not_touch
        self._placeholders[placeholder] = value
        return ""
    
    def param_exists(self, param_name: str) -> bool:
        """
        Method to check whether a given parameter exists and is enabled (i.e., not hidden based on other parameter selections) for the current 
        dataset at runtime.

        Arguments:
            param_name: A string for the name of the parameter
        
        Returns:
            A boolean for whether the parameter exists
        """
        return (param_name in self.prms and self.prms[param_name].is_enabled())


@dataclass
class ModelArgs(ContextArgs, BuildModelArgs):
    ctx: dict[str, Any]

    def __post_init__(self) -> None:
        ContextArgs.__post_init__(self)
        BuildModelArgs.__post_init__(self)
        self.ctx = self.ctx.copy()
    
    def is_placeholder(self, placeholder: str) -> bool:
        """
        Checks whether a name is a valid placeholder

        Arguments:
            placeholder: A string for the name of the placeholder
        
        Returns:
            A boolean for whether name exists
        """
        return placeholder in self._placeholders
    
    def get_placeholder_value(self, placeholder: str) -> Any | None:
        """
        Gets the value of a placeholder.

        USE WITH CAUTION. Do not use the return value directly in a SQL query since that could be prone to SQL injection

        Arguments:
            placeholder: A string for the name of the placeholder
        
        Returns:
            An type for the value of the placeholder
        """
        return self._placeholders.get(placeholder)


@dataclass
class DashboardArgs(ContextArgs):
    ctx: dict[str, Any]
    _get_dataset: Callable[[str, dict[str, Any]], Coroutine[Any, Any, pl.DataFrame]]

    def __post_init__(self) -> None:
        super().__post_init__()
        self.ctx = self.ctx.copy()

    async def dataset(self, name: str, *, fixed_parameters: dict[str, Any] = {}) -> pl.DataFrame:
        """
        Get dataset as DataFrame given dataset name. Can use this to access protected/private datasets regardless of user authenticated to the dashboard.

        The parameters used for the dataset include the parameter selections coming from the REST API and the fixed_parameters argument. The fixed_parameters takes precedence.

        Arguments:
            name: A string for the dataset name
            fixed_parameters: Parameters to set for this dataset (in addition to the ones set through real-time selections)
        
        Returns:
            A DataFrame for the result of the dataset
        """
        return await self._get_dataset(name, fixed_parameters)
