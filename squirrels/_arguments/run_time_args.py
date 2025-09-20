from typing import Callable, Any, Coroutine
import polars as pl

from .init_time_args import ParametersArgs, BuildModelArgs
from .._auth import BaseUser
from .._parameters import Parameter, TextValue


class ContextArgs(ParametersArgs):

    def __init__(
        self, param_args: ParametersArgs, 
        user: BaseUser | None, 
        prms: dict[str, Parameter],
        configurables: dict[str, str]
    ):
        super().__init__(param_args.project_path, param_args.proj_vars, param_args.env_vars)
        self.user = user
        self._prms = prms
        self._configurables = configurables
        self._placeholders = {}

    @property
    def prms(self) -> dict[str, Parameter]:
        """
        A dictionary of parameter names to parameter
        """
        return self._prms.copy()
    
    @property
    def configurables(self) -> dict[str, str]:
        """
        A dictionary of configurable name to value (set by application)
        """
        return self._configurables.copy()

    @property
    def _placeholders_copy(self) -> dict[str, Any]:
        """
        A dictionary of placeholder name to placeholder value
        """
        return self._placeholders.copy()

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


class ModelArgs(BuildModelArgs, ContextArgs):

    def __init__(
        self, ctx_args: ContextArgs, build_model_args: BuildModelArgs, 
        ctx: dict[str, Any]
    ):
        super(ContextArgs, self).__init__(ctx_args.project_path, ctx_args.proj_vars, ctx_args.env_vars)
        self._project_path = ctx_args.project_path
        self._proj_vars = ctx_args.proj_vars
        self._env_vars = ctx_args.env_vars
        self.user = ctx_args.user
        self._prms = ctx_args.prms
        self._configurables = ctx_args.configurables
        self._placeholders = ctx_args._placeholders_copy
        self._connections = build_model_args.connections
        self._dependencies = build_model_args.dependencies
        self._ref = build_model_args.ref
        self._run_external_sql = build_model_args.run_external_sql
        self._ctx = ctx

    @property
    def ctx(self) -> dict[str, Any]:
        """
        Dictionary of context variables
        """
        return self._ctx.copy()
    
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


class DashboardArgs(ParametersArgs):

    def __init__(
        self, param_args: ParametersArgs, 
        get_dataset: Callable[[str, dict[str, Any]], Coroutine[Any, Any, pl.DataFrame]]
    ):
        super().__init__(param_args.project_path, param_args.proj_vars, param_args.env_vars)
        self._get_dataset = get_dataset

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
