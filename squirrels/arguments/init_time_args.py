from typing import Any
from dataclasses import dataclass


@dataclass
class BaseArguments:
    _proj_vars: dict[str, Any]
    _env_vars: dict[str, Any]

    @property
    def proj_vars(self) -> dict[str, Any]:
        return self._proj_vars.copy()
    
    @property
    def env_vars(self) -> dict[str, Any]:
        return self._env_vars.copy()


@dataclass
class ConnectionsArgs(BaseArguments):
    pass


@dataclass
class ParametersArgs(BaseArguments):
    pass
