from typing import Callable, Any
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
    _get_credential: Callable[[str | None], tuple[str, str]]

    def get_credential(self, key: str | None) -> tuple[str, str]:
        """
        Return (username, password) tuple configured for credentials key in env.yaml

        If key is None, returns tuple of empty strings ("", "")

        Arguments:
            key: The credentials key
        
        Returns:
            A tuple of 2 strings
        """
        return self._get_credential(key)


@dataclass
class ParametersArgs(BaseArguments):
    pass
