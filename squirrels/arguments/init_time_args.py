from typing import Optional, Callable, Any
from dataclasses import dataclass


@dataclass
class BaseArguments:
    proj_vars: dict[str, Any]
    env_vars: dict[str, Any]


@dataclass
class ConnectionsArgs(BaseArguments):
    _get_credential: Callable[[str], tuple[str, str]]

    def __post_init__(self):
        self.get_credential = self._get_credential

    def get_credential(self, key: Optional[str]) -> tuple[str, str]:
        """
        Return (username, password) tuple configured for credentials key in environcfg.yaml

        If key is None, returns tuple of empty strings ("", "")

        Parameters:
            key: The credentials key
        
        Returns:
            A tuple of 2 strings
        """


@dataclass
class ParametersArgs(BaseArguments):
    pass
