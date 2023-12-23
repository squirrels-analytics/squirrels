from typing import Callable, Any
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

    def get_credential(self, key: str) -> tuple[str, str]:
        """
        Return (username, password) tuple configured for credentials key in environcfg.yaml

        Parameters:
            key: The credentials key
        
        Returns:
            A tuple of strings of size 2
        """


@dataclass
class ParametersArgs(BaseArguments):
    pass
