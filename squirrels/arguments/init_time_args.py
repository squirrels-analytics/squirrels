from typing import Callable, Any
from dataclasses import dataclass


@dataclass
class BaseArguments:
    proj_vars: dict[str, Any]
    env_vars: dict[str, Any]


@dataclass
class ConnectionsArgs(BaseArguments):
    get_credential: Callable[[str], tuple[str, str]]


@dataclass
class ParametersArgs(BaseArguments):
    pass
