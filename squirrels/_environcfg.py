from pathlib import Path
from typing import Any, Callable
from pydantic import BaseModel, Field, field_validator, ValidationError
import os, yaml

from . import _constants as c, _utils as u
from ._timer import timer, time

_GLOBAL_SQUIRRELS_CFG_FILE = u.join_paths(os.path.expanduser('~'), '.squirrels', c.ENV_CONFIG_FILE)


class _UserConfig(BaseModel, extra="allow"):
    username: str
    password: str
    is_internal: bool = False


class _CredentialsConfig(BaseModel):
    username: str
    password: str


class EnvironConfig(BaseModel):
    users: dict[str, _UserConfig] = Field(default_factory=dict)
    env_vars: dict[str, str] = Field(default_factory=dict)
    credentials: dict[str, _CredentialsConfig] = Field(default_factory=dict)
    secrets: dict[str, Any | None] = Field(default_factory=dict)

    @field_validator("users", mode="before")
    @classmethod
    def inject_username(cls, users: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
        processed_users = {}
        for username, user in users.items():
            processed_users[username] = {"username": username, **user}
        return processed_users
    
    def get_users(self) -> dict[str, _UserConfig]:
        return self.users.copy()
    
    def get_all_env_vars(self) -> dict[str, str]:
        return self.env_vars.copy()
    
    def get_credential(self, key: str | None) -> tuple[str, str]:
        if not key:
            return "", ""

        try:
            credential = self.credentials[key]
        except KeyError as e:
            raise u.ConfigurationError(f'No credentials configured for "{key}"') from e
    
        return credential.username, credential.password
    
    def get_secret(self, key: str, default_factory: Callable[[], Any]) -> Any:
        if self.secrets.get(key) is None:
            self.secrets[key] = default_factory()
        return self.secrets[key]


class EnvironConfigIO:
    obj: EnvironConfig
    
    @classmethod
    def load_from_file(cls) -> EnvironConfig:
        start = time.time()
        def load_yaml(filename: str | Path) -> dict[str, dict]:
            try:
                with open(filename, 'r') as f:
                    return yaml.safe_load(f)
            except FileNotFoundError:
                return {}
        
        master_env_config = load_yaml(_GLOBAL_SQUIRRELS_CFG_FILE)
        proj_env_config = load_yaml(c.ENV_CONFIG_FILE)

        for key in proj_env_config:
            master_env_config.setdefault(key, {})
            master_env_config[key].update(proj_env_config[key])
        
        try:
            cls.obj = EnvironConfig(**master_env_config)
        except ValidationError as e:
            raise u.ConfigurationError(f"Failed to process {c.ENV_CONFIG_FILE} file. " + str(e)) from e
        
        timer.add_activity_time(f"loading {c.ENV_CONFIG_FILE} file", start)
        return cls.obj
