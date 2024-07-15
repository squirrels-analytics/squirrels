from typing import Any, Optional, Callable
from dataclasses import dataclass
import os, yaml

from . import _constants as c, _utils as u
from ._timer import timer, time

_GLOBAL_SQUIRRELS_CFG_FILE = u.join_paths(os.path.expanduser('~'), '.squirrels', c.ENVIRON_CONFIG_FILE)


@dataclass
class _EnvironConfig:
    _users: dict[str, dict[str, Any]]
    _env_vars: dict[str, str]
    _credentials: dict[str, dict[str, str]]
    _secrets: dict[str, str]
    
    def __post_init__(self):
        for username, user in self._users.items():
            user[c.USER_NAME_KEY] = username
            if c.USER_PWD_KEY not in user:
                raise u.ConfigurationError(f"All users must have password in environcfg.yml")
        
        for key, cred in self._credentials.items():
            if c.USERNAME_KEY not in cred or c.PASSWORD_KEY not in cred:
                raise u.ConfigurationError(f'Either "{c.USERNAME_KEY}" or "{c.PASSWORD_KEY}" was not specified for credential "{key}"')
    
    def get_users(self) -> dict[str, dict[str, Any]]:
        return self._users.copy()
    
    def get_all_env_vars(self) -> dict[str, str]:
        return self._env_vars.copy()
    
    def get_credential(self, key: Optional[str]) -> tuple[str, str]:
        if not key:
            return "", ""

        try:
            credential = self._credentials[key]
        except KeyError as e:
            raise u.ConfigurationError(f'No credentials configured for "{key}"') from e
    
        return credential[c.USERNAME_KEY], credential[c.PASSWORD_KEY]
    
    def get_secret(self, key: str, *, default_factory: Optional[Callable[[],str]] = None) -> str:
        if not self._secrets.get(key) and default_factory is not None:
            self._secrets[key] = default_factory()
        return self._secrets.get(key)


class EnvironConfigIO:
    obj: _EnvironConfig
    
    @classmethod
    def LoadFromFile(cls):
        start = time.time()
        def load_yaml(filename: str) -> dict[str, dict]:
            try:
                with open(filename, 'r') as f:
                    return yaml.safe_load(f)
            except FileNotFoundError:
                return {}
        
        master_env_config = load_yaml(_GLOBAL_SQUIRRELS_CFG_FILE)
        proj_env_config1 = load_yaml(c.ENVIRON_CONFIG_FILE)
        proj_env_config2 = load_yaml(c.ENV_CONFIG_FILE)

        for project_config in [proj_env_config1, proj_env_config2]:
            for key in project_config:
                master_env_config.setdefault(key, {})
                master_env_config[key].update(project_config[key])
        
        users = master_env_config.get(c.USERS_KEY, {})
        env_vars = master_env_config.get(c.ENV_VARS_KEY, {})
        credentials = master_env_config.get(c.CREDENTIALS_KEY, {})
        secrets = master_env_config.get(c.SECRETS_KEY, {})

        cls.obj = _EnvironConfig(users, env_vars, credentials, secrets)
        timer.add_activity_time(f"loading {c.ENVIRON_CONFIG_FILE} file", start)
