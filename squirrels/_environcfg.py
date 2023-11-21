from typing import Dict, Tuple, Any, Optional, Callable
from dataclasses import dataclass
import os, yaml

from . import _constants as c, _utils as u
from ._timer import timer, time

_GLOBAL_SQUIRRELS_CFG_FILE = u.join_paths(os.path.expanduser('~'), '.squirrels', c.ENVIRON_CONFIG_FILE)


@dataclass
class _EnvironConfig:
    _users: Dict[str, Dict[str, Any]]
    _env_vars: Dict[str, str]
    _credentials: Dict[str, Dict[str, str]]
    _secrets: Dict[str, str]
    
    def __post_init__(self):
        for username, user in self._users.items():
            user[c.USER_NAME_KEY] = username
            if c.USER_PWD_KEY not in user:
                raise u.ConfigurationError(f"All users must have password in environcfg.yaml")
        
        for key, cred in self._credentials.items():
            if c.USERNAME_KEY not in cred or c.PASSWORD_KEY not in cred:
                raise u.ConfigurationError(f'Either "{c.USERNAME_KEY}" or "{c.PASSWORD_KEY}" was not specified for credential "{key}"')
    
    def get_users(self) -> Dict[str, Dict[str, Any]]:
        return self._users.copy()
    
    def get_all_env_vars(self) -> Dict[str, str]:
        return self._env_vars.copy()

    def get_env_var(self, key: str) -> str:
        try:
            return self._env_vars[key]
        except KeyError as e:
            raise u.ConfigurationError(f'No environment variable configured for "{key}"') from e
    
    def get_credential(self, key: Optional[str]) -> Tuple[str, str]:
        if not key:
            return "", ""

        try:
            credential = self._credentials[key]
        except KeyError as e:
            raise u.ConfigurationError(f'No credentials configured for "{key}"') from e
    
        return credential[c.USERNAME_KEY], credential[c.PASSWORD_KEY]
    
    def get_secret(self, key: str, *, default_factory: Optional[Callable[[],str]] = None) -> str:
        if key not in self._secrets and default_factory is not None:
            self._secrets[key] = default_factory()
        return self._secrets.get(key)


class EnvironConfigIO:
    obj: _EnvironConfig
    
    @classmethod
    def LoadFromFile(cls):
        start = time.time()
        def load_yaml(filename: str) -> Dict[str, Dict]:
            try:
                with open(filename, 'r') as f:
                    return yaml.safe_load(f)
            except FileNotFoundError:
                return {}
        
        config1 = load_yaml(_GLOBAL_SQUIRRELS_CFG_FILE)
        config2 = load_yaml(c.ENVIRON_CONFIG_FILE)

        for key in config2:
            config1.setdefault(key, {})
            config1[key].update(config2[key])
        
        users = config1.get("users", {})
        env_vars = config1.get("env_vars", {})
        credentials = config1.get("credentials", {})
        secrets = config1.get("secrets", {})

        cls.obj = _EnvironConfig(users, env_vars, credentials, secrets)
        timer.add_activity_time("loading environcfg.yaml file", start)