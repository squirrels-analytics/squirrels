from typing import Dict
from dataclasses import dataclass
from configparser import ConfigParser
import os, json

from squirrels.utils import ConfigurationError
from squirrels import constants as c

_SQUIRRELS_CFG_PATH = os.path.join(os.path.expanduser('~'), '.squirrelscfg')


@dataclass
class Credential:
    username: str
    password: str
    
    def __str__(self) -> str:
        redacted_pass = '*'*len(self.password)
        return f'username={self.username}, password={redacted_pass}'


class SquirrelsConfigParser(ConfigParser):
    def _get_creds_section(self):
        section_name: str = c.CREDENTIALS_KEY
        if not self.has_section(section_name):
            self.add_section(section_name)
        return self[section_name]
    
    def _json_str_to_credential(self, json_str: str) -> Credential:
        cred_dict = json.loads(json_str)
        return Credential(cred_dict[c.USERNAME_KEY], cred_dict[c.PASSWORD_KEY])
    
    def get_credential(self, key: str) -> Credential:
        section = self._get_creds_section()
        try:
            value = section[key]
        except KeyError as e:
            raise ConfigurationError(f'Credential key "{key}" has not been set. To set it, use $ squirrels set-credential {key}')
        return self._json_str_to_credential(value)

    def get_all_credentials(self) -> Dict[str, Credential]:
        section = self._get_creds_section()
        result = {}
        for key, value in section.items():
            result[key] = self._json_str_to_credential(value)
        return result

    def set_credential(self, key: str, credential: Credential) -> ConfigParser:
        section = self._get_creds_section()
        section[key] = json.dumps(credential.__dict__)
        return self
    
    def delete_credential(self, key: str) -> ConfigParser:
        section = self._get_creds_section()
        section.pop(key)
        return self


class SquirrelsConfigIOWrapper:
    def __init__(self) -> None:
        self.config = SquirrelsConfigParser()
        self.config.read(_SQUIRRELS_CFG_PATH)

    def get_credential(self, key: str) -> Credential:
        return self.config.get_credential(key)

    def print_all_credentials(self) -> None:
        credentials_dict = self.config.get_all_credentials()
        for key, cred in credentials_dict.items():
            print(f'{key}:', cred)

    def _write_config(self) -> None:
        with open(_SQUIRRELS_CFG_PATH, 'w') as f:
            self.config.write(f)

    def set_credential(self, key: str, user: str, pw: str) -> None:
        credential = Credential(user, pw)
        self.config.set_credential(key, credential)
        print(f'Credential key "{key}" set to: {credential}')
        self._write_config()

    def delete_credential(self, key: str) -> None:
        self.config.delete_credential(key)
        print(f'Credential key "{key}" has been deleted')
        self._write_config()

squirrels_config_io = SquirrelsConfigIOWrapper()
