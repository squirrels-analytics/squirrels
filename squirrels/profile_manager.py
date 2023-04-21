import os
from configparser import ConfigParser
from squirrels import constants as c

_PROFILE_FILE = os.path.join(os.path.expanduser('~'), '.squirrelscfg')

def _get_config():
    config = ConfigParser()
    config.read(_PROFILE_FILE)
    return config

def _get_profile_config_as_dict(config: ConfigParser, profile: str, hide_pass: bool):
    try:
        config_dict = dict(config[profile])
    except KeyError as e:
        raise ValueError(f'Profile "{profile}" (specified in manifest file) has not been configured. To configure, use CLI: "squirrels set-profile {profile}"') 
    if hide_pass:
        pw = config_dict[c.PASSWORD]
        config_dict[c.PASSWORD] = '*'*len(pw)
    return config_dict

def get_profiles():
    config = _get_config()
    return {section: _get_profile_config_as_dict(config, section, True) for section in config.sections()}

class Profile:
    def __init__(self, name):
        self.name = name
    
    def set(self, dialect, conn_url, username, password):
        settings = {
            c.DIALECT: dialect,
            c.CONN_URL: conn_url,
            c.USERNAME: username,
            c.PASSWORD: password
        }
        config = _get_config()
        if not config.has_section(self.name):
            config.add_section(self.name)
        
        for k, v in settings.items():
            config.set(self.name, k, v)
        
        with open(_PROFILE_FILE, 'w') as f:
            config.write(f)

    def get(self, hide_pass: bool = True):
        config = _get_config()
        return _get_profile_config_as_dict(config, self.name, hide_pass)

    def delete(self):
        config = _get_config()
        section_exists = config.has_section(self.name)
        if section_exists:
            config.remove_section(self.name)
            with open(_PROFILE_FILE, 'w') as f:
                config.write(f)
        return section_exists
