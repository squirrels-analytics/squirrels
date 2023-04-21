from typing import Dict
import json, copy, pytest

from squirrels import constants as c, credentials_manager as cm
from squirrels.utils import ConfigurationError


@pytest.fixture
def empty_config() -> cm.SquirrelsConfigParser:
    return cm.SquirrelsConfigParser()


@pytest.fixture
def config_with_empty_section(empty_config: cm.SquirrelsConfigParser) -> cm.SquirrelsConfigParser:
    config = copy.copy(empty_config)
    config.add_section(c.CREDENTIALS_KEY)
    return config


@pytest.fixture
def config_with_credential(config_with_empty_section: cm.SquirrelsConfigParser) -> cm.SquirrelsConfigParser:
    config = copy.copy(config_with_empty_section)
    config[c.CREDENTIALS_KEY]['test'] = json.dumps({'username': 'user1', 'password': 'pw1'})
    return config


@pytest.fixture
def config_with_two_credentials(config_with_credential: cm.SquirrelsConfigParser) -> cm.SquirrelsConfigParser:
    config = copy.copy(config_with_credential)
    config[c.CREDENTIALS_KEY]['test2'] = json.dumps({'username': 'user"2', 'password': "pass'word2"})
    return config


@pytest.fixture
def credential() -> cm.Credential:
    return cm.Credential('user1', 'pw1')


class TestCredential:
    def test_credential_to_str(self, credential: cm.Credential):
        assert str(credential) == 'username=user1, password=***'


class TestSquirrelsConfigParser:
    @pytest.mark.parametrize('config_name,key,expected', [
        ('config_with_credential', 'test', cm.Credential('user1', 'pw1')),
        ('config_with_two_credentials', 'test2', cm.Credential('user"2', "pass'word2"))
    ])
    def test_get_credential(self, config_name: str, key: str, expected: Dict[str, cm.Credential], 
                            request: pytest.FixtureRequest):
        config: cm.SquirrelsConfigParser = request.getfixturevalue(config_name)
        assert config.get_credential(key) == expected
    
    @pytest.mark.parametrize('config_name,expected', [
        ('empty_config', {}), 
        ('config_with_empty_section', {}), 
        ('config_with_credential', {'test': cm.Credential('user1', 'pw1')}),
        ('config_with_two_credentials', {
            'test': cm.Credential('user1', 'pw1'), 
            'test2': cm.Credential('user"2', "pass'word2")
        })
    ])
    def test_get_all_credentials(self, config_name: str, expected: Dict[str, cm.Credential], 
                                 request: pytest.FixtureRequest):
        config: cm.SquirrelsConfigParser = request.getfixturevalue(config_name)
        assert config.get_all_credentials() == expected
    
    @pytest.mark.parametrize('config_name,key,expected', [
        ('empty_config', 'test', 'config_with_credential'),
        ('config_with_empty_section', 'test', 'config_with_credential'),
        ('config_with_credential', 'test2', 'config_with_two_credentials')
    ])
    def test_set_credential(self, config_name: str, key: str, expected: str, 
                            request: pytest.FixtureRequest):
        config1: cm.SquirrelsConfigParser = request.getfixturevalue(config_name)
        config2: cm.SquirrelsConfigParser = request.getfixturevalue(expected)
        credential = cm.Credential('user1', 'pw1') if key == 'test' else cm.Credential('user"2', "pass'word2")
        assert config1.set_credential(key, credential) == config2
    
    @pytest.mark.parametrize('config_name,key,expected', [
        ('config_with_credential', 'test', 'config_with_empty_section'),
        ('config_with_two_credentials', 'test2', 'config_with_credential')
    ])
    def test_delete_credential(self, config_name: str, key: str, expected: str, 
                               request: pytest.FixtureRequest):
        config1: cm.SquirrelsConfigParser = request.getfixturevalue(config_name)
        config2: cm.SquirrelsConfigParser = request.getfixturevalue(expected)
        assert config1.delete_credential(key) == config2
    
    @pytest.mark.parametrize('config_name,key', [
        ('empty_config', 'test'),
        ('config_with_empty_section', 'test'),
        ('config_with_credential', 'test2')
    ])
    def test_invalid_get_credential(self, config_name: str, key: str, request: pytest.FixtureRequest):
        config: cm.SquirrelsConfigParser = request.getfixturevalue(config_name)
        with pytest.raises(ConfigurationError):
            config.get_credential(key)
        with pytest.raises(KeyError):
            config.delete_credential(key)
        