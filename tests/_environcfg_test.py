import pytest

from squirrels._environcfg import _EnvironConfig
from squirrels import _utils as u


def test_wrong_environcfg():
    users = {
        "user1": {
            "email": "user1@domain.com"
        }
    }
    with pytest.raises(u.ConfigurationError):
        _EnvironConfig(users, {}, {}, {}) # needs password

    credentials1 = {
        "cred1": {"password": ""}
    }
    credentials2 = {
        "cred2": {"username": ""}
    }
    with pytest.raises(u.ConfigurationError):
        _EnvironConfig({}, {}, credentials1, {}) # needs username
    with pytest.raises(u.ConfigurationError):
        _EnvironConfig({}, {}, credentials2, {}) # needs password


@pytest.fixture(scope="module")
def basic_users() -> dict:
    return {
        "user1": {
            "password": "secret1",
            "email": "user1@domain.com"
        },
        "user2": {
            "password": "secret2",
            "email": "user2@domain.com"
        }
    }


@pytest.fixture(scope="module")
def basic_env_vars() -> dict:
    return {
        "key1": "value1",
        "key2": "value2"
    }


@pytest.fixture(scope="module")
def basic_environcfg(basic_users: dict, basic_env_vars: dict) -> _EnvironConfig:
    credentials = {
        "credkey1": {
            "username": "test1",
            "password": "pass1"
        }
    }
    secrets = {"key1": "secret1"}
    return _EnvironConfig(basic_users, basic_env_vars, credentials, secrets)


def test_get_users(basic_environcfg: _EnvironConfig, basic_users: dict):
    assert basic_environcfg.get_users() == basic_users.copy()


def test_get_all_env_vars(basic_environcfg: _EnvironConfig, basic_env_vars: dict):
    assert basic_environcfg.get_all_env_vars() == basic_env_vars.copy()


def test_get_credential(basic_environcfg: _EnvironConfig):
    assert basic_environcfg.get_credential("credkey1") == ("test1", "pass1")
    with pytest.raises(u.ConfigurationError):
        basic_environcfg.get_credential("credkey5")


def test_get_secret(basic_environcfg: _EnvironConfig):
    assert basic_environcfg.get_secret("key1", default_factory=lambda: "value") == "secret1"
    assert basic_environcfg.get_secret("key10") == None
    assert basic_environcfg.get_secret("key10", default_factory=lambda: "value") == "value"
    assert basic_environcfg.get_secret("key10") == "value"
    assert basic_environcfg.get_secret("key11") == None
