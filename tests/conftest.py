import pytest

from squirrels._environcfg import EnvironConfigIO, _EnvironConfig

@pytest.fixture(scope="session", autouse=True)
def my_initial_code():
    users = {
        "johndoe": {
            "password": "qwerty",
            "is_admin": True,
            "organization": "org1"
        },
        "lisadoe": {
            "password": "abcd1234",
            "is_admin": True,
            "organization": "org2"
        }
    }
    credentials = {
        "test_cred_key": {
            "username": "user1",
            "password": "pass1"
        }
    }
    config = _EnvironConfig(users, {}, credentials, {})
    EnvironConfigIO.obj = config
