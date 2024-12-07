import pytest

from squirrels.arguments.init_time_args import ConnectionsArgs
from squirrels._environcfg import EnvironConfig
from squirrels._connection_set import ConnectionSet
from squirrels import _manifest as m


@pytest.fixture(scope="session")
def simple_env_config():
    users = {
        "johndoe": {
            "password": "qwerty",
            "is_internal": True,
            "email": "johndoe@org1.com",
            "organization": "org1"
        },
        "lisadoe": {
            "password": "abcd1234",
            "is_internal": True,
            "email": "lisadoe@org2.com",
            "organization": "org2"
        }
    }
    credentials = {
        "test_cred_key": {
            "username": "user1",
            "password": "pass1"
        }
    }
    return EnvironConfig(users=users, credentials=credentials) # type: ignore


@pytest.fixture(scope="session")
def simple_conn_args():
    return ConnectionsArgs({}, {})

@pytest.fixture(scope="session")
def simple_conn_set():
    return ConnectionSet()
