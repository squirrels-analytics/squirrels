import pytest

from squirrels.arguments.init_time_args import ConnectionsArgs
from squirrels._connection_set import ConnectionSet


@pytest.fixture(scope="session")
def simple_env_vars():
    return {
        "SQRL_SECRET_KEY": "test_secret_key"
    }

@pytest.fixture(scope="session")
def simple_conn_args():
    return ConnectionsArgs(".", {}, {})

@pytest.fixture(scope="session")
def simple_conn_set():
    return ConnectionSet()
