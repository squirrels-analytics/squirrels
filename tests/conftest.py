import pytest

from squirrels._arguments.init_time_args import ConnectionsArgs
from squirrels._connection_set import ConnectionSet


# Configure pytest-anyio to only use asyncio backend (not trio)
@pytest.fixture(scope="session")
def anyio_backend():
    return "asyncio"


@pytest.fixture(scope="session")
def simple_env_vars():
    return {
        "SQRL_SECRET_KEY": "test_secret_key"
    }

@pytest.fixture(scope="session")
def simple_conn_args():
    return ConnectionsArgs(project_path=".", proj_vars={}, env_vars={})

@pytest.fixture(scope="session")
def simple_conn_set():
    return ConnectionSet()
