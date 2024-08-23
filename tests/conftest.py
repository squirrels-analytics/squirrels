import pytest

from squirrels.arguments.init_time_args import ConnectionsArgs
from squirrels._environcfg import EnvironConfigIO, _EnvironConfig
from squirrels._connection_set import ConnectionSetIO, ConnectionSet
from squirrels import _manifest as m

@pytest.fixture(scope="session", autouse=True)
def my_initial_code():
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
    EnvironConfigIO.obj = _EnvironConfig(users, {}, credentials, {})

    m.ManifestIO.obj = m._ManifestConfig(
        project_variables=m.ProjectVarsConfig({"name":"", "major_version": 0}),
        packages=[],
        connections={},
        parameters=[],
        selection_test_sets={},
        dbviews={},
        federates={},
        datasets={},
        dashboards={},
        settings={}
    )

    ConnectionSetIO.args = ConnectionsArgs({}, {}, None)
    ConnectionSetIO.obj = ConnectionSet({})
