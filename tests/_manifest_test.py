from typing import Optional
import pytest

from squirrels import _manifest as m, _utils as u


## Project variables config

@pytest.mark.parametrize("data", [
    {"major_version": 1, "minor_version": 3},
    {"name": "my_name", "major_version": "1", "minor_version": 3},
    {"name": "my_name", "minor_version": 3},
    {"name": "my_name", "major_version": 1, "minor_version": "3"},
    {"name": "my_name", "major_version": 1}
])
def test_invalid_proj_vars(data: dict):
    with pytest.raises(u.ConfigurationError):
        m.ProjectVarsConfig(data)


@pytest.fixture(scope="module")
def proj_vars1() -> m.ProjectVarsConfig:
    data = {"name": "my_name", "major_version": 1, "minor_version": 3}
    return m.ProjectVarsConfig(data)


@pytest.fixture(scope="module")
def proj_vars2() -> m.ProjectVarsConfig:
    data = {"name": "my_name", "label": "my_label", "major_version": 1, "minor_version": 3}
    return m.ProjectVarsConfig(data)


@pytest.mark.parametrize("fixture,expected", [
    ("proj_vars1", "my_name"),
    ("proj_vars2", "my_label")
])
def test_proj_vars_get_label(fixture: str, expected: str, request: pytest.FixtureRequest):
    proj_vars: m.ProjectVarsConfig = request.getfixturevalue(fixture)
    assert proj_vars.get_label() == expected


## Package config

@pytest.mark.parametrize("data", [
    {"git": "test.git", "directory": "test"},
    {"directory": "test", "revision": "0.1.0"}
])
def test_invalid_package_config(data: dict):
    with pytest.raises(u.ConfigurationError):
        m.PackageConfig.from_dict(data)


@pytest.fixture(scope="module")
def package_config1() -> m.PackageConfig:
    data = {"git": "path/test.git", "revision": "0.1.0"}
    return m.PackageConfig.from_dict(data)


@pytest.fixture(scope="module")
def package_config2() -> m.PackageConfig:
    data = {"git": "my.git.test", "revision": "0.1.0"}
    return m.PackageConfig.from_dict(data)


@pytest.fixture(scope="module")
def package_config3() -> m.PackageConfig:
    data = {"git": "path/test.git", "directory": "mytest", "revision": "0.1.0"}
    return m.PackageConfig.from_dict(data)


@pytest.mark.parametrize("fixture,expected", [
    ("package_config1", "test"),
    ("package_config2", "my.git.test"),
    ("package_config3", "mytest")
])
def test_package_directory(fixture: str, expected: str, request: pytest.FixtureRequest):
    package: m.PackageConfig = request.getfixturevalue(fixture)
    assert package.directory == expected


## DB connection config

@pytest.mark.parametrize("data", [
    {"connection_name": "default"},
    {"url": "my/url"}
])
def test_invalid_db_conn_config(data: dict):
    with pytest.raises(u.ConfigurationError):
        m.DbConnConfig.from_dict(data)


@pytest.fixture(scope="module")
def db_conn_config1() -> m.DbConnConfig:
    data = {"connection_name": "default", "credentials": "test_cred_key", "url": "{username}:{password}/my/url"}
    return m.DbConnConfig.from_dict(data)


@pytest.mark.parametrize("fixture,expected", [
    ("db_conn_config1", "user1:pass1/my/url")
])
def test_db_conn_url(fixture: str, expected: str, request: pytest.FixtureRequest):
    db_conn: m.DbConnConfig = request.getfixturevalue(fixture)
    assert db_conn.url == expected


## Parameters config

@pytest.mark.parametrize("data", [
    {"name": "my_name", "type": "SingleSelectParameter", "factory": "Create"},
    {"name": "my_name", "type": "SingleSelectParameter", "args": {}},
    {"name": "my_name", "factory": "Create", "args": {}},
    {"type": "SingleSelectParameter", "factory": "Create", "args": {}}
])
def test_invalid_db_conn_config(data: dict):
    with pytest.raises(u.ConfigurationError):
        m.DbConnConfig.from_dict(data)


## Test sets config

@pytest.mark.parametrize("data", [
    {"user_attributes": {}, "parameters": {}}
])
def test_invalid_test_sets_config(data: dict):
    with pytest.raises(u.ConfigurationError):
        m.TestSetsConfig.from_dict(data)


## Dbview config

@pytest.mark.parametrize("data", [
    {"connection_name": "default"}
])
def test_invalid_dbview_config(data: dict):
    with pytest.raises(u.ConfigurationError):
        m.DbviewConfig.from_dict(data)


## Federate config

@pytest.mark.parametrize("data", [
    {"materialized": "table"}
])
def test_invalid_federate_config(data: dict):
    with pytest.raises(u.ConfigurationError):
        m.FederateConfig.from_dict(data)


## Dataset config

@pytest.mark.parametrize("data", [
    {"label": "My Dataset", "scope": "public"},
    {"name": "my_dataset", "label": "My Dataset", "scope": "not_exist"}
])
def test_invalid_dataset_config(data: dict):
    with pytest.raises(u.ConfigurationError):
        m.DatasetsConfig.from_dict(data)


@pytest.fixture(scope="module")
def dataset_config1() -> m.DatasetsConfig:
    data = {"name": "my_dataset"}
    return m.DatasetsConfig.from_dict(data)


@pytest.fixture(scope="module")
def dataset_config2() -> m.DatasetsConfig:
    data = {
        "name": "my_dataset", "label": "My Dataset", "model": "my_model", "scope": "protected", 
        "parameters": [], "args": {"key": "value"}
    }
    return m.DatasetsConfig.from_dict(data)


@pytest.mark.parametrize("fixture,expected", [
    ("dataset_config1", "my_dataset"),
    ("dataset_config2", "My Dataset")
])
def test_dataset_label(fixture: str, expected: str, request: pytest.FixtureRequest):
    dataset: m.DatasetsConfig = request.getfixturevalue(fixture)
    assert dataset.label == expected


@pytest.mark.parametrize("fixture,expected", [
    ("dataset_config1", "my_dataset"),
    ("dataset_config2", "my_model")
])
def test_dataset_model(fixture: str, expected: str, request: pytest.FixtureRequest):
    dataset: m.DatasetsConfig = request.getfixturevalue(fixture)
    assert dataset.model == expected


@pytest.mark.parametrize("fixture,expected", [
    ("dataset_config1", m.DatasetScope.PUBLIC),
    ("dataset_config2", m.DatasetScope.PROTECTED)
])
def test_dataset_scope(fixture: str, expected: m.DatasetScope, request: pytest.FixtureRequest):
    dataset: m.DatasetsConfig = request.getfixturevalue(fixture)
    assert dataset.scope == expected


@pytest.mark.parametrize("fixture,expected", [
    ("dataset_config1", None),
    ("dataset_config2", [])
])
def test_dataset_parameters(fixture: str, expected: Optional[list], request: pytest.FixtureRequest):
    dataset: m.DatasetsConfig = request.getfixturevalue(fixture)
    assert dataset.parameters == expected


@pytest.mark.parametrize("fixture,expected", [
    ("dataset_config1", {}),
    ("dataset_config2", {"key": "value"})
])
def test_dataset_args(fixture: str, expected: dict, request: pytest.FixtureRequest):
    dataset: m.DatasetsConfig = request.getfixturevalue(fixture)
    assert dataset.args == expected


## Full manifest config

@pytest.mark.parametrize("data", [
    {}
])
def test_invalid_manifest_config(data: dict):
    with pytest.raises(u.ConfigurationError):
        m._ManifestConfig.from_dict(data)


@pytest.fixture(scope="module")
def manifest_config1() -> m._ManifestConfig:
    data = {"project_variables": {"name": "", "major_version": 0, "minor_version": 1}}
    return m._ManifestConfig.from_dict(data)


@pytest.fixture(scope="module")
def manifest_config2() -> m._ManifestConfig:
    data = {
        "project_variables": {"name": "", "major_version": 0, "minor_version": 1}, 
        "packages": [{"git": "path/test.git", "revision": "0.1.0"}],
        "connections": [],
        "parameters": [],
        "selection_test_sets": [],
        "dbviews": [],
        "federates": [],
        "datasets": []
    }
    return m._ManifestConfig.from_dict(data)


@pytest.mark.parametrize("fixture,expected", [
    ("manifest_config1", []),
    ("manifest_config2", [m.PackageConfig(git_url="path/test.git", directory="test", revision="0.1.0")])
])
def test_manifest_packages(fixture: str, expected: list, request: pytest.FixtureRequest):
    manifest: m._ManifestConfig = request.getfixturevalue(fixture)
    assert manifest.packages == expected


@pytest.mark.parametrize("fixture,expected", [
    ("manifest_config1", []),
    ("manifest_config2", [])
])
def test_manifest_connections(fixture: str, expected: list, request: pytest.FixtureRequest):
    manifest: m._ManifestConfig = request.getfixturevalue(fixture)
    assert manifest.connections == expected


@pytest.mark.parametrize("fixture,expected", [
    ("manifest_config1", []),
    ("manifest_config2", [])
])
def test_manifest_parameters(fixture: str, expected: list, request: pytest.FixtureRequest):
    manifest: m._ManifestConfig = request.getfixturevalue(fixture)
    assert manifest.parameters == expected


@pytest.mark.parametrize("fixture,expected", [
    ("manifest_config1", {'default': m.TestSetsConfig(name='default', user_attributes={}, parameters={})}),
    ("manifest_config2", {'default': m.TestSetsConfig(name='default', user_attributes={}, parameters={})})
])
def test_manifest_test_sets(fixture: str, expected: dict, request: pytest.FixtureRequest):
    manifest: m._ManifestConfig = request.getfixturevalue(fixture)
    assert manifest.selection_test_sets == expected


@pytest.mark.parametrize("fixture,expected", [
    ("manifest_config1", {}),
    ("manifest_config2", {})
])
def test_manifest_dbviews(fixture: str, expected: dict, request: pytest.FixtureRequest):
    manifest: m._ManifestConfig = request.getfixturevalue(fixture)
    assert manifest.dbviews == expected


@pytest.mark.parametrize("fixture,expected", [
    ("manifest_config1", {}),
    ("manifest_config2", {})
])
def test_manifest_federates(fixture: str, expected: dict, request: pytest.FixtureRequest):
    manifest: m._ManifestConfig = request.getfixturevalue(fixture)
    assert manifest.federates == expected


@pytest.mark.parametrize("fixture,expected", [
    ("manifest_config1", {}),
    ("manifest_config2", {})
])
def test_manifest_datasets(fixture: str, expected: dict, request: pytest.FixtureRequest):
    manifest: m._ManifestConfig = request.getfixturevalue(fixture)
    assert manifest.datasets == expected
