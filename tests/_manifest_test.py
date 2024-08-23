from typing import Optional
import pytest

from squirrels import _manifest as m, _utils as u


## Project variables config

@pytest.mark.parametrize("data", [
    {"major_version": 1}, # missing name
    {"name": "my_name", "major_version": "1"}, # major_version not int
    {"name": "my_name"}, # missing major_version
])
def test_invalid_proj_vars(data: dict):
    with pytest.raises(u.ConfigurationError):
        m.ProjectVarsConfig(data)


@pytest.fixture(scope="module")
def proj_vars1() -> m.ProjectVarsConfig:
    data = {"name": "my_name", "major_version": 1}
    return m.ProjectVarsConfig(data)


@pytest.fixture(scope="module")
def proj_vars2() -> m.ProjectVarsConfig:
    data = {"name": "my_name", "label": "my_label", "major_version": 1}
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
    {"git": "test.git", "directory": "test"}, # missing revision
    {"directory": "test", "revision": "0.1.0"}, # missing git
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
    {"name": "default"}, # missing url
    {"url": "my/url"}, # missing name
])
def test_invalid_db_conn_config(data: dict):
    with pytest.raises(u.ConfigurationError):
        m.DbConnConfig.from_dict(data)


@pytest.fixture(scope="module")
def db_conn_config1() -> m.DbConnConfig:
    data = {"name": "default", "credential": "test_cred_key", "url": "{username}:{password}/my/url"}
    return m.DbConnConfig.from_dict(data)


@pytest.fixture(scope="module")
def db_conn_config2() -> m.DbConnConfig:
    data = {"name": "default", "url": "{username}:{password}/my/url"}
    return m.DbConnConfig.from_dict(data)


@pytest.mark.parametrize("fixture,expected", [
    ("db_conn_config1", "user1:pass1/my/url"),
    ("db_conn_config2", ":/my/url")
])
def test_db_conn_url(fixture: str, expected: str, request: pytest.FixtureRequest):
    db_conn: m.DbConnConfig = request.getfixturevalue(fixture)
    assert db_conn.url == expected


## Parameters config

@pytest.mark.parametrize("data", [
    {"type": "SingleSelectParameter", "factory": "CreateSimple"}, # missing arguments
    {"type": "SingleSelectParameter", "arguments": {}}, # missing factory
    {"factory": "CreateSimple", "arguments": {}}, # missing type
])
def test_invalid_db_conn_config(data: dict):
    with pytest.raises(u.ConfigurationError):
        m.ParametersConfig.from_dict(data)


## Test sets config

@pytest.mark.parametrize("data", [
    {} # missing name
])
def test_invalid_test_sets_config(data: dict):
    with pytest.raises(u.ConfigurationError):
        m.TestSetsConfig.from_dict(data)


@pytest.fixture(scope="module")
def test_sets_config1() -> m.TestSetsConfig:
    data = {"name": "test_set1"}
    return m.TestSetsConfig.from_dict(data)


@pytest.fixture(scope="module")
def test_sets_config2() -> m.TestSetsConfig:
    data = {"name": "test_set2", "user_attributes": {}}
    return m.TestSetsConfig.from_dict(data)


@pytest.mark.parametrize("fixture,expected", [
    ("test_sets_config1", False),
    ("test_sets_config2", True)
])
def test_is_authenticated(fixture: str, expected: bool, request: pytest.FixtureRequest):
    test_sets: m.TestSetsConfig = request.getfixturevalue(fixture)
    assert test_sets.is_authenticated == expected


## Dbview config

@pytest.mark.parametrize("data", [
    {"connection_name": "default"} # missing name
])
def test_invalid_dbview_config(data: dict):
    with pytest.raises(u.ConfigurationError):
        m.DbviewConfig.from_dict(data)


## Federate config

@pytest.mark.parametrize("data", [
    {"materialized": "table"} # missing name
])
def test_invalid_federate_config(data: dict):
    with pytest.raises(u.ConfigurationError):
        m.FederateConfig.from_dict(data)


## Dataset config

@pytest.mark.parametrize("data", [
    {"label": "My Dataset", "scope": "public"}, # missing name
    {"name": "my_dataset", "label": "My Dataset", "scope": "not_exist"} # invalid scope
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
        "parameters": [], "traits": {"key": "value"}, "default_test_set": "default"
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
    ("dataset_config1", []),
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
    assert dataset.traits == expected


## Full manifest config

@pytest.mark.parametrize("data", [
    {} # missing project_variables
])
def test_invalid_manifest_config(data: dict):
    with pytest.raises(u.ConfigurationError):
        m._ManifestConfig.from_dict(data)


@pytest.fixture(scope="module")
def manifest_config1() -> m._ManifestConfig:
    data = {"project_variables": {"name": "", "major_version": 0}}
    return m._ManifestConfig.from_dict(data)


@pytest.fixture(scope="module")
def manifest_config2() -> m._ManifestConfig:
    data = {
        "project_variables": {"name": "", "major_version": 0}, 
        "packages": [{"git": "path/test.git", "revision": "0.1.0"}],
        "connections": [],
        "parameters": [],
        "selection_test_sets": [],
        "dbviews": [],
        "federates": [],
        "datasets": [],
        "dashboards": [],
        "settings": {}
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
    assert list(manifest.connections.values()) == expected


@pytest.mark.parametrize("fixture,expected", [
    ("manifest_config1", []),
    ("manifest_config2", [])
])
def test_manifest_parameters(fixture: str, expected: list, request: pytest.FixtureRequest):
    manifest: m._ManifestConfig = request.getfixturevalue(fixture)
    assert manifest.parameters == expected


@pytest.mark.parametrize("fixture,expected", [
    ("manifest_config1", {}),
    ("manifest_config2", {})
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


@pytest.mark.parametrize("fixture,expected", [
    ("manifest_config1", {}),
    ("manifest_config2", {})
])
def test_manifest_dashboards(fixture: str, expected: dict, request: pytest.FixtureRequest):
    manifest: m._ManifestConfig = request.getfixturevalue(fixture)
    assert manifest.dashboards == expected


@pytest.mark.parametrize("fixture,expected", [
    ("manifest_config1", {}),
    ("manifest_config2", {})
])
def test_manifest_settings(fixture: str, expected: dict, request: pytest.FixtureRequest):
    manifest: m._ManifestConfig = request.getfixturevalue(fixture)
    assert manifest.datasets == expected
