from typing import Any
from pydantic import ValidationError
import pytest

from squirrels import _manifest as m


class TestProjectVarsConfig:
    @pytest.fixture(scope="class")
    def proj_vars1(self):
        data = {"name": "my_name", "major_version": 1}
        return m.ProjectVarsConfig(**data)
    
    @pytest.fixture(scope="class")
    def proj_vars2(self):
        data = {"name": "my_name", "label": "my_label", "major_version": 1}
        return m.ProjectVarsConfig(**data)
    
    @pytest.mark.parametrize("fixture,expected", [
        ("proj_vars1", "my_name"),
        ("proj_vars2", "my_label")
    ])
    def test_proj_vars_label(self, fixture: str, expected: str, request: pytest.FixtureRequest):
        proj_vars: m.ProjectVarsConfig = request.getfixturevalue(fixture)
        assert proj_vars.label == expected


class TestPackageConfig:
    @pytest.fixture(scope="class")
    def package_config1(self) -> m.PackageConfig:
        data = {"git": "path/test.git", "revision": "0.1.0"}
        return m.PackageConfig(**data)

    @pytest.fixture(scope="class")
    def package_config2(self) -> m.PackageConfig:
        data = {"git": "my.git.test", "revision": "0.1.0"}
        return m.PackageConfig(**data)

    @pytest.fixture(scope="class")
    def package_config3(self) -> m.PackageConfig:
        data = {"git": "path/test.git", "directory": "mytest", "revision": "0.1.0"}
        return m.PackageConfig(**data)
    
    @pytest.mark.parametrize("fixture,expected", [
        ("package_config1", "test"),
        ("package_config2", "my.git.test"),
        ("package_config3", "mytest")
    ])
    def test_package_directory(self, fixture: str, expected: str, request: pytest.FixtureRequest):
        package: m.PackageConfig = request.getfixturevalue(fixture)
        assert package.directory == expected


class TestDbConnConfig:
    @pytest.fixture(scope="class")
    def db_conn_config1(self) -> m.DbConnConfig:
        data = {"name": "default", "credential": "test_cred_key", "url": "{username}:{password}/my/url"}
        return m.DbConnConfig(**data)

    @pytest.fixture(scope="class")
    def db_conn_config2(self) -> m.DbConnConfig:
        data = {"name": "default", "url": "{username}:{password}/my/url"}
        return m.DbConnConfig(**data)

    @pytest.mark.parametrize("fixture,expected", [
        ("db_conn_config1", "user1:pass1/my/url"),
        ("db_conn_config2", ":/my/url")
    ])
    def test_db_conn_url(self, fixture: str, expected: str, request: pytest.FixtureRequest):
        db_conn: m.DbConnConfig = request.getfixturevalue(fixture)
        assert db_conn.url == expected


class TestDatasetConfig:
    @pytest.fixture(scope="class")
    def dataset_config1(self) -> m.DatasetConfig:
        data: dict[str, Any] = {"name": "my_dataset"}
        return m.DatasetConfig(**data)

    @pytest.fixture(scope="class")
    def dataset_config2(self) -> m.DatasetConfig:
        data = {
            "name": "my_dataset", "label": "My Dataset", "model": "my_model", "scope": "protected", 
            "parameters": [], "traits": {"key": "value"}, "default_test_set": "default"
        }
        return m.DatasetConfig(**data)

    @pytest.mark.parametrize("fixture,expected", [
        ("dataset_config1", "my_dataset"),
        ("dataset_config2", "My Dataset")
    ])
    def test_dataset_label(self, fixture: str, expected: str, request: pytest.FixtureRequest):
        dataset: m.DatasetConfig = request.getfixturevalue(fixture)
        assert dataset.label == expected
    
    @pytest.mark.parametrize("fixture,expected", [
        ("dataset_config1", "my_dataset"),
        ("dataset_config2", "my_model")
    ])
    def test_dataset_model(self, fixture: str, expected: str, request: pytest.FixtureRequest):
        dataset: m.DatasetConfig = request.getfixturevalue(fixture)
        assert dataset.model == expected

    @pytest.mark.parametrize("fixture,expected", [
        ("dataset_config1", m.DatasetScope.PUBLIC),
        ("dataset_config2", m.DatasetScope.PROTECTED)
    ])
    def test_dataset_scope(self, fixture: str, expected: m.DatasetScope, request: pytest.FixtureRequest):
        dataset: m.DatasetConfig = request.getfixturevalue(fixture)
        assert dataset.scope == expected

    @pytest.mark.parametrize("fixture,expected", [
        ("dataset_config1", []),
        ("dataset_config2", [])
    ])
    def test_dataset_parameters(self, fixture: str, expected: list, request: pytest.FixtureRequest):
        dataset: m.DatasetConfig = request.getfixturevalue(fixture)
        assert dataset.parameters == expected

    @pytest.mark.parametrize("fixture,expected", [
        ("dataset_config1", {}),
        ("dataset_config2", {"key": "value"})
    ])
    def test_dataset_traits(self, fixture: str, expected: dict, request: pytest.FixtureRequest):
        dataset: m.DatasetConfig = request.getfixturevalue(fixture)
        assert dataset.traits == expected
    
    def test_invalid_dataset(self):
        with pytest.raises(ValidationError):
            m.DatasetConfig(name="my_dataset", scope="not_valid") # type: ignore


class TestDashboardConfig:
    @pytest.fixture(scope="class")
    def dashboard_config1(self) -> m.DashboardConfig:
        data: dict[str, Any] = {"name": "my_dashboard"}
        return m.DashboardConfig(**data)

    @pytest.fixture(scope="class")
    def dashboard_config2(self) -> m.DashboardConfig:
        data = {
            "name": "my_dashboard", "label": "My Dataset", "scope": "protected", "parameters": []
        }
        return m.DashboardConfig(**data)
    
    def test_hash(self, dashboard_config1: m.DashboardConfig):
        assert hash(dashboard_config1) == hash("dashboard_my_dashboard")


class TestTestSetsConfig:
    @pytest.fixture(scope="class")
    def test_sets_config1(self) -> m.TestSetsConfig:
        data: dict[str, Any] = {"name": "test_set1"}
        return m.TestSetsConfig(**data)

    @pytest.fixture(scope="class")
    def test_sets_config2(self) -> m.TestSetsConfig:
        data = {"name": "test_set2", "user_attributes": {"role": "manager"}}
        return m.TestSetsConfig(**data)

    @pytest.fixture(scope="class")
    def test_sets_config3(self) -> m.TestSetsConfig:
        data = {"name": "test_set3", "user_attributes": {}}
        return m.TestSetsConfig(**data)

    @pytest.fixture(scope="class")
    def test_sets_config4(self) -> m.TestSetsConfig:
        data = {"name": "test_set4", "is_authenticated": True}
        return m.TestSetsConfig(**data)

    @pytest.mark.parametrize("fixture,expected", [
        ("test_sets_config1", False),
        ("test_sets_config2", True),
        ("test_sets_config3", False),
        ("test_sets_config4", True)
    ])
    def test_is_authenticated(self, fixture: str, expected: bool, request: pytest.FixtureRequest):
        test_sets: m.TestSetsConfig = request.getfixturevalue(fixture)
        assert test_sets.is_authenticated == expected

