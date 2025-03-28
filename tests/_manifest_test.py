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
        data = {"name": "default", "type": m.ConnectionType.SQLALCHEMY, "uri": "sqlite:///{project_path}/my/database.db"}
        return m.DbConnConfig(**data)

    @pytest.fixture(scope="class")
    def db_conn_config2(self) -> m.DbConnConfig:
        data = {"name": "default", "type": m.ConnectionType.CONNECTORX, "uri": "sqlite:///{project_path}/my/database.db"}
        return m.DbConnConfig(**data)

    @pytest.mark.parametrize("fixture,expected", [
        ("db_conn_config1", "sqlite:///./my/database.db"),
        ("db_conn_config2", "sqlite:///./my/database.db")
    ])
    def test_db_conn_url(self, fixture: str, expected: str, request: pytest.FixtureRequest):
        db_conn: m.DbConnConfig = request.getfixturevalue(fixture)
        db_conn.finalize_uri(".")
        assert db_conn.uri == expected


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
        ("dataset_config1", m.PermissionScope.PUBLIC),
        ("dataset_config2", m.PermissionScope.PROTECTED)
    ])
    def test_dataset_scope(self, fixture: str, expected: m.PermissionScope, request: pytest.FixtureRequest):
        dataset: m.DatasetConfig = request.getfixturevalue(fixture)
        assert dataset.scope == expected

    @pytest.mark.parametrize("fixture,expected", [
        ("dataset_config1", None),
        ("dataset_config2", [])
    ])
    def test_dataset_parameters(self, fixture: str, expected: list[str] | None, request: pytest.FixtureRequest):
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
        with pytest.raises(ValueError):
            m.DatasetConfig(name="my_dataset", scope="not_valid") # type: ignore


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

    @pytest.mark.parametrize("fixture,expected", [
        ("test_sets_config1", False),
        ("test_sets_config2", True),
        ("test_sets_config3", True),
    ])
    def test_is_authenticated(self, fixture: str, expected: bool, request: pytest.FixtureRequest):
        test_sets: m.TestSetsConfig = request.getfixturevalue(fixture)
        assert test_sets.is_authenticated == expected


class TestManifestDatasetTraits:
    def test_valid_dataset_traits(self):
        # Create a manifest with traits and datasets using those traits
        manifest = m.ManifestConfig(
            project_variables=m.ProjectVarsConfig(name="test", major_version=1),
            dataset_traits={
                "trait1": m.DatasetTraitConfig(name="trait1", default="default1"),
                "trait2": m.DatasetTraitConfig(name="trait2", default=42),
            },
            datasets={
                "dataset1": m.DatasetConfig(
                    name="dataset1", 
                    traits={"trait1": "custom1", "trait2": 100}
                ),
                "dataset2": m.DatasetConfig(
                    name="dataset2",
                    traits={"trait1": "value1"}
                ),
            }
        )
        
        # Verify traits were properly processed
        assert manifest.datasets["dataset1"].traits["trait1"] == "custom1"
        assert manifest.datasets["dataset1"].traits["trait2"] == 100
        assert manifest.datasets["dataset2"].traits["trait1"] == "value1"
        # Verify default value was applied to trait2 in dataset2
        assert manifest.datasets["dataset2"].traits["trait2"] == 42

    def test_undefined_trait(self):
        # Test that using an undefined trait raises a ValidationError
        with pytest.raises(ValueError) as excinfo:
            m.ManifestConfig(
                project_variables=m.ProjectVarsConfig(name="test", major_version=1),
                dataset_traits={
                    "trait1": m.DatasetTraitConfig(name="trait1", default="default1"),
                },
                datasets={
                    "dataset1": m.DatasetConfig(
                        name="dataset1", 
                        traits={"trait1": "custom1", "unknown_trait": "error"}
                    ),
                }
            )
        

class TestManifestConfig:
    @pytest.fixture(scope="class")
    def manifest_config1(self) -> m.ManifestConfig:
        selection_test_sets = {
            "test_set1": m.TestSetsConfig(name="test_set1"),
            "test_set2": m.TestSetsConfig(name="test_set2", datasets=["modelA"]),
            "test_set3": m.TestSetsConfig(name="test_set3", datasets=["modelB"]),
        }
        manifest_cfg = m.ManifestConfig(
            project_variables=m.ProjectVarsConfig(name="", major_version=0),
            selection_test_sets=selection_test_sets
        )
        return manifest_cfg


    @pytest.mark.parametrize("fixture,dataset,expected", [
        ("manifest_config1", "modelA", ["test_set1", "test_set2"]),
    ])
    def test_get_applicable_test_sets(self, fixture: str, dataset: str, expected: list[str], request: pytest.FixtureRequest):
        manifest_config1: m.ManifestConfig = request.getfixturevalue(fixture)
        assert manifest_config1.get_applicable_test_sets(dataset) == expected


class TestConnectionProperties:
    @pytest.fixture(scope="class")
    def sqlite_sqlalchemy_conn(self) -> m.ConnectionProperties:
        return m.ConnectionProperties(
            type=m.ConnectionType.SQLALCHEMY,
            uri="sqlite:///path/to/db.sqlite"
        )

    @pytest.fixture(scope="class")
    def postgres_sqlalchemy_conn(self) -> m.ConnectionProperties:
        return m.ConnectionProperties(
            type=m.ConnectionType.SQLALCHEMY,
            uri="postgresql+psycopg2://user:pass@localhost:5432/mydb"
        )

    @pytest.fixture(scope="class")
    def sqlite_connectorx_conn(self) -> m.ConnectionProperties:
        return m.ConnectionProperties(
            type=m.ConnectionType.CONNECTORX,
            uri="sqlite:///path/to/db.sqlite"
        )

    @pytest.fixture(scope="class")
    def postgres_connectorx_conn(self) -> m.ConnectionProperties:
        return m.ConnectionProperties(
            type=m.ConnectionType.CONNECTORX,
            uri="postgresql://user:pass@localhost:5432/mydb"
        )

    def test_engine_sqlalchemy(self, sqlite_sqlalchemy_conn: m.ConnectionProperties):
        assert sqlite_sqlalchemy_conn.engine is not None
        assert str(sqlite_sqlalchemy_conn.engine.url) == "sqlite:///path/to/db.sqlite"

    def test_engine_non_sqlalchemy(self, sqlite_connectorx_conn: m.ConnectionProperties):
        with pytest.raises(ValueError, match='Connection type "ConnectionType.CONNECTORX" does not support engine property'):
            _ = sqlite_connectorx_conn.engine

    @pytest.mark.parametrize("fixture,expected", [
        ("sqlite_sqlalchemy_conn", "sqlite"),
        ("postgres_sqlalchemy_conn", "postgres"),
        ("sqlite_connectorx_conn", "sqlite"),
        ("postgres_connectorx_conn", "postgres")
    ])
    def test_dialect(self, fixture: str, expected: str, request: pytest.FixtureRequest):
        conn: m.ConnectionProperties = request.getfixturevalue(fixture)
        assert conn.dialect == expected

    @pytest.mark.parametrize("fixture,expected", [
        ("sqlite_sqlalchemy_conn", "path/to/db.sqlite"),
        ("postgres_sqlalchemy_conn", "dbname=mydb user=user password=pass host=localhost port=5432"),
        ("sqlite_connectorx_conn", "/path/to/db.sqlite"),
        ("postgres_connectorx_conn", "dbname=mydb user=user password=pass host=localhost port=5432")
    ])
    def test_attach_uri_for_duckdb(self, fixture: str, expected: str, request: pytest.FixtureRequest):
        conn: m.ConnectionProperties = request.getfixturevalue(fixture)
        assert conn.attach_uri_for_duckdb == expected

    def test_attach_uri_unsupported_dialect(self):
        conn = m.ConnectionProperties(
            type=m.ConnectionType.CONNECTORX,
            uri="oracle://user:pass@localhost:1521/mydb"
        )
        assert conn.attach_uri_for_duckdb is None
