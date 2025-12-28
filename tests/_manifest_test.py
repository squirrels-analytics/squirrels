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
        data = {"name": "my_name", "label": "My Label", "major_version": 1}
        return m.ProjectVarsConfig(**data)
    
    @pytest.mark.parametrize("fixture,expected", [
        ("proj_vars1", "My Name"),
        ("proj_vars2", "My Label")
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
        data = {"name": "default", "type": m.ConnectionTypeEnum.SQLALCHEMY, "uri": "sqlite:///{project_path}/my/database.db"}
        return m.DbConnConfig(**data)

    @pytest.fixture(scope="class")
    def db_conn_config2(self) -> m.DbConnConfig:
        data = {"name": "default", "type": m.ConnectionTypeEnum.CONNECTORX, "uri": "sqlite:///{project_path}/my/database.db"}
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
            "parameters": []
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
        ("dataset_config1", None),
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

    def test_invalid_dataset(self):
        with pytest.raises(ValueError):
            m.DatasetConfig(name="my_dataset", scope="not_valid") # type: ignore
    
    @pytest.fixture(scope="class")
    def dataset_config_with_configurables(self) -> m.DatasetConfig:
        data = {
            "name": "my_dataset",
            "configurables": [
                {"name": "config1", "default": "value1"},
                {"name": "config2", "default": 123}
            ]
        }
        return m.DatasetConfig(**data)
    
    def test_dataset_configurables_missing_required_fields(self):
        """Test that name and default are required fields"""
        with pytest.raises(ValidationError):
            m.DatasetConfig(name="my_dataset", configurables=[{"name": "config1"}])
        
        with pytest.raises(ValidationError):
            m.DatasetConfig(name="my_dataset", configurables=[{"default": "value"}])


class TestTestSetsConfig:
    @pytest.fixture(scope="class")
    def test_sets_config1(self) -> m.TestSetsConfig:
        data: dict[str, Any] = {"name": "test_set1"}
        return m.TestSetsConfig(**data)

    @pytest.fixture(scope="class")
    def test_sets_config2(self) -> m.TestSetsConfig:
        data = {"name": "test_set2", "user": {"custom_fields": {"role": "manager"}}}
        return m.TestSetsConfig(**data)

    @pytest.fixture(scope="class")
    def test_sets_config3(self) -> m.TestSetsConfig:
        data = {"name": "test_set3", "user": {}}
        return m.TestSetsConfig(**data)


class TestConnectionProperties:
    @pytest.fixture(scope="class")
    def sqlite_sqlalchemy_conn(self) -> m.ConnectionProperties:
        return m.ConnectionProperties(
            type=m.ConnectionTypeEnum.SQLALCHEMY,
            uri="sqlite:///path/to/db.sqlite"
        )

    @pytest.fixture(scope="class")
    def postgres_sqlalchemy_conn(self) -> m.ConnectionProperties:
        return m.ConnectionProperties(
            type=m.ConnectionTypeEnum.SQLALCHEMY,
            uri="postgresql+psycopg2://user:pass@localhost:5432/mydb"
        )

    @pytest.fixture(scope="class")
    def sqlite_connectorx_conn(self) -> m.ConnectionProperties:
        return m.ConnectionProperties(
            type=m.ConnectionTypeEnum.CONNECTORX,
            uri="sqlite:///path/to/db.sqlite"
        )

    @pytest.fixture(scope="class")
    def postgres_connectorx_conn(self) -> m.ConnectionProperties:
        return m.ConnectionProperties(
            type=m.ConnectionTypeEnum.CONNECTORX,
            uri="postgresql://user:pass@localhost:5432/mydb"
        )

    def test_engine_sqlalchemy(self, sqlite_sqlalchemy_conn: m.ConnectionProperties):
        assert sqlite_sqlalchemy_conn.engine is not None
        assert str(sqlite_sqlalchemy_conn.engine.url) == "sqlite:///path/to/db.sqlite"

    def test_engine_non_sqlalchemy(self, sqlite_connectorx_conn: m.ConnectionProperties):
        with pytest.raises(ValueError):
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
        ("sqlite_sqlalchemy_conn", "sqlite:path/to/db.sqlite"),
        ("postgres_sqlalchemy_conn", "postgres:dbname=mydb user=user password=pass host=localhost port=5432"),
        ("sqlite_connectorx_conn", "sqlite:/path/to/db.sqlite"),
        ("postgres_connectorx_conn", "postgres:dbname=mydb user=user password=pass host=localhost port=5432")
    ])
    def test_attach_uri_for_duckdb(self, fixture: str, expected: str, request: pytest.FixtureRequest):
        conn: m.ConnectionProperties = request.getfixturevalue(fixture)
        assert conn.attach_uri_for_duckdb == expected

    def test_attach_uri_unsupported_dialect(self):
        conn = m.ConnectionProperties(
            type=m.ConnectionTypeEnum.CONNECTORX,
            uri="oracle://user:pass@localhost:1521/mydb"
        )
        assert conn.attach_uri_for_duckdb is None


class TestManifestConfigurables:
    @pytest.fixture(scope="class")
    def manifest_with_configurables(self) -> m.ManifestConfig:
        data = {
            "project_variables": {"name": "test_proj", "major_version": 1},
            "configurables": {
                "config1": {"name": "config1", "label": "Config 1", "default": "default1", "description": "First config"},
                "config2": {"name": "config2", "label": "Config 2", "default": "default2", "description": "Second config"}
            },
            "datasets": {
                "dataset1": {
                    "name": "dataset1",
                    "configurables": [
                        {"name": "config1", "default": "dataset1_value"}
                    ]
                },
                "dataset2": {
                    "name": "dataset2"
                }
            }
        }
        return m.ManifestConfig(**data)
    
    def test_get_default_configurables_no_dataset(self, manifest_with_configurables: m.ManifestConfig):
        defaults = manifest_with_configurables.get_default_configurables()
        assert defaults == {"config1": "default1", "config2": "default2"}
    
    def test_get_default_configurables_with_dataset_override(self, manifest_with_configurables: m.ManifestConfig):
        overrides = manifest_with_configurables.datasets["dataset1"].configurables
        defaults = manifest_with_configurables.get_default_configurables(overrides=overrides)
        assert defaults == {"config1": "dataset1_value", "config2": "default2"}
    
    def test_get_default_configurables_with_dataset_no_override(self, manifest_with_configurables: m.ManifestConfig):
        overrides = manifest_with_configurables.datasets["dataset2"].configurables
        defaults = manifest_with_configurables.get_default_configurables(overrides=overrides)
        assert defaults == {"config1": "default1", "config2": "default2"}
    
    def test_invalid_dataset_configurable(self):
        with pytest.raises(ValueError, match='references configurable "invalid_config"'):
            m.ManifestConfig(
                project_variables={"name": "test_proj", "major_version": 1},
                configurables={
                    "config1": {"name": "config1", "label": "Config 1", "default": "default1", "description": "First config"}
                },
                datasets={
                    "dataset1": {
                        "name": "dataset1",
                        "configurables": [
                            {"name": "invalid_config", "default": "value"}
                        ]
                    }
                }
            )
