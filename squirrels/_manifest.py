from typing import Optional
from dataclasses import dataclass
from enum import Enum
import yaml

from . import _constants as c, _utils as u
from ._environcfg import EnvironConfigIO
from ._timer import timer, time


@dataclass
class ManifestComponentConfig:
    @classmethod
    def validate_required(cls, data: dict, required_keys: list[str], section: str):
        for key in required_keys:
            if key not in data:
                raise u.ConfigurationError(f'Required field missing from {section}: {key}')


@dataclass
class ProjectVarsConfig(ManifestComponentConfig):
    data: dict

    def __post_init__(self):
        required_keys = [c.PROJECT_NAME_KEY, c.MAJOR_VERSION_KEY, c.MINOR_VERSION_KEY]
        self.validate_required(self.data, required_keys, c.PROJ_VARS_KEY)
        
        integer_keys = [c.MAJOR_VERSION_KEY, c.MINOR_VERSION_KEY]
        for key in integer_keys:
            if key in self.data and not isinstance(self.data[key], int):
                raise u.ConfigurationError(f'Project variable "{key}" must be an integer')
    
    def get_name(self) -> str:
        return str(self.data[c.PROJECT_NAME_KEY])
    
    def get_label(self) -> str:
        return str(self.data.get(c.PROJECT_LABEL_KEY, self.get_name()))
    
    def get_major_version(self) -> int:
        return self.data[c.MAJOR_VERSION_KEY]
    
    def get_minor_version(self) -> int:
        return self.data[c.MINOR_VERSION_KEY]


@dataclass
class PackageConfig(ManifestComponentConfig):
    git_url: str
    directory: str
    revision: str

    @classmethod
    def from_dict(cls, kwargs: dict):
        cls.validate_required(kwargs, [c.PACKAGE_GIT_KEY, c.PACKAGE_REVISION_KEY], c.PACKAGES_KEY)
        git_url = str(kwargs[c.PACKAGE_GIT_KEY])
        directory_raw = kwargs.get(c.PACKAGE_DIRECTORY_KEY)
        directory = git_url.split('/')[-1].removesuffix('.git') if directory_raw is None else str(directory_raw)
        revision = str(kwargs[c.PACKAGE_REVISION_KEY])
        return cls(git_url, directory, revision)


@dataclass
class DbConnConfig(ManifestComponentConfig):
    connection_name: str
    url: str

    @classmethod
    def from_dict(cls, kwargs: dict):
        cls.validate_required(kwargs, [c.DB_CONN_NAME_KEY, c.DB_CONN_URL_KEY], c.DB_CONNECTIONS_KEY)
        connection_name = str(kwargs[c.DB_CONN_NAME_KEY])
        credential_key = kwargs.get(c.CREDENTIALS_KEY)
        username, password = EnvironConfigIO.obj.get_credential(credential_key)
        url = str(kwargs[c.DB_CONN_URL_KEY]).format(username=username, password=password)
        return cls(connection_name, url)


@dataclass
class ParametersConfig(ManifestComponentConfig):
    name: str
    type: str
    factory: str
    arguments: dict

    @classmethod
    def from_dict(cls, kwargs: dict):
        all_keys = [c.PARAMETER_NAME_KEY, c.PARAMETER_TYPE_KEY, c.PARAMETER_FACTORY_KEY, c.PARAMETER_ARGS_KEY]
        cls.validate_required(kwargs, all_keys, c.PARAMETERS_KEY)
        args = {key: kwargs[key] for key in all_keys}
        return cls(**args)


@dataclass
class TestSetsConfig(ManifestComponentConfig):
    name: str
    user_attributes: dict
    parameters: dict

    @classmethod
    def from_dict(cls, kwargs: dict):
        cls.validate_required(kwargs, [c.TEST_SET_NAME_KEY], c.TEST_SETS_KEY)
        name = str(kwargs[c.TEST_SET_NAME_KEY])
        user_attributes = kwargs.get(c.TEST_SET_USER_ATTR_KEY, {})
        parameters = kwargs.get(c.TEST_SET_PARAMETERS_KEY, {})
        return cls(name, user_attributes, parameters)


@dataclass
class DbviewConfig(ManifestComponentConfig):
    name: str
    connection_name: str

    @classmethod
    def from_dict(cls, kwargs: dict):
        cls.validate_required(kwargs, [c.DBVIEW_NAME_KEY], c.DBVIEWS_KEY)
        name = str(kwargs[c.DBVIEW_NAME_KEY])
        connection_name = str(kwargs.get(c.DBVIEW_CONN_KEY, c.DEFAULT_DB_CONN))
        return cls(name, connection_name)


@dataclass
class FederateConfig(ManifestComponentConfig):
    name: str
    materialized: str

    @classmethod
    def from_dict(cls, kwargs: dict):
        cls.validate_required(kwargs, [c.FEDERATE_NAME_KEY], c.FEDERATES_KEY)
        name = str(kwargs[c.FEDERATE_NAME_KEY])
        materialized = str(kwargs.get(c.MATERIALIZED_KEY, c.DEFAULT_TABLE_MATERIALIZE))
        return cls(name, materialized)


class DatasetScope(Enum):
    PUBLIC = 0
    PROTECTED = 1
    PRIVATE = 2

@dataclass
class DatasetsConfig(ManifestComponentConfig):
    name: str
    label: str
    model: str
    scope: DatasetScope
    parameters: Optional[list[str]]
    args: dict

    @classmethod
    def from_dict(cls, kwargs: dict):
        cls.validate_required(kwargs, [c.DATASET_NAME_KEY], c.DATASETS_KEY)
        name = str(kwargs[c.DATASET_NAME_KEY])
        label = str(kwargs.get(c.DATASET_LABEL_KEY, name))
        model = str(kwargs.get(c.DATASET_MODEL_KEY, name))
        scope_raw = kwargs.get(c.DATASET_SCOPE_KEY)
        if scope_raw is None:
            scope = DatasetScope.PUBLIC
        else:
            scope = DatasetScope[str(scope_raw).upper()]
        parameters = kwargs.get(c.DATASET_PARAMETERS_KEY)
        args = kwargs.get(c.DATASET_ARGS_KEY, {})
        return cls(name, label, model, scope, parameters, args)


@dataclass
class _ManifestConfig:
    project_variables: ProjectVarsConfig
    packages: list[PackageConfig]
    db_connections: list[DbConnConfig]
    parameters: list[ParametersConfig]
    selection_test_sets: dict[str, TestSetsConfig]
    dbviews: dict[str, DbviewConfig]
    federates: dict[str, FederateConfig]
    datasets: dict[str, DatasetsConfig]
    settings: dict

    @classmethod
    def from_dict(cls, kwargs: dict):
        proj_vars = ProjectVarsConfig(kwargs[c.PROJ_VARS_KEY])
        packages = [PackageConfig.from_dict(x) for x in kwargs.get(c.PACKAGES_KEY, [])]
        db_conns = [DbConnConfig.from_dict(x) for x in kwargs.get(c.DB_CONNECTIONS_KEY, [])]
        params = [ParametersConfig.from_dict(x) for x in kwargs.get(c.PARAMETERS_KEY, [])]
        test_sets = {x[c.TEST_SET_NAME_KEY]: TestSetsConfig.from_dict(x) for x in kwargs.get(c.TEST_SETS_KEY, [])}
        dbviews = {x[c.DBVIEW_NAME_KEY]: DbviewConfig.from_dict(x) for x in kwargs.get(c.DBVIEWS_KEY, [])}
        federates = {x[c.FEDERATE_NAME_KEY]: FederateConfig.from_dict(x) for x in kwargs.get(c.FEDERATES_KEY, [])}
        datasets = {x[c.DATASET_NAME_KEY]: DatasetsConfig.from_dict(x) for x in kwargs.get(c.DATASETS_KEY, [])}
        settings = kwargs.get(c.SETTINGS_KEY, {})
        test_sets.setdefault(c.TEST_SET_DEFAULT_NAME, TestSetsConfig.from_dict({c.TEST_SET_NAME_KEY: c.TEST_SET_DEFAULT_NAME}))
        return cls(proj_vars, packages, db_conns, params, test_sets, dbviews, federates, datasets, settings)


class ManifestIO:
    obj: _ManifestConfig

    @classmethod
    def LoadFromFile(cls) -> None:
        EnvironConfigIO.LoadFromFile()
        
        start = time.time()
        raw_content = u.read_file(c.MANIFEST_FILE)
        env_config = EnvironConfigIO.obj.get_all_env_vars()
        content = u.render_string(raw_content, env_config)
        proj_config = yaml.safe_load(content)
        cls.obj = _ManifestConfig.from_dict(proj_config)
        timer.add_activity_time(f"loading {c.MANIFEST_FILE} file", start)
