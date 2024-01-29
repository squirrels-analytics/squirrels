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
    def _validate_required(cls, data: dict, required_keys: list[str], section: str):
        for key in required_keys:
            if key not in data:
                raise u.ConfigurationError(f'In {c.MANIFEST_FILE}, required field missing in {section}: {key}')
    
    @classmethod
    def from_dict(cls, kwargs: dict):
        return cls()


@dataclass
class ProjectVarsConfig(ManifestComponentConfig):
    data: dict

    def __post_init__(self):
        required_keys = [c.PROJECT_NAME_KEY, c.MAJOR_VERSION_KEY]
        self._validate_required(self.data, required_keys, c.PROJ_VARS_KEY)
        
        integer_keys = [c.MAJOR_VERSION_KEY]
        for key in integer_keys:
            if key in self.data and not isinstance(self.data[key], int):
                raise u.ConfigurationError(f'Project variable "{key}" must be an integer')
    
    @classmethod
    def from_dict(cls, kwargs: dict):
        return cls(kwargs)
    
    def get_name(self) -> str:
        return str(self.data[c.PROJECT_NAME_KEY])
    
    def get_label(self) -> str:
        return str(self.data.get(c.PROJECT_LABEL_KEY, self.get_name()))
    
    def get_major_version(self) -> int:
        return self.data[c.MAJOR_VERSION_KEY]


@dataclass
class PackageConfig(ManifestComponentConfig):
    git_url: str
    directory: str
    revision: str

    @classmethod
    def from_dict(cls, kwargs: dict):
        cls._validate_required(kwargs, [c.PACKAGE_GIT_KEY, c.PACKAGE_REVISION_KEY], c.PACKAGES_KEY)
        git_url = str(kwargs[c.PACKAGE_GIT_KEY])
        directory_raw = kwargs.get(c.PACKAGE_DIRECTORY_KEY)
        directory = git_url.split('/')[-1].removesuffix('.git') if directory_raw is None else str(directory_raw)
        revision = str(kwargs[c.PACKAGE_REVISION_KEY])
        return cls(git_url, directory, revision)


@dataclass
class DbConnConfig(ManifestComponentConfig):
    name: str
    url: str

    @classmethod
    def from_dict(cls, kwargs: dict):
        cls._validate_required(kwargs, [c.DB_CONN_NAME_KEY, c.DB_CONN_URL_KEY], c.DB_CONNECTIONS_KEY)
        name = str(kwargs[c.DB_CONN_NAME_KEY])
        credential_key = kwargs.get(c.DB_CONN_CRED_KEY)
        username, password = EnvironConfigIO.obj.get_credential(credential_key)
        url = str(kwargs[c.DB_CONN_URL_KEY]).format(username=username, password=password)
        return cls(name, url)


@dataclass
class ParametersConfig(ManifestComponentConfig):
    type: str
    factory: str
    arguments: dict

    @classmethod
    def from_dict(cls, kwargs: dict):
        all_keys = [c.PARAMETER_TYPE_KEY, c.PARAMETER_FACTORY_KEY, c.PARAMETER_ARGS_KEY]
        cls._validate_required(kwargs, all_keys, c.PARAMETERS_KEY)
        args = {key: kwargs[key] for key in all_keys}
        return cls(**args)


@dataclass
class TestSetsConfig(ManifestComponentConfig):
    name: str
    user_attributes: dict
    parameters: dict

    @classmethod
    def from_dict(cls, kwargs: dict):
        cls._validate_required(kwargs, [c.TEST_SET_NAME_KEY], c.TEST_SETS_KEY)
        name = str(kwargs[c.TEST_SET_NAME_KEY])
        user_attributes = kwargs.get(c.TEST_SET_USER_ATTR_KEY, {})
        parameters = kwargs.get(c.TEST_SET_PARAMETERS_KEY, {})
        return cls(name, user_attributes, parameters)


@dataclass
class DbviewConfig(ManifestComponentConfig):
    name: str
    connection_name: Optional[str]

    @classmethod
    def from_dict(cls, kwargs: dict):
        cls._validate_required(kwargs, [c.DBVIEW_NAME_KEY], c.DBVIEWS_KEY)
        name = str(kwargs[c.DBVIEW_NAME_KEY])
        connection_name = str(kwargs.get(c.DBVIEW_CONN_KEY))
        return cls(name, connection_name)


@dataclass
class FederateConfig(ManifestComponentConfig):
    name: str
    materialized: Optional[str]

    @classmethod
    def from_dict(cls, kwargs: dict):
        cls._validate_required(kwargs, [c.FEDERATE_NAME_KEY], c.FEDERATES_KEY)
        name = str(kwargs[c.FEDERATE_NAME_KEY])
        materialized = str(kwargs.get(c.MATERIALIZED_KEY))
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
    traits: dict

    @classmethod
    def from_dict(cls, kwargs: dict):
        cls._validate_required(kwargs, [c.DATASET_NAME_KEY], c.DATASETS_KEY)
        name = str(kwargs[c.DATASET_NAME_KEY])
        label = str(kwargs.get(c.DATASET_LABEL_KEY, name))
        model = str(kwargs.get(c.DATASET_MODEL_KEY, name))
        scope_raw = kwargs.get(c.DATASET_SCOPE_KEY)
        try:
            scope = DatasetScope[str(scope_raw).upper()] if scope_raw is not None else DatasetScope.PUBLIC
        except KeyError as e:
            scope_list = [scope.name.lower() for scope in DatasetScope]
            raise u.ConfigurationError(f'Scope not found for dataset "{name}". Scope must be one of {scope_list}') from e
        
        parameters = kwargs.get(c.DATASET_PARAMETERS_KEY)
        traits = kwargs.get(c.DATASET_TRAITS_KEY, {})
        return cls(name, label, model, scope, parameters, traits)


@dataclass
class _ManifestConfig:
    project_variables: ProjectVarsConfig
    packages: list[PackageConfig]
    connections: dict[str, DbConnConfig]
    parameters: list[ParametersConfig]
    selection_test_sets: dict[str, TestSetsConfig]
    dbviews: dict[str, DbviewConfig]
    federates: dict[str, FederateConfig]
    datasets: dict[str, DatasetsConfig]
    settings: dict

    @classmethod
    def _create_configs_as_dict(cls, config_cls: ManifestComponentConfig, kwargs: dict, section_key: str, name_key: str) -> dict:
        configs_dict = {}
        for x in kwargs.get(section_key, []):
            name = x[name_key]
            if name in configs_dict:
                raise u.ConfigurationError(f'In the "{section_key}" section of {c.MANIFEST_FILE}, the name/identifier "{name}" was specified multiple times')
            configs_dict[name] = config_cls.from_dict(x)
        return configs_dict

    @classmethod
    def from_dict(cls, kwargs: dict):
        settings: dict = kwargs.get(c.SETTINGS_KEY, {})

        try:
            proj_vars = ProjectVarsConfig(kwargs[c.PROJ_VARS_KEY])
        except KeyError as e:
            raise u.ConfigurationError(f'In {c.MANIFEST_FILE}, section for {c.PROJ_VARS_KEY} is required') from e
        
        packages = [PackageConfig.from_dict(x) for x in kwargs.get(c.PACKAGES_KEY, [])]
        all_package_dirs = set()
        for package in packages:
            if package.directory in all_package_dirs:
                raise u.ConfigurationError(f'In the "{c.PACKAGES_KEY}" section of {c.MANIFEST_FILE}, multiple target directories found for "{package.directory}"')
            all_package_dirs.add(package.directory)

        db_conns = cls._create_configs_as_dict(DbConnConfig, kwargs, c.DB_CONNECTIONS_KEY, c.DB_CONN_NAME_KEY)
        params = [ParametersConfig.from_dict(x) for x in kwargs.get(c.PARAMETERS_KEY, [])]

        test_sets = cls._create_configs_as_dict(TestSetsConfig, kwargs, c.TEST_SETS_KEY, c.TEST_SET_NAME_KEY)
        default_test_set: str = settings.get(c.TEST_SET_DEFAULT_USED_SETTING, c.DEFAULT_TEST_SET_NAME)
        test_sets.setdefault(default_test_set, TestSetsConfig.from_dict({c.TEST_SET_NAME_KEY: default_test_set}))

        dbviews = cls._create_configs_as_dict(DbviewConfig, kwargs, c.DBVIEWS_KEY, c.DBVIEW_NAME_KEY)
        federates = cls._create_configs_as_dict(FederateConfig, kwargs, c.FEDERATES_KEY, c.FEDERATE_NAME_KEY)
        datasets = cls._create_configs_as_dict(DatasetsConfig, kwargs, c.DATASETS_KEY, c.DATASET_NAME_KEY)

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
