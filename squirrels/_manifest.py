from functools import cached_property
from typing import Literal, Any
from urllib.parse import urlparse
from sqlalchemy import Engine, create_engine
from typing_extensions import Self
from enum import Enum
from pydantic import BaseModel, Field, field_validator, model_validator, ValidationInfo, ValidationError
import yaml, time, re

from . import _constants as c, _utils as u


class ProjectVarsConfig(BaseModel, extra="allow"):
    name: str
    label: str = ""
    description: str = ""
    major_version: int

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        if not re.fullmatch(r"[A-Za-z0-9_-]+", v):
            raise ValueError("Project name must only contain alphanumeric characters, underscores, and dashes.")
        return v

    @model_validator(mode="after")
    def finalize_label(self) -> Self:
        if self.label == "":
            self.label = u.to_title_case(self.name)
        return self


class PackageConfig(BaseModel):
    git: str
    revision: str
    directory: str = ""

    @model_validator(mode="after")
    def finalize_directory(self) -> Self:
        if self.directory == "":
            self.directory = self.git.split('/')[-1].removesuffix('.git')
        return self


class _ConfigWithNameBaseModel(BaseModel):
    name: str


class ConnectionTypeEnum(Enum):
    SQLALCHEMY = "sqlalchemy"
    CONNECTORX = "connectorx"
    ADBC = "adbc"
    DUCKDB = "duckdb"


class ConnectionProperties(BaseModel):
    """
    A class for holding the properties of a connection

    Arguments:
        type: The type of connection, one of "sqlalchemy", "connectorx", or "adbc"
        uri: The URI for the connection
    """
    label: str | None = None
    type: ConnectionTypeEnum = Field(default=ConnectionTypeEnum.SQLALCHEMY)
    uri: str
    sa_create_engine_args: dict[str, Any] = Field(default_factory=dict)

    @cached_property
    def engine(self) -> Engine:
        """
        Creates and caches a SQLAlchemy engine if the connection type is sqlalchemy.
        Returns None for other connection types.
        """
        if self.type == ConnectionTypeEnum.SQLALCHEMY:
            return create_engine(self.uri, **self.sa_create_engine_args)
        else:
            raise ValueError(f'Connection type "{self.type}" does not support engine property')

    @cached_property
    def dialect(self) -> str:
        default_dialect = None
        if self.type == ConnectionTypeEnum.SQLALCHEMY:
            dialect = self.engine.dialect.name
        elif self.type == ConnectionTypeEnum.DUCKDB:
            dialect = self.uri.split(':')[0]
            default_dialect = 'duckdb'
        else:
            url = urlparse(self.uri)
            dialect = url.scheme
        
        processed_dialect = next((d for d in ['sqlite', 'postgres', 'mysql', 'duckdb'] if dialect.lower().startswith(d)), default_dialect)
        dialect = processed_dialect if processed_dialect is not None else dialect
        return dialect
    
    @cached_property
    def attach_uri_for_duckdb(self) -> str | None:
        if self.type == ConnectionTypeEnum.DUCKDB:
            return self.uri
        elif self.type == ConnectionTypeEnum.SQLALCHEMY:
            url = self.engine.url
            host = url.host
            port = url.port
            username = url.username
            password = url.password
            database = url.database
            database_as_file = database if database is not None else ""
        else:
            url = urlparse(self.uri)
            host = url.hostname
            port = url.port
            username = url.username
            password = url.password
            database = url.path.lstrip('/')
            database_as_file = self.uri.replace(f"{self.dialect}://", "")
        
        if self.dialect in ('postgres', 'mysql'):
            attach_uri = f"{self.dialect}:dbname={database} user={username} password={password} host={host} port={port}"
        elif self.dialect == "sqlite":
            attach_uri = f"{self.dialect}:{database_as_file}"
        elif self.dialect == "duckdb":
            attach_uri = database_as_file
        else:
            attach_uri = None
        
        return attach_uri


class DbConnConfig(ConnectionProperties, _ConfigWithNameBaseModel):
    def finalize_uri(self, base_path: str) -> Self:
        self.uri = self.uri.format(project_path=base_path)
        return self


class DatasetConfigurablesConfig(BaseModel):
    name: str
    default: str


class ConfigurablesConfig(DatasetConfigurablesConfig):
    label: str = ""
    description: str = ""


class ParametersConfig(BaseModel):
    type: str
    factory: str
    arguments: dict[str, Any]


class PermissionScope(Enum):
    PUBLIC = 0
    PROTECTED = 1
    PRIVATE = 2


class AuthenticationEnforcement(Enum):
    REQUIRED = "required"
    OPTIONAL = "optional"
    DISABLED = "disabled"

class AuthenticationType(Enum):
    MANAGED = "managed"
    EXTERNAL = "external"

class AuthenticationConfig(BaseModel):
    enforcement: AuthenticationEnforcement = AuthenticationEnforcement.OPTIONAL
    type: AuthenticationType = AuthenticationType.MANAGED


class AnalyticsOutputConfig(_ConfigWithNameBaseModel):
    label: str = ""
    description: str = ""
    scope: PermissionScope = PermissionScope.PUBLIC
    parameters: list[str] | None = Field(default=None, description="The list of parameter names used by the dataset/dashboard")

    @model_validator(mode="after")
    def finalize_label(self) -> Self:
        if self.label == "":
            self.label = self.name
        return self

    @field_validator("scope", mode="before")
    @classmethod
    def validate_scope(cls, value: str, info: ValidationInfo) -> PermissionScope:
        try:
            return PermissionScope[str(value).upper()]
        except KeyError as e:
            name = info.data.get("name")
            scope_list = [scope.name.lower() for scope in PermissionScope]
            raise ValueError(f'Scope "{value}" is invalid for dataset/dashboard "{name}". Scope must be one of {scope_list}') from e


class DatasetConfig(AnalyticsOutputConfig):
    model: str = ""
    configurables: list[DatasetConfigurablesConfig] = Field(default_factory=list)
    
    def __hash__(self) -> int:
        return hash("dataset_"+self.name)

    @model_validator(mode="after")
    def finalize_model(self) -> Self:
        if self.model == "":
            self.model = self.name
        return self


class TestSetsUserConfig(BaseModel):
    access_level: Literal["admin", "member", "guest"] = "guest"
    custom_fields: dict[str, Any] = Field(default_factory=dict)

class TestSetsConfig(_ConfigWithNameBaseModel):
    user: TestSetsUserConfig = Field(default_factory=TestSetsUserConfig)
    parameters: dict[str, Any] = Field(default_factory=dict)
    configurables: dict[str, Any] = Field(default_factory=dict)


class ManifestConfig(BaseModel):
    project_variables: ProjectVarsConfig
    authentication: AuthenticationConfig = Field(default_factory=AuthenticationConfig)
    packages: list[PackageConfig] = Field(default_factory=list)
    connections: dict[str, DbConnConfig] = Field(default_factory=dict)
    parameters: list[ParametersConfig] = Field(default_factory=list)
    configurables: dict[str, ConfigurablesConfig] = Field(default_factory=dict)
    selection_test_sets: dict[str, TestSetsConfig] = Field(default_factory=dict)
    datasets: dict[str, DatasetConfig] = Field(default_factory=dict)
    base_path: str = "."

    @field_validator("packages")
    @classmethod
    def package_directories_are_unique(cls, packages: list[PackageConfig]) -> list[PackageConfig]:
        set_of_directories = set()
        for package in packages:
            if package.directory in set_of_directories:
                raise ValueError(f'In the packages section, multiple target directories found for "{package.directory}"')
            set_of_directories.add(package.directory)
        return packages
    
    @field_validator("connections", "selection_test_sets", "datasets", "configurables", mode="before")
    @classmethod
    def names_are_unique(cls, values: list[dict] | dict[str, dict], info: ValidationInfo) -> dict[str, dict]:
        if isinstance(values, list):
            values_as_dict = {}
            for obj in values:
                name = u.normalize_name(obj["name"])
                if name in values_as_dict:
                    raise ValueError(f'In the {info.field_name} section, the name "{name}" was specified multiple times')
                values_as_dict[name] = obj
        else:
            values_as_dict = values
        return values_as_dict
    
    @model_validator(mode="after")
    def finalize_connections(self) -> Self:
        for conn in self.connections.values():
            conn.finalize_uri(self.base_path)
        return self
    
    @model_validator(mode="after")
    def validate_authentication_and_scopes(self) -> Self:
        """
        Enforce authentication rules:
        - If authentication.is_required, no dataset may be PUBLIC.
        """
        if self.authentication.enforcement == AuthenticationEnforcement.REQUIRED:
            invalid = [name for name, ds in self.datasets.items() if ds.scope == PermissionScope.PUBLIC]
            if invalid:
                raise ValueError(
                    "Authentication is required, so datasets cannot be public. "
                    f"Update the scope for datasets: {invalid}"
                )
        return self
    
    @model_validator(mode="after")
    def validate_dataset_configurables(self) -> Self:
        """
        Validate that dataset configurables reference valid project-level configurables.
        """
        for dataset_name, dataset_cfg in self.datasets.items():
            for cfg_override in dataset_cfg.configurables:
                if cfg_override.name not in self.configurables:
                    raise ValueError(
                        f'Dataset "{dataset_name}" references configurable "{cfg_override.name}" which is not defined '
                        f'in the project configurables'
                    )
        return self
    
    def get_default_test_set(self) -> TestSetsConfig:
        """
        Raises KeyError if dataset name doesn't exist
        """
        default_default_test_set = TestSetsConfig(name=c.DEFAULT_TEST_SET_NAME)
        default_test_set = self.selection_test_sets.get(c.DEFAULT_TEST_SET_NAME, default_default_test_set)
        return default_test_set
    
    def get_default_configurables(self, dataset_name: str | None = None) -> dict[str, str]:
        """
        Return a dictionary of configurable name to its default value.
        
        If dataset_name is provided, merges project-level defaults with dataset-specific overrides.

        Supports both list- and dict-shaped internal storage for configurables.
        """
        defaults: dict[str, str] = {}
        for name, cfg in self.configurables.items():
            defaults[name] = str(cfg.default)
        
        # Apply dataset-specific overrides if dataset_name is provided
        if dataset_name is not None:
            dataset_cfg = self.datasets.get(dataset_name)
            if dataset_cfg:
                for cfg_override in dataset_cfg.configurables:
                    defaults[cfg_override.name] = cfg_override.default
        
        return defaults


class ManifestIO:
    @classmethod
    def load_from_file(cls, logger: u.Logger, project_path: str, envvars_unformatted: dict[str, str]) -> ManifestConfig:
        start = time.time()

        raw_content = u.read_file(u.Path(project_path, c.MANIFEST_FILE))
        content = u.render_string(raw_content, project_path=project_path, env_vars=envvars_unformatted)
        manifest_content: dict[str, Any] = yaml.safe_load(content)

        auth_cfg: dict[str, Any] = manifest_content.get("authentication", {})
        is_auth_required = bool(auth_cfg.get("is_required", False))

        if is_auth_required:
            # If authentication is required, assume PROTECTED when scope is not specified
            # while explicitly forbidding PUBLIC (enforced in model validator)
            datasets_raw = manifest_content.get("datasets", [])
            for ds in datasets_raw:
                if isinstance(ds, dict) and "scope" not in ds:
                    ds["scope"] = "protected"
        
        try:
            manifest_cfg = ManifestConfig(base_path=project_path, **manifest_content)
        except ValidationError as e:
            raise u.ConfigurationError(f"Failed to process {c.MANIFEST_FILE} file. " + str(e)) from e
        
        logger.log_activity_time(f"loading {c.MANIFEST_FILE} file", start)
        return manifest_cfg
