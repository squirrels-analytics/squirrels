from typing import Any
from typing_extensions import Self
from enum import Enum
from pydantic import BaseModel, Field, field_validator, model_validator, ValidationInfo, ValidationError
import yaml

from . import _constants as c, _utils as u
from ._environcfg import EnvironConfigIO
from ._timer import timer, time


class ProjectVarsConfig(BaseModel, extra="allow"):
    name: str
    label: str = ""
    major_version: int

    @model_validator(mode="after")
    def finalize_label(self) -> Self:
        if self.label == "":
            self.label = self.name
        return self


class PackageConfig(BaseModel):
    git: str
    directory: str = ""
    revision: str

    @model_validator(mode="after")
    def finalize_directory(self) -> Self:
        if self.directory == "":
            self.directory = self.git.split('/')[-1].removesuffix('.git')
        return self


class _ConfigWithNameBaseModel(BaseModel):
    name: str


class DbConnConfig(_ConfigWithNameBaseModel):
    credential: str | None = None
    url: str

    @model_validator(mode="after")
    def finalize_url(self) -> Self:
        username, password = EnvironConfigIO.obj.get_credential(self.credential)
        self.url = self.url.format(username=username, password=password)
        return self


class ParametersConfig(BaseModel):
    type: str
    factory: str
    arguments: dict[str, Any]


class DbviewConfig(_ConfigWithNameBaseModel):
    connection_name: str | None = None


class FederateConfig(_ConfigWithNameBaseModel):
    materialized: str | None = None


class DatasetScope(Enum):
    PUBLIC = 0
    PROTECTED = 1
    PRIVATE = 2


class AnalyticsOutputConfig(_ConfigWithNameBaseModel):
    label: str = ""
    description: str = ""
    scope: DatasetScope = DatasetScope.PUBLIC
    parameters: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def finalize_label(self) -> Self:
        if self.label == "":
            self.label = self.name
        return self

    @field_validator("scope", mode="before")
    @classmethod
    def validate_scope(cls, value: str, info: ValidationInfo) -> DatasetScope:
        try:
            return DatasetScope[str(value).upper()]
        except KeyError as e:
            name = info.data.get("name")
            scope_list = [scope.name.lower() for scope in DatasetScope]
            raise ValueError(f'Scope "{value}" is invalid for dataset/dashboard "{name}". Scope must be one of {scope_list}') from e


class DatasetConfig(AnalyticsOutputConfig):
    model: str = ""
    traits: dict = Field(default_factory=dict)
    default_test_set: str = ""
    
    def __hash__(self) -> int:
        return hash("dataset_"+self.name)

    @model_validator(mode="after")
    def finalize_model(self) -> Self:
        if self.model == "":
            self.model = self.name
        return self


class DashboardConfig(AnalyticsOutputConfig):
    format: str = ""
    
    def __hash__(self) -> int:
        return hash("dashboard_"+self.name)
    
    @model_validator(mode="after")
    def validate_format(self) -> Self:
        valid_formats = [c.PNG, c.HTML]
        if self.format != "" and self.format not in valid_formats:
            raise ValueError(f'Invalid format "{self.format}" for dashboard "{self.name}". Format must be one of: {valid_formats}')
        return self


class TestSetsConfig(_ConfigWithNameBaseModel):
    datasets: list[str] | None = None
    is_authenticated: bool = False
    user_attributes: dict[str, Any] = Field(default_factory=dict)
    parameters: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def finalize_is_authenticated(self) -> Self:
        if len(self.user_attributes) > 0:
            self.is_authenticated = True
        return self


class _ManifestConfig(BaseModel):
    project_variables: ProjectVarsConfig
    packages: list[PackageConfig] = Field(default_factory=list)
    connections: dict[str, DbConnConfig] = Field(default_factory=dict)
    parameters: list[ParametersConfig] = Field(default_factory=list)
    selection_test_sets: dict[str, TestSetsConfig] = Field(default_factory=dict)
    dbviews: dict[str, DbviewConfig] = Field(default_factory=dict)
    federates: dict[str, FederateConfig] = Field(default_factory=dict)
    datasets: dict[str, DatasetConfig] = Field(default_factory=dict)
    dashboards: dict[str, DashboardConfig] = Field(default_factory=dict)
    settings: dict[str, Any] = Field(default_factory=dict)

    @field_validator("packages")
    @classmethod
    def package_directories_are_unique(cls, packages: list[PackageConfig]) -> list[PackageConfig]:
        set_of_directories = set()
        for package in packages:
            if package.directory in set_of_directories:
                raise ValueError(f'In the packages section, multiple target directories found for "{package.directory}"')
            set_of_directories.add(package.directory)
        return packages
    
    @field_validator("connections", "selection_test_sets", "dbviews", "federates", "datasets", "dashboards", mode="before")
    @classmethod
    def names_are_unique(cls, values: list[dict], info: ValidationInfo) -> dict[str, dict]:
        values_as_dict = {}
        for obj in values:
            name = obj["name"]
            if name in values_as_dict:
                raise ValueError(f'In the {info.field_name} section, the name "{name}" was specified multiple times')
            values_as_dict[name] = obj
        return values_as_dict

    def get_default_test_set(self, dataset_name: str) -> TestSetsConfig:
        """
        Raises KeyError if dataset name doesn't exist
        """
        default_name_1 = self.datasets[dataset_name].default_test_set
        default_name_2 = self.settings.get(c.TEST_SET_DEFAULT_USED_SETTING, c.DEFAULT_TEST_SET_NAME)
        default_name = default_name_1 if default_name_1 else default_name_2
        default_test_set = self.selection_test_sets.get(default_name, TestSetsConfig(name=default_name))
        return default_test_set
    
    def get_dashboard_format(self, dashboard_name: str) -> str:
        """
        Raises KeyError if dashboard name doesn't exist
        """
        default_format = self.settings.get(c.DASHBOARDS_FORMAT_SETTING, c.PNG)
        dashboard_format = self.dashboards[dashboard_name].format
        return dashboard_format if dashboard_format else default_format


class ManifestIO:
    obj: _ManifestConfig

    @classmethod
    def load_from_file(cls) -> None:
        start = time.time()

        raw_content = u.read_file(c.MANIFEST_FILE)
        env_vars = EnvironConfigIO.obj.get_all_env_vars()
        content = u.render_string(raw_content, env_vars=env_vars)
        manifest_content = yaml.safe_load(content)
        try:
            cls.obj = _ManifestConfig(**manifest_content)
        except ValidationError as e:
            raise u.ConfigurationError(f"Failed to process {c.MANIFEST_FILE} file. " + str(e)) from e
        
        timer.add_activity_time(f"loading {c.MANIFEST_FILE} file", start)
