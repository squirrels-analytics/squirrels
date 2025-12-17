from typing import Any, Literal, Optional
from typing_extensions import Self
from pathlib import Path
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict
import json

from . import _constants as c


class SquirrelsEnvVars(BaseModel):
    """
    Pydantic model for managing and validating Squirrels environment variables.
    These variables are typically loaded from .env files or the system environment.
    """
    model_config = ConfigDict(serialize_by_alias=True)
    project_path: str
    
    # Security
    secret_key: Optional[str] = Field(
        None, alias=c.SQRL_SECRET_KEY, 
        description="Secret key for JWT encoding/decoding and other security operations"
    )
    secret_admin_password: Optional[str] = Field(
        None, alias=c.SQRL_SECRET_ADMIN_PASSWORD, 
        description="Password for the admin user"
    )
    
    # Auth
    auth_db_file_path: str = Field(
        "{project_path}/target/auth.sqlite", alias=c.SQRL_AUTH_DB_FILE_PATH, 
        description="Path to the SQLite authentication database"
    )
    auth_token_expire_minutes: float = Field(
        30, ge=0, alias=c.SQRL_AUTH_TOKEN_EXPIRE_MINUTES, 
        description="Expiration time for access tokens in minutes"
    )
    auth_credential_origins: list[str] = Field(
        ["https://squirrels-analytics.github.io"], alias=c.SQRL_AUTH_CREDENTIAL_ORIGINS, 
        description="Allowed origins for credentials (cookies)"
    )

    # Permissions
    elevated_access_level: Literal["admin", "member", "guest"] = Field(
        "admin", alias=c.SQRL_PERMISSIONS_ELEVATED_ACCESS_LEVEL, 
        description="Minimum access level to access the studio"
    )

    # Parameters Cache
    parameters_cache_size: int = Field(
        1024, ge=0, alias=c.SQRL_PARAMETERS_CACHE_SIZE, 
        description="Cache size for parameter configs"
    )
    parameters_cache_ttl_minutes: float = Field(
        60, gt=0, alias=c.SQRL_PARAMETERS_CACHE_TTL_MINUTES, 
        description="Cache TTL for parameter configs in minutes"
    )
    parameters_datasource_refresh_minutes: float = Field(
        60, alias=c.SQRL_PARAMETERS_DATASOURCE_REFRESH_MINUTES, 
        description="Interval in minutes for refreshing data sources. A non-positive value disables auto-refresh"
    )
    
    # Datasets Cache
    datasets_cache_size: int = Field(
        128, ge=0, alias=c.SQRL_DATASETS_CACHE_SIZE, 
        description="Cache size for dataset results"
    )
    datasets_cache_ttl_minutes: float = Field(
        60, gt=0, alias=c.SQRL_DATASETS_CACHE_TTL_MINUTES, 
        description="Cache TTL for dataset results in minutes"
    )
    datasets_max_rows_for_ai: int = Field(
        100, ge=0, alias=c.SQRL_DATASETS_MAX_ROWS_FOR_AI, 
        description="Max rows for AI queries"
    )
    datasets_max_rows_output: int = Field(
        100000, ge=0, alias=c.SQRL_DATASETS_MAX_ROWS_OUTPUT, 
        description="Max rows for dataset output"
    )
    datasets_sql_timeout_seconds: float = Field(
        2.0, gt=0, alias=c.SQRL_DATASETS_SQL_TIMEOUT_SECONDS, 
        description="Timeout for SQL operations in seconds"
    )

    # Dashboards Cache
    dashboards_cache_size: int = Field(
        128, ge=0, alias=c.SQRL_DASHBOARDS_CACHE_SIZE, 
        description="Cache size for dashboards"
    )
    dashboards_cache_ttl_minutes: float = Field(
        60, gt=0, alias=c.SQRL_DASHBOARDS_CACHE_TTL_MINUTES, 
        description="Cache TTL for dashboards in minutes"
    )
    
    # Seeds
    seeds_infer_schema: bool = Field(
        True, alias=c.SQRL_SEEDS_INFER_SCHEMA, 
        description="Whether to infer schema for seeds"
    )
    seeds_na_values: list[str] = Field(
        ["NA"], alias=c.SQRL_SEEDS_NA_VALUES, 
        description="List of N/A values for seeds"
    )

    # Connections
    connections_default_name_used: str = Field(
        "default", alias=c.SQRL_CONNECTIONS_DEFAULT_NAME_USED, 
        description="Default connection name to use"
    )

    # VDL
    vdl_catalog_db_path: str = Field(
        "ducklake:{project_path}/target/vdl_catalog.duckdb", alias=c.SQRL_VDL_CATALOG_DB_PATH, 
        description="Path to the DuckDB catalog database"
    )
    vdl_data_path: str = Field(
        "{project_path}/target/vdl_data/", alias=c.SQRL_VDL_DATA_PATH, 
        description="Path to the VDL data directory"
    )

    # Studio
    studio_base_url: str = Field(
        "https://squirrels-analytics.github.io/squirrels-studio-v1", alias=c.SQRL_STUDIO_BASE_URL, 
        description="Base URL for Squirrels Studio"
    )

    # Logging
    logging_log_level: str = Field(
        "INFO", alias=c.SQRL_LOGGING_LOG_LEVEL, 
        description="Logging level"
    )
    logging_log_format: str = Field(
        "text", alias=c.SQRL_LOGGING_LOG_FORMAT, 
        description="Logging format"
    )
    logging_log_to_file: bool = Field(
        False, alias=c.SQRL_LOGGING_LOG_TO_FILE, 
        description="Whether to log to file"
    )
    logging_log_file_size_mb: float = Field(
        50, gt=0, alias=c.SQRL_LOGGING_LOG_FILE_SIZE_MB, 
        description="Max log file size in MB"
    )
    logging_log_file_backup_count: int = Field(
        1, ge=0, alias=c.SQRL_LOGGING_LOG_FILE_BACKUP_COUNT, 
        description="Number of backup log files to keep"
    )
    
    @field_validator("project_path")
    @classmethod
    def validate_project_path_exists(cls, v: str) -> str:
        """Validate that the project_path is a folder that contains a squirrels.yml file."""
        path = Path(v)
        if not path.exists():
            raise ValueError(f"Project path does not exist: {v}")
        if not path.is_dir():
            raise ValueError(f"Project path must be a directory, not a file: {v}")
        # squirrels_yml = path / c.MANIFEST_FILE
        # if not squirrels_yml.exists():
        #     raise ValueError(f"Project path must contain a {c.MANIFEST_FILE} file: {v}")
        return v
    
    @field_validator("auth_credential_origins", mode="before")
    @classmethod
    def parse_origins(cls, v: Any) -> list[str]:
        if isinstance(v, str):
            res = [x.strip() for x in v.split(",") if x.strip()]
            return res or ["https://squirrels-analytics.github.io"]
        return v

    @field_validator("seeds_na_values", mode="before")
    @classmethod
    def parse_json_list(cls, v: Any) -> list[str]:
        if isinstance(v, str):
            try:
                parsed = json.loads(v)
                if not isinstance(parsed, list):
                    raise ValueError(f"The {c.SQRL_SEEDS_NA_VALUES} environment variable must be a JSON list")
                return parsed
            except json.JSONDecodeError:
                return []
        return v
    
    @field_validator("logging_log_to_file", "seeds_infer_schema", mode="before")
    @classmethod
    def parse_bool(cls, v: Any) -> bool:
        if isinstance(v, str):
            return v.lower() in ("true", "t", "1", "yes", "y", "on")
        return bool(v)

    @model_validator(mode="after")
    def format_paths_with_filepath(self) -> Self:
        """Format paths containing {filepath} placeholder with the actual filepath value."""
        self.auth_db_file_path = self.auth_db_file_path.format(project_path=self.project_path)
        self.vdl_catalog_db_path = self.vdl_catalog_db_path.format(project_path=self.project_path)
        self.vdl_data_path = self.vdl_data_path.format(project_path=self.project_path)
        return self
    