# Squirrels CLI commands
INIT_CMD = 'new'
GET_FILE_CMD = 'get-file'
DEPS_CMD = 'deps'
BUILD_CMD = 'build'
COMPILE_CMD = 'compile'
RUN_CMD = 'run'
DUCKDB_CMD = 'duckdb'

# Environment variables
SQRL_SECRET_KEY = 'SQRL.SECRET.KEY'
SQRL_SECRET_ADMIN_PASSWORD = 'SQRL.SECRET.ADMIN_PASSWORD'

SQRL_AUTH_DB_FILE_PATH = 'SQRL.AUTH.DB_FILE_PATH'
SQRL_AUTH_TOKEN_EXPIRE_MINUTES = 'SQRL.AUTH.TOKEN_EXPIRE_MINUTES'

SQRL_PARAMETERS_CACHE_SIZE = 'SQRL.PARAMETERS.CACHE_SIZE'
SQRL_PARAMETERS_CACHE_TTL_MINUTES = 'SQRL.PARAMETERS.CACHE_TTL_MINUTES'

SQRL_DATASETS_CACHE_SIZE = 'SQRL.DATASETS.CACHE_SIZE'
SQRL_DATASETS_CACHE_TTL_MINUTES = 'SQRL.DATASETS.CACHE_TTL_MINUTES'

SQRL_DASHBOARDS_CACHE_SIZE = 'SQRL.DASHBOARDS.CACHE_SIZE'
SQRL_DASHBOARDS_CACHE_TTL_MINUTES = 'SQRL.DASHBOARDS.CACHE_TTL_MINUTES'

SQRL_SEEDS_INFER_SCHEMA = 'SQRL.SEEDS.INFER_SCHEMA'
SQRL_SEEDS_NA_VALUES = 'SQRL.SEEDS.NA_VALUES'

SQRL_TEST_SETS_DEFAULT_NAME_USED = 'SQRL.TEST_SETS.DEFAULT_NAME_USED'

SQRL_CONNECTIONS_DEFAULT_NAME_USED = 'SQRL.CONNECTIONS.DEFAULT_NAME_USED'

SQRL_DUCKDB_VENV_DB_FILE_PATH = 'SQRL.DUCKDB_VENV.DB_FILE_PATH'

# Folder/File names
PACKAGE_DATA_FOLDER = 'package_data'
BASE_PROJECT_FOLDER = 'base_project'

GLOBAL_ENV_FOLDER = '.squirrels'
MANIFEST_JINJA_FILE = 'squirrels.yml.j2'
CONNECTIONS_YML_FILE = 'connections.yml'
PARAMETERS_YML_FILE = 'parameters.yml'
DASHBOARDS_YML_FILE = 'dashboards.yml'
MANIFEST_FILE = 'squirrels.yml'
DUCKDB_INIT_FILE = 'duckdb_init.sql'
DOTENV_FILE = '.env'
DOTENV_LOCAL_FILE = '.env.local'

LOGS_FOLDER = 'logs'
LOGS_FILE = 'squirrels.log'

DATABASE_FOLDER = 'assets'
PACKAGES_FOLDER = 'sqrl_packages'

MACROS_FOLDER = 'macros'
MACROS_FILE = 'macros_example.sql'

MODELS_FOLDER = 'models'
SOURCES_FILE = 'sources.yml'
BUILDS_FOLDER = 'builds'
BUILD_FILE_STEM = 'build_example'
DBVIEWS_FOLDER = 'dbviews'
DBVIEW_FILE_STEM = 'dbview_example'
FEDERATES_FOLDER = 'federates'
FEDERATE_FILE_STEM = 'federate_example'

DASHBOARDS_FOLDER = 'dashboards'
DASHBOARD_FILE_STEM = 'dashboard_example'

PYCONFIGS_FOLDER = 'pyconfigs'
AUTH_FILE = 'auth.py'
CONNECTIONS_FILE = 'connections.py'
CONTEXT_FILE = 'context.py'
PARAMETERS_FILE = 'parameters.py'
USER_FILE = 'user.py'
ADMIN_USERNAME = 'admin'

TARGET_FOLDER = 'target'
DB_FILE = 'auth.sqlite'
COMPILE_FOLDER = 'compile'
DUCKDB_VENV_FILE = 'venv.duckdb'

SEEDS_FOLDER = 'seeds'
SEED_CATEGORY_FILE_STEM = 'seed_categories'
SEED_SUBCATEGORY_FILE_STEM = 'seed_subcategories'

# Dataset setting names
DB_CONN_DEFAULT_USED_SETTING = 'connections.default_name_used'
DEFAULT_DB_CONN = 'default'
SEEDS_INFER_SCHEMA_SETTING = 'seeds.infer_schema'
SEEDS_NA_VALUES_SETTING = 'seeds.na_values'
DUCKDB_VENV_FILE_PATH_SETTING = 'duckdb_venv.file_path'

# Selection cfg sections
USER_ATTRIBUTES_SECTION = 'user_attributes'
PARAMETERS_SECTION = 'parameters'

# Init Command Choices
SQL_FILE_TYPE = 'sql'
PYTHON_FILE_TYPE = 'py'
FILE_TYPE_CHOICES = [SQL_FILE_TYPE, PYTHON_FILE_TYPE]

YML_FORMAT = 'yml'
PYTHON_FORMAT = 'py'
CONF_FORMAT_CHOICES = [YML_FORMAT, PYTHON_FORMAT]

PYTHON_FORMAT2 = 'py (recommended)'
CONF_FORMAT_CHOICES2 = [(PYTHON_FORMAT2, PYTHON_FORMAT), YML_FORMAT]

EXPENSES_DB = 'expenses.db'
WEATHER_DB = 'weather.db'

# Dashboard formats
PNG = "png"
HTML = "html"

# Function names
GET_USER_FROM_LOGIN_FUNC = "get_user_from_login"
GET_USER_FROM_TOKEN_FUNC = "get_user_from_token"
DEP_FUNC = "dependencies"
MAIN_FUNC = "main"

# Regex
date_regex = r"^\d{4}\-\d{2}\-\d{2}$"
color_regex = r"^#[0-9a-fA-F]{6}$"
