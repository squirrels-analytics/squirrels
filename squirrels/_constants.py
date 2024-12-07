# Squirrels CLI commands
INIT_CMD = 'new'
GET_FILE_CMD = 'get-file'
DEPS_CMD = 'deps'
BUILD_CMD = 'build'
COMPILE_CMD = 'compile'
RUN_CMD = 'run'

# Environment config keys
JWT_SECRET_KEY = 'jwt_secret'

# Folder/File names
PACKAGE_DATA_FOLDER = 'package_data'
BASE_PROJECT_FOLDER = 'base_project'
ASSETS_FOLDER = 'assets'
TEMPLATES_FOLDER = 'templates'

ENV_CONFIG_FILE = 'env.yml'
MANIFEST_JINJA_FILE = 'squirrels.yml.j2'
CONNECTIONS_YML_FILE = 'connections.yml'
PARAMETERS_YML_FILE = 'parameters.yml'
DASHBOARDS_YML_FILE = 'dashboards.yml'
MANIFEST_FILE = 'squirrels.yml'

LOGS_FOLDER = 'logs'
LOGS_FILE = 'squirrels.log'

DATABASE_FOLDER = 'assets'
PACKAGES_FOLDER = 'sqrl_packages'

MACROS_FOLDER = 'macros'

MODELS_FOLDER = 'models'
SOURCES_FILE = 'sources.yml'
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

TARGET_FOLDER = 'target'
COMPILE_FOLDER = 'compile'
DUCKDB_VENV_FILE = '.venv.duckdb'
DUCKDB_DEV_FILE = '.dev.duckdb'
DUCKDB_STG_FILE = '.stg.duckdb'

SEEDS_FOLDER = 'seeds'
SEED_CATEGORY_FILE_STEM = 'seed_categories'
SEED_SUBCATEGORY_FILE_STEM = 'seed_subcategories'

# Dataset setting names
AUTH_TOKEN_EXPIRE_SETTING = 'auth.token.expire_minutes'
PARAMETERS_CACHE_SIZE_SETTING = 'parameters.cache.size'
PARAMETERS_CACHE_TTL_SETTING = 'parameters.cache.ttl_minutes'
DATASETS_CACHE_SIZE_SETTING = 'datasets.cache.size'
DATASETS_CACHE_TTL_SETTING = 'datasets.cache.ttl_minutes'
DASHBOARDS_CACHE_SIZE_SETTING = 'dashboards.cache.size'
DASHBOARDS_CACHE_TTL_SETTING = 'dashboards.cache.ttl_minutes'
TEST_SET_DEFAULT_USED_SETTING = 'selection_test_sets.default_name_used'
DEFAULT_TEST_SET_NAME = 'default'
DB_CONN_DEFAULT_USED_SETTING = 'connections.default_name_used'
DEFAULT_DB_CONN = 'default'
SEEDS_INFER_SCHEMA_SETTING = 'seeds.infer_schema'
SEEDS_NA_VALUES_SETTING = 'seeds.na_values'

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
