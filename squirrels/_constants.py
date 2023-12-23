# Squirrels CLI commands
INIT_CMD = 'init'
DEPS_CMD = 'deps'
COMPILE_CMD = 'compile'
RUN_CMD = 'run'

# Manifest file keys
PROJ_VARS_KEY = 'project_variables'
PROJECT_NAME_KEY = 'name'
PROJECT_LABEL_KEY = 'label'
MAJOR_VERSION_KEY = 'major_version'
MINOR_VERSION_KEY = 'minor_version'

PACKAGES_KEY = 'packages'
PACKAGE_GIT_KEY = 'git'
PACKAGE_DIRECTORY_KEY = 'directory'
PACKAGE_REVISION_KEY = 'revision'

DB_CONNECTIONS_KEY = 'db_connections'
DB_CONN_NAME_KEY = 'connection_name'
DB_CONN_CRED_KEY = 'credential_key'
DB_CONN_URL_KEY = 'url'

DBVIEWS_KEY = 'dbviews'
DBVIEW_NAME_KEY = 'name'
DBVIEW_CONN_KEY = 'connection_name'
DEFAULT_DB_CONN = 'default'

FEDERATES_KEY = 'federates'
FEDERATE_NAME_KEY = 'name'
MATERIALIZED_KEY = 'materialized'
DEFAULT_TABLE_MATERIALIZE = 'table'

PARAMETERS_KEY = 'parameters'
PARAMETER_NAME_KEY = 'name'
PARAMETER_TYPE_KEY = 'type'
PARAMETER_FACTORY_KEY = 'factory'
PARAMETER_ARGS_KEY = 'arguments'

TEST_SETS_KEY = 'selection_test_sets'
TEST_SET_NAME_KEY = 'name'
DEFAULT_TEST_SET_NAME = 'default'
TEST_SET_USER_ATTR_KEY = 'user_attributes'
TEST_SET_PARAMETERS_KEY = 'parameters'

DATASETS_KEY = 'datasets'
DATASET_NAME_KEY = 'name'
DATASET_LABEL_KEY = 'label'
DATASET_MODEL_KEY = 'model'
DATASET_PARAMETERS_KEY = 'parameters'
DATASET_ARGS_KEY = 'args'

# deprecate these later
DATABASE_VIEWS_KEY = 'database_views'
FILE_KEY = 'file'
DB_CONNECTION_KEY = 'db_connection'
FINAL_VIEW_KEY = 'final_view'

DATASET_SCOPE_KEY = 'scope'
PUBLIC_SCOPE = 'public'
PROTECTED_SCOPE = 'protected'
PRIVATE_SCOPE = 'private'

SETTINGS_KEY = 'settings'

# Environment config keys
CREDENTIALS_KEY = 'credentials'
USERNAME_KEY = 'username'
PASSWORD_KEY = 'password'
USER_NAME_KEY = 'username'
USER_PWD_KEY = 'password'
SECRETS_KEY = 'secrets'
JWT_SECRET_KEY = 'jwt_secret'

# Folder/File names
PACKAGE_DATA_FOLDER = 'package_data'
BASE_PROJECT_FOLDER = 'base_project'
STATIC_FOLDER = 'static'
TEMPLATES_FOLDER = 'templates'

ENVIRON_CONFIG_FILE = 'environcfg.yml'
MANIFEST_JINJA_FILE = 'squirrels.yml.j2'
CONNECTIONS_YML_FILE = 'connections.yml'
PARAMETERS_YML_FILE = 'parameters.yml'
MANIFEST_FILE = 'squirrels.yml'
LU_DATA_FILE = 'lu_data.xlsx'

DATABASE_FOLDER = 'database'
PACKAGES_FOLDER = 'sqrl_packages'

MODELS_FOLDER = 'models'
DBVIEWS_FOLDER = 'dbviews'
DATABASE_VIEW_SQL_FILE = 'database_view1.sql'
DATABASE_VIEW_PY_FILE = 'database_view1.py'
FEDERATES_FOLDER = 'federates'
FEDERATE_SQL_NAME = 'dataset_example.sql'
FEDERATE_PY_NAME = 'dataset_example.py'

PYCONFIG_FOLDER = 'pyconfigs'
AUTH_FILE = 'auth.py'
CONNECTIONS_FILE = 'connections.py'
CONTEXT_FILE = 'context.py'
PARAMETERS_FILE = 'parameters.py'

TARGET_FOLDER = 'target'
COMPILE_FOLDER = 'compile'

OUTPUTS_FOLDER = 'outputs'
PARAMETERS_OUTPUT = 'parameters.json'
FINAL_VIEW_OUT_STEM = 'final_view'

# Dataset setting names
AUTH_TOKEN_EXPIRE_SETTING = 'auth.token.expire.minutes'
PARAMETERS_CACHE_SIZE_SETTING = 'parameters.cache.size'
PARAMETERS_CACHE_TTL_SETTING = 'parameters.cache.ttl.minutes'
RESULTS_CACHE_SIZE_SETTING = 'results.cache.size'
RESULTS_CACHE_TTL_SETTING = 'results.cache.ttl.minutes'
TEST_SET_DEFAULT_USED_SETTING = 'selection_test_sets.default_name_used'
DB_CONN_DEFAULT_USED_SETTING = 'db_connections.default_name_used'
DEFAULT_MATERIALIZE_SETTING = 'defaults.federates.materialized'

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

EXPENSES_DB_NAME = 'expenses'
WEATHER_DB_NAME = 'seattle-weather'
DATABASE_CHOICES = [EXPENSES_DB_NAME, WEATHER_DB_NAME]

# Function names
GET_USER_FUNC = "get_user_if_valid"
DEP_FUNC = "dependencies"
MAIN_FUNC = "main"
