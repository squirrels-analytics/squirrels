# Squirrels CLI commands
INIT_CMD = 'init'
LOAD_MODULES_CMD = 'load-modules'
TEST_CMD = 'test'
RUN_CMD = 'run'

# Manifest file keys
PROJ_VARS_KEY = 'project_variables'
MODULES_KEY = 'modules'
SETTINGS_KEY = 'settings'
PARAMETERS_KEY = 'parameters'

DB_CONNECTIONS_KEY = 'db_connections'
DB_CREDENTIALS_KEY = 'credential_key'
URL_KEY = 'url'

DATASETS_KEY = 'datasets'
DATASET_LABEL_KEY = 'label'
DATASET_PARAMETERS_KEY = 'parameters'
DATABASE_VIEWS_KEY = 'database_views'
FILE_KEY = 'file'
DB_CONNECTION_KEY = 'db_connection'
FINAL_VIEW_KEY = 'final_view'

SCOPE_KEY = 'scope'
PUBLIC_SCOPE = 'public'
PROTECTED_SCOPE = 'protected'
PRIVATE_SCOPE = 'private'

# Project variable keys
PRODUCT_KEY = 'product'
PRODUCT_LABEL_KEY = 'product_label'
MAJOR_VERSION_KEY = 'major_version'
MINOR_VERSION_KEY = 'minor_version'

# Environment config keys keys
CREDENTIALS_KEY = 'credentials'
USERNAME_KEY = 'username'
PASSWORD_KEY = 'password'
USER_NAME_KEY = 'username'
USER_PWD_KEY = 'password'
DEFAULT_DB_CONN = 'default'
SECRETS_KEY = 'secrets'
JWT_SECRET_KEY = 'jwt_secret'

# Folder/File names
PACKAGE_DATA_FOLDER = 'package_data'
BASE_PROJECT_FOLDER = 'base_project'
STATIC_FOLDER = 'static'
TEMPLATES_FOLDER = 'templates'

ENVIRON_CONFIG_FILE = 'environcfg.yaml'
MANIFEST_JINJA_FILE = 'squirrels.yaml.j2'
CONNECTIONS_YML_FILE = 'connections.yaml'
PARAMETERS_YML_FILE = 'parameters.yaml'
MANIFEST_FILE = 'squirrels.yaml'
LU_DATA_FILE = 'lu_data.xlsx'

DATABASE_FOLDER = 'database'
MODULES_FOLDER = 'modules'

DATASETS_FOLDER = 'datasets'
DATABASE_VIEW_STEM = 'database_view1'
DATABASE_VIEW_SQL_FILE = DATABASE_VIEW_STEM+'.sql.j2'
DATABASE_VIEW_PY_FILE = DATABASE_VIEW_STEM+'.py'
FINAL_VIEW_SQL_NAME = 'final_view.sql.j2'
FINAL_VIEW_PY_NAME = 'final_view.py'
SELECTIONS_CFG_FILE = 'selections.cfg'

PYCONFIG_FOLDER = 'pyconfigs'
AUTH_FILE = 'auth.py'
CONNECTIONS_FILE = 'connections.py'
CONTEXT_FILE = 'context.py'
PARAMETERS_FILE = 'parameters.py'

OUTPUTS_FOLDER = 'outputs'
PARAMETERS_OUTPUT = 'parameters.json'
FINAL_VIEW_OUT_STEM = 'final_view'

# Dataset setting names
AUTH_TOKEN_EXPIRE_SETTING = 'auth.token.expire.minutes'
PARAMETERS_CACHE_SIZE_SETTING = 'parameters.cache.size'
PARAMETERS_CACHE_TTL_SETTING = 'parameters.cache.ttl.minutes'
RESULTS_CACHE_SIZE_SETTING = 'results.cache.size'
RESULTS_CACHE_TTL_SETTING = 'results.cache.ttl.minutes'

# Selection cfg sections
USER_ATTRIBUTES_SECTION = 'user_attributes'
PARAMETERS_SECTION = 'parameters'

# Init Command Choices
SQL_FILE_TYPE = 'sql'
PYTHON_FILE_TYPE = 'py'
FILE_TYPE_CHOICES = [SQL_FILE_TYPE, PYTHON_FILE_TYPE]

YAML_FORMAT = 'yaml'
PYTHON_FORMAT = 'py'
CONF_FORMAT_CHOICES = [YAML_FORMAT, PYTHON_FORMAT]

PYTHON_FORMAT2 = 'py (recommended)'
CONF_FORMAT_CHOICES2 = [(PYTHON_FORMAT2, PYTHON_FORMAT), YAML_FORMAT]

EXPENSES_DB_NAME = 'expenses'
WEATHER_DB_NAME = 'seattle-weather'
DATABASE_CHOICES = [EXPENSES_DB_NAME, WEATHER_DB_NAME]
