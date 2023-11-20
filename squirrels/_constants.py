# Squirrels CLI commands
INIT_CMD = 'init'
LOAD_MODULES_CMD = 'load-modules'
GET_CREDS_CMD = 'get-all-credentials'
SET_CRED_CMD = 'set-credential'
DELETE_CRED_CMD = 'delete-credential'
TEST_CMD = 'test'
RUN_CMD = 'run'

# Manifest file keys
PROJ_VARS_KEY = 'project_variables'
DB_CONNECTIONS_KEY = 'db_connections'
DB_CREDENTIALS_KEY = 'credential_key'
URL_KEY = 'url'
DB_CONNECTION_KEY = 'db_connection'
MODULES_KEY = 'modules'
DATASET_LABEL_KEY = 'label'
DATASETS_KEY = 'datasets'
HEADERS_KEY = 'headers'
DATABASE_VIEWS_KEY = 'database_views'
FILE_KEY = 'file'
FINAL_VIEW_KEY = 'final_view'
BASE_PATH_KEY = 'base_path'
SETTINGS_KEY = 'settings'

SCOPE_KEY = 'scope'
PUBLIC_SCOPE = 'public'
PROTECTED_SCOPE = 'protected'
PRIVATE_SCOPE = 'private'

PARAMETERS_KEY = 'parameters'

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
ENVIRON_CONFIG_FILE = 'environcfg.yaml'
MANIFEST_FILE = 'squirrels.yaml'
CONNECTIONS_FILE = 'connections.py'
OUTPUTS_FOLDER = 'outputs'
MODULES_FOLDER = 'modules'
DATASETS_FOLDER = 'datasets'
PARAMETERS_FILE = 'parameters.py'
PARAMETERS_OUTPUT = 'parameters.json'
DATABASE_VIEW_SQL_FILE = 'database_view1.sql.j2'
DATABASE_VIEW_PY_FILE = 'database_view1.py'
FINAL_VIEW_SQL_NAME = 'final_view.sql.j2'
FINAL_VIEW_PY_NAME = 'final_view.py'
FINAL_VIEW_OUT_STEM = 'final_view'
CONTEXT_FILE = 'context.py'
SELECTIONS_CFG_FILE = 'selections.cfg'
LU_DATA_FILE = 'lu_data.xlsx'
AUTH_FILE = 'auth.py'

# Dataset setting names
AUTH_TOKEN_EXPIRE_SETTING = 'auth.token.expire.minutes'
PARAMETERS_CACHE_SIZE_SETTING = 'parameters.cache.size'
PARAMETERS_CACHE_TTL_SETTING = 'parameters.cache.ttl.seconds'
RESULTS_CACHE_SIZE_SETTING = 'results.cache.size'
RESULTS_CACHE_TTL_SETTING = 'results.cache.ttl.seconds'

# Selection cfg sections
USER_ATTRIBUTES_SECTION = 'user_attributes'
PARAMETERS_SECTION = 'parameters'

# Init Command Choices
FILE_TYPE_CHOICES = ['sql', 'py']
DATABASE_CHOICES = ['sample_database', 'seattle_weather']
