# Squirrels CLI commands
GET_PROFILES_CMD = 'get-all-profiles'
SET_PROFILE_CMD = 'set-profile'
DELETE_PROFILE_CMD = 'delete-profile'
INIT_CMD = 'init'
LOAD_MODULES_CMD = 'load-modules'
TEST_CMD = 'test'
RUN_CMD = 'run'

# Manifest file keys
DB_PROFILE_KEY = 'db_profile'
PROJ_VARS_KEY = 'project_variables'
MODULES_KEY = 'modules'
DATASET_LABEL_KEY = 'label'
DATASETS_KEY = 'datasets'
HEADERS_KEY = 'headers'
DATABASE_VIEWS_KEY = 'database_views'
DB_VIEW_NAME_KEY = 'name'
DB_VIEW_FILE_KEY = 'file'
FINAL_VIEW_KEY = 'final_view'
BASE_PATH_KEY = 'base_path'
SETTINGS_KEY = 'settings'

# Database profile keys
DIALECT = 'dialect'
CONN_URL = 'conn_url'
USERNAME = 'username'
PASSWORD = 'password'

# Folder/File names
MANIFEST_FILE = 'squirrels.yaml'
OUTPUTS_FOLDER = 'outputs'
MODULES_FOLDER = 'modules'
DATASETS_FOLDER = 'datasets'
PARAMETERS_MODULE = 'parameters'
PARAMETERS_FILE = 'parameters.py'
PARAMETERS_OUTPUT = 'parameters.json'
DATABASE_VIEW_NAME = 'database_view1'
FINAL_VIEW_NAME = 'final_view'
CONTEXT_FILE = 'context.py'

# Dataset setting names
PARAMETERS_CACHE_SIZE_SETTING = 'parameters.cache.size'
PARAMETERS_CACHE_TTL_SETTING = 'parameters.cache.ttl'
RESULTS_CACHE_SIZE_SETTING = 'results.cache.size'
RESULTS_CACHE_TTL_SETTING = 'results.cache.ttl'

# Activities to time
IMPORT_JINJA = 'import jinja'
IMPORT_SQLALCHEMY = 'import sqlalchemy'
IMPORT_PANDAS = 'import pandas'

# Selection cfg sections
PARAMETERS_SECTION = 'parameters'
HEADERS_SECTION = 'headers'
