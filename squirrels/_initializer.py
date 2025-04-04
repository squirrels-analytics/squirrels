from typing import Optional
from datetime import datetime
import inquirer, os, shutil, secrets

from . import _constants as c, _utils as u

base_proj_dir = u.Path(os.path.dirname(__file__), c.PACKAGE_DATA_FOLDER, c.BASE_PROJECT_FOLDER)

TMP_FOLDER = "tmp"


class Initializer:
    def __init__(self, *, project_name: Optional[str] = None, overwrite: bool = False):
        self.project_name = project_name
        self.overwrite = overwrite

    def _path_exists(self, filepath: u.Path) -> bool:
        return os.path.exists(filepath)
    
    def _files_have_same_content(self, file1: u.Path, file2: u.Path) -> bool:
        with open(file1, 'rb') as f1, open(file2, 'rb') as f2:
            return f1.read() == f2.read()
    
    def _add_timestamp_to_filename(self, path: u.Path) -> u.Path:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        new_filename = f"{path.stem}_{timestamp}{path.suffix}"
        return path.with_name(new_filename)
    
    def _copy_file(self, filepath: u.Path, *, src_folder: str = ""):
        src_path = u.Path(base_proj_dir, src_folder, filepath)
        
        filepath2 = u.Path(self.project_name, filepath) if self.project_name else filepath
        dest_dir = os.path.dirname(filepath2)
        if dest_dir != "":
            os.makedirs(dest_dir, exist_ok=True)
        
        perform_copy = True
        if self._path_exists(filepath2):
            old_filepath = filepath2
            if self._files_have_same_content(src_path, filepath2):
                perform_copy = False
                extra_msg = "Skipping... file contents is same as source"
            elif self.overwrite:
                extra_msg = "Overwriting file..."
            else:
                filepath2 = self._add_timestamp_to_filename(old_filepath)
                extra_msg = f'Creating file as "{filepath2}" instead...'
            print(f'File "{old_filepath}" already exists.', extra_msg)
        else:
            print(f'Creating file "{filepath2}"...')
        
        if perform_copy:
            shutil.copy(src_path, filepath2)

    def _copy_macros_file(self, filepath: str):
        self._copy_file(u.Path(c.MACROS_FOLDER, filepath))

    def _copy_models_file(self, filepath: str):
        self._copy_file(u.Path(c.MODELS_FOLDER, filepath))

    def _copy_build_file(self, filepath: str):
        self._copy_file(u.Path(c.MODELS_FOLDER, c.BUILDS_FOLDER, filepath))

    def _copy_dbview_file(self, filepath: str):
        self._copy_file(u.Path(c.MODELS_FOLDER, c.DBVIEWS_FOLDER, filepath))

    def _copy_federate_file(self, filepath: str):
        self._copy_file(u.Path(c.MODELS_FOLDER, c.FEDERATES_FOLDER, filepath))

    def _copy_database_file(self, filepath: str):
        self._copy_file(u.Path(c.DATABASE_FOLDER, filepath))
    
    def _copy_pyconfig_file(self, filepath: str):
        self._copy_file(u.Path(c.PYCONFIGS_FOLDER, filepath))
    
    def _copy_seed_file(self, filepath: str):
        self._copy_file(u.Path(c.SEEDS_FOLDER, filepath))
    
    def _copy_dashboard_file(self, filepath: str):
        self._copy_file(u.Path(c.DASHBOARDS_FOLDER, filepath))

    def _create_manifest_file(self, has_connections: bool, has_parameters: bool):
        def get_content(file_name: Optional[str]) -> str:
            if file_name is None:
                return ""
            
            yaml_path = u.Path(base_proj_dir, file_name)
            return yaml_path.read_text()
        
        file_name_dict = {
            "parameters": c.PARAMETERS_YML_FILE if has_parameters else None, 
            "connections": c.CONNECTIONS_YML_FILE if has_connections else None,
        }
        substitutions = {key: get_content(val) for key, val in file_name_dict.items()}
        
        manifest_template = get_content(c.MANIFEST_JINJA_FILE)
        manifest_content = u.render_string(manifest_template, **substitutions)
        output_path = u.Path(base_proj_dir, TMP_FOLDER, c.MANIFEST_FILE)
        output_path.write_text(manifest_content)
        
        self._copy_file(u.Path(c.MANIFEST_FILE), src_folder=TMP_FOLDER)
    
    def _copy_dotenv_files(self, admin_password: str | None = None):
        substitutions = {
            "random_secret_key": secrets.token_hex(32),
            "random_admin_password": admin_password if admin_password else secrets.token_urlsafe(8),
        }

        dotenv_path = u.Path(base_proj_dir, c.DOTENV_FILE)
        contents = u.render_string(dotenv_path.read_text(), **substitutions)

        output_path = u.Path(base_proj_dir, TMP_FOLDER, c.DOTENV_FILE)
        output_path.write_text(contents)

        self._copy_file(u.Path(c.DOTENV_FILE), src_folder=TMP_FOLDER)
        self._copy_file(u.Path(c.DOTENV_FILE + ".example"))

    def init_project(self, args):
        options = ["core", "connections", "parameters", "build", "federate", "dashboard"]
        _, CONNECTIONS, PARAMETERS, BUILD, FEDERATE, DASHBOARD = options

        # Add project name prompt if not provided
        if self.project_name is None:
            questions = [
                inquirer.Text('project_name', message="What is your project name? (leave blank to create in current directory)")
            ]
            answers = inquirer.prompt(questions)
            assert isinstance(answers, dict)
            self.project_name = answers['project_name']

        answers = { x: getattr(args, x) for x in options }
        if not any(answers.values()):
            questions = [
                inquirer.List(
                    CONNECTIONS, message=f"How would you like to configure the database connections?", choices=c.CONF_FORMAT_CHOICES
                ),
                inquirer.List(
                    PARAMETERS, message=f"How would you like to configure the parameters?", choices=c.CONF_FORMAT_CHOICES2
                ),
                inquirer.List(
                    BUILD, message="What's the file format for the build model?", choices=c.FILE_TYPE_CHOICES
                ),
                inquirer.List(
                    FEDERATE, message="What's the file format for the federated model?", choices=c.FILE_TYPE_CHOICES
                ),
                inquirer.Confirm(
                    DASHBOARD, message=f"Do you want to include a dashboard example?", default=False
                ),
                inquirer.Password(
                    "admin_password", message="What's the admin password? (leave blank to generate a random one)"
                ),
            ]
            answers = inquirer.prompt(questions)
            assert isinstance(answers, dict)
        
        def get_answer(key, default):
            """
            If key is in answers dict as None, using `.get` on a dictionary will return None even if a default is provided.

            For instance, the following prints None.
            >>> test_dict = {"key": None}
            >>> print(test_dict.get("key", "default"))

            This function will return the default value if the key is in the dict with value None.
            """
            answer = answers.get(key)
            return answer if answer is not None else default
        
        admin_password = get_answer("admin_password", None)
        
        connections_format = get_answer(CONNECTIONS, c.YML_FORMAT)
        connections_use_yaml = (connections_format == c.YML_FORMAT)
        connections_use_py = (connections_format == c.PYTHON_FORMAT)

        parameters_format = get_answer(PARAMETERS, c.PYTHON_FORMAT)
        parameters_use_yaml = (parameters_format == c.YML_FORMAT)
        parameters_use_py = (parameters_format == c.PYTHON_FORMAT)

        build_config_file = c.BUILD_FILE_STEM + ".yml"
        build_format = get_answer(BUILD, c.PYTHON_FILE_TYPE)
        if build_format == c.SQL_FILE_TYPE:
            build_file = c.BUILD_FILE_STEM + ".sql"
        elif build_format == c.PYTHON_FILE_TYPE:
            build_file = c.BUILD_FILE_STEM + ".py"
        else:
            raise NotImplementedError(f"Build model format '{build_format}' not supported")

        db_view_config_file = c.DBVIEW_FILE_STEM + ".yml"
        db_view_file = c.DBVIEW_FILE_STEM + ".sql"
    
        federate_config_file = c.FEDERATE_FILE_STEM + ".yml"
        federate_format = get_answer(FEDERATE, c.SQL_FILE_TYPE)
        if federate_format == c.SQL_FILE_TYPE:
            federate_file = c.FEDERATE_FILE_STEM + ".sql"
        elif federate_format == c.PYTHON_FILE_TYPE:
            federate_file = c.FEDERATE_FILE_STEM + ".py"
        else:
            raise NotImplementedError(f"Federate model format '{federate_format}' not supported")

        dashboards_enabled = get_answer(DASHBOARD, False)

        self._copy_dotenv_files(admin_password)
        self._create_manifest_file(connections_use_yaml, parameters_use_yaml)
        
        self._copy_file(u.Path(c.GITIGNORE_FILE))
        
        if connections_use_py:
            self._copy_pyconfig_file(c.CONNECTIONS_FILE)
        elif connections_use_yaml:
            pass # already included in squirrels.yml
        else:
            raise NotImplementedError(f"Format '{connections_format}' not supported for configuring database connections")
        
        if parameters_use_py:
            self._copy_pyconfig_file(c.PARAMETERS_FILE)
        elif parameters_use_yaml:
            pass # already included in squirrels.yml
        else:
            raise NotImplementedError(f"Format '{parameters_format}' not supported for configuring widget parameters")
        
        self._copy_pyconfig_file(c.USER_FILE)

        self._copy_pyconfig_file(c.CONTEXT_FILE)
        self._copy_seed_file(c.SEED_CATEGORY_FILE_STEM + ".csv")
        self._copy_seed_file(c.SEED_CATEGORY_FILE_STEM + ".yml")
        self._copy_seed_file(c.SEED_SUBCATEGORY_FILE_STEM + ".csv")
        self._copy_seed_file(c.SEED_SUBCATEGORY_FILE_STEM + ".yml")

        self._copy_macros_file(c.MACROS_FILE)

        self._copy_models_file(c.SOURCES_FILE)
        self._copy_build_file(build_file)
        self._copy_build_file(build_config_file)
        self._copy_dbview_file(db_view_file)
        self._copy_dbview_file(db_view_config_file)
        self._copy_federate_file(federate_file)
        self._copy_federate_file(federate_config_file)
        
        if dashboards_enabled:
            self._copy_dashboard_file(c.DASHBOARD_FILE_STEM + ".py")
            self._copy_dashboard_file(c.DASHBOARD_FILE_STEM + ".yml")
        
        self._copy_database_file(c.EXPENSES_DB)
        
        print(f"\nSuccessfully created new Squirrels project in current directory!\n")
    
    def get_file(self, args):
        if args.file_name == c.DOTENV_FILE:
            self._copy_dotenv_files()
            print(f"A random admin password was generated for your project. You can change it in the new {c.DOTENV_FILE} file.")
            print()
            print(f"IMPORTANT: Please ensure the {c.DOTENV_FILE} file is added to your {c.GITIGNORE_FILE} file.")
            print(f"You may also run `sqrl get-file {c.GITIGNORE_FILE}` to add a sample {c.GITIGNORE_FILE} file to your project.")
            print()
        elif args.file_name == c.GITIGNORE_FILE:
            self._copy_file(u.Path(c.GITIGNORE_FILE))
        elif args.file_name == c.MANIFEST_FILE:
            self._create_manifest_file(not args.no_connections, args.parameters)
        elif args.file_name in (c.USER_FILE, c.CONNECTIONS_FILE, c.PARAMETERS_FILE, c.CONTEXT_FILE):
            self._copy_pyconfig_file(args.file_name)
        elif args.file_name == c.MACROS_FILE:
            self._copy_macros_file(args.file_name)
        elif args.file_name == c.SOURCES_FILE:
            self._copy_models_file(args.file_name)
        elif args.file_name in (c.BUILD_FILE_STEM, c.DBVIEW_FILE_STEM, c.FEDERATE_FILE_STEM):
            if args.file_name == c.DBVIEW_FILE_STEM or args.format == c.SQL_FILE_TYPE:
                extension = ".sql"
            elif args.format == c.PYTHON_FILE_TYPE:
                extension = ".py"
            else:
                raise NotImplementedError(f"Format '{args.format}' not supported for {args.file_name}")
            
            if args.file_name == c.BUILD_FILE_STEM:
                copy_method = self._copy_build_file
            elif args.file_name == c.DBVIEW_FILE_STEM:
                copy_method = self._copy_dbview_file
            elif args.file_name == c.FEDERATE_FILE_STEM:
                copy_method = self._copy_federate_file
            else:
                raise NotImplementedError(f"File '{args.file_name}' not supported")
            
            copy_method(args.file_name + extension)
            copy_method(args.file_name + ".yml")
        elif args.file_name == c.DASHBOARD_FILE_STEM:
            self._copy_dashboard_file(args.file_name + ".py")
            self._copy_dashboard_file(args.file_name + ".yml")
        elif args.file_name in (c.EXPENSES_DB, c.WEATHER_DB):
            self._copy_database_file(args.file_name)
        elif args.file_name in (c.SEED_CATEGORY_FILE_STEM, c.SEED_SUBCATEGORY_FILE_STEM):
            self._copy_seed_file(args.file_name + ".csv")
            self._copy_seed_file(args.file_name + ".yml")
        else:
            raise NotImplementedError(f"File '{args.file_name}' not supported")
        