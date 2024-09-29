from typing import Optional
from datetime import datetime
import inquirer, os, shutil

from . import _constants as c, _utils as u

base_proj_dir = u.Path(os.path.dirname(__file__), c.PACKAGE_DATA_FOLDER, c.BASE_PROJECT_FOLDER)


class Initializer:
    def __init__(self, *, overwrite: bool = False):
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
        
        dest_dir = os.path.dirname(filepath)
        if dest_dir != "":
            os.makedirs(dest_dir, exist_ok=True)
        
        perform_copy = True
        if self._path_exists(filepath):
            old_filepath = filepath
            if self._files_have_same_content(src_path, filepath):
                perform_copy = False
                extra_msg = "Skipping... file contents is same as source"
            elif self.overwrite:
                extra_msg = "Overwriting file..."
            else:
                filepath = self._add_timestamp_to_filename(old_filepath)
                extra_msg = f'Creating file as "{filepath}" instead...'
            print(f'File "{old_filepath}" already exists.', extra_msg)
        else:
            print(f'Creating file "{filepath}"...')
        
        if perform_copy:
            shutil.copy(src_path, filepath)

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

    def _create_manifest_file(self, has_connections: bool, has_parameters: bool, has_dashboards: bool):
        TMP_FOLDER = "tmp"

        def get_content(file_name: Optional[str]) -> str:
            if file_name is None:
                return ""
            
            yaml_path = u.Path(base_proj_dir, file_name)
            return u.read_file(yaml_path)
        
        file_name_dict = {
            "parameters": c.PARAMETERS_YML_FILE if has_parameters else None, 
            "connections": c.CONNECTIONS_YML_FILE if has_connections else None,
            "dashboards": c.DASHBOARDS_YML_FILE if has_dashboards else None
        }
        substitutions = {key: get_content(val) for key, val in file_name_dict.items()}
        
        manifest_template = get_content(c.MANIFEST_JINJA_FILE)
        manifest_content = u.render_string(manifest_template, **substitutions)
        output_path = u.Path(base_proj_dir, TMP_FOLDER, c.MANIFEST_FILE)
        with open(u.Path(output_path), "w") as f:
            f.write(manifest_content)
        
        self._copy_file(u.Path(c.MANIFEST_FILE), src_folder=TMP_FOLDER)

    def init_project(self, args):
        options = ["core", "connections", "parameters", "dbview", "federate", "dashboard", "auth"]
        _, CONNECTIONS, PARAMETERS, DBVIEW, FEDERATE, DASHBOARD, AUTH = options

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
                    DBVIEW, message="What's the file format for the database view model?", choices=c.FILE_TYPE_CHOICES
                ),
                inquirer.List(
                    FEDERATE, message="What's the file format for the federated model?", choices=c.FILE_TYPE_CHOICES
                ),
                inquirer.Confirm(
                    DASHBOARD, message=f"Do you want to include a dashboard example?", default=False
                ),
                inquirer.Confirm(
                    AUTH, message=f"Do you want to add the '{c.AUTH_FILE}' file to enable custom API authentication?", default=False
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
        
        connections_format = get_answer(CONNECTIONS, c.YML_FORMAT)
        connections_use_yaml = (connections_format == c.YML_FORMAT)
        connections_use_py = (connections_format == c.PYTHON_FORMAT)

        parameters_format = get_answer(PARAMETERS, c.PYTHON_FORMAT)
        parameters_use_yaml = (parameters_format == c.YML_FORMAT)
        parameters_use_py = (parameters_format == c.PYTHON_FORMAT)

        dbview_format = get_answer(DBVIEW, c.SQL_FILE_TYPE)
        if dbview_format == c.SQL_FILE_TYPE:
            db_view_file = c.DBVIEW_FILE_STEM + ".sql"
        elif dbview_format == c.PYTHON_FILE_TYPE:
            db_view_file = c.DBVIEW_FILE_STEM + ".py"
        else:
            raise NotImplementedError(f"Dbview model format '{dbview_format}' not supported")
    
        federate_format = get_answer(FEDERATE, c.SQL_FILE_TYPE)
        if federate_format == c.SQL_FILE_TYPE:
            federate_file = c.FEDERATE_FILE_STEM + ".sql"
        elif federate_format == c.PYTHON_FILE_TYPE:
            federate_file = c.FEDERATE_FILE_STEM + ".py"
        else:
            raise NotImplementedError(f"Federate model format '{federate_format}' not supported")

        dashboards_enabled = get_answer(DASHBOARD, False)

        self._create_manifest_file(connections_use_yaml, parameters_use_yaml, dashboards_enabled)
        
        self._copy_file(u.Path(".gitignore"))
        self._copy_file(u.Path(c.ENV_CONFIG_FILE))
        
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
        
        self._copy_pyconfig_file(c.CONTEXT_FILE)
        self._copy_seed_file(c.CATEGORY_SEED_FILE)
        self._copy_seed_file(c.SUBCATEGORY_SEED_FILE)

        self._copy_dbview_file(db_view_file)
        self._copy_federate_file(federate_file)
        
        if dashboards_enabled:
            self._copy_dashboard_file(c.DASHBOARD_FILE_STEM + ".py")
        
        if get_answer(AUTH, False):
            self._copy_pyconfig_file(c.AUTH_FILE)

        self._copy_database_file(c.EXPENSES_DB)
        
        print(f"\nSuccessfully created new Squirrels project in current directory!\n")
    
    def get_file(self, args):
        if args.file_name == c.ENV_CONFIG_FILE:
            self._copy_file(u.Path(c.ENV_CONFIG_FILE))
            print("PLEASE ENSURE THE FILE IS INCLUDED IN .gitignore")
        elif args.file_name == c.MANIFEST_FILE:
            self._create_manifest_file(not args.no_connections, args.parameters, args.dashboards)
        elif args.file_name in (c.AUTH_FILE, c.CONNECTIONS_FILE, c.PARAMETERS_FILE, c.CONTEXT_FILE):
            self._copy_pyconfig_file(args.file_name)
        elif args.file_name in (c.DBVIEW_FILE_STEM, c.FEDERATE_FILE_STEM):
            if args.format == c.SQL_FILE_TYPE:
                extension = ".sql"
            elif args.format == c.PYTHON_FILE_TYPE:
                extension = ".py"
            else:
                raise NotImplementedError(f"Format '{args.format}' not supported for {args.file_name}")
            copy_method = self._copy_dbview_file if args.file_name == c.DBVIEW_FILE_STEM else self._copy_federate_file
            copy_method(args.file_name + extension)
        elif args.file_name == c.DASHBOARD_FILE_STEM:
            self._copy_dashboard_file(args.file_name + ".py")
        elif args.file_name in (c.EXPENSES_DB, c.WEATHER_DB):
            self._copy_database_file(args.file_name)
        else:
            raise NotImplementedError(f"File '{args.file_name}' not supported")
        