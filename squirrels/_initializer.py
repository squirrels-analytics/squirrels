from typing import Optional
import inquirer, os, shutil

from . import _constants as c, _utils as u

base_proj_dir = u.join_paths(os.path.dirname(__file__), c.PACKAGE_DATA_FOLDER, c.BASE_PROJECT_FOLDER)
dataset_dir = u.join_paths(c.DATASETS_FOLDER, "sample_dataset")


class Initializer:
    def __init__(self, overwrite: bool):
        self.overwrite = overwrite

    def _path_exists(self, filepath: str) -> bool:
        if not self.overwrite and os.path.exists(filepath):
            print(f'File "{filepath}" already exists. Creation skipped.')
            return True
        return False
    
    def _copy_file(self, filepath: str, *, src_folder: str = ""):
        if not self._path_exists(filepath):
            dest_dir = os.path.dirname(filepath)
            if dest_dir != "":
                os.makedirs(dest_dir, exist_ok=True)
            src_path = u.join_paths(base_proj_dir, src_folder, filepath)
            shutil.copy(src_path, filepath)

    def _copy_dataset_file(self, filepath: str):
        self._copy_file(u.join_paths(dataset_dir, filepath))

    def _copy_database_file(self, filepath: str):
        self._copy_file(u.join_paths(c.DATABASE_FOLDER, filepath))
    
    def _copy_pyconfigs_file(self, filepath: str):
        self._copy_file(u.join_paths(c.PYCONFIG_FOLDER, filepath))

    def init_project(self, args):
        options = ["core", "connections", "parameters", "db_view", "environcfg", "context", "final_view", "auth", "selections_cfg", "sample_db"]
        CORE, CONNECTIONS, PARAMETERS, DB_VIEW, ENVIRONCFG, CONTEXT, FINAL_VIEW, AUTH, SELECTIONS_CFG, SAMPLE_DB = options
        TMP_FOLDER = "tmp"
        IGNORES_FOLDER = "ignores"

        answers = { x: getattr(args, x) for x in options }
        if not any(answers.values()):
            core_questions = [
                inquirer.Confirm(CORE, 
                                 message="Include all core project files?",
                                 default=True)
            ]
            answers = inquirer.prompt(core_questions)
            
            if answers.get(CORE, False):
                conditional_questions = [
                    inquirer.List(CONNECTIONS,
                                  message=f"How would you like to configure the database connections?" ,
                                  choices=c.CONF_FORMAT_CHOICES),
                    inquirer.List(PARAMETERS,
                                  message=f"How would you like to configure the parameters?" ,
                                  choices=c.CONF_FORMAT_CHOICES2),
                    inquirer.List(DB_VIEW, 
                                  message="What's the file format for the database view?",
                                  choices=c.FILE_TYPE_CHOICES),
                ]
                answers.update(inquirer.prompt(conditional_questions))

            remaining_questions = [
                inquirer.Confirm(ENVIRONCFG,
                                 message=f"Do you want to add the '{c.ENVIRON_CONFIG_FILE}' file?" ,
                                 default=False),
                inquirer.Confirm(CONTEXT,
                                 message=f"Do you want to add a '{c.CONTEXT_FILE}' file?" ,
                                 default=False),
                inquirer.List(FINAL_VIEW, 
                              message="What's the file format for the final view (if any)?",
                              choices=["none"] + c.FILE_TYPE_CHOICES),
                inquirer.Confirm(AUTH,
                                 message=f"Do you want to add the '{c.AUTH_FILE}' file?" ,
                                 default=False),
                inquirer.Confirm(SELECTIONS_CFG,
                                 message=f"Do you want to add '{c.SELECTIONS_CFG_FILE}' and '{c.LU_DATA_FILE}' files?" ,
                                 default=False),
                inquirer.List(SAMPLE_DB, 
                              message="What sample sqlite database do you wish to use (if any)?",
                              choices=["none"] + c.DATABASE_CHOICES)
            ]
            answers.update(inquirer.prompt(remaining_questions))
        
        final_view_format = answers.get(FINAL_VIEW)
        if final_view_format in [None, "none"]:
            final_view_file = c.DATABASE_VIEW_STEM
        elif final_view_format == c.SQL_FILE_TYPE:
            final_view_file = c.FINAL_VIEW_SQL_NAME
        elif final_view_format == c.PYTHON_FILE_TYPE:
            final_view_file = c.FINAL_VIEW_PY_NAME
        else:
            raise NotImplementedError(f"Final view format '{final_view_format}' not supported")

        if answers.get(CORE, False):
            connections_format = answers.get(CONNECTIONS)
            connections_use_yaml = (connections_format is None or connections_format == c.YAML_FORMAT)
            connections_use_py = (connections_format == c.PYTHON_FORMAT)

            parameters_format = answers.get(PARAMETERS)
            parameters_use_yaml = (parameters_format == c.YAML_FORMAT)
            parameters_use_py = (parameters_format is None or parameters_format == c.PYTHON_FORMAT)

            db_view_format = answers.get(DB_VIEW)
            if db_view_format is None or db_view_format == c.SQL_FILE_TYPE:
                db_view_file = c.DATABASE_VIEW_SQL_FILE
            elif db_view_format == c.PYTHON_FILE_TYPE:
                db_view_file = c.DATABASE_VIEW_PY_FILE
            else:
                raise NotImplementedError(f"Database view format '{db_view_format}' not supported")
    
            def create_manifest_file():
                def get_content(file_name: Optional[str]) -> str:
                    if file_name is None:
                        return ""
                    
                    yaml_path = u.join_paths(base_proj_dir, file_name)
                    with open(yaml_path, "r") as f:
                        return f.read()
                
                file_name_dict = {
                    "parameters": c.PARAMETERS_YML_FILE if parameters_use_yaml else None, 
                    "connections": c.CONNECTIONS_YML_FILE if connections_use_yaml else None
                }
                substitutions = {key: get_content(val) for key, val in file_name_dict.items()}
                substitutions["db_view_file"] = db_view_file
                substitutions["final_view_file"] = final_view_file
                
                manifest_template = get_content(c.MANIFEST_JINJA_FILE)
                manifest_content = u.render_string(manifest_template, substitutions)
                output_path = u.join_paths(base_proj_dir, TMP_FOLDER, c.MANIFEST_FILE)
                with open(u.join_paths(output_path), "w") as f:
                    f.write(manifest_content)

            create_manifest_file()
            
            self._copy_file(".gitignore", src_folder=IGNORES_FOLDER)
            self._copy_file(c.MANIFEST_FILE, src_folder=TMP_FOLDER)
            
            if connections_use_py:
                self._copy_pyconfigs_file(c.CONNECTIONS_FILE)
            elif not connections_use_yaml:
                raise NotImplementedError(f"Format '{connections_format}' not supported for configuring database connections")
            
            if parameters_use_py:
                self._copy_pyconfigs_file(c.PARAMETERS_FILE)
            elif not parameters_use_yaml:
                raise NotImplementedError(f"Format '{parameters_format}' not supported for configuring widget parameters")
            
            self._copy_dataset_file(db_view_file)
        
        if answers.get(ENVIRONCFG, False):
            self._copy_file(c.ENVIRON_CONFIG_FILE)
        
        if answers.get(CONTEXT, False):
            self._copy_pyconfigs_file(c.CONTEXT_FILE)
        
        if answers.get(SELECTIONS_CFG, False):
            self._copy_dataset_file(c.SELECTIONS_CFG_FILE)
            self._copy_file(c.LU_DATA_FILE)
        
        if final_view_file != c.DATABASE_VIEW_STEM:
            self._copy_dataset_file(final_view_file)
        
        if answers.get(AUTH, False):
            self._copy_pyconfigs_file(c.AUTH_FILE)

        sample_db = answers.get(SAMPLE_DB)
        if sample_db is not None and sample_db != "none":
            if sample_db == c.EXPENSES_DB_NAME:
                self._copy_database_file(c.EXPENSES_DB_NAME+".db")
            elif sample_db == c.WEATHER_DB_NAME:
                self._copy_database_file(c.WEATHER_DB_NAME+".db")
            else:
                raise NotImplementedError(f"No database found called '{sample_db}'")
    