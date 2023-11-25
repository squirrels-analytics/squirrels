import inquirer, os, shutil

from . import _constants as c, _utils as u

base_proj_dir = u.join_paths(os.path.dirname(__file__), 'package_data', 'base_project')
dataset_dir = u.join_paths('datasets', 'sample_dataset')


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
            if dest_dir != '':
                os.makedirs(dest_dir, exist_ok=True)
            src_path = u.join_paths(base_proj_dir, src_folder, filepath)
            shutil.copy(src_path, filepath)

    def _copy_dataset_file(self, filepath: str):
        self._copy_file(u.join_paths(dataset_dir, filepath))

    def _copy_database_file(self, filepath: str):
        self._copy_file(u.join_paths('database', filepath))

    def init_project(self, args):
        options = ['core', 'db_view', 'environcfg', 'connections', 'context', 'final_view', 'auth', 'selections_cfg', 'sample_db']
        answers = { x: getattr(args, x) for x in options }
        if not any(answers.values()):
            core_questions = [
                inquirer.Confirm('core', 
                                 message="Include all core project files?",
                                 default=True)
            ]
            answers = inquirer.prompt(core_questions)
            
            if answers.get('core', False):
                conditional_questions = [
                    inquirer.List('db_view', 
                                  message="What's the file format for the database view?",
                                  choices=c.FILE_TYPE_CHOICES),
                ]
                answers.update(inquirer.prompt(conditional_questions))

            remaining_questions = [
                inquirer.Confirm('environcfg',
                                 message=f"Do you want to add the '{c.ENVIRON_CONFIG_FILE}' file?" ,
                                 default=False),
                inquirer.Confirm('connections',
                                 message=f"Do you want to add the '{c.CONNECTIONS_FILE}' file?" ,
                                 default=False),
                inquirer.Confirm('context',
                                 message=f"Do you want to add a '{c.CONTEXT_FILE}' file?" ,
                                 default=False),
                inquirer.List('final_view', 
                              message="What's the file format for the final view (if any)?",
                              choices=['none'] + c.FILE_TYPE_CHOICES),
                inquirer.Confirm('auth',
                                 message=f"Do you want to add the '{c.AUTH_FILE}' file?" ,
                                 default=False),
                inquirer.Confirm('selections_cfg',
                                 message=f"Do you want to add '{c.SELECTIONS_CFG_FILE}' and '{c.LU_DATA_FILE}' files?" ,
                                 default=False),
                inquirer.List('sample_db', 
                              message="What sample sqlite database do you wish to use (if any)?",
                              choices=['none'] + c.DATABASE_CHOICES)
            ]
            answers.update(inquirer.prompt(remaining_questions))

        if answers.get('core', False):
            self._copy_file(".gitignore", src_folder="ignores")
            self._copy_file(c.MANIFEST_FILE)
            self._copy_file(c.PARAMETERS_FILE)
            if answers.get('db_view') == 'py':
                self._copy_dataset_file(c.DATABASE_VIEW_PY_FILE)
            else:
                self._copy_dataset_file(c.DATABASE_VIEW_SQL_FILE)
        
        if answers.get('environcfg', False):
            self._copy_file(c.ENVIRON_CONFIG_FILE)
        
        if answers.get('connections', False):
            self._copy_file(c.CONNECTIONS_FILE)
        
        if answers.get('context', False):
            self._copy_dataset_file(c.CONTEXT_FILE)
        
        if answers.get('selections_cfg', False):
            self._copy_dataset_file(c.SELECTIONS_CFG_FILE)
            self._copy_file(c.LU_DATA_FILE)
        
        final_view_format = answers.get('final_view')
        if final_view_format == 'py':
            self._copy_dataset_file(c.FINAL_VIEW_PY_NAME)
        elif final_view_format == 'sql':
            self._copy_dataset_file(c.FINAL_VIEW_SQL_NAME)
        
        if answers.get('auth', False):
            self._copy_file(c.AUTH_FILE)

        sample_db = answers.get('sample_db')
        if sample_db == 'expenses':
            self._copy_database_file('expenses.db')
        elif sample_db == 'seattle-weather':
            self._copy_database_file('seattle_weather.db')
    