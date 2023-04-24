import inquirer, os, shutil
from squirrels import major_version, constants as c, utils

base_proj_dir = utils.join_paths(os.path.dirname(__file__), 'package_data', 'base_project')
dataset_dir = utils.join_paths('datasets', 'sample_dataset')


class Initializer:
    def __init__(self, overwrite: bool):
        self.overwrite = overwrite

    def _path_exists(self, filepath: str) -> bool:
        if not self.overwrite and os.path.exists(filepath):
            print(f'File "{filepath}" already exists. Creation skipped.')
            return True
        return False
    
    def _copy_file(self, filepath: str):
        if not self._path_exists(filepath):
            dest_dir = os.path.dirname(filepath)
            if dest_dir != '':
                os.makedirs(dest_dir, exist_ok=True)
            src_path = utils.join_paths(base_proj_dir, filepath)
            shutil.copy(src_path, filepath)

    def _copy_dataset_file(self, filepath: str):
        self._copy_file(utils.join_paths(dataset_dir, filepath))

    def _copy_database_file(self, filepath: str):
        self._copy_file(utils.join_paths('database', filepath))

    def _create_requirements_txt(self):
        filename = 'requirements.txt'
        if not self._path_exists(filename):
            next_major_version = int(major_version) + 1
            content = f'squirrels<{next_major_version}'
            with open(filename, 'w') as f:
                f.write(content)

    def init_project(self, args):
        options = ['core', 'context', 'selections_cfg', 'db_view', 'final_view', 'sample_db']
        answers = { x: getattr(args, x) for x in options }
        if not any(answers.values()):
            questions = [
                inquirer.Confirm('core',
                                message="Include all core project files?",
                                default=True),
                inquirer.Confirm('context',
                                message="Do you want to include a 'context.py' file?" ,
                                default=False),
                inquirer.Confirm('selections_cfg',
                                message="Do you want to include a 'selections.cfg' file?" ,
                                default=False),
                inquirer.List('db_view', 
                            message="What's the file format for the database view? (ignore if core project files are not included)",
                            choices=['sql', 'py']),
                inquirer.List('final_view', 
                            message="What's the file format for the final view (if any)?",
                            choices=['none', 'sql', 'py']),
                inquirer.List('sample_db', 
                            message="What sample sqlite database do you wish to use (if any)?",
                            choices=['none', 'sample_database', 'seattle_weather'])
            ]
            answers = inquirer.prompt(questions)

        if answers.get('core', False):
            self._copy_file('.gitignore')
            self._copy_file(c.MANIFEST_FILE)
            self._copy_file(c.PROJ_VARS_FILE)
            self._copy_file(c.CONNECTIONS_FILE)
            self._create_requirements_txt()
            self._copy_dataset_file(c.PARAMETERS_FILE)
            if answers.get('db_view') == 'py':
                self._copy_dataset_file(c.DATABASE_VIEW_PY_FILE)
            else:
                self._copy_dataset_file(c.DATABASE_VIEW_SQL_FILE)
        
        if answers.get('context', False):
            self._copy_dataset_file(c.CONTEXT_FILE)
        
        if answers.get('selections_cfg', False):
            self._copy_dataset_file(c.SELECTIONS_CFG_FILE)
        
        final_view_format = answers.get('final_view')
        if final_view_format == 'py':
            self._copy_dataset_file(c.FINAL_VIEW_PY_NAME)
        elif final_view_format == 'sql':
            self._copy_dataset_file(c.FINAL_VIEW_SQL_NAME)

        sample_db = answers.get('sample_db')
        if sample_db == 'sample_database':
            self._copy_database_file('sample_database.db')
        elif sample_db == 'seattle_weather':
            self._copy_database_file('seattle_weather.db')
    