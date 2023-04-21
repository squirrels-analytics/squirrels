import inquirer, os, shutil
from squirrels import major_version, constants as c

base_proj_dir = os.path.join(os.path.dirname(__file__), 'base_project')
dataset_dir = os.path.join('datasets', 'sample_dataset')


class Initializer:
    def __init__(self, overwrite: bool):
        self.overwrite = overwrite

    def path_exists(self, filepath: str) -> bool:
        if not self.overwrite and os.path.exists(filepath):
            print(f'File "{filepath}" already exists. Creation skipped.')
            return True
        return False
    
    def copy_file(self, filepath: str):
        if not self.path_exists(filepath):
            dest_dir = os.path.dirname(filepath)
            if dest_dir != '':
                os.makedirs(dest_dir, exist_ok=True)
            src_path = os.path.join(base_proj_dir, filepath)
            shutil.copy(src_path, filepath)

    def copy_dataset_file(self, filepath: str):
        self.copy_file(os.path.join(dataset_dir, filepath))

    def copy_database_file(self, filepath: str):
        self.copy_file(os.path.join('database', filepath))

    def create_requirements_txt(self):
        filename = 'requirements.txt'
        if not self.path_exists(filename):
            next_major_version = int(major_version) + 1
            content = f'squirrels<{next_major_version}'
            with open('requirements.txt', 'w') as f:
                f.write(content)

    def init_project(self, args):
        options = ['core', 'context', 'db_view', 'final_view', 'sample_db']
        answers = { x: getattr(args, x) for x in options }
        if not any(answers.values()):
            questions = [
                inquirer.Confirm('core',
                                message="Include all core project files?",
                                default=True),
                inquirer.Confirm('context',
                                message="Do you want to include a 'context.py' file?" ,
                                default=False),
                inquirer.List('db_view', 
                            message="What's the file format for the database view? (ignore if core project files are not included)",
                            choices=['sql', 'py']),
                inquirer.List('final_view', 
                            message="What's the file format for the final view (if any)?",
                            choices=['none', 'sql', 'py']),
                inquirer.List('sample_db', 
                            message="What sample sqlite database do you wish to use (if any)?",
                            choices=['none', 'seattle-weather'])
            ]
            answers = inquirer.prompt(questions)

        if answers.get('core', False):
            self.copy_file('.gitignore')
            self.create_requirements_txt()
            self.copy_file(c.MANIFEST_FILE)
            self.copy_dataset_file(c.PARAMETERS_FILE)
            if answers.get('db_view') == 'py':
                self.copy_dataset_file(c.DATABASE_VIEW_NAME + '.py')
            else:
                self.copy_dataset_file(c.DATABASE_VIEW_NAME + '.sql.j2')
        
        if answers.get('context', False):
            self.copy_dataset_file(c.CONTEXT_FILE)
        
        final_view_format = answers.get('final_view')
        if final_view_format == 'py':
            self.copy_dataset_file(c.FINAL_VIEW_NAME + '.py')
        elif final_view_format == 'sql':
            self.copy_dataset_file(c.FINAL_VIEW_NAME + '.sql.j2')

        sample_db = answers.get('sample_db')
        if sample_db == 'seattle-weather':
            self.copy_database_file('seattle_weather.db')
    