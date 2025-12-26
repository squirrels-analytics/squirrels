from argparse import ArgumentParser, _SubParsersAction
from pathlib import Path
import sys, asyncio, traceback, io, subprocess

sys.path.append('.')

from ._version import __version__
from ._api_server import ApiServer
from ._initializer import Initializer
from ._package_loader import PackageLoaderIO
from ._project import SquirrelsProject
from . import _constants as c, _utils as u
from ._compile_prompts import prompt_compile_options


def _run_duckdb_cli(project: SquirrelsProject, ui: bool):
    init_sql = u._read_duckdb_init_sql(datalake_db_path=project._vdl_catalog_db_path)
    
    target_init_path = None
    if init_sql:
        target_init_path = Path(c.TARGET_FOLDER, c.DUCKDB_INIT_FILE)
        target_init_path.parent.mkdir(parents=True, exist_ok=True)
        target_init_path.write_text(init_sql)
    
    init_args = ["-init", str(target_init_path)] if target_init_path else []
    command = ['duckdb']
    if ui:
        command.append('-ui')
    if init_args:
        command.extend(init_args)
    
    print("Starting DuckDB CLI with command:")
    print(f"$ {' '.join(command)}")
    print()
    print("To exit the DuckDB CLI, enter '.exit'")
    print()
    
    try:
        subprocess.run(command, check=True)
    except FileNotFoundError:
        print("DuckDB CLI not found. Please install it from: https://duckdb.org/docs/installation/")
    except subprocess.CalledProcessError:
        pass # ignore errors that occured on duckdb shell commands


def main():
    """
    Main entry point for the squirrels command line utilities.
    """
    # Create a parent parser with common logging options
    parent_parser = ArgumentParser(add_help=False)
    parent_parser.add_argument('-h', '--help', action="help", help="Show this help message and exit")
    parent_parser.add_argument('--log-level', type=str, choices=["DEBUG", "INFO", "WARNING"], help='Level of logging to use. Default is from SQRL_LOGGING__LOG_LEVEL environment variable or INFO.')
    parent_parser.add_argument('--log-format', type=str, choices=["text", "json"], help='Format of the log records. Default is from SQRL_LOGGING__LOG_FORMAT environment variable or text.')
    parent_parser.add_argument('--log-to-file', action='store_true', help='Enable logging to file(s) in the "logs/" folder with rotation and retention policies.')
    
    parser = ArgumentParser(description="Command line utilities from the squirrels python package", add_help=False, parents=[parent_parser])
    parser.add_argument('-V', '--version', action='store_true', help='Show the version and exit')
    subparsers = parser.add_subparsers(title='commands', dest='command')

    def add_subparser(subparsers: _SubParsersAction, cmd: str, help_text: str):
        subparser: ArgumentParser = subparsers.add_parser(cmd, description=help_text, help=help_text, add_help=False, parents=[parent_parser])
        return subparser

    new_parser = add_subparser(subparsers, c.NEW_CMD, 'Create a new squirrels project')
    
    new_parser.add_argument('name', nargs='?', type=str, help='The name of the project folder to create. Ignored if --curr-dir is used')
    new_parser.add_argument('--curr-dir', action='store_true', help='Create the project in the current directory')
    new_parser.add_argument('--use-defaults', action='store_true', help='Use default values for unspecified options (except project folder name) instead of prompting for input')
    new_parser.add_argument('--connections', type=str, choices=c.CONF_FORMAT_CHOICES, help=f'Configure database connections as yaml (default) or python')
    new_parser.add_argument('--parameters', type=str, choices=c.CONF_FORMAT_CHOICES, help=f'Configure parameters as python (default) or yaml')
    new_parser.add_argument('--build', type=str, choices=c.FILE_TYPE_CHOICES, help='Create build model as sql (default) or python file')
    new_parser.add_argument('--federate', type=str, choices=c.FILE_TYPE_CHOICES, help='Create federated model as sql (default) or python file')
    new_parser.add_argument('--dashboard', type=str, choices=['y', 'n'], help=f'Include (y) or exclude (n, default) a sample dashboard file')
    new_parser.add_argument('--admin-password', type=str, help='The password for the admin user. If --use-defaults is used, then a random password is generated')

    init_parser = add_subparser(subparsers, c.INIT_CMD, 'Create a new squirrels project in the current directory (alias for "new --curr-dir")')
    init_parser.add_argument('--use-defaults', action='store_true', help='Use default values for unspecified options instead of prompting for input')
    init_parser.add_argument('--connections', type=str, choices=c.CONF_FORMAT_CHOICES, help=f'Configure database connections as yaml (default) or python')
    init_parser.add_argument('--parameters', type=str, choices=c.CONF_FORMAT_CHOICES, help=f'Configure parameters as python (default) or yaml')
    init_parser.add_argument('--build', type=str, choices=c.FILE_TYPE_CHOICES, help='Create build model as sql (default) or python file')
    init_parser.add_argument('--federate', type=str, choices=c.FILE_TYPE_CHOICES, help='Create federated model as sql (default) or python file')
    init_parser.add_argument('--dashboard', type=str, choices=['y', 'n'], help=f'Include (y) or exclude (n, default) a sample dashboard file')
    init_parser.add_argument('--admin-password', type=str, help='The password for the admin user. If --use-defaults is used, then a random password is generated')

    def with_file_format_options(parser: ArgumentParser):
        help_text = "Create model as sql (default) or python file"
        parser.add_argument('--format', type=str, choices=c.FILE_TYPE_CHOICES, default=c.SQL_FILE_TYPE, help=help_text)
        return parser
    
    get_file_help_text = "Get a sample file for the squirrels project. If the file name already exists, it will be suffixed with a timestamp."
    get_file_parser = add_subparser(subparsers, c.GET_FILE_CMD, get_file_help_text)
    get_file_subparsers = get_file_parser.add_subparsers(title='file_name', dest='file_name')
    add_subparser(get_file_subparsers, c.DOTENV_FILE, f'Get sample {c.DOTENV_FILE} and {c.DOTENV_FILE}.example files')
    add_subparser(get_file_subparsers, c.GITIGNORE_FILE, f'Get a sample {c.GITIGNORE_FILE} file')
    manifest_parser = add_subparser(get_file_subparsers, c.MANIFEST_FILE, f'Get a sample {c.MANIFEST_FILE} file')
    manifest_parser.add_argument("--no-connections", action='store_true', help=f'Exclude the connections section')
    manifest_parser.add_argument("--parameters", action='store_true', help=f'Include the parameters section')
    manifest_parser.add_argument("--dashboards", action='store_true', help=f'Include the dashboards section')
    add_subparser(get_file_subparsers, c.USER_FILE, f'Get a sample {c.USER_FILE} file')
    add_subparser(get_file_subparsers, c.CONNECTIONS_FILE, f'Get a sample {c.CONNECTIONS_FILE} file')
    add_subparser(get_file_subparsers, c.PARAMETERS_FILE, f'Get a sample {c.PARAMETERS_FILE} file')
    add_subparser(get_file_subparsers, c.CONTEXT_FILE, f'Get a sample {c.CONTEXT_FILE} file')
    add_subparser(get_file_subparsers, c.MACROS_FILE, f'Get a sample {c.MACROS_FILE} file')
    add_subparser(get_file_subparsers, c.SOURCES_FILE, f'Get a sample {c.SOURCES_FILE} file')
    with_file_format_options(add_subparser(get_file_subparsers, c.BUILD_FILE_STEM, f'Get a sample build model file'))
    add_subparser(get_file_subparsers, c.DBVIEW_FILE_STEM, f'Get a sample dbview model file')
    with_file_format_options(add_subparser(get_file_subparsers, c.FEDERATE_FILE_STEM, f'Get a sample federate model file'))
    add_subparser(get_file_subparsers, c.DASHBOARD_FILE_STEM, f'Get a sample dashboard file')
    add_subparser(get_file_subparsers, c.EXPENSES_DB, f'Get the sample SQLite database on expenses')
    add_subparser(get_file_subparsers, c.WEATHER_DB, f'Get the sample SQLite database on weather')
    add_subparser(get_file_subparsers, c.SEED_CATEGORY_FILE_STEM, f'Get the sample seed files for categories lookup')
    add_subparser(get_file_subparsers, c.SEED_SUBCATEGORY_FILE_STEM, f'Get the sample seed files for subcategories lookup')
    
    deps_parser = add_subparser(subparsers, c.DEPS_CMD, f'Load all packages specified in {c.MANIFEST_FILE} (from git)')

    compile_parser = add_subparser(subparsers, c.COMPILE_CMD, 'Create rendered SQL files in the folder "./target/compile"')
    compile_parser.add_argument('-y', '--yes', action='store_true', help='Disable prompts and assume the default for all settings not overridden by CLI options')
    compile_parser.add_argument('-c', '--clear', action='store_true', help='Clear the "target/compile/" folder before compiling')
    compile_scope_group = compile_parser.add_mutually_exclusive_group(required=False)
    compile_scope_group.add_argument('--buildtime-only', action='store_true', help='Compile only buildtime models')
    compile_scope_group.add_argument('--runtime-only', action='store_true', help='Compile only runtime models')
    compile_test_set_group = compile_parser.add_mutually_exclusive_group(required=False)
    compile_test_set_group.add_argument('-t', '--test-set', type=str, help="The selection test set to use. If not specified, default selections are used (unless using --all-test-sets). Not applicable for buildtime models")
    compile_test_set_group.add_argument('-T', '--all-test-sets', action="store_true", help="Compile models for all selection test sets. Not applicable for buildtime models")
    compile_parser.add_argument('-s', '--select', type=str, help="Select single model to compile. If not specified, all models are compiled")
    compile_parser.add_argument('-r', '--runquery', action='store_true', help='Run runtime models and write CSV outputs too. Does not apply to buildtime models')
    
    build_parser = add_subparser(subparsers, c.BUILD_CMD, 'Build the Virtual Data Lake (VDL) for the project')
    build_parser.add_argument('-f', '--full-refresh', action='store_true', help='Drop all tables before building')
    build_parser.add_argument('-s', '--select', type=str, help="Select one static model to build. If not specified, all models are built")

    duckdb_parser = add_subparser(subparsers, c.DUCKDB_CMD, 'Run the duckdb command line tool')
    duckdb_parser.add_argument('--ui', action='store_true', help='Run the duckdb local UI')

    run_parser = add_subparser(subparsers, c.RUN_CMD, 'Run the API server')
    run_parser.add_argument('--build', action='store_true', help='Build the VDL first (without full refresh) before running the API server')
    run_parser.add_argument('--no-cache', action='store_true', help='Do not cache any api results')
    run_parser.add_argument('--host', type=str, default='127.0.0.1', help="The host to run on")
    run_parser.add_argument('--port', type=int, default=4465, help="The port to run on")

    args, _ = parser.parse_known_args()
    
    if args.version:
        print(__version__)
    elif args.command == c.NEW_CMD:
        Initializer(project_name=args.name, use_curr_dir=args.curr_dir).init_project(args)
    elif args.command == c.INIT_CMD:
        Initializer(project_name=None, use_curr_dir=True).init_project(args)
    elif args.command == c.GET_FILE_CMD:
        Initializer().get_file(args)
    elif args.command is None:
        print(f'Command is missing. Enter "squirrels -h" for help.')
    else:
        project = SquirrelsProject(
            load_dotenv_globally=True, 
            log_level=args.log_level, log_format=args.log_format, log_to_file=args.log_to_file
        )
        try:
            if args.command == c.DEPS_CMD:
                PackageLoaderIO.load_packages(project._logger, project._manifest_cfg, reload=True)
            elif args.command == c.BUILD_CMD:
                task = project.build(full_refresh=args.full_refresh, select=args.select)
                asyncio.run(task)
                print()
            elif args.command == c.DUCKDB_CMD:
                _run_duckdb_cli(project, args.ui)
            elif args.command == c.RUN_CMD:
                if args.build:
                    task = project.build(full_refresh=True)
                    asyncio.run(task)
                server = ApiServer(args.no_cache, project)
                server.run(args)
            elif args.command == c.COMPILE_CMD:
                # Derive final options with optional interactive prompts (unless --yes is provided)
                buildtime_only = args.buildtime_only
                runtime_only = args.runtime_only
                test_set = args.test_set
                do_all_test_sets = args.all_test_sets
                selected_model = args.select

                try:
                    if not args.yes:
                        buildtime_only, runtime_only, test_set, do_all_test_sets, selected_model = prompt_compile_options(
                            project,
                            buildtime_only=buildtime_only,
                            runtime_only=runtime_only,
                            test_set=test_set,
                            do_all_test_sets=do_all_test_sets,
                            selected_model=selected_model,
                        )

                    task = project.compile(
                        selected_model=selected_model, test_set=test_set, do_all_test_sets=do_all_test_sets, runquery=args.runquery,
                        clear=args.clear, buildtime_only=buildtime_only, runtime_only=runtime_only
                    )
                    asyncio.run(task)
                
                except KeyError:
                    pass
            else:
                print(f'Error: No such command "{args.command}". Enter "squirrels -h" for help.')

        except KeyboardInterrupt:
            pass
        except Exception as e:
            buffer = io.StringIO()
            traceback.print_exception(e, file=buffer)
            err_msg = buffer.getvalue()
            print(err_msg)
            project._logger.error(err_msg)
        finally:
            project.close()


if __name__ == '__main__':
    main()
