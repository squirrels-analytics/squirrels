from argparse import ArgumentParser, _SubParsersAction
import sys, asyncio, traceback, io

sys.path.append('.')

from ._version import __version__
from ._api_server import ApiServer
from ._initializer import Initializer
from ._package_loader import PackageLoaderIO
from .project import SquirrelsProject
from . import _constants as c


def main():
    """
    Main entry point for the squirrels command line utilities.
    """
    def with_help(parser: ArgumentParser):
        parser.add_argument('-h', '--help', action="help", help="Show this help message and exit")
        return parser

    parser = with_help(ArgumentParser(description="Command line utilities from the squirrels python package", add_help=False))
    parser.add_argument('-V', '--version', action='store_true', help='Show the version and exit')
    parser.add_argument('--log-level', type=str, choices=["DEBUG", "INFO", "WARNING"], default="INFO", help='Level of logging to use')
    parser.add_argument('--log-format', type=str, choices=["text", "json"], default="text", help='Format of the log records')
    parser.add_argument('--log-file', type=str, default=c.LOGS_FILE, help=f'Name of log file to write to in the "logs/" folder. Default is {c.LOGS_FILE}. If name is empty, then file logging is disabled')
    subparsers = parser.add_subparsers(title='commands', dest='command')

    def add_subparser(subparsers: _SubParsersAction, cmd: str, help_text: str):
        subparser = with_help(subparsers.add_parser(cmd, description=help_text, help=help_text, add_help=False))
        return subparser

    init_parser = add_subparser(subparsers, c.INIT_CMD, 'Initialize a squirrels project')
    init_parser.add_argument('-o', '--overwrite', action='store_true', help="Overwrite files that already exist")
    init_parser.add_argument('--core', action='store_true', help='Include all core files')
    init_parser.add_argument('--connections', type=str, choices=c.CONF_FORMAT_CHOICES, help=f'Configure database connections as yaml (default) or python')
    init_parser.add_argument('--parameters', type=str, choices=c.CONF_FORMAT_CHOICES, help=f'Configure parameters as python (default) or yaml')
    init_parser.add_argument('--dbview', type=str, choices=c.FILE_TYPE_CHOICES, help='Create database view model as sql (default) or python file')
    init_parser.add_argument('--federate', type=str, choices=c.FILE_TYPE_CHOICES, help='Create federated model as sql (default) or python file')
    init_parser.add_argument('--dashboard', action='store_true', help=f'Include a sample dashboard file')
    init_parser.add_argument('--auth', action='store_true', help=f'Include the {c.AUTH_FILE} file')

    def with_file_format_options(parser: ArgumentParser):
        help_text = "Create model as sql (default) or python file"
        parser.add_argument('--format', type=str, choices=c.FILE_TYPE_CHOICES, default=c.SQL_FILE_TYPE, help=help_text)
        return parser
    
    get_file_help_text = "Get a sample file for the squirrels project. If the file name already exists, it will be prefixed with a timestamp."
    get_file_parser = add_subparser(subparsers, c.GET_FILE_CMD, get_file_help_text)
    get_file_subparsers = get_file_parser.add_subparsers(title='file_name', dest='file_name')
    add_subparser(get_file_subparsers, c.ENV_CONFIG_FILE, f'Get a sample {c.ENV_CONFIG_FILE} file')
    manifest_parser = add_subparser(get_file_subparsers, c.MANIFEST_FILE, f'Get a sample {c.MANIFEST_FILE} file')
    manifest_parser.add_argument("--no-connections", action='store_true', help=f'Exclude the connections section')
    manifest_parser.add_argument("--parameters", action='store_true', help=f'Include the parameters section')
    manifest_parser.add_argument("--dashboards", action='store_true', help=f'Include the dashboards section')
    add_subparser(get_file_subparsers, c.AUTH_FILE, f'Get a sample {c.AUTH_FILE} file')
    add_subparser(get_file_subparsers, c.CONNECTIONS_FILE, f'Get a sample {c.CONNECTIONS_FILE} file')
    add_subparser(get_file_subparsers, c.PARAMETERS_FILE, f'Get a sample {c.PARAMETERS_FILE} file')
    add_subparser(get_file_subparsers, c.CONTEXT_FILE, f'Get a sample {c.CONTEXT_FILE} file')
    with_file_format_options(add_subparser(get_file_subparsers, c.DBVIEW_FILE_STEM, f'Get a sample dbview model file'))
    with_file_format_options(add_subparser(get_file_subparsers, c.FEDERATE_FILE_STEM, f'Get a sample federate model file'))
    add_subparser(get_file_subparsers, c.DASHBOARD_FILE_STEM, f'Get a sample dashboard file')
    add_subparser(get_file_subparsers, c.EXPENSES_DB, f'Get the sample SQLite database on expenses')
    add_subparser(get_file_subparsers, c.WEATHER_DB, f'Get the sample SQLite database on weather')
    
    add_subparser(subparsers, c.DEPS_CMD, f'Load all packages specified in {c.MANIFEST_FILE} (from git)')

    compile_parser = add_subparser(subparsers, c.COMPILE_CMD, 'Create rendered SQL files in the folder "./target/compile"')
    compile_dataset_group = compile_parser.add_mutually_exclusive_group(required=True)
    compile_dataset_group.add_argument('-d', '--dataset', type=str, help="Select dataset to use for dataset traits. Is required, unless using --all-datasets")
    compile_dataset_group.add_argument('-D', '--all-datasets', action="store_true", help="Compile models for all datasets. Only required if --dataset is not specified")
    compile_test_set_group = compile_parser.add_mutually_exclusive_group(required=False)
    compile_test_set_group.add_argument('-t', '--test-set', type=str, help="The selection test set to use. If not specified, default selections are used, unless using --all-test-sets")
    compile_test_set_group.add_argument('-T', '--all-test-sets', action="store_true", help="Compile models for all selection test sets")
    compile_parser.add_argument('-s', '--select', type=str, help="Select single model to compile. If not specified, all models for the dataset are compiled. Ignored if using --all-datasets")
    compile_parser.add_argument('-r', '--runquery', action='store_true', help='Runs all target models, and produce the results as csv files')

    run_parser = add_subparser(subparsers, c.RUN_CMD, 'Run the API server')
    run_parser.add_argument('--no-cache', action='store_true', help='Do not cache any api results')
    run_parser.add_argument('--host', type=str, default='127.0.0.1', help="The host to run on")
    run_parser.add_argument('--port', type=int, default=4465, help="The port to run on")

    args, _ = parser.parse_known_args()
    project = SquirrelsProject(log_level=args.log_level, log_format=args.log_format, log_file=args.log_file)
    
    if args.version:
        print(__version__)
    elif args.command == c.INIT_CMD:
        Initializer(overwrite=args.overwrite).init_project(args)
    elif args.command == c.GET_FILE_CMD:
        Initializer().get_file(args)
    elif args.command == c.DEPS_CMD:
        PackageLoaderIO.load_packages(project._logger, project._manifest_cfg, reload=True)
    elif args.command in [c.RUN_CMD, c.COMPILE_CMD]:
        try:
            if args.command == c.RUN_CMD:
                server = ApiServer(args.no_cache, project)
                server.run(args)
            elif args.command == c.COMPILE_CMD:
                task = project.compile(
                    dataset=args.dataset, do_all_datasets=args.all_datasets, selected_model=args.select, test_set=args.test_set, 
                    do_all_test_sets=args.all_test_sets, runquery=args.runquery
                )
                asyncio.run(task)
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
    elif args.command is None:
        print(f'Command is missing. Enter "squirrels -h" for help.')
    else:
        print(f'Error: No such command "{args.command}". Enter "squirrels -h" for help.')


if __name__ == '__main__':
    main()
