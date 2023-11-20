from argparse import ArgumentParser
import sys
sys.path.append('.')

from . import _constants as c
from ._version import __version__
from ._api_server import ApiServer
from ._renderer import RendererIOWrapper
from ._initializer import Initializer
from ._timed_imports import timer, time
from ._manifest import ManifestIO
from ._module_loader import ModuleLoaderIO
from ._environcfg import EnvironConfigIO
from ._connection_set_io import ConnectionSetIO
from ._parameter_sets import ParameterConfigsSetIO


def main():
    """
    Main entry point for the squirrels command line utilities.
    """
    start = time.time()
    parser = ArgumentParser(description="Command line utilities from the squirrels python package", add_help=False)
    parser.add_argument('-h', '--help', action="help", help="Show this help message and exit")
    parser.add_argument('-V', '--version', action='store_true', help='Show the version and exit')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose output')
    subparsers = parser.add_subparsers(title='commands', dest='command')

    init_parser = subparsers.add_parser(c.INIT_CMD, help='Initialize a squirrels project', add_help=False)
    init_parser.add_argument('-h', '--help', action="help", help="Show this help message and exit")
    init_parser.add_argument('--overwrite', action='store_true', help="Overwrite files that already exist")
    init_parser.add_argument('--core', action='store_true', help='Include all core files (squirrels.yaml, environcfg.yaml, parameters.py, database view, etc.)')
    init_parser.add_argument('--db-view', type=str, choices=c.FILE_TYPE_CHOICES, help='Create database view as sql (default) or python file. Ignored if "--core" is not specified')
    init_parser.add_argument('--connections', action='store_true', help=f'Include the {c.CONNECTIONS_FILE} file')
    init_parser.add_argument('--context', action='store_true', help=f'Include the {c.CONTEXT_FILE} file')
    init_parser.add_argument('--final-view', type=str, choices=c.FILE_TYPE_CHOICES, help='Include final view as sql or python file')
    init_parser.add_argument('--auth-file', action='store_true', help=f'Include the {c.AUTH_FILE} file')
    init_parser.add_argument('--selections-cfg', action='store_true', help=f'Include the {c.SELECTIONS_CFG_FILE} and {c.LU_DATA_FILE} files')
    init_parser.add_argument('--sample-db', type=str, choices=c.DATABASE_CHOICES, help='Sample sqlite database to include')

    module_parser = subparsers.add_parser(c.LOAD_MODULES_CMD, help='Load all modules in squirrels.yaml from git', add_help=False)
    module_parser.add_argument('-h', '--help', action="help", help="Show this help message and exit")

    test_parser = subparsers.add_parser(c.TEST_CMD, help='Create output files for rendered sql queries', add_help=False)
    test_parser.add_argument('-h', '--help', action="help", help="Show this help message and exit")
    test_parser.add_argument('dataset', type=str, help='Name of dataset (provided in squirrels.yaml) to test. Results are written in an "outputs" folder')
    test_parser.add_argument('-c', '--cfg', type=str, help="Configuration file for parameter selections. Path is relative to the dataset's folder")
    test_parser.add_argument('-d', '--data', type=str, help="Excel file with lookup data to avoid making a database connection. Path is relative to project root")
    test_parser.add_argument('-r', '--runquery', action='store_true', help='Runs all database queries and final view, and produce the results as csv files')

    run_parser = subparsers.add_parser(c.RUN_CMD, help='Run the builtin API server', add_help=False)
    run_parser.add_argument('-h', '--help', action="help", help="Show this help message and exit")
    run_parser.add_argument('--no-cache', action='store_true', help='Do not cache any api results')
    run_parser.add_argument('--debug', action='store_true', help='Show all "hidden parameters" in the parameters response')
    run_parser.add_argument('--host', type=str, help="The host to run on", default='127.0.0.1')
    run_parser.add_argument('--port', type=int, help="The port to run on", default=4465)

    args, _ = parser.parse_known_args()
    timer.verbose = args.verbose
    timer.add_activity_time('parsing arguments', start)

    if args.version:
        print(__version__)
    elif args.command == c.INIT_CMD:
        Initializer(args.overwrite).init_project(args)
    elif args.command == c.LOAD_MODULES_CMD:
        ManifestIO.LoadFromFile()
        ModuleLoaderIO.LoadModules()
    elif args.command in [c.RUN_CMD, c.TEST_CMD]:
        ManifestIO.LoadFromFile()
        ConnectionSetIO.LoadFromFile()
        
        excel_name = args.data if args.command == c.TEST_CMD else None
        ParameterConfigsSetIO.LoadFromFile(excel_file_name=excel_name)
        
        if args.command == c.RUN_CMD:
            server = ApiServer(args.no_cache, args.debug)
            server.run(args)
        elif args.command == c.TEST_CMD:
            rendererIO = RendererIOWrapper(args.dataset)
            rendererIO.write_outputs(args.cfg, args.runquery)
        
        ConnectionSetIO.Dispose()
    elif args.command is None:
        print(f'Command is missing. Enter "squirrels -h" for help.')
    else:
        print(f'Error: No such command "{args.command}". Enter "squirrels -h" for help.')
    
    timer.report_times()


if __name__ == '__main__':
    main()
