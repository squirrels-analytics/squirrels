from argparse import ArgumentParser
import sys, time, asyncio
sys.path.append('.')

from . import _constants as c
from ._timer import timer


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
    init_parser.add_argument('-o', '--overwrite', action='store_true', help="Overwrite files that already exist")
    init_parser.add_argument('--core', action='store_true', help='Include all core files')
    init_parser.add_argument('--connections', type=str, choices=c.CONF_FORMAT_CHOICES, help=f'Configure database connections as yaml (default) or python. Ignored if "--core" is not specified')
    init_parser.add_argument('--parameters', type=str, choices=c.CONF_FORMAT_CHOICES, help=f'Configure parameters as python (default) or yaml. Ignored if "--core" is not specified')
    init_parser.add_argument('--dbview', type=str, choices=c.FILE_TYPE_CHOICES, help='Create database view model as sql (default) or python file. Ignored if "--core" is not specified')
    init_parser.add_argument('--federate', type=str, choices=c.FILE_TYPE_CHOICES, help='Create federated model as sql (default) or python file. Ignored if "--core" is not specified')
    init_parser.add_argument('--auth', action='store_true', help=f'Include the {c.AUTH_FILE} file')
    init_parser.add_argument('--sample-db', type=str, choices=c.DATABASE_CHOICES, help='Sample sqlite database to include')

    module_parser = subparsers.add_parser(c.DEPS_CMD, help=f'Load all packages specified in {c.MANIFEST_FILE} (from git)', add_help=False)
    module_parser.add_argument('-h', '--help', action="help", help="Show this help message and exit")

    compile_parser = subparsers.add_parser(c.COMPILE_CMD, help='Create files for rendered sql queries in the "target/compile" folder', add_help=False)
    compile_parser.add_argument('-h', '--help', action="help", help="Show this help message and exit")
    compile_parser.add_argument('-d', '--dataset', type=str, help="Select dataset to use for dataset traits. If not specified, all models for all datasets are compiled")
    compile_parser.add_argument('-a', '--all-test-sets', action="store_true", help="Compile models for all selection test sets")
    compile_parser.add_argument('-t', '--test-set', type=str, help="The selection test set to use. Default selections are used if not specified. Ignored if using --all-test-sets")
    compile_parser.add_argument('-s', '--select', type=str, help="Select single model to compile. If not specified, all models for the dataset are compiled. Also, ignored if --dataset is not specified")
    compile_parser.add_argument('-r', '--runquery', action='store_true', help='Runs all target models, and produce the results as csv files')

    run_parser = subparsers.add_parser(c.RUN_CMD, help='Run the builtin API server', add_help=False)
    run_parser.add_argument('-h', '--help', action="help", help="Show this help message and exit")
    run_parser.add_argument('--no-cache', action='store_true', help='Do not cache any api results')
    run_parser.add_argument('--debug', action='store_true', help='Show all "hidden parameters" in the parameters response')
    run_parser.add_argument('--host', type=str, default='127.0.0.1', help="The host to run on")
    run_parser.add_argument('--port', type=int, default=4465, help="The port to run on")

    args, _ = parser.parse_known_args()
    timer.verbose = args.verbose
    timer.add_activity_time('parsing arguments', start)
    
    from . import __version__
    from ._api_server import ApiServer
    from ._models import ModelsIO
    from ._initializer import Initializer
    from ._manifest import ManifestIO
    from ._package_loader import PackageLoaderIO
    from ._connection_set import ConnectionSetIO
    from ._parameter_sets import ParameterConfigsSetIO

    if args.version:
        print(__version__)
    elif args.command == c.INIT_CMD:
        Initializer(args.overwrite).init_project(args)
    elif args.command == c.DEPS_CMD:
        ManifestIO.LoadFromFile()
        PackageLoaderIO.LoadPackages(reload=True)
    elif args.command in [c.RUN_CMD, c.COMPILE_CMD]:
        ManifestIO.LoadFromFile()
        ConnectionSetIO.LoadFromFile()
        try:
            ParameterConfigsSetIO.LoadFromFile()
            ModelsIO.LoadFiles()
            
            if args.command == c.RUN_CMD:
                server = ApiServer(args.no_cache, args.debug)
                server.run(args)
                pass
            elif args.command == c.COMPILE_CMD:
                task = ModelsIO.WriteOutputs(args.dataset, args.select, args.all_test_sets, args.test_set, args.runquery)
                asyncio.run(task)
        finally:
            ConnectionSetIO.Dispose()
    elif args.command is None:
        print(f'Command is missing. Enter "squirrels -h" for help.')
    else:
        print(f'Error: No such command "{args.command}". Enter "squirrels -h" for help.')


if __name__ == '__main__':
    main()
