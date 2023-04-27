from typing import List, Tuple, Optional
from argparse import ArgumentParser
import sys, time, pwinput
sys.path.append('.')

from squirrels import constants as c, __version__
from squirrels import module_loader as ml, manifest as mf, credentials_manager as cm, connection_set as cs
from squirrels.api_server import ApiServer
from squirrels.renderer import RendererIOWrapper
from squirrels.initializer import Initializer
from squirrels.timed_imports import timer


def prompt_user_pw(args_values: Optional[List[str]]) -> Tuple[str, str]:
    if args_values is not None:
        user, pw = args_values
    else:
        user = input("Enter username: ")
        pw = pwinput.pwinput("Enter password: ")
    return user, pw


def main():
    start = time.time()
    parser = ArgumentParser(description="Command line utilities from the squirrels python package")
    parser.add_argument('-v', '--version', action='store_true', help='Show the version and exit')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose output')
    subparsers = parser.add_subparsers(title='commands', dest='command')

    init_parser = subparsers.add_parser(c.INIT_CMD, help='Initialize a squirrels project')
    init_parser.add_argument('--no-overwrite', action='store_true', help="Don't overwrite files if they already exist")
    init_parser.add_argument('--core', action='store_true', help='Include all core files')
    init_parser.add_argument('--context', action='store_true', help=f'Include the {c.CONTEXT_FILE} file')
    init_parser.add_argument('--selections-cfg', action='store_true', help=f'Include the {c.SELECTIONS_CFG_FILE} file')
    init_parser.add_argument('--db-view', type=str, choices=['sql', 'py'], help='Create database view as sql (default) or python file')
    init_parser.add_argument('--final-view', type=str, choices=['sql', 'py'], help='Include final view as sql or python file')
    init_parser.add_argument('--sample-db', type=str, choices=['seattle-weather'], help='Sample sqlite database to include')

    subparsers.add_parser(c.LOAD_MODULES_CMD, help='Load all the modules specified in squirrels.yaml from git')

    def _add_profile_argument(parser: ArgumentParser):
        parser.add_argument('key', type=str, help='Key to the database connection credential')

    subparsers.add_parser(c.GET_CREDS_CMD, help='Get all database connection credential keys')

    set_cred_parser = subparsers.add_parser(c.SET_CRED_CMD, help='Set a database connection credential key')
    _add_profile_argument(set_cred_parser)
    set_cred_parser.add_argument('--values', type=str, nargs=2, help='The username and password')

    delete_cred_parser = subparsers.add_parser(c.DELETE_CRED_CMD, help='Delete a database connection credential key')
    _add_profile_argument(delete_cred_parser)

    test_parser = subparsers.add_parser(c.TEST_CMD, help='For a given dataset, create outputs for parameter API response and rendered sql queries')
    test_parser.add_argument('dataset', type=str, help='Name of dataset (provided in squirrels.yaml) to test. Results are written in an "outputs" folder')
    test_parser.add_argument('-c', '--cfg', type=str, help="Configuration file for parameter selections. Path is relative to the dataset's folder")
    test_parser.add_argument('-d', '--data', type=str, help="Excel file with lookup data to avoid making a database connection. Path is relative to the dataset's folder")
    test_parser.add_argument('-r', '--runquery', action='store_true', help='Runs all database queries and final view, and produce the results as csv files')

    run_parser = subparsers.add_parser(c.RUN_CMD, help='Run the builtin API server')
    run_parser.add_argument('--no-cache', action='store_true', help='Do not cache any api results')
    run_parser.add_argument('--debug', action='store_true', help='In debug mode, all "hidden parameters" show in the parameters response')
    run_parser.add_argument('--host', type=str, default='127.0.0.1')
    run_parser.add_argument('--port', type=int, default=8000)
    timer.add_activity_time('parsing arguments', start)

    start = time.time()
    args, _ = parser.parse_known_args()
    timer.verbose = args.verbose

    if args.version:
        print(__version__)
    elif args.command == c.INIT_CMD:
        Initializer(not args.no_overwrite).init_project(args)
    elif args.command == c.LOAD_MODULES_CMD:
        manifest = mf.from_file()
        ml.load_modules(manifest)
    elif args.command == c.GET_CREDS_CMD: 
        cm.squirrels_config_io.print_all_credentials()
    elif args.command == c.SET_CRED_CMD:
        user, pw = prompt_user_pw(args.values)
        cm.squirrels_config_io.set_credential(args.key, user, pw)
    elif args.command == c.DELETE_CRED_CMD:
        cm.squirrels_config_io.delete_credential(args.key)
    elif args.command in [c.RUN_CMD, c.TEST_CMD]:
        manifest = mf.from_file()
        conn_set = cs.from_file(manifest)
        if args.command == c.RUN_CMD:
            server = ApiServer(manifest, conn_set, args.no_cache, args.debug)
            server.run(args)
        elif args.command == c.TEST_CMD:
            rendererIO = RendererIOWrapper(args.dataset, manifest, conn_set, args.data)
            rendererIO.write_outputs(args.cfg, args.runquery)
        conn_set.dispose()
    elif args.command is None:
        print(f'Command is missing. Enter "squirrels -h" for help.')
    else:
        print(f'Error: No such command "{args.command}". Enter "squirrels -h" for help.')
    
    timer.report_times()


if __name__ == '__main__':
    main()
