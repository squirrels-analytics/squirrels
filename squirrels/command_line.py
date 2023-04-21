import sys, time
sys.path.append('.')

import pwinput
from squirrels import constants as c, profile_manager as pm, __version__
from squirrels import module_loader as ml, api_server
from squirrels.renderer import Renderer
from squirrels.initializer import Initializer
from squirrels.utils import timer
from argparse import ArgumentParser


def get_profiles():
    for key, value in pm.get_profiles().items():
        print(key + ": " + str(value))


def set_profile(args):
    profile = pm.Profile(args.name)
    if args.values:
        dialect, url, user, pw = args.values
    else:
        print("-- suggested PyPI packages for various sql drivers can be found here: " +
              "https://superset.apache.org/docs/databases/installing-database-drivers/")

        dialect = input("Enter sql dialect + driver: ")
        url = input("Enter connection URL [host:port/database]: ")
        user = input("Enter username: ")
        pw = pwinput.pwinput("Enter password: ")
    
    profile.set(dialect, url, user, pw)
    
    print(f"\nProfile '{args.name}' has been set with following values:")
    print(profile.get())


def delete_profile(args):
    profile_existed = pm.Profile(args.name).delete()
    if profile_existed:
        print(f"Profile '{args.name}' was deleted")
    else:
        print(f"Profile '{args.name}' does not exist")


def main():
    start = time.time()
    parser = ArgumentParser(description="Command line utilities from the squirrels python package")
    parser.add_argument('-V', '--version', action='store_true', help='Show the version and exit')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose output')
    subparsers = parser.add_subparsers(title='commands', dest='command')

    def _add_profile_argument(parser):
        parser.add_argument('name', type=str, help='Name of the database connection profile (provided in squirrels.yaml)')

    init_parser = subparsers.add_parser(c.INIT_CMD, help='Initialize a squirrels project')
    init_parser.add_argument('--overwrite', action='store_true', help='Overwrite files if already exist')
    init_parser.add_argument('--core', action='store_true', help='Include all core files')
    init_parser.add_argument('--context', action='store_true', help=f'Include the {c.CONTEXT_FILE} file')
    init_parser.add_argument('--db-view', type=str, choices=['sql', 'py'], help='Create database view as sql (default) or python file')
    init_parser.add_argument('--final-view', type=str, choices=['sql', 'py'], help='Include final view as sql or python file')
    init_parser.add_argument('--sample-db', type=str, choices=['seattle-weather'], help='Sample sqlite database to include')

    subparsers.add_parser(c.LOAD_MODULES_CMD, help='Load all the modules specified in squirrels.yaml from git')

    subparsers.add_parser(c.GET_PROFILES_CMD, help='Get all database connection profile names and values')

    set_profile_parser = subparsers.add_parser(c.SET_PROFILE_CMD, help='Set a database connection profile')
    _add_profile_argument(set_profile_parser)
    set_profile_parser.add_argument('--values', type=str, nargs=4, help='The sql dialect, connection url, username, and password')

    delete_profile_parser = subparsers.add_parser(c.DELETE_PROFILE_CMD, help='Delete a database connection profile')
    _add_profile_argument(delete_profile_parser)

    test_parser = subparsers.add_parser(c.TEST_CMD, help='For a given dataset, create outputs for parameter API response and rendered sql queries')
    test_parser.add_argument('dataset', type=str, help='Name of dataset (provided in squirrels.yaml) to test. Results are written in an "outputs" folder')
    test_parser.add_argument('-c', '--cfg', type=str, help='Configuration file for parameter selections. Path is relative to a specific dataset folder')
    test_parser.add_argument('-d', '--data', type=str, help='Sample lookup data to avoid making a database connection. Path is relative to a specific dataset folder')
    test_parser.add_argument('-r', '--runquery', action='store_true', help='Runs all database queries and final view, and produce the results as csv files')

    run_parser = subparsers.add_parser(c.RUN_CMD, help='Run the builtin API server')
    run_parser.add_argument('--no-cache', action='store_true', help='Do not cache any api results')
    run_parser.add_argument('--debug', action='store_true', help='In debug mode, all "hidden parameters" show in parameters response')
    run_parser.add_argument('--host', type=str, default='127.0.0.1')
    run_parser.add_argument('--port', type=int, default=8000)
    timer.add_activity_time('creating argparser', start)

    start = time.time()
    args, _ = parser.parse_known_args()
    if args.version:
        print(__version__)
    elif args.command == c.GET_PROFILES_CMD:
        get_profiles()
    elif args.command == c.SET_PROFILE_CMD:
        set_profile(args)
    elif args.command == c.DELETE_PROFILE_CMD:
        delete_profile(args)
    elif args.command == c.TEST_CMD:
        Renderer(args.dataset, args.cfg, args.data).write_outputs(args.runquery)
        timer.add_activity_time('all of write_outputs', start)
    elif args.command == c.RUN_CMD:
        api_server.run(args.no_cache, args.debug, args)
    elif args.command == c.LOAD_MODULES_CMD:
        ml.load_modules()
    elif args.command == c.INIT_CMD:
        Initializer(args.overwrite).init_project(args)
    elif args.command is None:
        print(f'Error: Missing command. Enter "squirrels -h" for help.')
    else:
        print(f'Error: No such command "{args.command}". Enter "squirrels -h" for help.')
    
    timer.report_times(args.verbose)


if __name__ == '__main__':
    main()
