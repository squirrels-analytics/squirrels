from typing import Dict, List, Optional, Union
from types import ModuleType
from pathlib import Path
from importlib.machinery import SourceFileLoader
import time, jinja2 as j2

FilePath = Union[str, Path]


# Custom Exceptions
class InvalidInputError(Exception):
    pass

class ConfigurationError(Exception):
    pass


# Custom Classes for utilities
class Timer:
    def __init__(self, verbose = True, limit = 1e7):
        self.verbose = verbose
        self.limit = limit
        self.times: Dict[str, List[float]] = dict()
        self.count = 0
    
    def add_activity_time(self, activity, my_start):
        if not self.verbose or self.count >= self.limit:
            return
        if activity not in self.times:
            self.times[activity] = list()
        self.times[activity].append((time.time()-my_start) * 10**3)
        self.count += 1
    
    def report_times(self):
        if self.verbose:
            for activity, time_list in self.times.items():
                total_time = sum(time_list)
                avg_time = total_time / len(time_list)
                print()
                print(f'Time statistics for "{activity}":')
                print(f'  Total time: {total_time}ms')
                print(f'  Average time: {avg_time}ms')
                print(f'  All times: {time_list}')


# Utility functions/variables
timer = Timer()

j2_env = j2.Environment(loader=j2.FileSystemLoader('.'))

def import_file_as_module(filepath: Optional[FilePath]) -> ModuleType:
    filepath = str(filepath) if filepath is not None else None
    return SourceFileLoader(filepath, filepath).load_module() if filepath is not None else None

def join_paths(*paths: FilePath) -> Path:
    return Path(*paths)

def normalize_name(name: str):
    return name.replace('-', '_')

def normalize_name_for_api(name: str):
    return name.replace('_', '-')
