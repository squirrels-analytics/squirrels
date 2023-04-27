from typing import Dict, List
import time

from squirrels import constants as c


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

timer = Timer()


start = time.time()
import pandas
timer.add_activity_time(c.IMPORT_PANDAS, start)

start = time.time()
import jinja2
timer.add_activity_time(c.IMPORT_JINJA, start)
