from typing import Dict, Tuple
import time


class Timer:
    def __init__(self, verbose: bool = False):
        self.times: Dict[str, Tuple[str, str]] = dict()
        self.verbose = verbose
    
    def add_activity_time(self, activity: str, start: float):
        time_taken = (time.time()-start) * 10**3
        total_time, count = self.times.get(activity, (0, 0))
        self.times[activity] = (total_time+time_taken, count+1)
        if self.verbose:
            print(f'Time taken for "{activity}": {time_taken}ms')
    
    def report_times(self):
        if self.verbose:
            for activity, time_stats in self.times.items():
                total_time, count = time_stats
                avg_time = total_time / count
                print()
                print(f'Time statistics for "{activity}":')
                print(f'  Total time: {total_time}ms')
                print(f'  Average time: {avg_time}ms')

timer = Timer()


start = time.time()
import pandas
timer.add_activity_time("import pandas", start)

start = time.time()
import jinja2
timer.add_activity_time("import jinja", start)
