import time

class InvalidInputError(Exception):
    pass


class Timer:
    def __init__(self):
        self.times = {}
    
    def add_activity_time(self, activity, my_start):
        self.times[activity] = self.times.get(activity, 0) + (time.time()-my_start) * 10**3
    
    def report_times(self, verbose):
        if verbose:
            for activity, time in self.times.items():
                print(f'The time of execution of "{activity}" is:', time, "ms")

timer = Timer()
