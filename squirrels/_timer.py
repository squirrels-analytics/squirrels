import time


class Timer:
    def __init__(self, verbose: bool = False):
        self.times: dict[str, list[float]] = dict()
        self.verbose = verbose
    
    def add_activity_time(self, activity: str, start: float):
        if self.verbose:
            time_taken = (time.time()-start) * 10**3
            times_list = self.times.setdefault(activity, list())
            times_list.append(time_taken)
            print(f'Time taken for "{activity}": {time_taken}ms')
    
    def report_times(self):
        if self.verbose:
            for activity, times_list in self.times.items():
                total_time = sum(times_list)
                avg_time = total_time / len(times_list)
                print()
                print(f'Time statistics for "{activity}":')
                print(f'  Total time: {total_time}ms')
                print(f'  Average time: {avg_time}ms')

timer = Timer(True)
