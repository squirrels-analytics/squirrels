from datetime import datetime
import time


class Timer:
    def __init__(self):
        # self.times: dict[str, list[float]] = dict()
        self.verbose = False
    
    def _get_dt_from_timestamp(self, timestamp) -> str:
        return datetime.fromtimestamp(timestamp).strftime('%H:%M:%S.%f')
    
    def add_activity_time(self, activity: str, start_timestamp: float) -> None:
        if self.verbose:
            end_timestamp = time.time()
            time_taken = round((end_timestamp-start_timestamp) * 10**3, 3)
            # times_list = self.times.setdefault(activity, list())
            # times_list.append(time_taken)
            print(f'Time taken for "{activity}": {time_taken}ms')

            start_datetime = self._get_dt_from_timestamp(start_timestamp)
            end_datetime = self._get_dt_from_timestamp(end_timestamp)
            print(f'--> start time: "{start_datetime}", end time: "{end_datetime}"')
            print()
    
    # def report_times(self):
    #     if self.verbose:
    #         for activity, times_list in self.times.items():
    #             total_time = sum(times_list)
    #             avg_time = total_time / len(times_list)
    #             print()
    #             print(f'Time statistics for "{activity}":')
    #             print(f'  Total time: {total_time}ms')
    #             print(f'  Average time: {avg_time}ms')

timer = Timer()
