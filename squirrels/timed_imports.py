import time

from squirrels import utils, constants as c

start = time.time()
import pandas
from pandasql import sqldf
utils.timer.add_activity_time(c.IMPORT_PANDAS, start)

start = time.time()
import jinja2
utils.timer.add_activity_time(c.IMPORT_JINJA, start)
