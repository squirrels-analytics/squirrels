import pandas as pd
import sqlite3, os

os.chdir(os.path.dirname(__file__))

df = pd.read_csv('seattle-weather.csv')

# Fact table
datetime_col = pd.to_datetime(df['date'])
df['year'] = datetime_col.dt.year
df['quarter'] = 'Q' + datetime_col.dt.quarter.astype(str)
df['month_order'] = datetime_col.dt.month
df['month_name'] = datetime_col.dt.month_name()
df['day_of_year'] = datetime_col.dt.day_of_year

df['start_of_year'] = datetime_col.apply(lambda x: pd.Timestamp(year=x.year, month=1, day=1)).dt.strftime('%Y-%m-%d')
df['start_of_quarter'] = datetime_col.apply(lambda x: pd.Timestamp(year=x.year, month=(x.month-1)//3*3+1, day=1)).dt.strftime('%Y-%m-%d')
df['start_of_month'] = datetime_col.apply(lambda x: pd.Timestamp(year=x.year, month=x.month, day=1)).dt.strftime('%Y-%m-%d')
df['start_of_week'] = datetime_col.apply(lambda x: x - pd.Timedelta(days=(x.dayofweek+1)%7)).dt.strftime('%Y-%m-%d')

# Dim table
start_of_year = df['start_of_year'].unique()
start_of_quarter = df['start_of_quarter'].unique()
start_of_month = df['start_of_month'].unique()
lookup_df = pd.DataFrame({
    'start_of_time': list(start_of_year) + list(start_of_quarter) + list(start_of_month),
    'time_type_id': ([0] * len(start_of_year)) + 
                      ([1] * len(start_of_quarter)) + 
                      ([2] * len(start_of_month)),
})

time_type_df = pd.DataFrame({
    'index': [0, 1, 2],
    'value': ['Year', 'Quarter', 'Month'],
    'column': ['start_of_year', 'start_of_quarter', 'start_of_month']
})


# Connect to SQLite database
conn = sqlite3.connect('seattle_weather.db')
try:
    df.to_sql('weather', conn, index=False, if_exists='replace')
    lookup_df.to_sql('time_lookup', conn, if_exists='replace')
    time_type_df.to_sql('time_type', conn, index=False, if_exists='replace')
finally:
    conn.close()
