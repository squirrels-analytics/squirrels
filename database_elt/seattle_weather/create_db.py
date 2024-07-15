import pandas as pd
import sqlite3, os

os.chdir(os.path.dirname(__file__))

df = pd.read_csv('seattle-weather.csv')

# Connect to SQLite database
conn = sqlite3.connect('weather.db')
try:
    df.to_sql('weather', conn, index=False, if_exists='replace')
finally:
    conn.close()
