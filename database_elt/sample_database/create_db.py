import sqlite3, pandas as pd

df = pd.DataFrame({
    "dim1": ["a", "a", "b", "b", "b"],
    "metric1": [2.5, 7.5, 3.8, 5.1, 8.6],
    "metric2": [2, 8, 4, 6, 10]
})

# Connect to SQLite database
conn = sqlite3.connect('sample_database.db')
try:
    df.to_sql('fact_table', conn, index=False, if_exists='replace')
finally:
    conn.close()
