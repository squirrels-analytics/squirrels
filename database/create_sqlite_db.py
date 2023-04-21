import csv
import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect('data/test.db')
c = conn.cursor()

# Create the table
STOCK_DATA_TABLE = 'stock_data'
c.execute(f'DROP TABLE IF EXISTS {STOCK_DATA_TABLE}')
c.execute(f'''
CREATE TABLE {STOCK_DATA_TABLE}(
    ticker TEXT,
    trading_date TEXT,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    adj_close REAL,
    volume REAL,
    week_value TEXT,
    month_value TEXT,
    quarter_value TEXT,
    year_value INTEGER,
    daily_return REAL,
    PRIMARY KEY (ticker, trading_date)
)
''')

LU_TICKERS_TABLE = 'lu_tickers'
c.execute(f'DROP TABLE IF EXISTS {LU_TICKERS_TABLE}')
c.execute(f'''
CREATE TABLE {LU_TICKERS_TABLE}(
    ticker_id INTEGER,
    ticker TEXT,
    ticker_order INTEGER
)
''')

# Read the CSV file and insert the data into the table
def load_table_from_csv(csv_file: str, table_name: str):
    base_path = 'data/csv_files/'
    with open(base_path + csv_file, 'r') as f:
        reader = csv.reader(f)
        next(reader) # skip header
        q_marks = None
        for row in reader:
            if q_marks is None:
                q_marks = ', '.join(['?']*len(row))
            c.execute(f"INSERT INTO {table_name} VALUES ({q_marks})", row)

load_table_from_csv(LU_TICKERS_TABLE + '.csv', LU_TICKERS_TABLE)
load_table_from_csv(STOCK_DATA_TABLE + '.csv', STOCK_DATA_TABLE)

# Commit the changes and close the connection
conn.commit()
conn.close()
