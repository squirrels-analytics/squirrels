from faker import Faker
import sqlite3, polars as pl, tqdm, numpy as np

fake = Faker()

# Define the number of transactions to generate
num_transactions = 1_000
batches = 100
batch_size = num_transactions // batches

# Generate the data
descriptions_df = pl.DataFrame({
    'description': [fake.sentence() for _ in range(10**4)]
})

df_list: list[pl.DataFrame] = []
rng = np.random.default_rng()
for _ in tqdm.tqdm(range(batches)):
    df_current = descriptions_df.sample(batch_size, with_replacement=True, shuffle=True)
    df_current = df_current.with_columns(
        pl.lit(rng.integers(
            np.datetime64('2024-01-01').astype(int), 
            np.datetime64('2025-01-01').astype(int), 
            size=batch_size
        ).astype('datetime64[D]')).alias('date'),
        pl.lit(rng.integers(0, 14, size=batch_size)).alias('subcategory_id'),
        pl.lit(rng.exponential(30, size=batch_size).round(2)).alias('amount'),
    )
    df_list.append(df_current)

df = pl.concat(df_list).select('date', 'subcategory_id', 'amount', 'description')
df = df.sort('date').with_row_index(name='id', offset=1)

# Connect to SQLite database
conn = sqlite3.connect('expenses.db')

try:
    # Create the expenses table
    conn.execute("DROP TABLE IF EXISTS expenses")
    conn.execute('''
    CREATE TABLE IF NOT EXISTS expenses (
        id INTEGER PRIMARY KEY,
        date DATE,
        subcategory_id INTEGER,
        amount DECIMAL(10,2),
        description TEXT
    )
    ''')

    # Convert DataFrame to records and insert into database
    records = df.to_numpy().tolist()
    conn.executemany(
        'INSERT INTO expenses (id, date, subcategory_id, amount, description) VALUES (?, ?, ?, ?, ?)',
        records
    )

    # Commit changes and close connection
    conn.commit()
finally:
    conn.close()
