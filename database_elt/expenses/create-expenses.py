import sqlite3, random, polars as pl, tqdm, numpy as np

def generate_expense_description(vendors=None, items=None, adjectives=None):
    """
    Generates a random expense description.

    Args:
        categories: A list of expense categories (e.g., ["Groceries", "Dining", "Travel", "Utilities"]).
        vendors: A list of vendor names (e.g., ["Walmart", "Starbucks", "Amazon", "Gas Station"]).
        items: A list of items purchased (e.g., ["Milk", "Coffee", "Laptop", "Gas"]).
        adjectives: A list of adjectives to add variety(e.g., ["Monthly", "Quick", "Online", "Unexpected"]).

    Returns:
        A randomly generated expense description string.
    """

    if vendors is None:
        vendors = ["Vendor A", "Vendor B", "Vendor C", "Vendor D", "Vendor E", "Online Store", "Local Shop", "Restaurant"]
    if items is None:
        items = ["Item 1", "Item 2", "Item 3", "Item 4", "Item 5", "Service", "Subscription", "Purchase", "Merchandise", "Goods"]
    if adjectives is None:
        adjectives = ["Daily", "Monthly", "Quick", "Online", "Unexpected", "Recurring", "Personal", "Business"]

    description_parts = []

    # Choose a category, vendor, and item
    adjective = random.choice(adjectives)
    vendor = random.choice(vendors)
    item = random.choice(items)

    # Build the description
    description_parts.append(f"{adjective} {item} - {vendor}")

    return " ".join(description_parts).strip()

# Define the number of transactions to generate
num_transactions = 1_000
batches = 100
batch_size = num_transactions // batches

# Generate the data
descriptions_df = pl.DataFrame({
    'description': [generate_expense_description() for _ in range(10**4)]
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
