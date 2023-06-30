import sqlite3, pandas as pd

df = pd.read_csv("sample-transactions.csv")

cat_cols = ["Category", "Subcategory"]
df_cat = df[df.Category != 'Income'][cat_cols].drop_duplicates(subset=cat_cols)
df_cat['Category_ID'] = pd.factorize(df_cat['Category'])[0]
df_cat['Subcategory_ID'] = pd.factorize(df_cat['Subcategory'])[0]

# Connect to SQLite database
conn = sqlite3.connect('sample_database.db')
try:
    df.to_sql('transactions', conn, index=False, if_exists='replace')
    df_cat.to_sql('categories', conn, index=False, if_exists='replace')
finally:
    conn.close()
