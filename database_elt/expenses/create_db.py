import sqlite3, csv, pandas as pd

df = pd.read_csv("sample-transactions.csv")

cat_cols = ["category", "subcategory"]
df_subcat = df[df.category != 'Income'][cat_cols].drop_duplicates(subset=cat_cols)
df_subcat['category_id'] = pd.factorize(df_subcat['category'])[0]
df_subcat['subcategory_id'] = pd.factorize(df_subcat['subcategory'])[0]

df_cat = df_subcat[['category_id', 'category']].drop_duplicates()
df_subcat = df_subcat.drop('category', axis=1)

# Connect to SQLite database
conn = sqlite3.connect('expenses.db')
try:
    df.to_sql('transactions', conn, index=False, if_exists='replace')
    df_cat.to_csv('seed_categories.csv', index=False, quoting=csv.QUOTE_ALL)
    df_subcat.to_csv('seed_subcategories.csv', index=False, quoting=csv.QUOTE_ALL)
finally:
    conn.close()
