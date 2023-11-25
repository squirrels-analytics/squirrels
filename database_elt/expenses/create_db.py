import sqlite3, pandas as pd

df = pd.read_csv("sample-transactions.csv")

cat_cols = ["Category", "Subcategory"]
df_subcat = df[df.Category != 'Income'][cat_cols].drop_duplicates(subset=cat_cols)
df_subcat['Category_ID'] = pd.factorize(df_subcat['Category'])[0]
df_subcat['Subcategory_ID'] = pd.factorize(df_subcat['Subcategory'])[0]

df_cat = df_subcat[['Category_ID', 'Category']].drop_duplicates()
df_subcat = df_subcat.drop('Category', axis=1)

# Connect to SQLite database
conn = sqlite3.connect('expenses.db')
try:
    df.to_sql('transactions', conn, index=False, if_exists='replace')
    df_cat.to_sql('categories', conn, index=False, if_exists='replace')
    df_subcat.to_sql('subcategories', conn, index=False, if_exists='replace')
finally:
    conn.close()
