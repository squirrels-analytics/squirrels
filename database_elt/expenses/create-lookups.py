import sqlite3
import csv
from io import StringIO

category_id_mapping = """
"category_id","subcategory_id"
0,0
0,1
1,2
2,3
3,4
1,5
2,6
1,7
4,8
4,9
2,10
3,11
2,12
4,13
"""

# Connect to the SQLite database
conn = sqlite3.connect('expenses.db')
try:
    cursor = conn.cursor()

    # Create the category_mapping table
    cursor.execute('''
    DROP TABLE IF EXISTS category_mapping
    ''')
    cursor.execute('''
    CREATE TABLE category_mapping (
        category_id INTEGER,
        subcategory_id INTEGER,
        PRIMARY KEY (subcategory_id)
    )
    ''')

    # Parse the CSV string and insert data
    csv_file = StringIO(category_id_mapping.strip())
    csv_reader = csv.DictReader(csv_file)

    # Insert the data
    for row in csv_reader:
        cursor.execute('''
        INSERT OR REPLACE INTO category_mapping (category_id, subcategory_id)
        VALUES (?, ?)
        ''', (int(row['category_id']), int(row['subcategory_id'])))

    # Commit the changes and close the connection
    conn.commit()

finally:
    conn.close()
