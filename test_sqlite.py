import sqlite3

try:
    conn = sqlite3.connect('sample.db')
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    print("Tables in the database:", tables)
    conn.close()
except sqlite3.DatabaseError as e:
    print(f"Error accessing database: {e}")