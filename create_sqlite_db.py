import sqlite3
conn = sqlite3.connect('sample.db')
cursor = conn.cursor()

     # Create tables
cursor.execute('''
         CREATE TABLE IF NOT EXISTS customers (
             customer_id INTEGER PRIMARY KEY,
             name TEXT NOT NULL,
             email TEXT
         )
     ''')
cursor.execute('''
         CREATE TABLE IF NOT EXISTS products (
             product_id INTEGER PRIMARY KEY,
             name TEXT NOT NULL,
             price REAL
         )
     ''')
cursor.execute('''
         CREATE TABLE IF NOT EXISTS orders (
             order_id INTEGER PRIMARY KEY,
             customer_id INTEGER,
             product_id INTEGER,
             quantity INTEGER,
             FOREIGN KEY (customer_id) REFERENCES customers (customer_id),
             FOREIGN KEY (product_id) REFERENCES products (product_id)
         )
     ''')

     # Insert sample data
cursor.execute("INSERT INTO customers (name, email) VALUES ('John Doe', 'john@example.com')")
cursor.execute("INSERT INTO customers (name, email) VALUES ('Jane Smith', 'jane@example.com')")
cursor.execute("INSERT INTO products (name, price) VALUES ('Laptop', 999.99)")
cursor.execute("INSERT INTO products (name, price) VALUES ('Phone', 499.99)")
cursor.execute("INSERT INTO orders (customer_id, product_id, quantity) VALUES (1, 1, 2)")
cursor.execute("INSERT INTO orders (customer_id, product_id, quantity) VALUES (2, 2, 1)")

conn.commit()
conn.close()