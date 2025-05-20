import sqlite3

# Connect to SQLite database
conn = sqlite3.connect('sample.db')
cursor = conn.cursor()

# Drop existing tables if they exist
cursor.execute('DROP TABLE IF EXISTS orders')
cursor.execute('DROP TABLE IF EXISTS products')
cursor.execute('DROP TABLE IF EXISTS customers')

# Create tables with enhanced schema
cursor.execute('''
    CREATE TABLE customers (
        customer_id INTEGER PRIMARY KEY,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        email TEXT UNIQUE NOT NULL,
        phone TEXT,
        address TEXT,
        city TEXT,
        country TEXT,
        credit_limit REAL,
        registration_date TEXT
    )
''')

cursor.execute('''
    CREATE TABLE products (
        product_id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        price REAL NOT NULL,
        category TEXT,
        stock_quantity INTEGER,
        manufacturer TEXT,
        release_date TEXT,
        discount REAL
    )
''')

cursor.execute('''
    CREATE TABLE orders (
        order_id INTEGER PRIMARY KEY,
        customer_id INTEGER NOT NULL,
        product_id INTEGER NOT NULL,
        quantity INTEGER NOT NULL,
        order_date TEXT NOT NULL,
        status TEXT,
        total_price REAL,
        shipping_address TEXT,
        payment_method TEXT,
        FOREIGN KEY (customer_id) REFERENCES customers (customer_id),
        FOREIGN KEY (product_id) REFERENCES products (product_id)
    )
''')

# Insert sample data
# Customers
cursor.execute("INSERT INTO customers (customer_id, first_name, last_name, email, phone, address, city, country, credit_limit, registration_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", 
               (1, 'John', 'Doe', 'john.doe@email.com', '123-456-7890', '123 Maple St', 'New York', 'USA', 5000.00, '2024-01-15'))
cursor.execute("INSERT INTO customers (customer_id, first_name, last_name, email, phone, address, city, country, credit_limit, registration_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", 
               (2, 'Jane', 'Smith', 'jane.smith@email.com', '234-567-8901', '456 Oak St', 'London', 'UK', 3000.00, '2024-03-22'))
cursor.execute("INSERT INTO customers (customer_id, first_name, last_name, email, phone, address, city, country, credit_limit, registration_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", 
               (3, 'Alice', 'Johnson', 'alice.j@email.com', '345-678-9012', '789 Pine St', 'Toronto', 'Canada', 7000.00, '2024-06-10'))
cursor.execute("INSERT INTO customers (customer_id, first_name, last_name, email, phone, address, city, country, credit_limit, registration_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", 
               (4, 'Bob', 'Brown', 'bob.brown@email.com', '456-789-0123', '101 Elm St', 'Sydney', 'Australia', 4000.00, '2024-08-05'))

# Products
cursor.execute("INSERT INTO products (product_id, name, price, category, stock_quantity, manufacturer, release_date, discount) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", 
               (1, 'Laptop', 999.99, 'Electronics', 50, 'TechCorp', '2023-11-01', 10.00))
cursor.execute("INSERT INTO products (product_id, name, price, category, stock_quantity, manufacturer, release_date, discount) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", 
               (2, 'T-Shirt', 19.99, 'Clothing', 200, 'FashionInc', '2024-02-15', 5.00))
cursor.execute("INSERT INTO products (product_id, name, price, category, stock_quantity, manufacturer, release_date, discount) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", 
               (3, 'Smartphone', 699.99, 'Electronics', 30, 'TechCorp', '2024-05-01', 15.00))
cursor.execute("INSERT INTO products (product_id, name, price, category, stock_quantity, manufacturer, release_date, discount) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", 
               (4, 'Headphones', 49.99, 'Electronics', 100, 'SoundTech', '2024-07-01', 20.00))

# Orders
cursor.execute("INSERT INTO orders (order_id, customer_id, product_id, quantity, order_date, status, total_price, shipping_address, payment_method) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", 
               (1, 1, 1, 2, '2025-01-10', 'Shipped', 1799.98, '123 Maple St, New York, USA', 'Credit Card'))
cursor.execute("INSERT INTO orders (order_id, customer_id, product_id, quantity, order_date, status, total_price, shipping_address, payment_method) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", 
               (2, 2, 2, 5, '2025-02-15', 'Pending', 94.95, '456 Oak St, London, UK', 'PayPal'))
cursor.execute("INSERT INTO orders (order_id, customer_id, product_id, quantity, order_date, status, total_price, shipping_address, payment_method) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", 
               (3, 3, 3, 1, '2025-03-20', 'Delivered', 594.99, '789 Pine St, Toronto, Canada', 'Credit Card'))
cursor.execute("INSERT INTO orders (order_id, customer_id, product_id, quantity, order_date, status, total_price, shipping_address, payment_method) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", 
               (4, 2, 1, 1, '2025-04-01', 'Cancelled', 899.99, '456 Oak St, London, UK', 'PayPal'))
cursor.execute("INSERT INTO orders (order_id, customer_id, product_id, quantity, order_date, status, total_price, shipping_address, payment_method) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", 
               (5, 4, 4, 3, '2025-05-01', 'Shipped', 119.97, '101 Elm St, Sydney, Australia', 'Credit Card'))

# Commit changes and close connection
conn.commit()
conn.close()