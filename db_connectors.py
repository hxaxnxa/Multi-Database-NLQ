import sqlite3
import psycopg2
from pymongo import MongoClient
import redis
import pandas as pd
from bson.objectid import ObjectId
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def execute_sqlite_query(db_path, query):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def execute_postgres_query(db_params, query):
    conn = psycopg2.connect(
        dbname=db_params.get("dbname", os.getenv("POSTGRES_DBNAME")),
        user=db_params.get("user", os.getenv("POSTGRES_USER")),
        password=db_params.get("password", os.getenv("POSTGRES_PASSWORD")),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432")
    )
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def execute_mongodb_query(db_name, query):
    mongodb_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
    client = MongoClient(mongodb_uri)
    db = client[db_name]
    try:
        # Check if query is a list (aggregation pipeline) or a dictionary (find query)
        if isinstance(query, list):
            # Use the 'orders' collection for aggregation pipelines
            collection = db['orders']
            results = collection.aggregate(query)
        else:
            # Fallback for find queries (though not expected with current query_generator.py)
            collection = db[query.get('collection', 'orders')]
            results = collection.find(query.get('filter', {}))

        # Convert results to a list and handle ObjectId
        results_list = []
        for doc in results:
            if '_id' in doc:
                doc['_id'] = str(doc['_id'])  # Convert ObjectId to string
            results_list.append(doc)
        
        df = pd.DataFrame(results_list) if results_list else pd.DataFrame()
        return df
    finally:
        client.close()

def execute_redis_query(query):
    r = redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        password=os.getenv("REDIS_PASSWORD", None),
        decode_responses=True
    )
    try:
        if not isinstance(query, dict) or 'key' not in query:
            print("Error: Invalid query format. Expected {'key': '<key_name>'}")
            return pd.DataFrame()

        key_pattern = query['key']
        print(f"Executing Redis query with key pattern: {key_pattern}")  # Debug log

        # Fetch matching keys
        try:
            matching_keys = r.keys(key_pattern)
            print(f"Matching keys: {matching_keys}")  # Debug log
        except redis.RedisError as e:
            print(f"Error fetching keys for pattern {key_pattern}: {str(e)}")
            return pd.DataFrame()

        if not matching_keys:
            print("No matching keys found")
            return pd.DataFrame()

        # Process each key and fetch related data
        results = []
        for key in matching_keys:
            try:
                key_type = r.type(key)
                print(f"Type of {key}: {key_type}")  # Debug log
                if key_type != 'hash':
                    print(f"Skipping {key} because type is {key_type}, expected hash")
                    continue

                # Fetch order data
                order_data = r.hgetall(key)
                if not order_data:
                    print(f"No data found for {key}")
                    continue

                # Convert order data with proper types
                result = {'key': key}
                for field, value in order_data.items():
                    try:
                        if field in ['total_price', 'price', 'discount', 'stock_quantity']:
                            result[field] = float(value)
                        elif field in ['order_id', 'customer_id', 'product_id', 'quantity']:
                            result[field] = int(value)
                        else:
                            result[field] = value
                    except (ValueError, TypeError) as e:
                        print(f"Error converting field {field} with value {value} in key {key}: {str(e)}")
                        result[field] = value

                # Fetch customer data
                customer_id = result.get('customer_id')
                customer_data = r.hgetall(f"customer:{customer_id}") if customer_id else {}
                for field, value in customer_data.items():
                    try:
                        if field == 'credit_limit':
                            result[f"customer_{field}"] = float(value)
                        else:
                            result[f"customer_{field}"] = value
                    except (ValueError, TypeError) as e:
                        print(f"Error converting customer field {field} with value {value}: {str(e)}")
                        result[f"customer_{field}"] = value

                # Fetch product data
                product_id = result.get('product_id')
                product_data = r.hgetall(f"product:{product_id}") if product_id else {}
                for field, value in product_data.items():
                    try:
                        if field in ['price', 'discount', 'stock_quantity']:
                            result[f"product_{field}"] = float(value)
                        else:
                            result[f"product_{field}"] = value
                    except (ValueError, TypeError) as e:
                        print(f"Error converting product field {field} with value {value}: {str(e)}")
                        result[f"product_{field}"] = value

                results.append(result)
            except redis.RedisError as e:
                print(f"Error processing key {key}: {str(e)}")
                continue

        if not results:
            print("No matching records found after processing keys")
            return pd.DataFrame()

        # Create DataFrame from results
        df = pd.DataFrame(results)
        print(f"Initial DataFrame:\n{df}")  # Debug log

        # Apply filters
        if 'year' in query:
            df = df[df['order_date'].str.startswith(str(query['year']))]
            print(f"Filtered DataFrame (year = {query['year']}):\n{df}")

        if 'category' in query:
            df = df[df['product_category'] == query['category']]
            print(f"Filtered DataFrame (category = {query['category']}):\n{df}")

        if 'manufacturer' in query:
            df = df[df['product_manufacturer'] == query['manufacturer']]
            print(f"Filtered DataFrame (manufacturer = {query['manufacturer']}):\n{df}")

        if 'customer_city' in query:
            df = df[df['customer_city'] == query['customer_city']]
            print(f"Filtered DataFrame (customer_city = {query['customer_city']}):\n{df}")

        if 'price_condition' in query:
            try:
                if query['price_condition'].get('gt'):
                    threshold = float(query['price_condition']['gt'])
                    df = df[df['product_price'] > threshold]
                    print(f"Filtered DataFrame (price > {threshold}):\n{df}")
                elif query['price_condition'].get('lt'):
                    threshold = float(query['price_condition']['lt'])
                    df = df[df['product_price'] < threshold]
                    print(f"Filtered DataFrame (price < {threshold}):\n{df}")
            except (ValueError, TypeError) as e:
                print(f"Error applying price filter: {str(e)}")
                return pd.DataFrame()

        if 'date_condition' in query:
            date_cond = query['date_condition']
            if 'lt' in date_cond:
                df = df[df['order_date'] < date_cond['lt']]
                print(f"Filtered DataFrame (order_date < {date_cond['lt']}):\n{df}")
            if 'gt' in date_cond:
                df = df[df['order_date'] > date_cond['gt']]
                print(f"Filtered DataFrame (order_date > {date_cond['gt']}):\n{df}")

        if 'discount_condition' in query:
            discount_cond = query['discount_condition']
            if 'gt' in discount_cond:
                df = df[df['product_discount'].astype(float) > discount_cond['gt']]
                print(f"Filtered DataFrame (discount > {discount_cond['gt']}):\n{df}")
            if 'lt' in discount_cond:
                df = df[df['product_discount'].astype(float) < discount_cond['lt']]
                print(f"Filtered DataFrame (discount < {discount_cond['lt']}):\n{df}")

        if 'stock_condition' in query:
            stock_cond = query['stock_condition']
            if 'gt' in stock_cond:
                df = df[df['product_stock_quantity'].astype(float) > stock_cond['gt']]
                print(f"Filtered DataFrame (stock_quantity > {stock_cond['gt']}):\n{df}")
            if 'lt' in stock_cond:
                df = df[df['product_stock_quantity'].astype(float) < stock_cond['lt']]
                print(f"Filtered DataFrame (stock_quantity < {stock_cond['lt']}):\n{df}")

        if 'credit_limit_condition' in query:
            credit_limit_cond = query['credit_limit_condition']
            if 'gt' in credit_limit_cond:
                df = df[df['customer_credit_limit'].astype(float) > credit_limit_cond['gt']]
                print(f"Filtered DataFrame (credit_limit > {credit_limit_cond['gt']}):\n{df}")
            if 'lt' in credit_limit_cond:
                df = df[df['customer_credit_limit'].astype(float) < credit_limit_cond['lt']]
                print(f"Filtered DataFrame (credit_limit < {credit_limit_cond['lt']}):\n{df}")

        if 'release_date_condition' in query:
            release_date_cond = query['release_date_condition']
            if 'gt' in release_date_cond:
                df = df[df['product_release_date'] > release_date_cond['gt']]
                print(f"Filtered DataFrame (release_date > {release_date_cond['gt']}):\n{df}")
            if 'lt' in release_date_cond:
                df = df[df['product_release_date'] < release_date_cond['lt']]
                print(f"Filtered DataFrame (release_date < {release_date_cond['lt']}):\n{df}")

        # Handle specific query requirements
        if 'total spending' in query.get('nl_query', '').lower():
            # Group by customer and sum total_price
            df['customer_name'] = df['customer_first_name'] + ' ' + df['customer_last_name']
            df = df.groupby('customer_name').agg({
                'total_price': 'sum'
            }).reset_index()
            df.rename(columns={'total_price': 'total_spending'}, inplace=True)
            print(f"DataFrame after grouping by customer for total spending:\n{df}")

        elif 'total quantity ordered' in query.get('nl_query', '').lower():
            # Group by category and sum quantity
            df = df.groupby('product_category').agg({
                'quantity': 'sum'
            }).reset_index()
            df.rename(columns={'product_category': 'category', 'quantity': 'total_quantity'}, inplace=True)
            print(f"DataFrame after grouping by category for total quantity:\n{df}")

        elif 'average total price' in query.get('nl_query', '').lower():
            # Calculate average total_price
            avg_price = df['total_price'].astype(float).mean()
            df = pd.DataFrame({'average_total_price': [avg_price]})
            print(f"DataFrame with average total price:\n{df}")

        elif 'phone numbers' in query.get('nl_query', '').lower():
            # Select phone numbers
            df['phone'] = df['customer_phone']
            df = df[['phone']].drop_duplicates()
            print(f"DataFrame with phone numbers:\n{df}")

        elif 'names and emails' in query.get('nl_query', '').lower():
            # Select customer names and emails
            df['customer_name'] = df['customer_first_name'] + ' ' + df['customer_last_name']
            df['email'] = df['customer_email']
            df['product_name'] = df['product_name']
            df = df[['customer_name', 'email', 'product_name', 'total_price']]
            print(f"DataFrame with names, emails, product names, and total prices:\n{df}")

        return df

    finally:
        r.close()