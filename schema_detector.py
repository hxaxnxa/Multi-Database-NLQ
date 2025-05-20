import sqlite3
import psycopg2
from pymongo import MongoClient
import redis
import json
from langchain_google_genai import ChatGoogleGenerativeAI
from dotenv import load_dotenv
import os

load_dotenv()

def get_sqlite_schema(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    schema = {}
    for table in tables:
        table_name = table[0]
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()
        schema[table_name] = [(col[1], col[2]) for col in columns]
    conn.close()
    return schema

def get_postgres_schema(db_params):
    # Update connection parameters for PostgreSQL
    conn = psycopg2.connect(
        dbname=db_params.get("dbname", os.getenv("POSTGRES_DBNAME")),
        user=db_params.get("user", os.getenv("POSTGRES_USER")),
        password=db_params.get("password", os.getenv("POSTGRES_PASSWORD")),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432")
    )
    cursor = conn.cursor()
    cursor.execute("""
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public';
    """)
    rows = cursor.fetchall()
    schema = {}
    for table_name, column_name, data_type in rows:
        if table_name not in schema:
            schema[table_name] = []
        schema[table_name].append((column_name, data_type))
    conn.close()
    return schema

def get_mongodb_schema(db_name):
    # Update MongoDB connection
    mongodb_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
    client = MongoClient(mongodb_uri)
    db = client[db_name]
    schema = {}
    for collection_name in db.list_collection_names():
        collection = db[collection_name]
        sample_doc = collection.find_one()
        if sample_doc:
            schema[collection_name] = [(k, type(v).__name__) for k, v in sample_doc.items() if k != '_id']
    client.close()
    return schema

def get_redis_schema():
    # Update Redis connection
    r = redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        password=os.getenv("REDIS_PASSWORD", None),
        decode_responses=True
    )
    try:
        schema = {}
        keys = r.keys('*:*')
        for key in keys:
            try:
                # Split the key into type and ID (e.g., "customer:1" -> "customer", "1")
                key_type, key_id = key.split(':')
                if key_type not in schema:
                    schema[key_type + "s"] = []  # Pluralize to match other DBs (e.g., "customers")
                
                # Check the type of the key
                redis_type = r.type(key)
                print(f"Type of {key}: {redis_type}")  # Debug log
                if redis_type != "hash":
                    print(f"Skipping {key} because type is {redis_type}, expected hash")
                    continue
                
                # Retrieve the value using HGETALL for hash keys
                hash_data = r.hgetall(key)
                if not hash_data:
                    print(f"No data found for {key}")
                    continue
                
                # Convert hash data to a dictionary with proper type inference
                fields = {}
                for field, value in hash_data.items():
                    try:
                        if field in ['price', 'total_price', 'credit_limit', 'discount', 'stock_quantity', 'quantity']:
                            fields[field] = float(value)
                        elif field in ['customer_id', 'product_id', 'order_id']:
                            fields[field] = int(value)
                        else:
                            fields[field] = value
                    except (ValueError, TypeError):
                        fields[field] = value  # Fallback to string if conversion fails
                
                # Add the fields and their types to the schema
                schema[key_type + "s"].append([(k, type(v).__name__) for k, v in fields.items()])
            except (ValueError, redis.RedisError) as e:
                print(f"Error processing key {key}: {str(e)}")  # Debug log
                continue
        
        # Flatten the schema to match the format expected by other functions
        for key_type in schema:
            if schema[key_type]:
                # Take the first sample's fields as the schema (assuming consistency)
                schema[key_type] = schema[key_type][0]
            else:
                del schema[key_type]  # Remove empty schemas
        
        if not schema:
            print("No valid hash keys found to determine schema")
            return {"error": "No valid hash keys found to determine schema"}
        
        return schema
    finally:
        r.close()

def generate_schema_description(schema):
    llm = ChatGoogleGenerativeAI(model="gemini-1.5-flash", api_key=os.getenv("GEMINI_API_KEY"))
    schema_str = "\n".join([f"Table/Collection: {table}\nColumns/Fields: {cols}" for table, cols in schema.items()])
    prompt = f"Describe the following database schema in natural language:\n{schema_str}"
    response = llm.invoke(prompt)
    return response.content