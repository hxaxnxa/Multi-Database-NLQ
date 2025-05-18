import sqlite3
import psycopg2
from pymongo import MongoClient
import redis
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
    schema = {}
    keys = r.keys('*:*')
    for key in keys:
        key_type, key_id = key.split(':')
        if key_type not in schema:
            schema[key_type] = []
        fields = r.hgetall(key)
        schema[key_type].append([(k, type(v).__name__) for k, v in fields.items()])
    r.close()
    return schema

def generate_schema_description(schema):
    llm = ChatGoogleGenerativeAI(model="gemini-1.5-flash", api_key=os.getenv("GEMINI_API_KEY"))
    schema_str = "\n".join([f"Table/Collection: {table}\nColumns/Fields: {cols}" for table, cols in schema.items()])
    prompt = f"Describe the following database schema in natural language:\n{schema_str}"
    response = llm.invoke(prompt)
    return response.content