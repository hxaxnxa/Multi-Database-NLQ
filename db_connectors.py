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
    collection = db[query['collection']]
    results = collection.find(query.get('filter', {}))
    # Convert results to a list and handle ObjectId
    results_list = []
    for doc in results:
        doc['_id'] = str(doc['_id'])  # Convert ObjectId to string
        results_list.append(doc)
    df = pd.DataFrame(results_list)
    client.close()
    return df

def execute_redis_query(query):
    r = redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        password=os.getenv("REDIS_PASSWORD", None),
        decode_responses=True
    )
    key_pattern = query['key']
    matching_keys = r.keys(key_pattern)
    if not matching_keys:
        r.close()
        return pd.DataFrame()
    
    results = []
    for key in matching_keys:
        result = r.hgetall(key)
        result['key'] = key
        results.append(result)
    
    df = pd.DataFrame(results)
    r.close()
    return df