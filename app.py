import streamlit as st
from schema_detector import get_sqlite_schema, get_postgres_schema, get_mongodb_schema, get_redis_schema, generate_schema_description
from db_connectors import execute_sqlite_query, execute_postgres_query, execute_mongodb_query, execute_redis_query
from query_generator import generate_query
import json
import sqlite3
from dotenv import load_dotenv
import os
import redis
import pandas as pd

# Load environment variables
load_dotenv()

st.title("NLQ Pipeline with Multiple Databases")

# Database selection
db_type = st.selectbox("Select Database", ["SQLite", "PostgreSQL", "MongoDB", "Redis"])

# Database parameters
if db_type == "SQLite":
    db_config = {"db_path": "sample.db"}
elif db_type == "PostgreSQL":
    db_config = {
        "dbname": os.getenv("POSTGRES_DBNAME", "sample"),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", ""),
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5432")
    }
elif db_type == "MongoDB":
    db_config = {"db_name": os.getenv("MONGODB_DBNAME", "sample")}
else:  # Redis
    db_config = {}

# Get schema
try:
    if db_type == "SQLite":
        schema = get_sqlite_schema(db_config["db_path"])
    elif db_type == "PostgreSQL":
        schema = get_postgres_schema(db_config)
    elif db_type == "MongoDB":
        schema = get_mongodb_schema(db_config["db_name"])
    else:
        schema = get_redis_schema()
except sqlite3.DatabaseError as e:
    st.error(f"Error accessing SQLite database: {str(e)}. Please ensure 'sample.db' exists and is a valid SQLite database.")
    st.stop()
except Exception as e:
    st.error(f"Error retrieving schema: {str(e)}")
    st.stop()

# Display schema
st.subheader("Database Schema")
schema_desc = generate_schema_description(schema)
st.write(schema_desc)

# Natural language query input
nl_query = st.text_input("Enter your query in natural language (e.g., 'Show all customers')", key="nl_query_input")

if st.button("Execute Query"):
    if nl_query:
        # Clear previous results to avoid caching issues
        st.session_state.pop("query_result", None)
        
        # Generate query
        generated_query = generate_query(nl_query, schema, db_type.lower())
        st.write(f"Generated Query: {generated_query}")  # Debug output
        
        # Execute query
        try:
            if db_type == "SQLite":
                result = execute_sqlite_query(db_config["db_path"], generated_query)
            elif db_type == "PostgreSQL":
                result = execute_postgres_query(db_config, generated_query)
            elif db_type == "MongoDB":
                try:
                    generated_query_dict = json.loads(generated_query)
                except json.JSONDecodeError as e:
                    st.error(f"Invalid MongoDB query format: {generated_query}. Expected a JSON string. Error: {str(e)}")
                    st.stop()
                result = execute_mongodb_query(db_config["db_name"], generated_query_dict)
            else:  # Redis
                try:
                    generated_query_dict = json.loads(generated_query)
                except json.JSONDecodeError as e:
                    st.error(f"Invalid Redis query format: {generated_query}. Expected a JSON string. Error: {str(e)}")
                    st.stop()
                result = execute_redis_query(generated_query_dict)
                
                # For Redis queries, fetch additional data for joins and apply transformations
                if db_type == "Redis" and "customer" in nl_query.lower() and "product" in nl_query.lower():
                    redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
                    try:
                        enriched_results = []
                        for _, order in result.iterrows():
                            # Fetch product
                            product_key = f"product:{order['product_id']}"
                            product_data = None
                            try:
                                product_hash = redis_client.hgetall(product_key)
                                if product_hash:
                                    product_data = {}
                                    for field, value in product_hash.items():
                                        if field in ["price", "discount", "stock_quantity"]:
                                            product_data[field] = float(value)
                                        else:
                                            product_data[field] = value
                            except redis.RedisError as e:
                                print(f"Error fetching product {product_key}: {str(e)}")
                                continue
                            # Fetch customer
                            customer_key = f"customer:{order['customer_id']}"
                            customer_data = None
                            try:
                                customer_hash = redis_client.hgetall(customer_key)
                                if customer_hash:
                                    customer_data = {}
                                    for field, value in customer_hash.items():
                                        if field in ["credit_limit"]:
                                            customer_data[field] = float(value)
                                        else:
                                            customer_data[field] = value
                            except redis.RedisError as e:
                                print(f"Error fetching customer {customer_key}: {str(e)}")
                                continue
                            if customer_data and product_data:
                                enriched_row = {
                                    "customer_name": f"{customer_data['first_name']} {customer_data['last_name']}",
                                    "email": customer_data.get("email"),
                                    "phone": customer_data.get("phone"),
                                    "city": customer_data.get("city"),
                                    "country": customer_data.get("country"),
                                    "credit_limit": customer_data.get("credit_limit"),
                                    "registration_date": customer_data.get("registration_date"),
                                    "product_name": product_data.get("name"),
                                    "category": product_data.get("category"),
                                    "price": product_data.get("price"),
                                    "stock_quantity": product_data.get("stock_quantity"),
                                    "manufacturer": product_data.get("manufacturer"),
                                    "release_date": product_data.get("release_date"),
                                    "discount": product_data.get("discount"),
                                    "order_id": int(order.get("order_id")),
                                    "quantity": int(order.get("quantity")),
                                    "order_date": order.get("order_date"),
                                    "total_price": float(order.get("total_price")),
                                    "status": order.get("status"),
                                    "shipping_address": order.get("shipping_address"),
                                    "payment_method": order.get("payment_method")
                                }
                                enriched_results.append(enriched_row)
                        if enriched_results:
                            result = pd.DataFrame(enriched_results)
                        else:
                            result = pd.DataFrame(columns=["customer_name", "email", "phone", "city", "country", "credit_limit", "registration_date", "product_name", "category", "price", "stock_quantity", "manufacturer", "release_date", "discount", "order_id", "quantity", "order_date", "total_price", "status", "shipping_address", "payment_method"])
                    finally:
                        redis_client.close()
                
            st.session_state.query_result = result
        except Exception as e:
            st.error(f"Error executing query: {str(e)}")
    else:
        st.warning("Please enter a query.")

# Display the result if it exists
if "query_result" in st.session_state:
    st.subheader("Query Result")
    if not st.session_state.query_result.empty:
        st.dataframe(st.session_state.query_result)
    else:
        st.warning("No results found.")