import streamlit as st
from schema_detector import get_sqlite_schema, get_postgres_schema, get_mongodb_schema, get_redis_schema, generate_schema_description
from db_connectors import execute_sqlite_query, execute_postgres_query, execute_mongodb_query, execute_redis_query
from query_generator import generate_query
import json
import sqlite3
from dotenv import load_dotenv
import os

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
                
            st.session_state.query_result = result
        except Exception as e:
            st.error(f"Error executing query: {str(e)}")
    else:
        st.warning("Please enter a query.")

# Display the result if it exists
if "query_result" in st.session_state:
    st.subheader("Query Result")
    st.dataframe(st.session_state.query_result)