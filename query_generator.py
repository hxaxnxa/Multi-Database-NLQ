from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.prompts import PromptTemplate
from dotenv import load_dotenv
import os
import json
import re

load_dotenv()

def clean_sql_query(query_str):
    """
    Clean the LLM-generated SQL query by removing Markdown formatting and unexpected text.
    Args:
        query_str (str): The raw output from the LLM.
    Returns:
        str: The cleaned SQL query.
    """
    # Remove Markdown code block formatting (e.g., ```sql ... ```)
    query_str = re.sub(r'```sql\s*', '', query_str, flags=re.IGNORECASE)
    query_str = re.sub(r'```', '', query_str)
    
    # Remove any leading "sql" prefix (case-insensitive)
    query_str = re.sub(r'^\s*sql\s+', '', query_str, flags=re.IGNORECASE)
    
    # Remove any explanatory text before the query (e.g., "Here is the query: SELECT ...")
    query_str = re.sub(r'^.*?(SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP)\b', r'\1', query_str, flags=re.IGNORECASE)
    
    # Strip whitespace and ensure the query ends with a semicolon
    query_str = query_str.strip()
    if not query_str.endswith(';'):
        query_str += ';'
    
    return query_str

def generate_query(nl_query, schema, db_type):
    llm = ChatGoogleGenerativeAI(model="gemini-1.5-flash", api_key=os.getenv("GEMINI_API_KEY"))
    
    schema_str = "\n".join([f"Table/Collection: {table}\nColumns/Fields: {cols}" for table, cols in schema.items()])
    
    if db_type in ['sqlite', 'postgresql']:
        prompt_template = PromptTemplate(
            input_variables=["schema", "query"],
            template="Given the schema:\n{schema}\nGenerate an SQL query for the following natural language query:\n{query}\nReturn only the SQL query as a string, without any Markdown formatting or additional text. For example, return 'SELECT * FROM customers;' directly."
        )
    elif db_type == 'mongodb':
        prompt_template = PromptTemplate(
            input_variables=["schema", "query"],
            template="Given the schema:\n{schema}\nGenerate a MongoDB query for the following natural language query:\n{query}\nReturn the query as a JSON string in the format {{\"collection\": \"<collection_name>\", \"filter\": {{<filter_conditions>}}}}. For example, to find all customers, return {{\"collection\": \"customers\", \"filter\": {{}}}}. Ensure the output is a valid JSON string without any Markdown formatting or additional text."
        )
    else:  # redis
        prompt_template = PromptTemplate(
            input_variables=["schema", "query"],
            template="Given the schema:\n{schema}\nGenerate a Redis query for the following natural language query:\n{query}\nReturn the query as a JSON string in the format {{\"key\": \"<key_name>\"}}. Match the query to the schema: for queries requesting all records of a type (e.g., 'show all customers'), use a pattern like 'customer:*'; for queries requesting a specific record with an ID (e.g., 'show customer with ID 1'), use the exact key like 'customer:1'. Examples: for 'show all customers', return {{\"key\": \"customer:*\"}}; for 'what are the orders', return {{\"key\": \"order:*\"}}; for 'show customer with ID 1', return {{\"key\": \"customer:1\"}}. Ensure the output is a valid JSON string without any Markdown formatting or additional text."
        )

    prompt = prompt_template.format(schema=schema_str, query=nl_query)
    response = llm.invoke(prompt)
    generated_query = response.content.strip()

    # Clean the query for SQL databases
    if db_type in ['sqlite', 'postgresql']:
        generated_query = clean_sql_query(generated_query)
    # Validate JSON for MongoDB and Redis, with stricter validation for Redis
    elif db_type in ['mongodb', 'redis']:
        try:
            query_dict = json.loads(generated_query)
            if db_type == 'redis':
                # Ensure specific queries with an ID don't use wildcards
                if "id" in nl_query.lower() and '*' in query_dict['key']:
                    # Extract the ID from the query (e.g., "ID 1")
                    id_match = re.search(r'id\s+(\d+)', nl_query.lower())
                    if id_match:
                        id_value = id_match.group(1)
                        # Determine the key type (customer, order, product)
                        if "customer" in nl_query.lower():
                            return f'{{"key": "customer:{id_value}"}}'
                        elif "order" in nl_query.lower():
                            return f'{{"key": "order:{id_value}"}}'
                        elif "product" in nl_query.lower():
                            return f'{{"key": "product:{id_value}"}}'
        except json.JSONDecodeError:
            # If the LLM fails to generate valid JSON, return a default query
            if db_type == 'mongodb':
                return '{"collection": "customers", "filter": {}}'
            else:  # redis
                # Fallback based on query content
                if "id" in nl_query.lower():
                    id_match = re.search(r'id\s+(\d+)', nl_query.lower())
                    id_value = id_match.group(1) if id_match else "1"
                    if "customer" in nl_query.lower():
                        return f'{{"key": "customer:{id_value}"}}'
                    elif "order" in nl_query.lower():
                        return f'{{"key": "order:{id_value}"}}'
                    else:
                        return f'{{"key": "product:{id_value}"}}'
                else:
                    if "customer" in nl_query.lower():
                        return '{"key": "customer:*"}'
                    elif "order" in nl_query.lower():
                        return '{"key": "order:*"}'
                    else:
                        return '{"key": "product:*"}'
    
    return generated_query