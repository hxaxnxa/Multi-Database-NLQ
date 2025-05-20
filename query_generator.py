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
    query_str = re.sub(r'```sql\s*', '', query_str, flags=re.IGNORECASE)
    query_str = re.sub(r'```', '', query_str)
    query_str = re.sub(r'^\s*sql\s+', '', query_str, flags=re.IGNORECASE)
    query_str = re.sub(r'^.*?(SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP)\b', r'\1', query_str, flags=re.IGNORECASE)
    query_str = query_str.strip()
    if not query_str.endswith(';'):
        query_str += ';'
    return query_str

def clean_json_query(query_str):
    """
    Clean the LLM-generated JSON query (for MongoDB or Redis) by ensuring proper brace balancing.
    Args:
        query_str (str): The raw output from the LLM.
    Returns:
        str: The cleaned JSON string.
    """
    query_str = re.sub(r'```json\s*', '', query_str, flags=re.IGNORECASE)
    query_str = re.sub(r'```', '', query_str)
    query_str = re.sub(r'^[^[{]*', '', query_str)
    query_str = query_str.strip()

    # Balance braces in the array (MongoDB) or object (Redis)
    open_braces = 0
    open_brackets = 0
    in_quotes = False
    for char in query_str:
        if char == '"' and (query_str[max(0, query_str.index(char)-1)] != '\\'):
            in_quotes = not in_quotes
        if not in_quotes:
            if char == '{':
                open_braces += 1
            elif char == '}':
                open_braces -= 1
            elif char == '[':
                open_brackets += 1
            elif char == ']':
                open_brackets -= 1

    # Add closing braces/brackets if needed
    while open_braces > 0:
        query_str += '}'
        open_braces -= 1
    while open_braces < 0:
        query_str = '{' + query_str
        open_braces += 1
    while open_brackets > 0:
        query_str += ']'
        open_brackets -= 1
    while open_brackets < 0:
        query_str = '[' + query_str
        open_brackets += 1

    return query_str

def generate_query(nl_query, schema, db_type):
    llm = ChatGoogleGenerativeAI(model="gemini-1.5-flash", api_key=os.getenv("GEMINI_API_KEY"))
    
    schema_str = "\n".join([f"Table/Collection: {table}\nColumns/Fields: {cols}" for table, cols in schema.items()])
    
    if db_type in ['sqlite', 'postgresql']:
        prompt_template = PromptTemplate(
            input_variables=["schema", "query", "db_type"],
            template="Given the schema:\n{schema}\nGenerate an SQL query for the following natural language query in {db_type}:\n{query}\nReturn only the SQL query as a string, without any Markdown formatting or additional text. For example, return 'SELECT * FROM customers;' directly. Use EXTRACT(YEAR FROM column) for year extraction in PostgreSQL, and strftime('%Y', column) for SQLite. For date comparisons (e.g., 'before 2025-05-20'), use direct comparisons like 'column < ''2025-05-20''' if the column is in 'YYYY-MM-DD' format; avoid unnecessary strftime or EXTRACT unless extracting specific parts (e.g., year). For 'after' date conditions (e.g., 'after 2024-01-01'), use 'column > ''2024-01-01''' (strictly greater than). Interpret 'ordered more than once' as quantity > 1 in a single order unless specified otherwise. For discount calculations, assume discount is stored as a percentage (e.g., 15.00 for 15%) and adjust conditions accordingly (e.g., 'discount greater than 10%' means discount > 10). For phrases like 'products costing more than X', interpret as the unit price (products.price), not the total order price (orders.total_price), unless the prompt explicitly mentions 'total cost' or 'total price'. Ensure GROUP BY includes all non-aggregated columns in the SELECT clause. Add DISTINCT to SELECT when querying for emails to avoid duplicates. Add meaningful aliases for aggregated columns (e.g., AVG(column) AS avg_column)."
        )
    elif db_type == 'mongodb':
        prompt_template = PromptTemplate(
            input_variables=["schema", "query"],
            template="Given the schema:\n{schema}\nGenerate a MongoDB aggregation pipeline for the following natural language query:\n{query}\nReturn the pipeline as a JSON string in the format [{{\"stage\": \"value\"}}, ...] using aggregation pipeline stages ($lookup, $match, $group, $project, etc.) for joins, filtering, and aggregation. Assume the pipeline will be applied to the 'orders' collection for queries involving multiple entities (e.g., customers and products). Use $lookup to join with other collections, $match for filtering, $group for aggregations, and $project for selecting fields. For customer names, concatenate first_name and last_name using $concat (e.g., {{ \"$concat\": [\"$customer.first_name\", \" \", \"$customer.last_name\"] }}). For year-based filtering (e.g., 'in 2025'), use date range comparisons like {{ \"$gte\": \"2025-01-01\", \"$lte\": \"2025-12-31\" }} instead of $regex. Ensure all aggregation pipelines include a $project stage to exclude _id unless explicitly needed. If the prompt asks for fields not in the schema (e.g., 'address'), use available fields like 'city' or 'country' instead. For example, to join orders with customers, return [{{\"$lookup\": {{ \"from\": \"customers\", \"localField\": \"customer_id\", \"foreignField\": \"customer_id\", \"as\": \"customer\" }} }}]. Ensure the output is a valid JSON string without any Markdown formatting or additional text."
        )
    else:  # redis
        prompt_template = PromptTemplate(
            input_variables=["schema", "query"],
            template="Given the schema:\n{schema}\nGenerate a Redis query for the following natural language query:\n{query}\nReturn the query as a JSON string in the format {{\"key\": \"<key_name>\"}}. Match the query to the schema: for queries requesting all records of a type (e.g., 'show all customers'), use a pattern like 'customer:*'; for queries requesting a specific record with an ID (e.g., 'show customer with ID 1'), use the exact key like 'customer:1'; for queries involving multiple entities (e.g., 'customers who ordered products'), use 'order:*'. Add conditions as fields: for numeric filtering (e.g., 'price greater than 500'), include 'price_condition' with 'gt' or 'lt' subfields; for date filtering (e.g., 'before 2025-03-01'), include 'date_condition'; for year filtering (e.g., 'in 2025'), include 'year'; for categorical filtering (e.g., 'category Electronics'), include 'category'; for manufacturer, include 'manufacturer'; for city, include 'customer_city'; for discount (stored as percentage, e.g., 15.00 for 15%), include 'discount_condition'; for stock quantity or credit limit, include 'stock_condition' or 'credit_limit_condition'. Do not include aggregation instructions like 'avg_total_price' in the query; aggregations should be handled by the application. Examples: for 'products with price greater than 500', return {{\"key\": \"product:*\", \"price_condition\": {{\"gt\": 500}}}}; for 'orders in 2025 with category Electronics', return {{\"key\": \"order:*\", \"year\": 2025, \"category\": \"Electronics\"}}. Ensure the output is a valid JSON string without any Markdown formatting or additional text."
        )

    prompt = prompt_template.format(schema=schema_str, query=nl_query, db_type=db_type.upper())
    response = llm.invoke(prompt)
    generated_query = response.content.strip()
    print(f"Generated query for '{nl_query}': {generated_query}")  # Debug log

    # Clean the query based on database type
    if db_type in ['sqlite', 'postgresql']:
        generated_query = clean_sql_query(generated_query)
    elif db_type in ['mongodb', 'redis']:
        generated_query = clean_json_query(generated_query)
        print(f"Cleaned query for '{nl_query}': {generated_query}")  # Debug log
        try:
            query_dict = json.loads(generated_query)
            if db_type == 'redis':
                if "key" not in query_dict:
                    raise ValueError("Redis query must have a 'key' field")
                if "id" in nl_query.lower() and '*' in query_dict['key']:
                    id_match = re.search(r'id\s+(\d+)', nl_query.lower())
                    if id_match:
                        id_value = id_match.group(1)
                        if "customer" in nl_query.lower():
                            query_dict = {"key": f"customer:{id_value}"}
                        elif "order" in nl_query.lower():
                            query_dict = {"key": f"order:{id_value}"}
                        elif "product" in nl_query.lower():
                            query_dict = {"key": f"product:{id_value}"}
                if "customer" in nl_query.lower() and ("order" in nl_query.lower() or "product" in nl_query.lower()):
                    query_dict["key"] = "order:*"
                # Remove any aggregation-related fields
                query_dict = {k: v for k, v in query_dict.items() if k not in ['avg_total_price']}
                # Add conditions based on the query if not already present
                price_match = re.search(r'price\s*(greater than|above|less than|below)\s*(\d+(\.\d+)?)', nl_query.lower())
                if price_match and "price_condition" not in query_dict:
                    condition, value = price_match.groups()[0], float(price_match.groups()[1])
                    if "greater than" in condition or "above" in condition:
                        query_dict["price_condition"] = {"gt": value}
                    elif "less than" in condition or "below" in condition:
                        query_dict["price_condition"] = {"lt": value}
                discount_match = re.search(r'discount\s*(greater than|less than)\s*(\d+(\.\d+)?)', nl_query.lower())
                if discount_match and "discount_condition" not in query_dict:
                    condition, value = discount_match.groups()[0], float(discount_match.groups()[1])
                    if value < 1:  # Convert decimal to percentage if needed
                        value *= 100
                    if "greater than" in condition:
                        query_dict["discount_condition"] = {"gt": value}
                    elif "less than" in condition:
                        query_dict["discount_condition"] = {"lt": value}
                date_match = re.search(r'(before|after)\s*(\d{4}-\d{2}-\d{2})', nl_query.lower())
                if date_match and "date_condition" not in query_dict:
                    condition, date = date_match.groups()
                    if "before" in condition:
                        query_dict["date_condition"] = {"lt": date}
                    elif "after" in condition:
                        query_dict["date_condition"] = {"gt": date}
                year_match = re.search(r'in\s*(\d{4})', nl_query.lower())
                if year_match and "year" not in query_dict:
                    query_dict["year"] = int(year_match.group(1))
                if "category" in nl_query.lower() and "category" not in query_dict:
                    category_match = re.search(r'(Electronics|Clothing)', nl_query, re.IGNORECASE)
                    if category_match:
                        query_dict["category"] = category_match.group(1)
                if "manufacturer" in nl_query.lower() and "manufacturer" not in query_dict:
                    manufacturer_match = re.search(r'(TechCorp|FashionInc|SoundTech)', nl_query, re.IGNORECASE)
                    if manufacturer_match:
                        query_dict["manufacturer"] = manufacturer_match.group(1)
                if "city" in nl_query.lower() and "customer_city" not in query_dict:
                    city_match = re.search(r'(New York|Los Angeles|Chicago|Miami)', nl_query, re.IGNORECASE)
                    if city_match:
                        query_dict["customer_city"] = city_match.group(1)
                for field in ["stock_quantity", "credit_limit"]:
                    condition_match = re.search(rf'{field}\s*(greater than|above|less than|below)\s*(\d+(\.\d+)?)', nl_query.lower())
                    if condition_match:
                        condition, value = condition_match.groups()[0], float(condition_match.groups()[1])
                        condition_key = "stock_condition" if field == "stock_quantity" else "credit_limit_condition"
                        if condition_key not in query_dict:
                            if "greater than" in condition or "above" in condition:
                                query_dict[condition_key] = {"gt": value}
                            elif "less than" in condition or "below" in condition:
                                query_dict[condition_key] = {"lt": value}
                generated_query = json.dumps(query_dict)  # Convert back to JSON string
        except (json.JSONDecodeError, ValueError) as e:
            print(f"Error parsing generated query: {str(e)}")  # Debug log
            if db_type == 'mongodb':
                return '[]'  # Empty pipeline as fallback
            else:  # redis
                query_dict = {"key": "order:*"}  # Default to orders for joins
                if "id" in nl_query.lower():
                    id_match = re.search(r'id\s+(\d+)', nl_query.lower())
                    id_value = id_match.group(1) if id_match else "1"
                    if "customer" in nl_query.lower():
                        query_dict = {"key": f"customer:{id_value}"}
                    elif "order" in nl_query.lower():
                        query_dict = {"key": f"order:{id_value}"}
                    elif "product" in nl_query.lower():
                        query_dict = {"key": f"product:{id_value}"}
                elif "customer" in nl_query.lower():
                    query_dict["key"] = "customer:*"
                elif "order" in nl_query.lower():
                    query_dict["key"] = "order:*"
                elif "product" in nl_query.lower():
                    query_dict["key"] = "product:*"
                price_match = re.search(r'price\s*(greater than|above|less than|below)\s*(\d+(\.\d+)?)', nl_query.lower())
                if price_match:
                    condition, value = price_match.groups()[0], float(price_match.groups()[1])
                    if "greater than" in condition or "above" in condition:
                        query_dict["price_condition"] = {"gt": value}
                    elif "less than" in condition or "below" in condition:
                        query_dict["price_condition"] = {"lt": value}
                discount_match = re.search(r'discount\s*(greater than|less than)\s*(\d+(\.\d+)?)', nl_query.lower())
                if discount_match:
                    condition, value = discount_match.groups()[0], float(discount_match.groups()[1])
                    if value < 1:  # Convert decimal to percentage
                        value *= 100
                    if "greater than" in condition:
                        query_dict["discount_condition"] = {"gt": value}
                    elif "less than" in condition:
                        query_dict["discount_condition"] = {"lt": value}
                date_match = re.search(r'(before|after)\s*(\d{4}-\d{2}-\d{2})', nl_query.lower())
                if date_match:
                    condition, date = date_match.groups()
                    if "before" in condition:
                        query_dict["date_condition"] = {"lt": date}
                    elif "after" in condition:
                        query_dict["date_condition"] = {"gt": date}
                year_match = re.search(r'in\s*(\d{4})', nl_query.lower())
                if year_match:
                    query_dict["year"] = int(year_match.group(1))
                if "category" in nl_query.lower():
                    category_match = re.search(r'(Electronics|Clothing)', nl_query, re.IGNORECASE)
                    if category_match:
                        query_dict["category"] = category_match.group(1)
                if "manufacturer" in nl_query.lower():
                    manufacturer_match = re.search(r'(TechCorp|FashionInc|SoundTech)', nl_query, re.IGNORECASE)
                    if manufacturer_match:
                        query_dict["manufacturer"] = manufacturer_match.group(1)
                if "city" in nl_query.lower():
                    city_match = re.search(r'(New York|Los Angeles|Chicago|Miami)', nl_query, re.IGNORECASE)
                    if city_match:
                        query_dict["customer_city"] = city_match.group(1)
                for field in ["stock_quantity", "credit_limit"]:
                    condition_match = re.search(rf'{field}\s*(greater than|above|less than|below)\s*(\d+(\.\d+)?)', nl_query.lower())
                    if condition_match:
                        condition, value = condition_match.groups()[0], float(condition_match.groups()[1])
                        condition_key = "stock_condition" if field == "stock_quantity" else "credit_limit_condition"
                        if "greater than" in condition or "above" in condition:
                            query_dict[condition_key] = {"gt": value}
                        elif "less than" in condition or "below" in condition:
                            query_dict[condition_key] = {"lt": value}
                return json.dumps(query_dict)
    
    return generated_query