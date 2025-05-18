# Multi-Database Natural Language Query Pipeline

## Overview

MD-NLQ is a powerful tool that transforms natural language queries into database operations across multiple database systems. It allows users to interact with databases using plain English through a streamlit interface, eliminating the need to write complex SQL or NoSQL queries manually.

## Features

- **Multi-Database Support**: Compatible with SQLite, MongoDB, PostgreSQL, and Redis (for key-value pairs)
- **Natural Language Processing**: Converts user prompts into proper database queries
- **Automated Schema Detection**: Uses Gemini 1.5 Flash LLM model to understand database structure
- **Interactive UI**: Built with Streamlit for a smooth user experience
- **Data Visualization**: Displays query results in table format using dataframes

## Project Structure

```
NLQPROJECT/
├── _pycache_/
├── venv/
├── .env                    # Environment variables and configuration
├── app.py                  # Main application entry point
├── create_sqlite_db.py     # Script for SQLite database creation
├── db_connectors.py        # Database connection handlers
├── query_generator.py      # Converts NL to database queries
├── README.md               # Project documentation
├── requirements.txt        # Project dependencies
├── sample.db               # Sample SQLite database for testing
├── schema_detector.py      # Database schema analysis using Gemini 1.5
└── test_sqlite.py          # Testing module for SQLite functionality
```

## Installation

1. Clone the repository:
```bash
git clone https://github.com/hxaxnxa/Multi-Database-NLQ.git
cd Multi-Database-NLQ
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install the required dependencies:
```bash
pip install -r requirements.txt
```

4. Set up your environment variables in `.env` file:
```
# PostgreSQL Configuration
POSTGRES_HOST=your_postgres_host
POSTGRES_PORT=your_postgres_port
POSTGRES_DBNAME=your_postgres_database
POSTGRES_USER=your_postgres_username
POSTGRES_PASSWORD=your_postgres_password

# MongoDB Configuration
MONGODB_URI=mongodb://localhost:27017/

# Redis Configuration
REDIS_HOST=your_redis_host
REDIS_PORT=your_redis_port
REDIS_DB=your_redis_database

# Other configurations
GEMINI_API_KEY=your_gemini_api_key
```

## Usage

1. Start the application:
```bash
streamlit run app.py
```

2. Open your browser and navigate to the provided local URL (typically http://localhost:8501)

3. Select the database type you want to query

4. Enter your natural language query in the text input field

5. View the results displayed in a table format

## Supported Databases

1. **SQLite**
   - Embedded SQL database engine
   - Sample database included for testing

2. **MongoDB**
   - NoSQL document database
   - Configure connection in `.env` file

3. **PostgreSQL**
   - Relational database
   - Configure connection in `.env` file

4. **Redis**
   - In-memory data store
   - Currently supports key-value pair operations only
   - Redis Search functionality not implemented

## How It Works

1. **Schema Detection**: The `schema_detector.py` module uses Gemini 1.5 Flash to analyze and understand the database structure
2. **Query Generation**: User's natural language input is processed by `query_generator.py`
3. **Database Connection**: The appropriate connector from `db_connectors.py` is used based on the selected database
4. **Query Execution**: The generated query is executed against the selected database
5. **Result Visualization**: Query results are displayed in a dataframe format

## Development

### Adding Support for New Database Types

To add support for additional database systems:

1. Update `db_connectors.py` with a new connector class
2. Implement schema detection for the new database in `schema_detector.py`
3. Add query generation rules in `query_generator.py`
4. Update the UI in `app.py` to include the new database option

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Acknowledgements

- This project utilizes Google's Gemini 1.5 Flash LLM for natural language processing
- Built with Streamlit for the user interface
- Supports multiple database systems for flexibility in data storage
