import os
import logging
import sqlparse
import sqlalchemy as db
import pandas as pd
import time
# from langchain.chat_models import ChatOpenAI  # REMOVE
from langchain.llms import HuggingFaceHub  # ADD
from langchain.llms import OpenAI  # Keep this for embeddings, if used
from connection import sql_conn, db_host, db_user, db_password, db_port, db_name
from langchain.utilities import SQLDatabase
from langchain.sql_database import SQLDatabase
from langchain.chains import create_sql_query_chain
from vectordb import fetch_schema
from dotenv import load_dotenv

load_dotenv()

# Logger setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ✅ Initialize SQL Database Connection
if sql_conn:
    print("✅ Connected successfully!")
    try:
        db = SQLDatabase.from_uri(
            f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}",
            sample_rows_in_table_info=3
        )
        print("✅ Database connected in LangChain.")
    except Exception as e:
        print(f"❌ Error creating SQLDatabase: {e}")
        exit(1)
else:
    print("❌ Failed to connect to database.")
    exit(1)

# ✅ Initialize LLM (Query Generation) - Using HuggingFace Hub
HUGGINGFACEHUB_API_TOKEN = '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'  # Get from environment variable

# Check if the API token is available
if not HUGGINGFACEHUB_API_TOKEN:
    print("❌ HUGGINGFACEHUB_API_TOKEN not found in environment variables. Please set it.")
    exit(1)

# SPECIFY A MODEL EXPLICITLY, and ensure it's suitable for text generation.
model_id = "google/flan-t5-xxl"  # The flan-t5-xxl Model still does not support text2text-generation directly.  This has to change

# Try different Text generation model
model_id = "google/flan-t5-base"  #  It's also efficient, though less capable.  Change this with a LLama, Falcon, or other model if possible
# model_id = "meta-llama/Llama-2-7b-hf" # requires authentication and more setup
model_id = "meta-llama/Llama-3.3-70B-Instruct"
llm = HuggingFaceHub(repo_id=model_id, model_kwargs={"temperature":0.2, "max_length": 512}, huggingfacehub_api_token=HUGGINGFACEHUB_API_TOKEN)

# llm = ChatOpenAI(
#     model="gpt-3.5-turbo",
#     openai_api_key=os.getenv("OPENAI_API_KEY"))  # API key for query generation


# ✅ Initialize OpenAI Embedding for Vector DB (Separate Key)
# embedding_function = OpenAIEmbeddings(
#     openai_api_key=os.getenv("OPENAI_API_KEY_VECTORDB")  # API key for Chroma embedding
# )
def extract_sql_query(response):
    # Check if the response contains the SQL code block
    if "```sql" in response:
        # Split the response to isolate the SQL code
        parts = response.split("```sql")
        if len(parts) > 1:
            sql_code = parts[2].split("```")[0].strip()  # Remove any remaining ```
            return sql_code
    # If no SQL code block is found, return None
    return None

# def execute_query(question):
#     """Generate and execute SQL query with schema context."""
#     try:
#         # Retrieve schema details
#         schema_info = fetch_schema(question)

#         print("Retrieved Schema info or table infos are: ", schema_info)
#         return schema_info
#     except Exception as e:
#         print(f"❌ Error generating and executing query: {e}")
#         return None, None
    
def execute_query(question):
    """Generate and execute SQL query with schema context."""
    try:
        # Retrieve schema details
        schema_info = fetch_schema(question)

        print("Retrieved Schema info or table infos are: ", schema_info)

        # Generate SQL query with proper schema knowledge
        prompt = f"""
        Given the following table schema details:
        {schema_info}

        and

        Generate a **valid SQL query** for the following question:
        {question}

        Ensure that:
        - **Correct tables are used**
        - **Joins are included** if multiple tables are involved
        - **WHERE conditions** are properly formatted

        Return ONLY the SQL query, surrounded by triple backticks (```sql ... ```). Do not include any other text.
        """

        # Call the LLM to generate the query
        #  Using the .predict method because we are no longer using ChatOpenAI
        response = llm.predict(prompt)
        # Print the full response to inspect its structure
        print("Full Response:", response)
        logger.info(f"Original Queryyy: {response}")
        # cleaned_query = clean_sql_query(response)

        cleaned_query = extract_sql_query(response) # Set cleaned query to result
        logger.info(f"Cleaned Queryyy: {cleaned_query}")
        print('cleaned_queryyy',cleaned_query)
        print("Generated Query:\n", cleaned_query)

        # Execute the generated query
        try:
            result = db.run(cleaned_query)
            return cleaned_query, result
        except Exception as e:
            print(f"❌ Error executing the query: {e}")
            print(f"Query: {cleaned_query}")  # Print the failing query
            return cleaned_query, None # Correct return value
    except Exception as e:
        print(f"❌ Error generating and executing query: {e}")
        return None, None