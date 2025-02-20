import os
import chromadb
import pandas as pd
import re
from sentence_transformers import SentenceTransformer
import shutil  # For directory deletion
import uuid  # For generating unique IDs
import time  # For directory deletion retry
# --- Directory Deletion Functions ---
def remove_directory(path):
    """Robustly remove a directory, even if it's busy."""
    if os.path.exists(path):
        try:
            shutil.rmtree(path)
            print(f"Successfully deleted directory: {path}")
        except OSError as e:
            print(f"Error deleting directory {path}: {e}")
            print("Retrying directory deletion...")
            time.sleep(1)  # Wait a moment
            try:
                shutil.rmtree(path)  # Try again
                print(f"Successfully deleted directory on retry: {path}")
            except Exception as e2:
                print(f"Failed to delete directory {path} after retry: {e2}")

# --- End Directory Deletion Functions ---

# Load the schema Excel file (all sheets)
file_path = r"C:\Users\P Y T\Documents\PythonProjects\querygpt\table_schema.xlsx"
table_df = pd.read_excel(file_path, sheet_name="table_description")  # Table descriptions
column_df = pd.read_excel(file_path, sheet_name="column_description")  # Column metadata
query_df = pd.read_excel(file_path, sheet_name="sample_queries")  # Sample queries

# Combine metadata for better understanding

# Process Table Metadata
table_df["metadata"] = table_df.apply(lambda row:
    f"Table: {row['Table Name']}, Description: {row['Table Description']}", axis=1)

# Process Column Metadata with PK/FK relationships
column_df["metadata"] = column_df.apply(lambda row:
    f"Table: {row['Table Name']}, Column: {row['Column Name']}, Data Type: {row['Data Type']}, "
    f"Primary Key: {row.get('Primary Key', 'No')}, Foreign Key: {row.get('Foreign Key', 'None')}, "
    f"Description: {row.get('Description', 'No Info')}", axis=1)

# Process Sample Queries
query_df["metadata"] = query_df.apply(lambda row:
    f"Table: {row['Table Name']}, Sample Query: {row['Sample SQL Query']}", axis=1)

# Initialize vector database
chroma_client = chromadb.PersistentClient(path="./chroma_db")

# --- Explicitly Delete Collections If They Exist ---
table_collection_name = "table_collection"
column_collection_name = "column_collection"
query_collection_name = "query_collection"

def delete_collection_if_exists(client, name):
    try:
        client.delete_collection(name=name)
        print(f"Successfully deleted collection: {name}")
    except chromadb.errors.CollectionNotFound:
        print(f"Collection not found, skipping deletion: {name}")
    except Exception as e:
        print(f"Error deleting collection {name}: {e}")

delete_collection_if_exists(chroma_client, table_collection_name)
delete_collection_if_exists(chroma_client, column_collection_name)
delete_collection_if_exists(chroma_client, query_collection_name)
# --- End Explicit Collection Deletion ---

# --- Delete Existing Chroma Databases (VERY IMPORTANT!) ---
directories_to_delete = ["./table_db", "./column_db", "./query_db"]
for dir_path in directories_to_delete:
    remove_directory(dir_path)
# --- End Directory Deletion ---

# Choose an embedding model
embedding_model = SentenceTransformer('all-MiniLM-L6-v2')  # Using SBERT model

# Function to add data with IDs
def add_data_to_collection(collection, documents, embedding_model):
    ids = [str(uuid.uuid4()) for _ in range(len(documents))]  # Generate unique IDs
    embeddings = embedding_model.encode(documents, convert_to_tensor=True).tolist()  # Generate embeddings
    collection.add(documents=documents, embeddings=embeddings, ids=ids)

# Store metadata in Chroma
table_store = chroma_client.create_collection(name="table_collection")  # Explicitly create
add_data_to_collection(table_store, table_df["metadata"].tolist(), embedding_model)

column_store = chroma_client.create_collection(name="column_collection")  # Explicitly create
add_data_to_collection(column_store, column_df["metadata"].tolist(), embedding_model)

query_store = chroma_client.create_collection(name="query_collection")  # Explicitly create
add_data_to_collection(query_store, query_df["metadata"].tolist(), embedding_model)

# The chroma_client.close() method does not exist for PersistentClient. Remove this line.
# The client will release resources when it goes out of scope.
#chroma_client.close()

print("✅ Table, column, and query metadata stored in vector DBs!")

def fetch_schema(question):
    """Retrieve relevant schema details for a given query, including tables, columns, and sample queries."""
    # Step 1: Retrieve relevant tables

    # Re-initialize the Chroma client inside the function to ensure it's available.
    chroma_client_fetch = chromadb.PersistentClient(path="./chroma_db")
    table_store = chroma_client_fetch.get_collection(name="table_collection")
    column_store = chroma_client_fetch.get_collection(name="column_collection")
    query_store = chroma_client_fetch.get_collection(name="query_collection")

    table_results = table_store.query(query_texts=[question], n_results=2)
    table_context = table_results.get('documents', [])  # Handle potential missing 'documents' key and None value

    print("table_results", table_results)
    print('table_context',table_context)
    
    # Initialize an empty list to store relevant table names
    relevant_tables = []
    # Iterate over each description in table_context
    for doc in table_context[0]:
        # Use a regular expression to find the table name after 'Table: '
        match = re.search(r"Table:\s*(\S+)", doc)
        if match:
            # Append the table name to the relevant_tables list
            relevant_tables.append(match.group(1))
        else:
        # Handle the case where no match is found
           print(f"Warning: No table name found in description: {doc}")
     
    # Print the list of relevant tables
    print('relevant_tables',relevant_tables)

    column_results = []
    for table in relevant_tables:
        col_results = column_store.query(query_texts=[f"Columns of {table}"], n_results=5)
        column_results.extend(col_results.get('documents', []))
    column_context = column_results
    print('column_results',column_results)

    # Retrieve sample queries
    query_results = []
    for table in relevant_tables:
        q_results = query_store.query(query_texts=[f"Sample queries for {table}"], n_results=2)
        query_results.extend(q_results.get('documents', []))
    query_context = query_results        
    print('query_results',query_results)
    
    # # Extract table names
    # relevant_tables = []
    # if table_context and isinstance(table_context, list):  # Ensure table_context is a list and not empty
    #     relevant_tables = [doc.split(",")[0].split(":")[1].strip() if isinstance(doc, str) else "" for doc in table_context]  # Handle non-string documents
    #     relevant_tables = [table for table in relevant_tables if table] # Remove empty strings that result from non-string documents
    #     print('relevant_tables',relevant_tables)
    # else:
    #     print("Warning: No table context found for question:", question)

    # # Step 2: Retrieve relevant columns
    # column_results = []
    # for table in relevant_tables:
    #     col_results = column_store.query(query_texts=[f"{table} {question}"], n_results=2)
    #     column_results.extend(col_results.get('documents', []))
    # column_context = column_results
    # print('column_context',column_context)

    # # Step 3: Retrieve sample queries
    # query_results = []
    # for table in relevant_tables:
    #     q_results = query_store.query(query_texts=[f"{table} {question}"], n_results=2)
    #     query_results.extend(q_results.get('documents', []))
    # query_context = query_results

    # print('query_context',query_context)
    # table_context_str = [str(item) for item in relevant_tables]
    # column_context_str = [str(item) for item in column_results]

    table_context_str = "\n".join(str(item) for item in relevant_tables)
    column_context_str = "\n".join(str(item) for sublist in column_results for item in sublist)
    query_context_str = "\n".join(str(item) for sublist in query_results for item in sublist)

    context = "\n".join(table_context_str + column_context_str)
    context = f"{table_context_str}\n\n{column_context_str}\n\nSample Queriesss:\n{query_context_str}"
    print('contexttt',context)


    # Combine context for better query generation
    # context = "\n".join(table_context + column_context)

    # if query_context:
    #     context += "\n\nSample Queries:\n" + "\n".join(query_context_str)

    # Close the client after using it in this function
    #chroma_client_fetch.close() # no close method
    return context