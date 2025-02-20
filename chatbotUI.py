import streamlit as st
from main import fetch_schema,execute_query


###### AFTER THE ui IS RENDERED START THE DB CONNECTION ####

st.title("SQL Chatbot with Gemini & Vector DB")

question = st.text_input("Ask your question:")

if st.button("Execute"):
    if question:
        schema_info = fetch_schema(question)
        cleaned_query, query_result = execute_query(question)

        if cleaned_query and query_result is not None:
            st.write("Generated SQL Query:")
            st.code(cleaned_query, language="sql")
            st.write("Query Result:")
            st.write(query_result)
        else:
            st.write("Error generating query.")
    else:
        st.write("Please enter a question.")


### ON CLOSING THE APPLICSTION PLEASE CLOSE THE db CONNECTION ####
