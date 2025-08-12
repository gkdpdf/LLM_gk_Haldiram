from pathlib import Path
import os
import pandas as pd
import re
import requests
import glob


from sqlalchemy import create_engine,  text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.types import Integer, Float, String

from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_openai import ChatOpenAI
from langchain_community.utilities import SQLDatabase
from dotenv import load_dotenv
load_dotenv(override=True)


# def configure_db():
#     # ✅ Create MySQL engine using pymysql
#     mysql_engine = create_engine(
#         "mysql+pymysql://root:Iameighteeni%4018@127.0.0.1:3306/txt2sql"
#     )

#     csv_folder = Path.cwd() / "cooked_data_gk"
#     for csv_file in glob.glob(str(csv_folder / "*.csv")):
#         table_name = Path(csv_file).stem.lower()
#         df = pd.read_csv(csv_file)

#         # ✅ Save each CSV as table in MySQL
#         df.to_sql(name=table_name, con=mysql_engine, index=False, if_exists="replace")
#         print(f"✅ Loaded table: {table_name}")

#     # ✅ Return LangChain-compatible MySQL connection using pymysql
#     return mysql_engine,SQLDatabase.from_uri(
#         "mysql+pymysql://root:Iameighteeni%4018@127.0.0.1:3306/txt2sql"
#     )

# # 🔌 Connect to DB and print tables
# mysql_engine, db = configure_db()
# print("📄 Tables Loaded:", db.get_table_names())

import urllib.parse 

# Load engine and knowledge base
password = urllib.parse.quote_plus("Iameighteeni@18")


def return_db_postgres():
    # ✅ Return LangChain-compatible PostgreSQL connection
    return SQLDatabase.from_uri(
        f"postgresql+psycopg2://postgres:{password}@localhost:5432/LLM_Haldiram_primary"
    )

# 🔌 Connect to DB and print tables
db = return_db_postgres()
print("📄 Tables Loaded:", db.get_table_names())



llm = ChatOpenAI(model="gpt-4o", temperature=0)

result_summary_prompt = ChatPromptTemplate.from_messages([
    ("system", 
     "You are a helpful assistant that summarizes SQL query results for a WhatsApp chatbot. "
     "Your summaries must be very concise, use plain language, and fit in as little short sentences as possible. "
     "Avoid technical details or SQL jargon or user query. Focus only on the key insight."
     "If the query results are empty, clearly say that no relevant data was found and suggest the user try a different or more specific query."),
    ("human", 
     "SQL query:\n{sql_query}\n\nResults:\n{query_result}\n\n"
     "Reply with a short WhatsApp-friendly summary of the results.")
])

result_summary_chain = result_summary_prompt | llm | StrOutputParser()

def execute_sql_query(state: dict) -> dict:
    sql_query = state["sql_query"]
    try:
        raw_result = db.run(sql_query)
        state["dataframe"] = raw_result
        state["query_results"] = "Query executed successfully"
    except Exception as e:
        state["query_results"] = "Query execution failed"
        state["query_error_message"] = [f"❌ Error: {e}"]
        state["failed_query"] = [sql_query]
    return state

