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


def configure_db_postgres():
    # ✅ Create PostgreSQL engine using psycopg2
    pg_engine = create_engine(
        "postgresql+psycopg2://postgres:12345678@localhost:5432/LLM_Haldiram"
    )

    csv_folder = Path.cwd() / "cooked_data_gk"
    for csv_file in glob.glob(str(csv_folder / "*.csv")):
        table_name = Path(csv_file).stem.lower()
        df = pd.read_csv(csv_file)

        # ✅ Save each CSV as table in PostgreSQL
        df.to_sql(name=table_name, con=pg_engine, index=False, if_exists="replace")
        print(f"✅ Loaded table: {table_name}")

    # ✅ Return LangChain-compatible PostgreSQL connection
    return pg_engine, SQLDatabase.from_uri(
        "postgresql+psycopg2://postgres:12345678@localhost:5432/LLM_Haldiram"
    )

# 🔌 Connect to DB and print tables
pg_engine, db = configure_db_postgres()
print("📄 Tables Loaded:", db.get_table_names())



llm = ChatOpenAI(model="gpt-4o", temperature=0)

result_summary_prompt = ChatPromptTemplate.from_messages([
    ("system", "You are a helpful assistant that summarizes SQL query results for humans."),
    ("human", "Here is the SQL query: \n{sql_query}\n\nAnd here are the results:\n{query_result}\n\nPlease summarize them in a human-readable format.")
])

result_summary_chain = result_summary_prompt | llm | StrOutputParser()

def execute_sql_query(state: dict) -> dict:
    sql_query = state["sql_query"]
    try:
        raw_result = db.run(sql_query)
        # Generate human-readable summary
        summary = result_summary_chain.invoke({
            "sql_query": sql_query,
            "query_result": raw_result
        })
        state["query_result"] = summary
    except Exception as e:
        state["query_result"] = f"❌ Error: {e}"
    return state

