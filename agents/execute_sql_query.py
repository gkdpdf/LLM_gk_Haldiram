# -*- coding: utf-8 -*-
from pathlib import Path
import os
import re
import glob
import pandas as pd

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_openai import ChatOpenAI
from langchain_community.utilities import SQLDatabase
from dotenv import load_dotenv

# =========================================================
# Setup
# =========================================================
load_dotenv(override=True)

# =========================================================
# DB bootstrap: load all CSVs into Postgres and return engine + LC SQLDatabase
# =========================================================
def configure_db_postgres():
    # ✅ Create PostgreSQL engine using psycopg2
    pg_engine = create_engine(
        "postgresql+psycopg2://postgres:12345678@localhost:5432/LLM_Haldiram_primary"
    )

    csv_folder = Path.cwd() / "cooked_data_gk"
    for csv_file in glob.glob(str(csv_folder / "*.csv")):
        table_name = Path(csv_file).stem.lower()
        df = pd.read_csv(csv_file)
        # ✅ Save each CSV as table in PostgreSQL
        df.to_sql(name=table_name, con=pg_engine, index=False, if_exists="replace")
        print(f"✅ Loaded table: {table_name}")

    # ✅ LangChain-compatible Postgres connection
    db = SQLDatabase.from_uri(
        "postgresql+psycopg2://postgres:12345678@localhost:5432/LLM_Haldiram_primary"
    )
    return pg_engine, db

# 🔌 Connect to DB and print tables (use non-deprecated API)
pg_engine, db = configure_db_postgres()
print("📄 Tables Loaded:", db.get_usable_table_names())

# =========================================================
# (Optional) LLM summarizer for results
# =========================================================
llm = ChatOpenAI(model="gpt-4o", temperature=0)

result_summary_prompt = ChatPromptTemplate.from_messages([
    ("system", "You are a helpful assistant that summarizes PostgreSQL query results for humans."),
    ("human", "Here is the PostgreSQL query: \n{sql_query}\n\nAnd here are the results:\n{query_result}\n\nPlease summarize them in a human-readable format.")
])
result_summary_chain = result_summary_prompt | llm | StrOutputParser()

# Toggle prose summaries
USE_LLM_SUMMARY = False  # set True if you want a human summary instead of raw table/scalar

# =========================================================
# Query executor
# =========================================================
def execute_sql_query(state: dict) -> dict:
    """
    Expects state["sql_query"].
    Executes SELECT safely against Postgres, returns:
      - scalar if 1x1 result
      - pretty table string otherwise
      - or 'No data found' if empty
    Optionally summarizes with LLM if USE_LLM_SUMMARY = True.
    """
    sql_query = (state.get("sql_query") or "").strip()
    if not sql_query:
        state["final_answer"] = "No SQL to execute."
        state["query_result"] = state["final_answer"]
        return state

    # Safety: allow only SELECTs
    if not re.match(r"^\s*select\b", sql_query, flags=re.IGNORECASE):
        state["final_answer"] = "Only SELECT queries are allowed."
        state["query_result"] = state["final_answer"]
        return state

    try:
        with pg_engine.connect() as conn:
            df = pd.read_sql(text(sql_query), conn)

        # No rows
        if df.empty:
            out = "No data found for your query."
            state["query_result"] = out
            state["final_answer"] = out
            return state

        # Single scalar (1x1)
        if df.shape == (1, 1):
            val = df.iat[0, 0]
            out = "NULL" if pd.isna(val) else str(val)
            state["query_result"] = out
            state["final_answer"] = out
            return state

        # Multi-row/col → to_string table
        table_str = df.to_string(index=False, max_cols=20, max_rows=50)

        if USE_LLM_SUMMARY:
            summary = result_summary_chain.invoke({
                "sql_query": sql_query,
                "query_result": table_str
            })
            state["query_result"] = table_str
            state["final_answer"] = summary
        else:
            state["query_result"] = table_str
            state["final_answer"] = table_str

    except Exception as e:
        err = f"❌ Error executing query: {e}"
        state["query_result"] = err
        state["final_answer"] = err

    return state

# =========================================================
# Example usage
# =========================================================
if __name__ == "__main__":
    # Example: sum query to get a scalar
    state = {
        "sql_query": "SELECT SUM(invoiced_total_quantity) AS total_qty FROM tbl_primary;"
    }
    print(execute_sql_query(state)["final_answer"])

    # Example: non-aggregated
    state2 = {
        "sql_query": "SELECT product_name, invoiced_total_quantity FROM tbl_primary WHERE product_name ILIKE '%bubble gum%' LIMIT 10;"
    }
    print(execute_sql_query(state2)["final_answer"])
