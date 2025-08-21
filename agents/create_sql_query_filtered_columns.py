from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import StrOutputParser, PydanticOutputParser
from pydantic import BaseModel, Field, RootModel
from typing import List, Union, Literal
from langchain_core.prompts import ChatPromptTemplate
from tools.date_tool import get_current_date
from dotenv import load_dotenv
import pickle
import json
import re
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

load_dotenv(override=True)

def get_today_str():
    return date.today().strftime("%Y-%m-%d")  



with open ("kb_haldiram_primary_azam.pkl", "rb") as f:
    total_table_dict = pickle.load(f)
    
with open("relationship_tables.txt", "r", encoding="utf-8") as f:
    relationship_json_str = f.read()


def filter_dict_tables(total_table_dict, tables):
    filtered_dict = {k:total_table_dict[k] for k in tables if k in total_table_dict}
    return filtered_dict

# Create pydantic parser for the filtered columns
class FilterNo(RootModel[List[Literal["no"]]]):
    """Represents the no-filter case: exactly ['no']"""
    pass

class FilterYes(RootModel[List[Union[str, List[str]]]]):
    """Represents yes + one or more [table, column, value] triplets"""
    pass
    
class SQLQueryWithFilters(BaseModel):
    sql_query : str
    filters : Union[FilterNo, FilterYes]
    
def create_markdown_from_dict(annotated_dict: dict) -> str:
    """
    Converts a dictionary of annotated table descriptions into markdown format.

    Args:
        annotated_dict (dict): A dictionary where keys are table names and values are JSON-like strings.

    Returns:
        str: Markdown-formatted string.
    """
    markdown_blocks = []
    for table_name, annotated_text in annotated_dict.items():
        markdown_block = f"### **{table_name}**\n```json\n{annotated_text}\n```\n"
        markdown_blocks.append(markdown_block)
    
    return "\n\n".join(markdown_blocks)

llm = ChatOpenAI(model="gpt-4o", temperature=0)

today = get_today_str()

def create_sql_query(state: dict) -> dict:
    sql_tables = state["tables"]
    user_query = state["cleaned_user_query"]

    # Filter schema and format
    filtered_dict = filter_dict_tables(total_table_dict, sql_tables)
    filtered_markdown = create_markdown_from_dict(filtered_dict)
    
    FORMAT_SHAPE = """{{
  "sql_query": "<runnable PostgreSQL SQL only>",
  "filters": ["no"] OR ["yes", ["<table>","<column>","<value(s)>"], ...]
}}""".strip()


    sql_query_prompt = ChatPromptTemplate.from_messages([
        ("system", f"""
    You are an expert in SQL and relational databases.

    Your job is to produce TWO things from the user's request:
    A) A correct, runnable SQL query for **PostgreSQL** (use only the provided tables/columns).
    B) A FILTER LIST describing string-column filters implied by the user's question, in EXACTLY one of these forms:
    - ["yes", ["<table>","<column>","<values exactly as in user question or mapped from samples>"], ...]
    - ["no"]

    You should take help of these things to write a **correct SQL query** using:
    1. The user query.
    2. The schema of the database (with tables, columns, and datatypes).
    3. The relationships between the tables (foreign key joins).

    Current date time is : {today}

    Hard rules:
    1) Use ONLY table and column names that appear verbatim in the provided schema. Do not invent or abbreviate names. If the user asks for a metric, map it to the exact matching column name from the schema.
    2) For KPI thresholds FIRST aggregate with GROUP BY, THEN filter using HAVING.
    3) Use COALESCE on numeric aggregates to avoid NULLs.
    4) **Dates in Postgres**:
    - Today: CURRENT_DATE or now()
    - Start of current month: date_trunc('month', CURRENT_DATE)
    - Last month range:
        bill_date >= date_trunc('month', CURRENT_DATE - INTERVAL '1 month')
        AND bill_date < date_trunc('month', CURRENT_DATE)
    - Intervals: INTERVAL '7 days', INTERVAL '1 month'
    - Convert string to date: TO_DATE(string, 'YYYY-MM-DD')

    FILTER LIST RULES (string columns only):
    - Consider only **string-type** columns that narrow the dataset (e.g., city, state, channel, payment_type).
    - If the user mentions values that differ from sample values (e.g., "New York" vs DB uses "NY"), **suggest values mapped to samples** when obvious from samples.
    - If user says "credit card or boleto", output "credit card, boleto".
    - Do NOT include IDs, order_id, customer_id, product_id, etc., unless explicitly filtered in the question.
    - If only numeric/date constraints are implied (or none), return ["no"].

    OUTPUT FORMAT (STRICT):
    Return **exactly one minified JSON object** (no markdown, no code fences, no extra text) that conforms to:
    {FORMAT_SHAPE}

    Constraints:
    - The JSON must be valid and parseable.
    - "sql_query" must be a string.
    - "filters" must be either ["no"] OR an array starting with "yes" followed by one or more 3-element string arrays.
    - Do NOT include explanations or comments outside the JSON.
    - Do NOT wrap the JSON in backticks.

    📚 Schema:
    {filtered_markdown}

    🔗 Table Relationships:
    {relationship_json_str}
        """),
        ("human", "User query: {user_query}\nOnly use these tables: {tables}")
    ])

    
    
    parser = PydanticOutputParser(pydantic_object=SQLQueryWithFilters)

    chain = sql_query_prompt | llm | parser

    raw_output = chain.invoke({
        "user_query": user_query,
        "tables": ", ".join(sql_tables)
    })

    # Update state
    state["sql_query"] = raw_output.sql_query
    state["filter_extractor"] = raw_output.filters
    return state
