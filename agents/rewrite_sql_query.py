from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import StrOutputParser
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

def rewrite_sql_query(state: dict) -> dict:
    # Inputs from state
    state["retry_count"] = state.get("retry_count", 0) + 1
    sql_tables    = state["tables"]
    user_query    = state["cleaned_user_query"]
    failed_query  = state["failed_query"]  # support either key
    error_message = state["query_results"] # support either key

    # Filter schema (your helpers assumed to exist)
    filtered_dict      = filter_dict_tables(total_table_dict, sql_tables)
    filtered_markdown  = create_markdown_from_dict(filtered_dict)

    # Build prompt (one prompt that works with or without failure context)
    sql_query_prompt = ChatPromptTemplate.from_messages([
        ("system", f"""
You are an expert in SQL and relational databases.

Your job: write a CORRECT SQL query using:
1) The user's request
2) The provided schema (tables, columns, datatypes)
3) The table relationships (foreign keys)

Based on the error message of the  previous error message and previous SQL query, FIX the issues and produce a corrected query.

Hard rules:
1) Use ONLY table/column names that appear verbatim in the schema. Do not invent/abbreviate names.
2) For KPI thresholds: FIRST aggregate with GROUP BY, THEN filter using HAVING.
3) Use COALESCE on numeric aggregates to avoid NULLs.
4) Date handling (Postgres examples):
   - Today: CURRENT_DATE or now()
   - Start of current month: date_trunc('month', CURRENT_DATE)
   - Last month range:
     bill_date >= date_trunc('month', CURRENT_DATE - INTERVAL '1 month')
     AND bill_date <  date_trunc('month', CURRENT_DATE)
   - Intervals: INTERVAL '7 days', INTERVAL '1 month'
   - String→date: TO_DATE(string, 'YYYY-MM-DD')

Return ONLY a valid SQL statement as plain text. No markdown, no comments, no prose.

📚 Schema:
{filtered_markdown}

🔗 Table Relationships:
{relationship_json_str}
        """),
        ("human", """
User query:
{user_query}

Use ONLY these tables:
{tables}

Previous SQL (this query was giving error while executing):
{previous_sql}

Error message from database (may be empty if none):
{error_message}
""")
    ])

    chain = sql_query_prompt | llm | StrOutputParser()

    output = chain.invoke({
        "user_query": user_query,
        "tables": ", ".join(sql_tables),
        "previous_sql": failed_query,
        "error_message": error_message 
    })

    # Strip accidental code fences if any
    cleaned_sql_query = re.sub(r"^```sql\s*|^```\s*|```$", "", output.strip(), flags=re.IGNORECASE).strip()
    state["sql_query"] = cleaned_sql_query
    return state