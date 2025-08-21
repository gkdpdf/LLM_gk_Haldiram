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


llm = ChatOpenAI(model="gpt-4o", temperature=0)


def add_filter_sql_query(state: dict) -> dict:
    # Inputs from state
    query = state["sql_query"]
    filters = state["fuzz_match"]  # e.g. ["yes", ["orders","payment_type","credit card, boleto"], ...]

    # Early exit if no filters
    if not filters or (isinstance(filters, list) and len(filters) == 1 and str(filters[0]).lower() == "no"):
        return state

    # Normalize: if string, try to parse; else ensure JSON serializable
    if isinstance(filters, str):
        try:
            filters = json.loads(filters)
        except Exception:
            # keep as raw string if already JSON-like; LLM will handle
            pass

    filters_json = json.dumps(filters, ensure_ascii=False)

    sql_query_prompt = ChatPromptTemplate.from_messages([
        ("system",
         "You are an expert SQL rewriter for PostgreSQL.\n"
         "Task: Given an original SQL SELECT query and a JSON filter list, return a new SQL query that replaces the filter values.\n"
         "Output MUST be SQL only (no markdown, no backticks, no commentary)."),
        ("system",
         "Rewrite rules:\n"
         "1) Do NOT rename or remove existing tables, columns, joins, or predicates.\n"
         "4) Respect existing table aliases (e.g., use 'tp.channel' not 'tbl_Primary.channel').\n"
         "5) Only apply string filters from the filter list. For multiple values on the same (table, column), use IN ('v1','v2',...).\n"
         "6) Filter list shape is one of:\n"
         "   - [\"no\"]\n"
         "   - [\"yes\", [\"<table>\",\"<column>\",\"<value(s) comma-separated>\"], ...]\n"
         "   It may also appear as: [\"yes\", [\"table name:<t>\",\"column_name:<c>\",\"filter_value:<v>\"], ...].\n"
         "7) Trim values and keep them as string literals; escape single quotes with doubled quotes (e.g., O''Reilly).\n"
         "9) Do NOT add numeric/date conditions—only the provided string filters.\n"
         "10) Return one single-line SQL string."),
        ("human",
         "Original SQL:\n{query}\n\n"
         "Filters (JSON):\n{filters_json}\n\n"
         "Return: SQL only.")
    ])

    chain = sql_query_prompt | llm | StrOutputParser()
    output = chain.invoke({
        "query": query,
        "filters_json": filters_json
    })

    # Remove accidental fences if any
    cleaned_sql_query = re.sub(r"^```(?:sql)?\s*|\s*```$", "", output.strip(), flags=re.IGNORECASE).strip()

    state["sql_query"] = cleaned_sql_query
    return state