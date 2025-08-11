from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from dotenv import load_dotenv
import json, re

load_dotenv(override=True)

with open("annotated_schema_haldiram_primary.md", "r") as f:
    schema_markdown = f.read()

allowed_tables = {
    "tbl_distributor_master",
    "tbl_primary",
    "tbl_product_master",
    "tbl_superstockist_master"
}

alias_map = {
    "product_master": "tbl_product_master",
    "primary": "tbl_primary",
    "distributor_master": "tbl_distributor_master",
    "superstockist_master": "tbl_superstockist_master"
}

def fix_aliases(output: list[str]) -> list[str]:
    return [alias_map.get(t, t) for t in output]

llm = ChatOpenAI(model="gpt-4o", temperature=0)

query_clean_prompt = ChatPromptTemplate.from_messages([
    ("system", f"""
You are an expert in SQL and relational databases.

Given a user query and a markdown-formatted database schema, return a valid **JSON list** of only the table names required to answer the query.

ONLY return a list like:
["tbl_product_master", "tbl_primary"]

Do not explain anything or return aliases.
Use only exact matches from this list:

{sorted(list(allowed_tables))}

Schema:
{schema_markdown}
    """),
    ("human", "User query: {user_query}")
])

chain = query_clean_prompt | llm | StrOutputParser()

def _fallback_tables(user_query: str) -> list[str]:
    q = user_query.lower()
    chosen = set()

    # Always include the main fact table
    chosen.add("tbl_primary")

    # Heuristics for common joins
    if any(k in q for k in ["product", "sku", "item", "pack", "bhujia", "base_pack", "design"]):
        chosen.add("tbl_product_master")
    if "distributor" in q:
        chosen.add("tbl_distributor_master")
    if "super stockist" in q or "superstockist" in q:
        chosen.add("tbl_superstockist_master")

    # Clamp to allowed set; ensure at least tbl_primary
    picked = [t for t in chosen if t in allowed_tables]
    return picked or ["tbl_primary"]

def find_tables_node(state: dict) -> dict:
    user_query = state["cleaned_user_query"]
    raw_output = (chain.invoke({"user_query": user_query}) or "").strip()

    # Strip code fences if model wrapped the JSON
    raw_output = re.sub(r"^```json\s*|^```\s*|```$", "", raw_output, flags=re.I | re.M).strip()

    output_list = None
    try:
        candidate = json.loads(raw_output)
        if isinstance(candidate, list):
            output_list = [str(t) for t in candidate]
    except json.JSONDecodeError:
        output_list = None

    if output_list:
        fixed_tables = fix_aliases(output_list)
        valid_tables = [tbl for tbl in fixed_tables if tbl in allowed_tables]
        if not valid_tables:
            valid_tables = _fallback_tables(user_query)
    else:
        valid_tables = _fallback_tables(user_query)

    state["tables"] = valid_tables
    return state
