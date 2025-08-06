from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from dotenv import load_dotenv
import json

load_dotenv(override=True)

# 🔹 Load schema markdown
with open("annotated_schema_haldiram.md", "r") as f:
    schema_markdown = f.read()

# 🔹 Allowed table names
allowed_tables = {
    "tbl_closingstockdb",
    "tbl_primary",
    "tbl_product_master",
    "tbl_secondary",
    "tbl_ss_closingstocksuper",
    "tbl_ss_db_superstockist",
    "tbl_ss_delhi_db",
    "tbl_ss_shipment"
}

# 🔹 Common alias corrections (add more if needed)
alias_map = {
    "product_master": "tbl_product_master",
    "primary": "tbl_primary",
    "secondary": "tbl_secondary",
    "shipment": "tbl_ss_shipment",
    "closingstockdb": "tbl_closingstockdb",
    "ss_closingstocksuper": "tbl_ss_closingstocksuper",
    "ss_db_superstockist": "tbl_ss_db_superstockist",
    "ss_delhi_db": "tbl_ss_delhi_db"
}

def fix_aliases(output: list[str]) -> list[str]:
    return [alias_map.get(t, t) for t in output]

# 🔹 LLM setup
llm = ChatOpenAI(model="gpt-4o", temperature=0)

# 🔹 Prompt setup
query_clean_prompt = ChatPromptTemplate.from_messages([
    ("system", f"""
You are an expert in SQL and relational databases.

Given a user query and a markdown-formatted database schema, return a valid **JSON list** of only the table names required to answer the query.

⚠️ ONLY return a list like:
["tbl_product_master", "tbl_secondary"]

⚠️ Do not explain anything or return aliases.
⚠️ Do not include markdown or bullet points.
⚠️ Use only exact matches from this list:

{sorted(list(allowed_tables))}

Schema:
{schema_markdown}
    """),
    ("human", "User query: {user_query}")
])

# 🔹 Chain with StrOutputParser
chain = query_clean_prompt | llm | StrOutputParser()

# 🔹 Node function
def find_tables_node(state: dict) -> dict:
    user_query = state["cleaned_user_query"]
    raw_output = chain.invoke({"user_query": user_query})

    try:
        # Parse JSON safely
        output_list = json.loads(raw_output)
    except json.JSONDecodeError:
        raise ValueError(f"❌ Failed to parse output as JSON list:\n{raw_output}")

    # Normalize via alias map
    fixed_tables = fix_aliases(output_list)

    # Final validation against allowed tables
    valid_tables = [tbl for tbl in fixed_tables if tbl in allowed_tables]

    if not valid_tables:
        raise ValueError(f"❌ No valid tables found in output: {fixed_tables}")

    state["tables"] = valid_tables
    return state
