# agents/sql_cleaned_query_agent.py
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from dotenv import load_dotenv
import os

load_dotenv(override=True)

with open("annotated_schema_haldiram_primary.md", "r", encoding="utf-8") as f:
    schema_markdown = f.read()

llm = ChatOpenAI(model="gpt-4o", temperature=0)
# llm = ChatOpenAI(model="gpt-oss-120B", temperature=0)
query_clean_prompt = ChatPromptTemplate.from_messages([
    ("system", f"""
You are a product manager for a Text-to-SQL system.

You will be given:
- A database schema (markdown format)
- A user's natural language query

Your job is to:
1. Rewrite the user's query more precisely using correct table/column names from the schema
2. Preserve the original intent of the user
3. Ensure that the query uses exact column or table names found in the schema when possible

Do NOT generate SQL. The output must remain in natural language but schema-aligned.

Schema:
{schema_markdown}
"""),
    ("human", "User query: {user_query}")
])

chain = query_clean_prompt | llm | StrOutputParser()

# ✅ your relevance keywords (tweak as needed)
RELEVANT_KEYWORDS = {
    "invoice", "invoiced", "sales", "revenue", "product", "sku", "stock",
    "inventory", "customer", "retailer", "order", "quantity", "bill",
    "scheme", "distributor", "super_stockist"
}

def clean_query_node(state: dict) -> dict:
    user_query = state.get("user_query", "") or ""
    cleaned = chain.invoke({"user_query": user_query})
    state["cleaned_user_query"] = cleaned

    # Simple heuristic relevance check on the *cleaned* query
    q = cleaned.lower()
    if not any(k in q for k in RELEVANT_KEYWORDS):
        # 🔚 mark final answer so the graph routes to END
        state["final_answer"] = "No info available"

    return state
