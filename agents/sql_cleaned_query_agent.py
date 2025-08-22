# agents/sql_cleaned_query_agent.py
from __future__ import annotations
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from dotenv import load_dotenv
import re

load_dotenv(override=True)

with open("annotated_schema_haldiram_primary.md", "r", encoding="utf-8") as f:
    schema_markdown = f.read()

llm = ChatOpenAI(model="gpt-4o", temperature=0)
qa_llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

query_clean_prompt = ChatPromptTemplate.from_messages([
    ("system", f"""
You are a product manager for a Text-to-SQL system.

You will be given:
- A database schema (markdown format)
- A user's natural language query

Your job is to:
1) Rewrite the user's query more precisely using correct table/column names from the schema.
2) Preserve the original intent.
3) Ensure the query uses exact column/table names from the schema when possible.

CRITICAL:
- If the user's question is NOT related to the provided schema (e.g., general knowledge, jokes),
  return EXACTLY this token (without quotes): OUT_OF_SCOPE
- Do NOT invent columns or tables that are not in the schema.
- Do NOT generate SQL. Output must remain natural-language and schema-aligned.
Schema:
{schema_markdown}
"""),
    ("human", "User query: {user_query}")
])

chain = query_clean_prompt | llm | StrOutputParser()

GREETING_PATTERNS = re.compile(
    r"^(hi|hello|hey|hlo|good\s*(morning|afternoon|evening)|what'?s up|how are you)\b",
    re.IGNORECASE
)
NON_DB_PATTERNS = re.compile(
    r"\b(can you|sing|dance|tell me a joke|who are you|what is your name|do you love me|"
    r"like|dislike|song|movie|film|lyrics)\b",
    re.IGNORECASE
)
BUSINESS_TOKENS = {
    "sale","sales","invoice","invoiced","quantity","qty","revenue","amount",
    "sku","product","pack","stock","inventory","order","bill","distributor",
    "super_stockist","super stockist","region","zone","state","month","year",
    "volume","value","price","mrp"
}
EXAMPLE_SALES_QUERY = 'Try a sales question like: “Sales of Bhujia in the last 3 months.”'

def _general_answer_with_cta(user_query: str) -> str:
    msgs = [
        {"role": "system", "content": "Answer concisely (<= 120 words). Be clear and neutral."},
        {"role": "user", "content": user_query}
    ]
    resp = qa_llm.invoke(msgs)
    base = (resp.content or "").strip()
    if base.endswith(("?", ".", "!", "…")):
        return f"{base}\n\n{EXAMPLE_SALES_QUERY}"
    return f"{base}\n\n{EXAMPLE_SALES_QUERY}"

def clean_query_node(state: dict) -> dict:
    user_query = (state.get("user_query") or "").strip()

    if GREETING_PATTERNS.search(user_query) or NON_DB_PATTERNS.search(user_query):
        state["final_answer"] = True
        state["query_result"] = _general_answer_with_cta(user_query)
        return state

    cleaned = (chain.invoke({"user_query": user_query}) or "").strip()

    if cleaned == "OUT_OF_SCOPE":
        state["final_answer"] = True
        state["query_result"] = _general_answer_with_cta(user_query)
        return state

    if not any(tok in cleaned.lower() for tok in BUSINESS_TOKENS):
        state["final_answer"] = True
        state["query_result"] = _general_answer_with_cta(user_query)
        return state

    state["cleaned_user_query"] = cleaned
    state["final_answer"] = False
    return state
