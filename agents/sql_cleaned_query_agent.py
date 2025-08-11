# agents/sql_cleaned_query_agent.py
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from dotenv import load_dotenv
import re

load_dotenv(override=True)

with open("annotated_schema_haldiram_primary.md", "r", encoding="utf-8") as f:
    schema_markdown = f.read()

# LLM used to clean/schema-align queries
llm = ChatOpenAI(model="gpt-4o", temperature=0)

# Lightweight LLM for general Q&A answers (non-DB)
qa_llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

# --- Prompt for schema-aligned cleaning ---
# (returns natural-language rewrite OR the literal token OUT_OF_SCOPE)
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
- If the user's question is NOT related to the provided schema (e.g., general knowledge, jokes, songs, movies, what is LLM, etc.),
  return EXACTLY this token (without quotes): OUT_OF_SCOPE
- Do NOT invent columns or tables that are not in the schema.
- Do NOT generate SQL. Output must remain natural-language and schema-aligned when in scope.

Schema:
{schema_markdown}
"""),
    ("human", "User query: {user_query}")
])

chain = query_clean_prompt | llm | StrOutputParser()

# Heuristics
GREETING_PATTERNS = re.compile(
    r"^(hi|hello|hey|hlo|good\s*(morning|afternoon|evening)|what'?s up|how are you)\b",
    re.IGNORECASE
)
NON_DB_PATTERNS = re.compile(
    r"\b(can you|sing|dance|tell me a joke|who are you|what is your name|do you love me|"
    r"like|dislike|song|movie|film|lyrics|bollywood|actor|actress|saiyaara)\b",
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
    """Answer non-DB queries concisely and append a sales data CTA line."""
    msgs = [
        {"role": "system", "content": (
            "You are a helpful assistant. Answer concisely (<= 120 words). "
            "Be clear and neutral."
        )},
        {"role": "user", "content": user_query}
    ]
    resp = qa_llm.invoke(msgs)
    base = (resp.content or "").strip()
    # Always append a one-line sales data example (CTA)
    if base.endswith(("?", ".", "!", "…")):
        return f"{base}\n\n{EXAMPLE_SALES_QUERY}"
    return f"{base}\n\n{EXAMPLE_SALES_QUERY}"

def clean_query_node(state: dict) -> dict:
    user_query = (state.get("user_query") or "").strip()

    # Quick guard: greet/chit-chat → general answer + CTA (no SQL)
    if GREETING_PATTERNS.search(user_query) or NON_DB_PATTERNS.search(user_query):
        state["final_answer"] = True
        state["query_result"] = _general_answer_with_cta(user_query)
        return state

    cleaned = (chain.invoke({"user_query": user_query}) or "").strip()

    # If cleaner declares OUT_OF_SCOPE → general answer + CTA (no SQL)
    if cleaned == "OUT_OF_SCOPE":
        state["final_answer"] = True
        state["query_result"] = _general_answer_with_cta(user_query)
        return state

    # Last guard: if not obviously business-like, answer generally + CTA
    if not any(tok in cleaned.lower() for tok in BUSINESS_TOKENS):
        state["final_answer"] = True
        state["query_result"] = _general_answer_with_cta(user_query)
        return state

    # Otherwise proceed to table-finding → SQL
    state["cleaned_user_query"] = cleaned
    state["final_answer"] = False
    return state
