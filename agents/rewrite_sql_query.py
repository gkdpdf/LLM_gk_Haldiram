# agents/rewrite_sql_query.py
from __future__ import annotations
import json, re
from typing import Dict
from langchain_openai import ChatOpenAI
try:
    with open("kb_haldiram_primary.pkl", "rb") as f:
        _KB: Dict[str, str] = pickle.load(f)
except Exception:
    _KB = {}

try:
    with open("relationship_tables.txt", "r", encoding="utf-8") as f:
        _REL_RAW = f.read()
except Exception:
    _REL_RAW = ""

_llm = ChatOpenAI(model="gpt-4o", temperature=0)

_SYSTEM_PROMPT = (
    "You are an expert SQL fixer for PostgreSQL. "
    "Given a user intent, a HARD allowlist of tables, the failing SQL, and the error, "
    "return ONLY a corrected SQL query. Do not include explanations or comments. "
    "CRITICAL RULES:\n"
    "1) Reference tables EXCLUSIVELY from tables_allowed. No other tables or CTEs.\n"
    "2) Prefer existing numeric measures in the chosen fact. Do not invent columns.\n"
    "3) If product naming needs product master, use base_pack_design_name in tbl_product_master.\n"
    "4) Keep syntax compatible with PostgreSQL.\n"
    f"5) While creating sql query ensure that columns and tables are existing in {_KB} and only use defined relationships in {_REL_RAW}\n"
)

def rewrite_sql_query(state: Dict) -> Dict:
    user_query = state.get("user_query", "")
    prev_sql = state.get("sql_query", "")
    error_msg = state.get("error_message", "")
    tables = state.get("tables", []) or state.get("allowed_tables", []) or []

    messages = [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {"role": "user", "content": json.dumps({
            "user_intent": user_query,
            "tables_allowed": tables,
            "previous_sql": prev_sql,
            "error_message": error_msg
        })}
    ]

    try:
        resp = _llm.invoke(messages)
        fixed_sql = (resp.content or "").strip()
        fixed_sql = re.sub(r"^```sql\s*|^```\s*|```$", "", fixed_sql, flags=re.I|re.M).strip()
        state["sql_query"] = fixed_sql
    except Exception as e:
        state["error_message"] = f"Rewrite failed: {e}"

    state["retry_count"] = int(state.get("retry_count", 0)) + 4
    return state
