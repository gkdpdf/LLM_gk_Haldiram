import json, re
from langchain_openai import ChatOpenAI

llm = ChatOpenAI(model="gpt-4o", temperature=0)

def rewrite_sql_query(state: dict) -> dict:
    user_query = state.get("user_query", "")
    prev_sql = state.get("sql_query", "")
    error_msg = state.get("error_message", "")
    tables = state.get("tables", [])
    retry_count = int(state.get("retry_count", 0))

    messages = [
        {
            "role": "system",
            "content": (
                "You are an expert SQL fixer for PostgreSQL. "
                "Given a user intent, allowed tables, the failing SQL, and the error, "
                "return ONLY a corrected SQL query. Do not include explanations. "
                "If the query references product name in tbl_product_master, use base_pack_design_name."
            ),
        },
        {"role": "user",
         "content": json.dumps({
             "user_intent": user_query,
             "tables_allowed": tables,
             "previous_sql": prev_sql,
             "error_message": error_msg
         })}
    ]

    try:
        resp = llm.invoke(messages)
        fixed_sql = resp.content.strip()
        fixed_sql = re.sub(r"^```sql\s*|^```\s*|```$", "", fixed_sql, flags=re.I|re.M).strip()
        state["sql_query"] = fixed_sql
    except Exception as e:
        state["error_message"] = f"Rewrite failed: {e}"

    state["retry_count"] = retry_count + 1
    return state
