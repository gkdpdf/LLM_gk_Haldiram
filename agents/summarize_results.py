import pandas as pd
import json, re
from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage

SUMMARY_MAX_ROWS_PREVIEW = 10
USE_LLM_SUMMARY = True

_llm = ChatOpenAI(model="gpt-4o", temperature=0)

def _basic_summary(df: pd.DataFrame) -> str:
    if df.empty:
        return "No rows matched your criteria."
    if df.shape == (1, 1):
        return f"Result: {df.iat[0,0]}"
    num_cols = df.select_dtypes(include="number").columns.tolist()
    if len(num_cols) == 1 and len(df) <= 10000:
        col = num_cols[0]
        total = df[col].sum()
        head = df.head(SUMMARY_MAX_ROWS_PREVIEW).to_string(index=False)
        return f"Rows: {len(df)} | Columns: {df.shape[1]}\n\n{head}\n\nΣ Total {col}: {total}"
    head = df.head(SUMMARY_MAX_ROWS_PREVIEW).to_string(index=False)
    return f"Rows: {len(df)} | Columns: {df.shape[1]}\n\n{head}"

def _llm_polish_summary(df: pd.DataFrame, user_query: str) -> str:
    try:
        sample = df.head(min(100, len(df)))
        records = sample.to_dict(orient="records")
        sys = SystemMessage(content=(
            "You summarize SQL results for business users. "
            "Given the user's question and a small sample of rows, produce a concise summary "
            "(1–5 bullets), highlight totals/trends. Keep under 120 words."
        ))
        hum = HumanMessage(content=json.dumps({
            "question": user_query,
            "columns": list(sample.columns),
            "rows_sample": records,
            "row_count_total": len(df)
        }))
        resp = _llm.invoke([sys, hum])
        txt = resp.content.strip()
        txt = re.sub(r"^```(markdown|text)?", "", txt).strip()
        txt = re.sub(r"```$", "", txt).strip()
        return txt
    except Exception:
        return ""

def summarize_results(state: dict) -> dict:
    # Pass-through friendly messages
    if state.get("final_answer") and state.get("query_result"):
        return state

    if not state.get("exec_success"):
        err = state.get("error_message", "Unknown error")
        state["query_result"] = f"⚠️ Query execution failed.\n\n{err}"
        return state

    rows = state.get("rows", [])
    cols = state.get("columns", [])
    user_q = state.get("cleaned_user_query") or state.get("user_query") or ""

    try:
        df = pd.DataFrame(rows, columns=cols if cols else None)
    except Exception:
        df = pd.DataFrame(rows)

    base_summary = _basic_summary(df)

    if USE_LLM_SUMMARY and len(df) > 0 and len(df) <= 50000:
        llm_summary = _llm_polish_summary(df, user_q)
        if llm_summary:
            state["query_result"] = f"{base_summary}\n\n—\n{llm_summary}"
            return state

    state["query_result"] = base_summary
    return state
