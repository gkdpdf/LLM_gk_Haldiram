# agents/summarize_results.py
from __future__ import annotations
from typing import Dict
import pandas as pd

SUMMARY_MAX_ROWS_PREVIEW = 10

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

def summarize_results(state: Dict) -> Dict:
    if state.get("final_answer") and state.get("query_result"):
        return state

    if not state.get("exec_success"):
        err = state.get("error_message", "Unknown error")
        state["query_result"] = f"⚠️ Query execution failed.\n\n{err}"
        return state

    rows = state.get("rows", [])
    cols = state.get("columns", [])
    try:
        df = pd.DataFrame(rows, columns=cols if cols else None)
    except Exception:
        df = pd.DataFrame(rows)

    state["query_result"] = _basic_summary(df)
    return state
