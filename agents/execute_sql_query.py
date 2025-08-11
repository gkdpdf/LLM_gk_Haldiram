from sqlalchemy import text, create_engine
import re

# Minimal, self-contained engine for execution
engine = create_engine("postgresql+psycopg2://postgres:12345678@localhost:5432/LLM_Haldiram_primary")

def _table_columns(table: str) -> dict[str, str]:
    q = text("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = :t
    """)
    with engine.connect() as conn:
        rows = conn.execute(q, {"t": table}).fetchall()
    return {r[0]: r[1] for r in rows}

def _best_date_col(table: str) -> str | None:
    cols = _table_columns(table)
    date_cols = [c for c, t in cols.items() if ("date" in t.lower() or "timestamp" in t.lower())]
    if not date_cols:
        return None
    return date_cols[0]

def execute_sql_query(state: dict) -> dict:
    sql = (state.get("sql_query") or "").strip().rstrip(";")
    params = state.get("sql_params") or {}
    already_repaired = state.get("exec_repaired_once", False)

    try:
        with engine.begin() as conn:
            result = conn.execution_options(stream_results=True).execute(text(sql), params)
            rows = result.fetchall()
            cols = list(result.keys()) if hasattr(result, "keys") else []

        state.update({
            "exec_success": True,
            "error_message": "",
            "rows": rows,
            "columns": cols
        })
        return state

    except Exception as e:
        err = str(e)
        state.update({
            "exec_success": False,
            "error_message": err,
            "rows": [],
            "columns": []
        })

        # Single dynamic repair for missing date column in tbl_primary
        if not already_repaired:
            m = re.search(r'column\s+"?([a-zA-Z0-9_\.]+)"?\s+does not exist', err, flags=re.I)
            if m:
                ident = m.group(1)
                table_hint = None
                col = ident
                if "." in ident:
                    table_hint, col = ident.split(".", 1)
                if (table_hint is None) or (table_hint.lower() == "tbl_primary"):
                    real_date = _best_date_col("tbl_primary")
                    if real_date:
                        fixed = sql
                        if table_hint:
                            fixed = re.sub(rf"\b{re.escape(table_hint)}\.{re.escape(col)}\b",
                                           f"{table_hint}.{real_date}", fixed, flags=re.I)
                        fixed = re.sub(rf"\b{re.escape(col)}\b", real_date, fixed, flags=re.I)
                        state["sql_query"] = fixed
                        state["exec_repaired_once"] = True
                        return execute_sql_query(state)

    return state
