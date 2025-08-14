# agents/execute_sql_query.py
from __future__ import annotations
from typing import Dict, List, Any
from sqlalchemy import text
from sqlalchemy.engine import Engine

def execute_sql_query(state: Dict) -> Dict:
    sql = (state.get("sql_query") or "").strip()
    if not sql:
        state.update(exec_success=False, error_message="Empty SQL", rows=[], columns=[])
        return state

    engine: Engine = state.get("engine")
    if engine is None:
        state.update(exec_success=False, error_message="No SQLAlchemy engine in state", rows=[], columns=[])
        return state

    try:
        with engine.connect() as conn:
            rs = conn.execute(text(sql))
            try:
                rows: List[Any] = rs.fetchall()
                cols = list(rs.keys())
            except Exception:
                rows, cols = [], []
        state.update(exec_success=True, error_message="", rows=rows, columns=cols)
    except Exception as e:
        state.update(exec_success=False, error_message=str(e), rows=[], columns=[])
    return state
