# service.py
from __future__ import annotations

from pathlib import Path
import glob
from typing import TypedDict, Any, Optional, List
import inspect
import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.types import Date, Numeric, String, Integer

try:
    from langchain_openai import ChatOpenAI
except Exception:
    ChatOpenAI = None  # optional

from langgraph.graph import StateGraph, START, END

# === Agents ===
try:
    from agents.sql_cleaned_query_agent import clean_query_node
except Exception:
    def clean_query_node(state: dict) -> dict:
        uq = (state.get("user_query") or "").strip()
        state["cleaned_user_query"] = uq
        state["final_answer"] = False
        return state

from agents.find_tables import find_tables_node
from agents.create_sql_query import create_sql_query
from agents.execute_sql_query import execute_sql_query
from agents.check_entity_node import check_entity_node
from agents.summarize_results import summarize_results

try:
    from agents.rewrite_sql_query import rewrite_sql_query
    _REWRITE_AVAILABLE = True
except Exception:
    _REWRITE_AVAILABLE = False
    def rewrite_sql_query(state: dict) -> dict:
        state["retry_count"] = int(state.get("retry_count", 0)) + 1
        return state

# ---------------------------------------------------------------------
_engine: Optional[Engine] = None
_workflow = None
_llm = None

__all__ = ["get_engine", "llm_reply"]

load_dotenv(override=True)

TABLE_COLUMN_TYPES = {
    "tbl_primary": {
        "bill_date": Date,
        "sales_order_date": Date,
        "invoiced_total_quantity": Numeric,
        "ordered_quantity": Numeric,
        "distributor_id": String,
        "distributor_name": String,
        "product_id": String,
        "product_name": String,
        "super_stockist_id": Integer,
        "super_stockist_name": String,
    },
    "tbl_shipment": {
        "invoice_date": Date,
        "actual_billed_quantity": Numeric,
        "sold_to_party": Integer,
        "sold_to_party_name": String,
        "city": String,
        "sales_district": String,
        "supplying_plant": String,
        "material": String,
        "material_description": String,
    },
    "tbl_product_master": {
        "base_pack_design_name": String,
        "product_name": String,
        "alternate_product_category": String,
        "product_id": String,
        "product": String,
        "material": String,
        "product_erp_id": String,
    },
    "tbl_superstockist_master": {
        "superstockist_name": String,
        "super_stockist_name": String,
        "superstockist_id": Integer,
        "state": String,
        "region": String,
        "city": String,
        "sales_district": String,
    },
    "tbl_distributor_master": {
        "superstockist_name": String,
        "distributor_name": String,
        "name": String,
        "distributor_erp_id": String,
        "distributor_code": String,
        "state": String,
        "city_of_warehouse_address": String,
    },
}

DATE_COLUMNS = {
    "tbl_primary": {
        "bill_date": "%d/%m/%y",
        "sales_order_date": "%d/%m/%y",
    },
    "tbl_shipment": {
        "invoice_date": "%d/%m/%y",
    },
}

def _configure_db_postgres() -> Engine:
    dsn = os.getenv("DATABASE_URL") or "postgresql+psycopg2://postgres:12345678@localhost:5432/LLM_Haldiram_primary"
    pg_engine = create_engine(dsn, pool_pre_ping=True)
    csv_folder = Path.cwd() / "cooked_data_gk"
    if csv_folder.exists():
        for csv_file in glob.glob(str(csv_folder / "*.csv")):
            table_name = Path(csv_file).stem.lower()
            try:
                df = pd.read_csv(csv_file)
            except Exception as e:
                print(f"❌ Could not read {csv_file}: {e}")
                continue

            if table_name in DATE_COLUMNS:
                for col, fmt in DATE_COLUMNS[table_name].items():
                    if col in df.columns:
                        try:
                            df[col] = pd.to_datetime(df[col], format=fmt, errors="coerce").dt.date
                        except Exception as e:
                            print(f"❌ Date parsing failed for {table_name}.{col}: {e}")

            dtype_mapping = TABLE_COLUMN_TYPES.get(table_name, {})
            try:
                df.to_sql(
                    name=table_name,
                    con=pg_engine,
                    index=False,
                    if_exists="replace",
                    dtype=dtype_mapping
                )
                print(f"✅ Loaded table: {table_name}")
            except Exception as e:
                print(f"❌ Error loading {table_name}: {e}")

    return pg_engine

def _build_llm():
    if ChatOpenAI is None:
        return None
    try:
        return ChatOpenAI(model="gpt-4o", temperature=0, max_retries=2)
    except Exception:
        return None

class FinalState(TypedDict, total=False):
    user_query: str
    cleaned_user_query: str
    tables: list[str]
    allowed_tables: list[str]
    sql_query: str
    query_result: str
    exec_success: bool
    error_message: str
    rows: list
    columns: list
    retry_count: int
    final_answer: bool
    route_preference: str
    session_id: str
    engine: Any

MAX_RETRIES = 2

def _exec_router(s: dict):
    if s.get("final_answer"):
        return "summarize_results"
    if s.get("exec_success"):
        return "summarize_results"
    if int(s.get("retry_count", 0)) < MAX_RETRIES and _REWRITE_AVAILABLE:
        return "rewrite_sql_query"
    return "summarize_results"

def _build_workflow():
    g = StateGraph(FinalState)
    g.add_node("clean_query_node", clean_query_node)
    g.add_node("check_entity_node", lambda s, _e=_engine: check_entity_node(s, _e))
    g.add_node("find_tables_node", find_tables_node)
    g.add_node("create_sql_query", create_sql_query)
    g.add_node("execute_sql_query", execute_sql_query)
    if _REWRITE_AVAILABLE:
        g.add_node("rewrite_sql_query", rewrite_sql_query)
    g.add_node("summarize_results", summarize_results)

    g.add_edge(START, "clean_query_node")
    g.add_conditional_edges(
        "clean_query_node",
        lambda s: "summarize_results" if s.get("final_answer") else "check_entity_node",
        {"summarize_results": "summarize_results", "check_entity_node": "check_entity_node"}
    )
    g.add_edge("check_entity_node", "find_tables_node")
    g.add_edge("find_tables_node", "create_sql_query")
    g.add_edge("create_sql_query", "execute_sql_query")
    g.add_conditional_edges(
        "execute_sql_query",
        _exec_router,
        {"summarize_results": "summarize_results"} | ({"rewrite_sql_query": "rewrite_sql_query"} if _REWRITE_AVAILABLE else {})
    )
    if _REWRITE_AVAILABLE:
        g.add_edge("rewrite_sql_query", "execute_sql_query")
    g.add_edge("summarize_results", END)
    return g.compile()

def _ensure_initialized():
    global _engine, _llm, _workflow
    if _engine is None:
        _engine = _configure_db_postgres()
        print("✅ DB ready (service.py).")
    if _llm is None:
        _llm = _build_llm()
    if _workflow is None:
        _workflow = _build_workflow()

def get_engine() -> Engine:
    _ensure_initialized()
    return _engine  # type: ignore

def llm_reply(
    txt: str,
    *,
    session_id: str | None = None,
    route_pref: str | None = None,
    allowed_tables: Optional[List[str]] = None,
) -> dict:
    _ensure_initialized()
    state: FinalState = {
        "user_query": txt,
        "engine": _engine,
        "retry_count": 0,
        "final_answer": False,
    }
    if route_pref:
        state["route_preference"] = route_pref
    if allowed_tables:
        state["allowed_tables"] = list(allowed_tables)
    if session_id:
        state["session_id"] = session_id
    return _workflow.invoke(state)
