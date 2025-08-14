# service.py
from pathlib import Path
import glob
from typing import TypedDict, Any, Optional
import inspect
from dotenv import load_dotenv

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.types import Date, Numeric, String, Integer, TIMESTAMP

from langchain_openai import ChatOpenAI
from langgraph.graph import StateGraph, START, END

# === Agents (must exist in ./agents/)
from agents.sql_cleaned_query_agent import clean_query_node  # your existing cleaner
from agents.find_tables import find_tables_node
from agents.create_sql_query import create_sql_query
from agents.execute_sql_query import execute_sql_query
from agents.check_entity_node import check_entity_node
from agents.summarize_results import summarize_results          # your existing summarizer
from agents.rewrite_sql_query import rewrite_sql_query          # your existing rewriter

# ---------------------------------------------------------------------
# Globals (lazy init)
# ---------------------------------------------------------------------
_engine: Optional[Engine] = None
_workflow = None
_llm = None

__all__ = ["get_engine", "llm_reply"]

# ---------------------------------------------------------------------
# Config / helpers
# ---------------------------------------------------------------------
load_dotenv(override=True)

TABLE_COLUMN_TYPES = {
    "tbl_primary": {
        "bill_date": Date,
        "sales_order_date": Date,
        "invoiced_total_quantity": Numeric,
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
        "industy_segment_name": String,
        "pack_size_name": String,
        "base_pack_design_name": String,
        "industy_segment_id": String,
        "pack_size_id": String,
        "base_pack_design_id": String,
        "product_name": String,
        "ptr": Numeric,
        "ptd": Numeric,
        "display_mrp": Integer,
        "mrp": Integer,
        "alternate_product_category": String,
        "product_id": String,
        "is_promoted": String,
        "product_weight_in_gm": Integer,
    },
    "tbl_superstockist_master": {
        "superstockist_name": String,
        "superstockist_id": Integer,
    },
    "tbl_distributor_master": {
        "superstockist_name": String,
        "distributor_name": String,
        "distributor_erp_id": Integer,
        "distributor_channel": String,
        "distributor_segmentation": String,
        "state": String,
        "city_of_warehouse_address": String,
        "temp_created_date": Date,
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
    # Change DSN if needed
    pg_engine = create_engine(
        "postgresql+psycopg2://postgres:12345678@localhost:5432/LLM_Haldiram_primary"
    )
    csv_folder = Path.cwd() / "cooked_data_gk"
    if csv_folder.exists():
        for csv_file in glob.glob(str(csv_folder / "*.csv")):
            table_name = Path(csv_file).stem.lower()
            df = pd.read_csv(csv_file)

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
    return ChatOpenAI(
        model="gpt-4o",
        temperature=0,
        max_tokens=None,
        timeout=None,
        max_retries=2,
    )

class FinalState(TypedDict, total=False):
    user_query: str
    cleaned_user_query: str
    tables: list[str]
    sql_query: str
    query_result: str
    exec_success: bool
    error_message: str
    rows: list
    columns: list
    identified_entity: str | None
    matched_entity_value: str | None
    confidence: float
    method: str
    fallback_intents: list
    retry_count: int
    final_answer: bool
    awaiting_route_choice: bool
    route_preference: str
    session_id: str
    measure_override: str
    date_override: str
    months_override: int
    engine: Any

MAX_RETRIES = 2

def _exec_router(s: dict):
    if s.get("final_answer"):
        return "summarize_results"
    if s.get("exec_success"):
        return "summarize_results"
    if int(s.get("retry_count", 0)) < MAX_RETRIES:
        return "rewrite_sql_query"
    return "summarize_results"

def _check_entity_wrapper(state):
    # Avoid signature mismatch crashes
    _ensure_initialized()
    try:
        arity = len(inspect.signature(check_entity_node).parameters)
    except Exception:
        arity = 2
    if arity >= 2:
        return check_entity_node(state, _engine)
    return check_entity_node(state)

def _build_workflow():
    graph = StateGraph(FinalState)
    graph.add_node("clean_query_node", clean_query_node)
    graph.add_node("check_entity_node", lambda s, _e=_engine: check_entity_node(s, _e))
    graph.add_node("find_tables_node", find_tables_node)
    graph.add_node("create_sql_query", create_sql_query)
    graph.add_node("execute_sql_query", execute_sql_query)
    graph.add_node("rewrite_sql_query", rewrite_sql_query)
    graph.add_node("summarize_results", summarize_results)

    graph.add_edge(START, "clean_query_node")
    graph.add_conditional_edges(
        "clean_query_node",
        lambda s: "summarize_results" if s.get("final_answer") else "check_entity_node",
        {"summarize_results": "summarize_results", "check_entity_node": "check_entity_node"}
    )
    graph.add_edge("check_entity_node", "find_tables_node")
    graph.add_edge("find_tables_node", "create_sql_query")
    graph.add_edge("create_sql_query", "execute_sql_query")
    graph.add_conditional_edges(
        "execute_sql_query",
        _exec_router,
        {"summarize_results": "summarize_results", "rewrite_sql_query": "rewrite_sql_query"}
    )
    graph.add_edge("rewrite_sql_query", "execute_sql_query")
    graph.add_edge("summarize_results", END)
    return graph.compile()

def _ensure_initialized():
    global _engine, _llm, _workflow
    if _engine is None:
        _engine = _configure_db_postgres()
        print("✅ DB ready (service.py).")
    if _llm is None:
        _llm = _build_llm()
    if _workflow is None:
        _workflow = _build_workflow()

# -------- Public API --------
def get_engine() -> Engine:
    _ensure_initialized()
    return _engine  # type: ignore

def llm_reply(txt: str, *, session_id: str | None = None, route_pref: str | None = None) -> dict:
    _ensure_initialized()
    initial_state: FinalState = {
        "user_query": txt,
        "engine": _engine,
    }
    if session_id:
        initial_state["session_id"] = session_id
    if route_pref:
        initial_state["route_preference"] = route_pref
    return _workflow.invoke(initial_state)
