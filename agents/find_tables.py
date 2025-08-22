# agents/find_tables.py
from __future__ import annotations
from typing import Dict, List
from sqlalchemy import inspect
from sqlalchemy.engine import Engine

def find_tables_node(state: Dict) -> Dict:
    """
    If UI passed state['allowed_tables'], use it.
    Else inspect DB and filter by route:
      - Primary  -> all except 'tbl_shipment'
      - Shipment -> all except 'tbl_primary'
    """
    engine: Engine = state.get("engine")
    route = (state.get("route_preference") or "").lower().strip()

    if state.get("allowed_tables"):
        state["tables"] = list(state["allowed_tables"])
        return state

    if engine is None:
        state["tables"] = []
        return state

    insp = inspect(engine)
    all_tables: List[str] = sorted(insp.get_table_names())

    if route == "shipment":
        allowed = [t for t in all_tables if t != "tbl_primary"]
    else:
        allowed = [t for t in all_tables if t != "tbl_shipment"]

    state["tables"] = allowed
    return state
