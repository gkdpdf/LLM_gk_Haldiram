# agents/check_entity_node.py
from __future__ import annotations
from typing import Dict, List, Tuple, Optional
from sqlalchemy import text
from sqlalchemy.engine import Engine
import re
from difflib import SequenceMatcher

# Probe order favors ACTORS first (so "sb marke plus" hits superstockist, not product)
PRIMARY_FIRST  = ["super_stockist_name", "distributor_name", "product_name", "material_description", "material"]
SHIPMENT_FIRST = ["sold_to_party_name", "material_description", "city", "sales_district", "material"]

PRODUCT_DIM_COLS       = ["product_name", "base_pack_design_name", "material_description", "alternate_product_category"]
DISTRIBUTOR_DIM_COLS   = ["distributor_name"]
SUPERSTOCKIST_DIM_COLS = ["superstockist_name"]

STOPWORDS = {
    "total","overall","sales","sale","sold","amount","value","qty","quantity","units","pieces","pcs",
    "of","for","in","last","this","that","these","those","month","months","week","weeks","day","days","year","years",
    "please","pls","plz","clarify","your","you","me","kindly","the","and","or","to","from","by","on","at","a","an","is","are","was","were"
}

_USER_Q_RE = re.compile(r"USER QUESTION:\s*(.*)", re.IGNORECASE | re.DOTALL)
def _effective_text(s: str) -> str:
    if not s: return ""
    m = _USER_Q_RE.search(s)
    return (m.group(1).strip() if m else s.strip())

# ---------- DB helpers ----------
def _table_exists(engine: Engine, table: str) -> bool:
    with engine.connect() as conn:
        return conn.execute(text(
            "SELECT 1 FROM information_schema.tables WHERE table_name = :t LIMIT 1"
        ), {"t": table}).fetchone() is not None

def _existing_columns(engine: Engine, table: str) -> List[str]:
    with engine.connect() as conn:
        return [r[0] for r in conn.execute(
            text("SELECT column_name FROM information_schema.columns WHERE table_name = :t"),
            {"t": table}
        ).fetchall()]

def _distinct_values(engine: Engine, table: str, column: str, limit: int = 4000) -> List[str]:
    sql = text(f'SELECT DISTINCT {column} FROM "{table}" WHERE {column} IS NOT NULL LIMIT :lim')
    with engine.connect() as conn:
        return [str(r[0]).strip() for r in conn.execute(sql, {"lim": limit}) if r[0] is not None]

# ---------- fuzzy matching ----------
def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())

def _score(user_text: str, candidate: str) -> float:
    if not candidate: return 0.0
    u = _norm(user_text)
    c = _norm(candidate)
    if not u or not c: return 0.0
    contain = 1.0 if u in c else 0.0
    toks = [t for t in re.findall(r"[a-z0-9]+", (user_text or "").lower()) if t not in STOPWORDS]
    cov = (sum(1 for t in toks if t in c) / len(toks)) if toks else 0.0
    ratio = SequenceMatcher(None, u, c).ratio()
    return max(ratio, 0.85 * contain + 0.15 * cov)

def _best_match(user_text: str, values: List[str]) -> Tuple[Optional[str], float]:
    best_v, best_s = None, 0.0
    for v in values:
        s = _score(user_text, v)
        if s > best_s:
            best_v, best_s = v, s
    return best_v, best_s

def check_entity_node(state: Dict, engine: Engine) -> Dict:
    """
    Fuzzy-detect entity mentions (super stockist / distributor / product).
    Writes:
      - route_preference ('primary'|'shipment') if missing
      - identified_entity (column name)
      - matched_entity_value (DB string)
      - entity_physical_col ('table.column')
      - confidence (0..1)
    """
    if engine is None:
        state.update(final_answer=True, query_result="-- Error: No SQLAlchemy engine in state.")
        return state

    raw_in = (state.get("user_query") or state.get("cleaned_user_query") or "").strip()
    user_text = _effective_text(raw_in)
    if not user_text:
        state.update(identified_entity=None, matched_entity_value=None, entity_physical_col=None, confidence=0.0)
        return state

    route_pref = (state.get("route_preference") or "").lower().strip()
    if route_pref not in ("primary", "shipment"):
        s = user_text.lower()
        route_pref = "shipment" if any(k in s for k in ("shipment", "dispatch", "secondary", "delivery", "invoice")) else "primary"
    state["route_preference"] = route_pref

    # Probe order: prefer actors first
    if route_pref == "primary":
        fact_tables = ["tbl_primary"]
        fact_cols_order = PRIMARY_FIRST
    else:
        fact_tables = ["tbl_shipment", "tbl_dispatch", "tbl_secondary", "tbl_shipments"]
        fact_cols_order = SHIPMENT_FIRST

    candidates: List[Tuple[str, str, str, float]] = []

    # 1) fact tables
    for tbl in fact_tables:
        if not _table_exists(engine, tbl): continue
        existing = set(_existing_columns(engine, tbl))
        for col in [c for c in fact_cols_order if c in existing]:
            try:
                vals = _distinct_values(engine, tbl, col)
            except Exception:
                continue
            mv, sc = _best_match(user_text, vals)
            if mv:
                candidates.append((tbl, col, mv, sc))

    # 2) dimensions — superstockist, distributor, then product master
    if _table_exists(engine, "tbl_superstockist_master"):
        ex = set(_existing_columns(engine, "tbl_superstockist_master"))
        for col in [c for c in SUPERSTOCKIST_DIM_COLS if c in ex]:
            try:
                vals = _distinct_values(engine, "tbl_superstockist_master", col)
            except Exception:
                vals = []
            mv, sc = _best_match(user_text, vals)
            if mv:
                candidates.append(("tbl_superstockist_master", col, mv, sc))

    if _table_exists(engine, "tbl_distributor_master"):
        ex = set(_existing_columns(engine, "tbl_distributor_master"))
        for col in [c for c in DISTRIBUTOR_DIM_COLS if c in ex]:
            try:
                vals = _distinct_values(engine, "tbl_distributor_master", col)
            except Exception:
                vals = []
            mv, sc = _best_match(user_text, vals)
            if mv:
                candidates.append(("tbl_distributor_master", col, mv, sc))

    if _table_exists(engine, "tbl_product_master"):
        ex = set(_existing_columns(engine, "tbl_product_master"))
        for col in [c for c in PRODUCT_DIM_COLS if c in ex]:
            try:
                vals = _distinct_values(engine, "tbl_product_master", col)
            except Exception:
                vals = []
            mv, sc = _best_match(user_text, vals)
            if mv:
                candidates.append(("tbl_product_master", col, mv, sc))

    # Rank: actors > score > string length
    def _priority(col: str) -> int:
        if "super_stockist" in col or "superstockist" in col: return 3
        if "distributor" in col: return 2
        if "product" in col or "material" in col or "description" in col: return 1
        return 0

    if candidates:
        candidates.sort(key=lambda t: (_priority(t[1]), t[3], len(t[2] or "")), reverse=True)
        tbl, col, mv, sc = candidates[0]
        state.update(
            identified_entity=col,
            matched_entity_value=mv,
            entity_physical_col=f"{tbl}.{col}",
            confidence=float(sc),
            final_answer=False
        )
    else:
        state.update(
            identified_entity=None,
            matched_entity_value=None,
            entity_physical_col=None,
            confidence=0.0,
            final_answer=False
        )

    return state
