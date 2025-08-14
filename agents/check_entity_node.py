# agents/check_entity_node.py
from __future__ import annotations
from typing import Dict, List, Tuple, Optional
from sqlalchemy import text
from sqlalchemy.engine import Engine
import re

# --- Which columns to probe on each table (we'll filter by actual existence) ---
PRIMARY_FACT_COLS   = ["product_name", "distributor_name", "super_stockist_name", "material_description", "material"]
SHIPMENT_FACT_COLS  = ["material_description", "sold_to_party_name", "city", "sales_district", "material"]

PRODUCT_DIM_COLS         = ["product_name", "base_pack_design_name", "material_description", "alternate_product_category"]
DISTRIBUTOR_DIM_COLS     = ["distributor_name"]
SUPERSTOCKIST_DIM_COLS   = ["superstockist_name"]

# If your prompt wraps the user question like "USER QUESTION: ...", extract just that
_USER_Q_RE = re.compile(r"USER QUESTION:\s*(.*)", re.IGNORECASE | re.DOTALL)

STOPWORDS = {
    "total","overall","sales","sale","of","for","in","last","this","that","these","those",
    "month","months","week","weeks","day","days","year","years",
    "value","amount","qty","quantity","show","give","tell","find",
    "please","pls","plz","clarify","your","you","me","kindly","the","and","or","to","from","by","on","at","a","an","is","are","was","were"
}

# ------------------------ DB helpers ------------------------

def _table_exists(engine: Engine, table: str) -> bool:
    q = text("""SELECT 1 FROM information_schema.tables WHERE table_name = :t LIMIT 1""")
    with engine.connect() as conn:
        return conn.execute(q, {"t": table}).fetchone() is not None

def _existing_columns(engine: Engine, table: str) -> List[str]:
    q = text("SELECT column_name FROM information_schema.columns WHERE table_name = :t")
    with engine.connect() as conn:
        return [r[0] for r in conn.execute(q, {"t": table}).fetchall()]

def _distinct_values(engine: Engine, table: str, column: str, limit: int = 2000) -> List[str]:
    sql = text(f'SELECT DISTINCT {column} FROM "{table}" WHERE {column} IS NOT NULL LIMIT :lim')
    with engine.connect() as conn:
        return [str(r[0]).strip() for r in conn.execute(sql, {"lim": limit}) if r[0] is not None]

# ------------------------ text normalization & matching ------------------------

def _effective_text(s: str) -> str:
    """Prefer only the actual user question part if present."""
    if not s:
        return ""
    m = _USER_Q_RE.search(s)
    return (m.group(1).strip() if m else s.strip())

def _norm(s: str) -> str:
    """Lowercase and strip all non-alphanumeric characters (remove spaces/punct)."""
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())

def _needle_from_user(text_q: str) -> str:
    """
    Build a robust matching 'needle' from the user question:
    - tokenizes on alnum
    - drops stopwords
    - concatenates remaining tokens (so 'v h trading' -> 'vhtrading')
    """
    toks = [t for t in re.findall(r"[a-z0-9]+", (text_q or "").lower()) if t not in STOPWORDS]
    if not toks:
        return ""
    return "".join(toks)

def _best_match(user_text: str, values: List[str]) -> Tuple[Optional[str], float]:
    """
    Try two strategies:
      1) normalized-substring using needle (best for things like 'v h trading')
      2) plain substring (lowercased)
    Return (matched_value, confidence 0..1)
    """
    if not values:
        return None, 0.0

    # Strategy 1: normalized-needle containment
    needle = _needle_from_user(user_text)
    if needle:
        best_v = None
        best_score = 0
        for v in values:
            v_norm = _norm(v)
            if needle and needle in v_norm:
                # score: longer needle gets slightly higher confidence
                score = min(1.0, 0.5 + len(needle) / max(8, len(v_norm) + 1))
                if score > best_score:
                    best_v, best_score = v, score
        if best_v:
            return best_v, best_score

    # Strategy 2: plain substring (lowercased)
    t = (user_text or "").lower()
    best_v = None
    best_len = 0
    for v in values:
        vl = (v or "").lower()
        if vl and vl in t and len(vl) > best_len:
            best_v, best_len = v, len(vl)
    if best_v:
        conf = min(1.0, 0.4 + best_len / max(8, len(t) + 1))
        return best_v, conf

    return None, 0.0

# ------------------------ main node ------------------------

def check_entity_node(state: Dict, engine: Engine) -> Dict:
    """
    Detects entity mentions (product/distributor/superstockist) from DB content.
    Writes to state:
      - route_preference ('primary' or 'shipment')
      - identified_entity (column name)
      - matched_entity_value (string)
      - entity_physical_col ('table.column')
      - confidence (float 0..1)
    Does NOT finalize answer unless route is unknown (asks once).
    """
    if engine is None:
        state.update(final_answer=True, query_result="-- Error: No SQLAlchemy engine in state.")
        return state

    # Prefer the original user text; cleaners may add helper words
    raw_in = (state.get("user_query") or state.get("cleaned_user_query") or "").strip()
    user_text = _effective_text(raw_in)

    # Resolve route (Streamlit usually provides it)
    route_pref = (state.get("route_preference") or "").lower().strip()
    if route_pref not in ("primary", "shipment"):
        s = user_text.lower()
        if any(k in s for k in ("shipment", "dispatch", "secondary", "delivery", "invoice")):
            route_pref = "shipment"
        elif "primary" in s:
            route_pref = "primary"
        else:
            state.update(
                query_result="Do you want *primary* data or *shipment* data? Reply with 'primary' or 'shipment'.",
                final_answer=True,
                awaiting_route_choice=True
            )
            return state
    state["route_preference"] = route_pref

    # Tables/columns to probe
    if route_pref == "primary":
        fact_tables = ["tbl_primary"]
        fact_pref_cols = PRIMARY_FACT_COLS
    else:
        fact_tables = ["tbl_shipment", "tbl_dispatch", "tbl_secondary", "tbl_shipments"]
        fact_pref_cols = SHIPMENT_FACT_COLS

    candidates: List[Tuple[str, str, str, float]] = []  # (table, column, matched_value, confidence)

    # 1) Probe fact tables first (priority)
    for tbl in fact_tables:
        if not _table_exists(engine, tbl):
            continue
        existing = set(_existing_columns(engine, tbl))
        probe_cols = [c for c in fact_pref_cols if c in existing]
        for col in probe_cols:
            try:
                vals = _distinct_values(engine, tbl, col)
            except Exception:
                continue
            mv, conf = _best_match(user_text, vals)
            if mv:
                candidates.append((tbl, col, mv, conf))

    # 2) Probe dimensions next
    # product master
    if _table_exists(engine, "tbl_product_master"):
        existing = set(_existing_columns(engine, "tbl_product_master"))
        probe_cols = [c for c in PRODUCT_DIM_COLS if c in existing]
        for col in probe_cols:
            try:
                vals = _distinct_values(engine, "tbl_product_master", col)
            except Exception:
                vals = []
            mv, conf = _best_match(user_text, vals)
            if mv:
                candidates.append(("tbl_product_master", col, mv, conf))

    # distributor master
    if _table_exists(engine, "tbl_distributor_master"):
        existing = set(_existing_columns(engine, "tbl_distributor_master"))
        probe_cols = [c for c in DISTRIBUTOR_DIM_COLS if c in existing]
        for col in probe_cols:
            try:
                vals = _distinct_values(engine, "tbl_distributor_master", col)
            except Exception:
                vals = []
            mv, conf = _best_match(user_text, vals)
            if mv:
                candidates.append(("tbl_distributor_master", col, mv, conf))

    # superstockist master
    if _table_exists(engine, "tbl_superstockist_master"):
        existing = set(_existing_columns(engine, "tbl_superstockist_master"))
        probe_cols = [c for c in SUPERSTOCKIST_DIM_COLS if c in existing]
        for col in probe_cols:
            try:
                vals = _distinct_values(engine, "tbl_superstockist_master", col)
            except Exception:
                vals = []
            mv, conf = _best_match(user_text, vals)
            if mv:
                candidates.append(("tbl_superstockist_master", col, mv, conf))

    # Pick the best candidate (highest confidence; tie-breaker: longer matched string)
    if candidates:
        candidates.sort(key=lambda x: (x[3], len(x[2] or "")), reverse=True)
        tbl, col, mv, conf = candidates[0]
        state.update(
            identified_entity=col,
            matched_entity_value=mv,
            entity_physical_col=f"{tbl}.{col}",
            confidence=float(conf),
            final_answer=False
        )
    else:
        # No entity found; let planner fall back (date-only or token-based)
        state.update(
            identified_entity=None,
            matched_entity_value=None,
            entity_physical_col=None,
            confidence=0.0,
            final_answer=False
        )

    return state
