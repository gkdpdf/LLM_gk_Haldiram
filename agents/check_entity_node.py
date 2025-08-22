# agents/check_entity_node.py
from __future__ import annotations
from typing import Dict, List, Tuple
from sqlalchemy import text
from sqlalchemy.engine import Engine
import re
from difflib import SequenceMatcher
from functools import lru_cache

# Probe order: actors first
PRIMARY_FIRST  = ["super_stockist_name", "distributor_name", "product_name", "material_description", "material"]
SHIPMENT_FIRST = ["sold_to_party_name", "material_description", "city", "sales_district", "material"]

PRODUCT_DIM_COLS       = ["base_pack_design_name","product_name","material_description","material",
                          "product_id","product","material_code","product_erp_id"]
DISTRIBUTOR_DIM_COLS   = ["distributor_name","name","distributor_erp_id","distributor_code"]
SUPERSTOCKIST_DIM_COLS = ["superstockist_name","super_stockist_name","superstockist_id","sold_to_party_name"]

STOPWORDS = {
    "total","overall","sales","sale","sold","amount","value","qty","quantity","quantities","units","pieces","pcs",
    "of","for","in","last","this","that","these","those","month","months","week","weeks","day","days","year","years",
    "please","pls","plz","clarify","your","you","me","kindly","the","and","or","to","from","by","on","at","a","an",
    "is","are","was","were","my","what","which","who","has","have","with","per","across","each","area","state","region",
    "sku","skus","product","products","mom","m-o-m","growth","growing","increase","decrease","decline","trend","trends",
    "top","best","highest","most","less","more","greater","than","bought","buy","distinct","count"
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
            text("SELECT column_name FROM information_schema.columns WHERE table_name = :t ORDER BY ordinal_position"),
            {"t": table}
        ).fetchall()]

@lru_cache(maxsize=256)
def _distinct_cached(db_key: str, table: str, column: str, limit: int) -> Tuple[str, ...]:
    from sqlalchemy import create_engine
    eng = create_engine(db_key)
    sql = text(f'SELECT DISTINCT {column} FROM "{table}" WHERE {column} IS NOT NULL LIMIT :lim')
    with eng.connect() as conn:
        vals = [str(r[0]).strip() for r in conn.execute(sql, {"lim": limit}) if r[0] is not None]
    return tuple(vals)

def _distinct_values(engine: Engine, table: str, column: str, limit: int = 4000) -> List[str]:
    try:
        db_key = engine.url.render_as_string(hide_password=True)
        return list(_distinct_cached(db_key, table, column, limit))
    except Exception:
        sql = text(f'SELECT DISTINCT {column} FROM "{table}" WHERE {column} IS NOT NULL LIMIT :lim')
        with engine.connect() as conn:
            return [str(r[0]).strip() for r in conn.execute(sql, {"lim": limit}) if r[0] is not None]

# ---------- token & confirm helpers ----------
def _tokens(q: str) -> List[str]:
    words = re.findall(r"[A-Za-z0-9][A-Za-z0-9_\-]+", (q or ""))
    out = []
    seen = set()
    for w in words:
        wl = w.lower()
        if wl in STOPWORDS: continue
        if len(wl) < 3: continue
        if wl not in seen:
            seen.add(wl)
            out.append(wl)
    return out

def _match_ok(token: str, candidate: str) -> bool:
    t = token.lower()
    c = (candidate or "").lower()
    if not t or not c: return False
    words = re.findall(r"[a-z0-9]+", c)

    if len(t) < 4:
        for w in words:
            if t == w or SequenceMatcher(None, t, w).ratio() >= 0.92:
                return True
        return False

    if t in c: return True
    for w in words:
        if SequenceMatcher(None, t, w).ratio() >= 0.90:
            return True
    return False

def _collect_matches(engine: Engine, table: str, col: str, toks: List[str]) -> bool:
    """Return True if any DISTINCT value matches tokens (strict-ish)."""
    try:
        values = _distinct_values(engine, table, col)
    except Exception:
        return False
    for v in values:
        if any(_match_ok(t, v) for t in toks):
            return True
    return False

def check_entity_node(state: Dict, engine: Engine) -> Dict:
    """
    Distinct-based entity confirmer (strict; no fuzzy scores).
    Writes:
      - route_preference
      - confirmed_entities: ['distributor','product','superstockist']
      - tokens: normalized tokens extracted from user text
    """
    if engine is None:
        state.update(final_answer=True, query_result="-- Error: No SQLAlchemy engine in state.")
        return state

    raw_in = (state.get("user_query") or state.get("cleaned_user_query") or "").strip()
    user_text = _effective_text(raw_in)
    toks = _tokens(user_text)

    route_pref = (state.get("route_preference") or "").lower().strip()
    if route_pref not in ("primary", "shipment"):
        s = user_text.lower()
        route_pref = "shipment" if any(k in s for k in ("shipment", "dispatch", "secondary", "delivery", "invoice")) else "primary"
    state["route_preference"] = route_pref

    if not toks:
        state.update(confirmed_entities=[], tokens=[], final_answer=False)
        return state

    if route_pref == "primary":
        fact_tables = ["tbl_primary"]
        fact_cols_order = PRIMARY_FIRST
    else:
        fact_tables = ["tbl_shipment", "tbl_dispatch", "tbl_secondary", "tbl_shipments"]
        fact_cols_order = SHIPMENT_FIRST

    confirmed = set()

    # Look in fact tables first
    for tbl in fact_tables:
        if not _table_exists(engine, tbl): continue
        existing = set(_existing_columns(engine, tbl))
        for col in [c for c in fact_cols_order if c in existing]:
            ok = _collect_matches(engine, tbl, col, toks)
            if not ok: continue
            cl = col.lower()
            if "sold_to_party" in cl or "super_stockist" in cl or "superstockist" in cl:
                confirmed.add("superstockist")
            elif "distributor" in cl:
                confirmed.add("distributor")
            elif "product" in cl or "material" in cl or "description" in cl:
                confirmed.add("product")

    # Dimensions
    if _table_exists(engine, "tbl_superstockist_master"):
        ex = set(_existing_columns(engine, "tbl_superstockist_master"))
        for col in [c for c in SUPERSTOCKIST_DIM_COLS if c in ex]:
            if _collect_matches(engine, "tbl_superstockist_master", col, toks):
                confirmed.add("superstockist")

    if _table_exists(engine, "tbl_distributor_master"):
        ex = set(_existing_columns(engine, "tbl_distributor_master"))
        for col in [c for c in DISTRIBUTOR_DIM_COLS if c in ex]:
            if _collect_matches(engine, "tbl_distributor_master", col, toks):
                confirmed.add("distributor")

    if _table_exists(engine, "tbl_product_master"):
        ex = set(_existing_columns(engine, "tbl_product_master"))
        for col in [c for c in PRODUCT_DIM_COLS if c in ex]:
            if _collect_matches(engine, "tbl_product_master", col, toks):
                confirmed.add("product")

    state.update(
        confirmed_entities=sorted(confirmed),
        tokens=toks,
        final_answer=False
    )
    return state
