# agents/create_sql_query.py
from __future__ import annotations
import json, pickle, re
from typing import Dict, List, Optional, Tuple
from sqlalchemy import text
from sqlalchemy.engine import Engine

# ---------- Load schema & relationships ----------
with open("kb_haldiram_primary.pkl", "rb") as f:
    _TOTAL_TABLE_DICT_RAW: Dict[str, str] = pickle.load(f)
with open("relationship_tables.txt", "r", encoding="utf-8") as f:
    REL_JSON_STR = f.read()

_COL_LINE_RE = re.compile(
    r'^([a-zA-Z0-9_]+)\s*:\s*.*?\bdatatype:\s*([A-Za-z0-9_ ()/\-\[\]\.]+)\s*$',
    re.IGNORECASE
)

SHIPMENT_HINTS = ("shipment", "dispatch", "secondary", "delivery", "invoice")

HEURISTIC_JOINS: List[Tuple[str, str]] = [
    ("product_id", "product_id"),
    ("material", "product_id"),                 # shipment → product mapping
    ("base_pack_design_id", "base_pack_design_id"),
    ("sku_id", "sku_id"),
    ("distributor_id", "distributor_erp_id"),   # primary → distributor master
    ("super_stockist_id", "superstockist_id"),  # primary → superstockist master
    ("sold_to_party", "superstockist_id"),      # shipment → superstockist master
]

FACT_PRODUCT_TEXT_PREF = ["product_name","material_description","base_pack_design_name","material"]
PM_TEXT_PREF  = ["product_name","base_pack_design_name","material_description","alternate_product_category"]

# ---------- Helpers ----------
def _safe_json_load(txt: str):
    try: return json.loads(txt)
    except Exception: return None

def _parse_relationships(raw: str):
    rel = _safe_json_load(raw)
    out: List[Tuple[str, str, str, str]] = []
    if not rel: return out
    items = rel.get("relationships") if isinstance(rel, dict) and "relationships" in rel else rel
    if not isinstance(items, list): return out
    for r in items:
        lt = r.get("left_table") or r.get("table_a") or r.get("from_table")
        lc = r.get("left_column") or r.get("col_a") or r.get("from_column")
        rt = r.get("right_table") or r.get("table_b") or r.get("to_table")
        rc = r.get("right_column") or r.get("col_b") or r.get("to_column")
        if lt and lc and rt and rc: out.append((lt, lc, rt, rc))
    return out

REL_EDGES = _parse_relationships(REL_JSON_STR)

def _direct_join(a: str, b: str) -> Optional[Tuple[str, str]]:
    for lt, lc, rt, rc in REL_EDGES:
        if lt == a and rt == b: return lc, rc
        if lt == b and rt == a: return rc, lc
    return None

def _kb_columns_for_from_text(table: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    txt = _TOTAL_TABLE_DICT_RAW.get(table)
    if not isinstance(txt, str): return out
    for line in txt.splitlines():
        m = _COL_LINE_RE.match(line.strip())
        if m: out[m.group(1)] = m.group(2)
    return out

def _columns_from_information_schema(engine: Engine, table: str) -> Dict[str, str]:
    if engine is None: return {}
    q = text("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = :t ORDER BY ordinal_position
    """)
    with engine.connect() as conn:
        rows = conn.execute(q, {"t": table}).fetchall()
    return {name: dtype for name, dtype in rows}

def _existing_columns(engine: Engine, table: str) -> List[str]:
    q = text("SELECT column_name FROM information_schema.columns WHERE table_name = :t")
    with engine.connect() as conn:
        return [r[0] for r in conn.execute(q, {"t": table}).fetchall()]

def _kb_columns_for(table: str, engine: Engine | None = None) -> Dict[str, str]:
    cols = _kb_columns_for_from_text(table)
    return cols or (_columns_from_information_schema(engine, table) if engine else {})

def _table_exists(engine: Engine, table: str) -> bool:
    with engine.connect() as conn:
        return conn.execute(
            text("""SELECT 1 FROM information_schema.tables WHERE table_name = :t LIMIT 1"""),
            {"t": table}
        ).fetchone() is not None

# ---------- Query understanding ----------
_USER_Q_RE = re.compile(r"USER QUESTION:\s*(.*)", re.IGNORECASE | re.DOTALL)

def _effective_question_from_original(state: dict) -> str:
    raw = (state.get("user_query") or "").strip()
    m = _USER_Q_RE.search(raw)
    return (m.group(1).strip() if m else raw)

def _months(text_s: str) -> int | None:
    """Return N months if user asked for a range, else None (no default)."""
    t = (text_s or "").lower()
    m = re.search(r"last\s+(\d+)\s*month", t)
    if m:
        try: return max(1, int(m.group(1)))
        except Exception: return None
    if "last 3 months" in t or "last three months" in t or "last 3 month" in t:
        return 3
    if "last month" in t:
        return 1
    return None  # <-- no silent default

def _extract_tokens(q: str) -> List[str]:
    STOP = {
        "total","overall","sales","sale","of","for","in","last","this","that","these","those",
        "month","months","week","weeks","day","days","year","years",
        "value","amount","qty","quantity","sold","units","pieces","pcs",
        "show","give","tell","find","with",  # <-- 'with' added
        "please","pls","plz","clarify","your","you","me","kindly","the",
        "and","or","to","from","by","on","at","a","an","is","are","was","were"
    }
    words = re.findall(r"[A-Za-z0-9][A-Za-z0-9_\-]+", q or "")
    seen, toks = set(), []
    for w in words:
        wl = w.lower()
        if wl not in seen and len(wl) >= 3 and wl not in STOP:
            seen.add(wl)
            toks.append(w)
    return toks[:4]

def _parse_qty_threshold(q: str) -> tuple[str | None, int | None]:
    """
    Detect comparative numeric constraints like:
      - less than 10 / under 10 / below 10 / < 10
      - greater than 10 / over 10 / above 10 / > 10
      - equal to 10 / = 10 / exactly 10
    Return (op, value) e.g. ("<", 10) or (None, None)
    """
    t = (q or "").lower()
    m = re.search(r"(less\s+than|under|below|<)\s*(\d+)", t)
    if m: return "<", int(m.group(2))
    m = re.search(r"(greater\s+than|over|above|>)\s*(\d+)", t)
    if m: return ">", int(m.group(2))
    m = re.search(r"(=|equal\s+to|exactly)\s*(\d+)", t)
    if m: return "=", int(m.group(2))
    return None, None

def _wants_distinct_products(q: str) -> bool:
    t = (q or "").lower()
    return any(kw in t for kw in ("distinct product", "unique product", "product-wise", "product wise", "products with"))

# ---------- Column picking ----------
def _pick_date_col(cols: Dict[str, str], override: str | None = None) -> Optional[str]:
    if override and override in cols: return override
    for cand in ("sales_order_date","bill_date","invoice_date"):
        if cand in cols: return cand
    for c, t in cols.items():
        if "date" in c.lower() or "timestamp" in t.lower(): return c
    return None

def _pick_measure_col(cols: Dict[str, str], override: str | None = None) -> Optional[str]:
    if override and override in cols: return override
    for cand in ("invoiced_total_quantity","invoice_value","sales_value","actual_billed_quantity","amount","value","qty","quantity"):
        if cand in cols: return cand
    for c, t in cols.items():
        tl = t.lower()
        if any(k in tl for k in ("numeric","double","real","integer","bigint","smallint","decimal")): return c
    return None

def _pick_product_key(cols: Dict[str, str]) -> Optional[str]:
    for cand in ("product_id","material","product_name","material_description"):
        if cand in cols: return cand
    return None

def _first_common_key(engine: Engine, fact: str, dim: str) -> Optional[Tuple[str, str]]:
    fcols = _kb_columns_for(fact, engine); dcols = _kb_columns_for(dim, engine)
    edge = _direct_join(fact, dim)
    if edge:
        fkey, dkey = edge
        if fkey in fcols and dkey in dcols: return fkey, dkey
    for fkey, dkey in HEURISTIC_JOINS:
        if fkey in fcols and dkey in dcols: return fkey, dkey
    return None

# ---------- Main ----------
def create_sql_query(state: dict) -> dict:
    qtext = _effective_question_from_original(state)
    route_pref = (state.get("route_preference") or "").lower().strip()
    engine: Engine = state.get("engine")

    # route
    wants_shipment = any(w in qtext.lower() for w in SHIPMENT_HINTS)
    if route_pref in ("primary","shipment"): route = route_pref
    elif wants_shipment:                      route = "shipment"
    else:
        state.update(
            query_result="Do you want *primary* data or *shipment* data? Reply with 'primary' or 'shipment'.",
            final_answer=True, awaiting_route_choice=True
        )
        return state

    # fact
    if route == "primary":
        if not _table_exists(engine,"tbl_primary"):
            state.update(query_result="-- Error: primary table (tbl_primary) not available in DB.", final_answer=True)
            return state
        fact = "tbl_primary"
    else:
        fact = None
        for cand in ("tbl_shipment","tbl_dispatch","tbl_secondary","tbl_shipments"):
            if _table_exists(engine, cand): fact = cand; break
        if not fact:
            state.update(query_result="-- Error: shipment table not available; please choose 'primary'.", final_answer=True)
            return state

    fact_cols = _kb_columns_for(fact, engine=engine)
    if not fact_cols:
        state.update(query_result=f"-- Error: could not read columns for {fact}.", final_answer=True)
        return state

    date_col = _pick_date_col(fact_cols, state.get("date_override"))
    measure  = _pick_measure_col(fact_cols, state.get("measure_override"))
    if not measure:
        state.update(query_result=f"-- Error: no numeric measure column found in {fact}.", final_answer=True)
        return state

    # Time window only if explicitly asked
    months = _months(qtext)

    # Entity from detector
    physical_col = state.get("entity_physical_col")
    matched_value = state.get("matched_entity_value")
    confidence = float(state.get("confidence") or 0.0)

    # Numeric condition?
    cmp_op, cmp_val = _parse_qty_threshold(qtext)
    wants_distinct = _wants_distinct_products(qtext)

    filters: List[str] = []
    if months and date_col:
        filters.append(f"p.{date_col} >= (CURRENT_DATE - INTERVAL '{months} months')")

    # If detector gave a confident entity, keep it (product/distributor/etc.)
    join_sql = ""
    if physical_col and matched_value and confidence >= 0.6:
        try:
            etbl, ecol = physical_col.split(".", 1)
        except ValueError:
            etbl, ecol = None, None
        if etbl and ecol:
            if etbl == fact and ecol in fact_cols:
                filters.append(f"LOWER(p.{ecol}) ILIKE LOWER('%{matched_value}%')")
            else:
                keys = _first_common_key(engine, fact, etbl)
                if keys:
                    fkey, dkey = keys
                    join_sql = f'JOIN "{etbl}" d ON p.{fkey} = d.{dkey}'
                    filters.append(f"LOWER(d.{ecol}) ILIKE LOWER('%{matched_value}%')")

    # ---- DISTINCT PRODUCTS with a numeric threshold on sold qty ----
    if wants_distinct and cmp_op and cmp_val is not None:
        prod_key = _pick_product_key(fact_cols)
        if not prod_key:
            # last resort: try via product master key
            keys = _first_common_key(engine, fact, "tbl_product_master")
            if keys:
                fkey, dkey = keys
                prod_key = f"d.{dkey}"
                join_sql = f'JOIN "tbl_product_master" d ON p.{fkey} = d.{dkey}'
            else:
                state.update(query_result="-- Error: cannot determine a product key to group by.", final_answer=True)
                return state

        where_sql = ("WHERE " + " AND ".join(filters)) if filters else ""
        # If prod_key includes alias d., group by that; else p.<col>
        gkey = prod_key if "." in prod_key else f"p.{prod_key}"

        sql = f"""
SELECT COUNT(*) AS num_products
FROM (
  SELECT {gkey}
  FROM "{fact}" p
  {join_sql}
  {where_sql}
  GROUP BY {gkey}
  HAVING COALESCE(SUM(p.{measure}), 0) {cmp_op} {cmp_val}
) s
""".strip()

        state["sql_query"] = sql
        state["final_answer"] = False
        return state

    # ---- Generic fallback (no numeric threshold request) ----
    # Product-ish tokens only (avoid junk like "with")
    tokens = _extract_tokens(qtext)
    if tokens:
        present = [c for c in FACT_PRODUCT_TEXT_PREF if c in fact_cols]
        if present:
            ors = []
            for c in present:
                for tok in tokens:
                    ors.append(f"LOWER(p.{c}) ILIKE LOWER('%{tok}%')")
            filters.append("(" + " OR ".join(ors) + ")")

    where_sql = ("WHERE " + " AND ".join(filters)) if filters else ""
    sql = f"""
SELECT
  SUM(p.{measure}) AS total_value
FROM "{fact}" p
{join_sql}
{where_sql}
LIMIT 1
""".strip()

    state["sql_query"] = sql
    state["final_answer"] = False
    return state
