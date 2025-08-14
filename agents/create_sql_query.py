# agents/create_sql_query.py
from __future__ import annotations
import re, json, pickle
from typing import Dict, List, Optional, Tuple
from sqlalchemy import text
from sqlalchemy.engine import Engine

# -------- Optional KB & relationships (used if available) --------
try:
    with open("kb_haldiram_primary.pkl", "rb") as f:
        _KB: Dict[str, str] = pickle.load(f)
except Exception:
    _KB = {}

try:
    with open("relationship_tables.txt", "r", encoding="utf-8") as f:
        _REL_RAW = f.read()
except Exception:
    _REL_RAW = ""

def _safe_json(x: str):
    try: return json.loads(x)
    except Exception: return None

def _parse_relationships(raw: str):
    rel = _safe_json(raw)
    out: List[Tuple[str,str,str,str]] = []
    if not rel: return out
    items = rel.get("relationships") if isinstance(rel, dict) and "relationships" in rel else rel
    if not isinstance(items, list): return out
    for r in items:
        lt = r.get("left_table") or r.get("table_a") or r.get("from_table")
        lc = r.get("left_column") or r.get("col_a") or r.get("from_column")
        rt = r.get("right_table") or r.get("table_b") or r.get("to_table")
        rc = r.get("right_column") or r.get("col_b") or r.get("to_column")
        if lt and lc and rt and rc:
            out.append((lt, lc, rt, rc))
    return out

REL_EDGES = _parse_relationships(_REL_RAW)

# ---------------- DB helpers ----------------
def _table_exists(engine: Engine, table: str) -> bool:
    with engine.connect() as conn:
        return conn.execute(text("""
            SELECT 1 FROM information_schema.tables
            WHERE table_name = :t LIMIT 1
        """), {"t": table}).fetchone() is not None

def _columns(engine: Engine, table: str) -> Dict[str, str]:
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = :t
            ORDER BY ordinal_position
        """), {"t": table}).fetchall()
    return {r[0]: r[1] for r in rows}

def _existing_cols(engine: Engine, table: str) -> List[str]:
    return list(_columns(engine, table).keys())

def _direct_join(a: str, b: str) -> Optional[Tuple[str,str]]:
    for lt, lc, rt, rc in REL_EDGES:
        if lt == a and rt == b: return lc, rc
        if lt == b and rt == a: return rc, lc
    return None

def _first_working_key(engine: Engine, fact: str, dim: str) -> Optional[Tuple[str,str]]:
    fcols = _columns(engine, fact)
    dcols = _columns(engine, dim)
    rel = _direct_join(fact, dim)
    if rel and rel[0] in fcols and rel[1] in dcols:
        return rel
    pairs = [
        ("product_id","product_id"),
        ("material","product_id"),
        ("base_pack_design_id","base_pack_design_id"),
        ("sku_id","sku_id"),
        ("distributor_id","distributor_erp_id"),
        ("super_stockist_id","superstockist_id"),
        ("sold_to_party","superstockist_id"),
    ]
    for fkey, dkey in pairs:
        if fkey in fcols and dkey in dcols:
            return (fkey, dkey)
    for c in fcols:
        if c in dcols:
            return (c, c)
    return None

# ---------------- Parse real question ----------------
_USER_Q_RE = re.compile(r"USER QUESTION:\s*(.*)", re.IGNORECASE | re.DOTALL)
def _question(state: dict) -> str:
    raw = (state.get("user_query") or "").strip()
    m = _USER_Q_RE.search(raw)
    return (m.group(1).strip() if m else raw)

_NUM_WORDS = {
    "one":1,"two":2,"three":3,"four":4,"five":5,"six":6,
    "seven":7,"eight":8,"nine":9,"ten":10,"eleven":11,"twelve":12
}

def _parse_window(q: str) -> Optional[str]:
    t = (q or "").lower()
    m = re.search(r"last\s+(\d+)\s*(month|months|week|weeks|day|days|year|years)\b", t)
    if m:
        n, unit = int(m.group(1)), m.group(2)
        if not unit.endswith("s"): unit += "s"
        return f"{n} {unit}"
    m = re.search(r"last\s+(one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)\s*(month|months|week|weeks|day|days|year|years)\b", t)
    if m:
        n = _NUM_WORDS[m.group(1)]
        unit = m.group(2)
        if not unit.endswith("s"): unit += "s"
        return f"{n} {unit}"
    if re.search(r"\blast\s+month\b", t): return "1 months"
    if re.search(r"\blast\s+week\b",  t): return "1 weeks"
    if re.search(r"\blast\s+day\b",   t): return "1 days"
    return None

def _strip_time_numbers(q: str) -> str:
    t = (q or "").lower()
    t = re.sub(r"last\s+\d+\s*(months?|weeks?|days?|years?)", "", t)
    t = re.sub(r"last\s+(one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)\s*(months?|weeks?|days?|years?)", "", t)
    return t

def _parse_topn(q: str) -> Optional[int]:
    t = (q or "").lower()
    if not re.search(r"\b(top|highest|best|most)\b", t):
        return None
    t_wo_time = _strip_time_numbers(t)
    m = re.search(r"\b(\d{1,3})\b", t_wo_time)
    if m: return int(m.group(1))
    if re.search(r"\b(most|best|highest)\b", t): return 1
    if "top" in t: return 10
    return None

def _parse_metric_hint(q: str) -> str:
    t = (q or "").lower()
    if any(k in t for k in ("revenue","value","amount","rs","₹","inr")): return "value"
    if any(k in t for k in ("qty","quantity","units","volume","pieces","pcs","billed","sold")): return "qty"
    return "auto"

# ---------- Entity hints ----------
_SUPERSTOCKIST_HINT = re.compile(r"super\s*stock(?:ist|er|iest|ists|ers)?|sold[_\s-]*to[_\s-]*party|sold[_\s-]*to[_\s-]*party[_\s-]*name", re.IGNORECASE)
_DISTRIBUTOR_HINT   = re.compile(r"distribut(?:or|ers|ion)?", re.IGNORECASE)
_PRODUCT_HINT       = re.compile(r"\b(product|sku|material)\b", re.IGNORECASE)

def _explicit_entity_from_text(q: str) -> Optional[str]:
    if _SUPERSTOCKIST_HINT.search(q): return "superstockist"
    if _DISTRIBUTOR_HINT.search(q):   return "distributor"
    if _PRODUCT_HINT.search(q):       return "product"
    return None

def _breakdown_kind_from_text(q: str) -> Optional[str]:
    """Return entity kind if the user asked for a breakdown 'by <entity>'."""
    t = (q or "").lower()
    if re.search(r"\bby\s+product(s)?\b", t): return "product"
    if re.search(r"\bby\s+distribut(or|or[s])\b", t): return "distributor"
    if re.search(r"\bby\s+super\s*stock(?:ist|er|ists)?\b", t): return "superstockist"
    if re.search(r"\bby\s+plant\b", t): return "plant"
    if re.search(r"\bby\s+city\b", t): return "city"
    return None

# ------------- Tokens (for name/description LIKE filters) -------------
STOP = {
    "total","overall","sales","sale","sold","amount","value","qty","quantity","units","volume","pieces","pcs",
    "for","of","in","by","last","this","that","these","those","month","months","week","weeks","day","days","year","years",
    "the","a","an","and","or","to","from","with","has","have","is","are","was","were","most","top","best","highest",
    "product","products","distributor","distributors","super","stockist","stockists","stocker","stockers","sb","s","b"  # keep generic
}

def _tokens(q: str) -> List[str]:
    words = re.findall(r"[A-Za-z0-9][A-Za-z0-9_\-]+", (q or ""))
    out, seen = [], set()
    for w in words:
        wl = w.lower()
        if wl in STOP: continue
        if len(wl) < 3: continue
        if wl not in seen:
            seen.add(wl)
            out.append(wl)
    return out

def _or_like(prefix: str, cols: List[str], toks: List[str]) -> Optional[str]:
    if not cols or not toks: return None
    terms = []
    for c in cols:
        for t in toks:
            terms.append(f"LOWER({prefix}.{c}) ILIKE LOWER('%{t}%')")
    return "(" + " OR ".join(terms) + ")"

# ---------------- Measure & date pickers ----------------
def _pick_measure(engine: Engine, fact: str, hint: str) -> Optional[str]:
    cols = _columns(engine, fact)
    names = list(cols.keys())
    value_pref = [c for c in names if any(k in c.lower() for k in ("invoice_value","sales_value","amount","value"))]
    qty_pref   = [c for c in names if any(k in c.lower() for k in ("invoiced_total_quantity","actual_billed_quantity","qty","quantity"))]
    if hint == "value" and value_pref: return value_pref[0]
    if hint == "qty"   and qty_pref:   return qty_pref[0]
    for c in qty_pref:   return c
    for c in value_pref: return c
    for c, t in cols.items():
        tl = (t or "").lower()
        if any(k in tl for k in ("int","numeric","decimal","double","real","bigint","smallint")):
            return c
    return None

def _pick_date(engine: Engine, fact: str) -> Optional[str]:
    cols = _columns(engine, fact)
    for cand in ("sales_order_date","bill_date","invoice_date"):
        if cand in cols: return cand
    for c, t in cols.items():
        if "date" in c.lower() or "timestamp" in (t or "").lower():
            return c
    return None

# ---------------- Main ----------------
def create_sql_query(state: dict) -> dict:
    qtext = _question(state)
    engine: Engine = state.get("engine")
    if engine is None:
        state.update(final_answer=True, query_result="-- Error: No DB engine.")
        return state

    # Route (UI sets this, else infer)
    rp = (state.get("route_preference") or "").lower().strip()
    if rp not in ("primary","shipment"):
        rp = "shipment" if any(w in qtext.lower() for w in ("shipment","dispatch","secondary","delivery","invoice")) else "primary"

    # Fact table
    if rp == "primary":
        fact = "tbl_primary" if _table_exists(engine,"tbl_primary") else None
    else:
        fact = None
        for cand in ("tbl_shipment","tbl_dispatch","tbl_secondary","tbl_shipments"):
            if _table_exists(engine, cand):
                fact = cand; break
    if not fact:
        state.update(final_answer=True, query_result=f"-- Error: {rp} table not available in DB.")
        return state

    fact_cols = _existing_cols(engine, fact)

    # Time window (explicit only)
    window = _parse_window(qtext)
    date_col = _pick_date(engine, fact)

    # Metric
    metric_hint = _parse_metric_hint(qtext)
    measure = _pick_measure(engine, fact, metric_hint)
    if not measure:
        state.update(final_answer=True, query_result=f"-- Error: Could not pick a numeric measure from {fact}.")
        return state

    # Tokens for LIKE searches (e.g., 'bhujia')
    toks = _tokens(qtext)

    # Entity detection from previous node (actor/product)
    identified_entity = (state.get("identified_entity") or "").lower()
    matched_value = state.get("matched_entity_value")
    confidence = float(state.get("confidence") or 0.0)
    physical_col = state.get("entity_physical_col")  # e.g., 'tbl_superstockist_master.superstockist_name'

    # We only do a grouping if the user asked for a list/breakdown
    topn = _parse_topn(qtext)
    breakdown_kind = _breakdown_kind_from_text(qtext)
    wants_list = topn is not None or breakdown_kind is not None

    # Build filters
    filters: List[str] = []
    join_sql = ""

    if window and date_col:
        filters.append(f"p.{date_col} >= (CURRENT_DATE - INTERVAL '{window}')")

    # --------- ACTOR FILTERS (superstockist/distributor) ----------
    def _add_superstockist_filter():
        nonlocal join_sql
        # Shipment: prefer direct name column
        if "sold_to_party_name" in fact_cols:
            # use matched value if decent; also allow name tokens from query
            if matched_value and confidence >= 0.30:
                filters.append(f"LOWER(p.sold_to_party_name) ILIKE LOWER('%{matched_value}%')")
            # add loose tokens, too (e.g., 'marke')
            name_like = _or_like("p", ["sold_to_party_name"], toks)
            if name_like: filters.append(name_like)
        else:
            # Join to superstockist master via best key
            dim = "tbl_superstockist_master"
            if _table_exists(engine, dim):
                keys = _first_working_key(engine, fact, dim) or ("sold_to_party","superstockist_id")
                fkey, dkey = keys
                join_sql += f'\nLEFT JOIN "{dim}" d_ss ON p.{fkey} = d_ss.{dkey}'
                dim_cols = _existing_cols(engine, dim)
                name_col = "superstockist_name" if "superstockist_name" in dim_cols else dkey
                if matched_value and confidence >= 0.30:
                    filters.append(f"LOWER(d_ss.{name_col}) ILIKE LOWER('%{matched_value}%')")
                name_like = _or_like("d_ss", [name_col], toks)
                if name_like: filters.append(name_like)

    def _add_distributor_filter():
        nonlocal join_sql
        if rp == "primary":
            if "distributor_name" in fact_cols:
                if matched_value and confidence >= 0.50:
                    filters.append(f"LOWER(p.distributor_name) ILIKE LOWER('%{matched_value}%')")
                name_like = _or_like("p", ["distributor_name"], toks)
                if name_like: filters.append(name_like)
            else:
                dim = "tbl_distributor_master"
                if _table_exists(engine, dim):
                    keys = _first_working_key(engine, fact, dim) or ("distributor_id","distributor_erp_id")
                    fkey, dkey = keys
                    join_sql += f'\nLEFT JOIN "{dim}" d_dist ON p.{fkey} = d_dist.{dkey}'
                    dim_cols = _existing_cols(engine, dim)
                    name_col = "distributor_name" if "distributor_name" in dim_cols else dkey
                    if matched_value and confidence >= 0.50:
                        filters.append(f"LOWER(d_dist.{name_col}) ILIKE LOWER('%{matched_value}%')")
                    name_like = _or_like("d_dist", [name_col], toks)
                    if name_like: filters.append(name_like)

    # Apply actor filters based on either explicit hint or detector
    explicit_kind = _explicit_entity_from_text(qtext)
    if (explicit_kind == "superstockist") or ("superstockist" in identified_entity):
        _add_superstockist_filter()
    if rp == "primary" and ((explicit_kind == "distributor") or ("distributor" in identified_entity)):
        _add_distributor_filter()

    # --------- PRODUCT TEXT FILTERS ----------
    product_text_cols = [c for c in ("product_name","material_description","base_pack_design_name","material") if c in fact_cols]
    prod_like = _or_like("p", product_text_cols, toks)
    if prod_like:
        filters.append(prod_like)

    # WHERE
    where_sql = ("WHERE " + " AND ".join(filters)) if filters else ""

    # --------- OUTPUT SHAPE ----------
    if wants_list:
        # Determine grouping column based on breakdown/topn intent
        # Default product grouping if not specified
        g_key, g_name, g_join = None, None, ""
        if breakdown_kind == "superstockist":
            if "sold_to_party_name" in fact_cols:
                g_key, g_name = "p.sold_to_party", "p.sold_to_party_name" if "sold_to_party" in fact_cols else "p.sold_to_party_name"
            else:
                dim = "tbl_superstockist_master"
                if _table_exists(engine, dim):
                    keys = _first_working_key(engine, fact, dim) or ("sold_to_party","superstockist_id")
                    fkey, dkey = keys
                    g_join = f'\nLEFT JOIN "{dim}" d_gss ON p.{fkey} = d_gss.{dkey}'
                    dim_cols = _existing_cols(engine, dim)
                    name_col = "superstockist_name" if "superstockist_name" in dim_cols else dkey
                    g_key, g_name = f"p.{fkey}", f"d_gss.{name_col}"
        elif breakdown_kind == "distributor" and rp == "primary":
            if "distributor_name" in fact_cols:
                g_key, g_name = "p.distributor_id" if "distributor_id" in fact_cols else "p.distributor_name", "p.distributor_name"
            else:
                dim = "tbl_distributor_master"
                if _table_exists(engine, dim):
                    keys = _first_working_key(engine, fact, dim) or ("distributor_id","distributor_erp_id")
                    fkey, dkey = keys
                    g_join = f'\nLEFT JOIN "{dim}" d_gd ON p.{fkey} = d_gd.{dkey}'
                    name_col = "distributor_name" if "distributor_name" in _existing_cols(engine, dim) else dkey
                    g_key, g_name = f"p.{fkey}", f"d_gd.{name_col}"
        else:
            # product grouping
            if "material" in fact_cols:
                g_key = "p.material"
                g_name = "p.material_description" if "material_description" in fact_cols else None
            elif "product_id" in fact_cols:
                g_key = "p.product_id"
                g_name = "p.product_name" if "product_name" in fact_cols else None

        gb = ", ".join([x for x in [g_key, g_name] if x])
        name_sql = f", {g_name} AS display_name" if g_name else ""
        extra_join = g_join

        limit_sql = f"\nLIMIT {int(topn)}" if topn else ""
        sql = f"""
SELECT
  {g_key} AS entity_key{name_sql},
  SUM(p.{measure}) AS total_sales
FROM "{fact}" p{join_sql}{extra_join}
{where_sql}
GROUP BY {gb}
ORDER BY SUM(p.{measure}) DESC{limit_sql}
""".strip()
        state["sql_query"] = sql
        state["final_answer"] = False
        state["route"] = rp
        return state

    # Otherwise: single SUM over filtered rows (your “total sales by sb marke for Bhujia product” case)
    sql = f"""
SELECT
  SUM(p.{measure}) AS total_value
FROM "{fact}" p{join_sql}
{where_sql}
""".strip()

    state["sql_query"] = sql
    state["final_answer"] = False
    state["route"] = rp
    return state
