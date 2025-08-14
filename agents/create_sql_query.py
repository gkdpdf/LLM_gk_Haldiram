# agents/create_sql_query.py
from __future__ import annotations
import re, json, pickle
from typing import Dict, List, Optional, Tuple
from sqlalchemy import text
from sqlalchemy.engine import Engine

# ---------- Optional KB / relationships (used if present) ----------
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

# ---------- DB helpers ----------
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
    # common pairs and reasonable fallbacks
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
    # any identical column name
    for c in fcols:
        if c in dcols:
            return (c, c)
    return None

# ---------- Parse the real user question ----------
_USER_Q_RE = re.compile(r"USER QUESTION:\s*(.*)", re.IGNORECASE | re.DOTALL)
def _question(state: dict) -> str:
    raw = (state.get("user_query") or "").strip()
    m = _USER_Q_RE.search(raw)
    return (m.group(1).strip() if m else raw)

# words→numbers for natural time windows
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
    """Remove 'last X units' so we don't mistake X for top-N."""
    t = (q or "").lower()
    t = re.sub(r"last\s+\d+\s*(months?|weeks?|days?|years?)", "", t)
    t = re.sub(r"last\s+(one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)\s*(months?|weeks?|days?|years?)", "", t)
    return t

def _parse_topn(q: str) -> Optional[int]:
    t = (q or "").lower()
    if not re.search(r"\b(top|highest|best|most)\b", t):
        return None
    # ignore numbers that were part of time windows
    t_wo_time = _strip_time_numbers(t)
    m = re.search(r"\b(\d{1,3})\b", t_wo_time)
    if m:
        return int(m.group(1))
    # if "most/best/highest" with no explicit N → pick 1
    if re.search(r"\b(most|best|highest)\b", t):
        return 1
    # generic "top" with no N → default 10
    if "top" in t:
        return 10
    return None

def _parse_metric_hint(q: str) -> str:
    t = (q or "").lower()
    if any(k in t for k in ("revenue","value","amount","rs","₹","inr")):
        return "value"
    if any(k in t for k in ("qty","quantity","units","volume","pieces","pcs","billed","sold")):
        return "qty"
    return "auto"

# ---------- Entity detection (schema-driven, with explicit keyword overrides) ----------
_SUPERSTOCKIST_HINT = re.compile(r"super\s*stock(?:ist|er|iest|ists|ers)?|sold[_\s-]*to[_\s-]*party|sold[_\s-]*to[_\s-]*party[_\s-]*name", re.IGNORECASE)
_DISTRIBUTOR_HINT   = re.compile(r"distribut(?:or|ers|ion)?", re.IGNORECASE)
_PRODUCT_HINT       = re.compile(r"\b(product|sku|material)\b", re.IGNORECASE)

def _explicit_entity_from_text(q: str) -> Optional[str]:
    if _SUPERSTOCKIST_HINT.search(q): return "superstockist"
    if _DISTRIBUTOR_HINT.search(q):   return "distributor"
    if _PRODUCT_HINT.search(q):       return "product"
    return None

def _candidate_entities(engine: Engine, fact: str) -> List[Dict]:
    """Candidates from actual columns present."""
    fcols = _existing_cols(engine, fact)
    cands: List[Dict] = []
    # product-like
    prod_cols = [c for c in fcols if any(k in c.lower() for k in ("product","material","sku","base_pack_design"))]
    if prod_cols:
        cands.append({"kind":"product","fact_cols":prod_cols,"dim":"tbl_product_master","name_cols":["product_name","base_pack_design_name","material_description"]})
    # distributor-like
    dist_cols = [c for c in fcols if "distributor" in c.lower()]
    if dist_cols:
        cands.append({"kind":"distributor","fact_cols":dist_cols,"dim":"tbl_distributor_master","name_cols":["distributor_name"]})
    # superstockist-like
    ss_cols = [c for c in fcols if ("super_stockist" in c.lower()) or (c.lower() in ("sold_to_party","sold_to_party_name"))]
    if ss_cols:
        cands.append({"kind":"superstockist","fact_cols":ss_cols,"dim":"tbl_superstockist_master","name_cols":["superstockist_name"]})
    # other dimensions (optional)
    plant_cols = [c for c in fcols if "supplying_plant" in c.lower()]
    if plant_cols:
        cands.append({"kind":"plant","fact_cols":plant_cols,"dim":None,"name_cols":[]})
    city_cols = [c for c in fcols if c.lower()=="city"]
    if city_cols:
        cands.append({"kind":"city","fact_cols":city_cols,"dim":None,"name_cols":[]})
    return cands

def _score_against_columns(q: str, names: List[str]) -> float:
    t = re.sub(r"[^a-z0-9 ]+"," ", (q or "").lower())
    toks = set(w for w in t.split() if len(w) >= 3)
    best = 0.0
    for n in names:
        nn = n.replace("_"," ").lower()
        ntoks = set(w for w in nn.split() if len(w) >= 3)
        if not ntoks: continue
        inter = len(toks & ntoks)
        if inter:
            score = inter / len(ntoks)
            best = max(best, score)
    return best

def _pick_entity_for_question(engine: Engine, fact: str, q: str) -> Optional[Dict]:
    cands = _candidate_entities(engine, fact)
    if not cands: return None
    explicit = _explicit_entity_from_text(q)
    if explicit:
        # choose the candidate matching explicit kind if available
        for c in cands:
            if c["kind"] == explicit:
                return c
    # else score by name similarity
    best, best_score = None, -1.0
    for c in cands:
        score = _score_against_columns(q, c["fact_cols"] + c["name_cols"])
        if score > best_score:
            best, best_score = c, score
    return best

# ---------- Measure & date ----------
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

# ---------- Main ----------
def create_sql_query(state: dict) -> dict:
    qtext = _question(state)
    engine: Engine = state.get("engine")
    if engine is None:
        state.update(final_answer=True, query_result="-- Error: No DB engine.")
        return state

    # Route (Primary vs Shipment). If UI didn't set, infer.
    rp = (state.get("route_preference") or "").lower().strip()
    if rp not in ("primary","shipment"):
        rp = "shipment" if any(w in qtext.lower() for w in ("shipment","dispatch","secondary","delivery","invoice")) else "primary"

    # Fact
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

    # Time window (explicit only)
    window = _parse_window(qtext)
    date_col = _pick_date(engine, fact)

    # Entity and measure
    entity = _pick_entity_for_question(engine, fact, qtext)
    metric_hint = _parse_metric_hint(qtext)
    measure = _pick_measure(engine, fact, metric_hint)
    if not measure:
        state.update(final_answer=True, query_result=f"-- Error: Could not pick a numeric measure from {fact}.")
        return state

    # Top-N
    topn = _parse_topn(qtext)

    # Filters
    filters: List[str] = []
    if window and date_col:
        filters.append(f"p.{date_col} >= (CURRENT_DATE - INTERVAL '{window}')")
    where_sql = ("WHERE " + " AND ".join(filters)) if filters else ""

    # If user explicitly asked for distributors while on Shipment (no distributor keys), nudge
    if rp == "shipment" and _explicit_entity_from_text(qtext) == "distributor":
        state.update(final_answer=True, query_result="Distributor data is available in **Primary**. Switch the domain to Primary and try again.")
        return state

    join_sql = ""
    select_key, select_name = None, None
    group_by: List[str] = []

    if entity:
        fcols = _existing_cols(engine, fact)

        # ---------- SUPERSTOCKIST on SHIPMENT (your exact ask) ----------
        if entity["kind"] == "superstockist" and rp == "shipment":
            # Prefer fact name if present
            if "sold_to_party_name" in fcols:
                select_name = "p.sold_to_party_name"
                select_key  = "p.sold_to_party" if "sold_to_party" in fcols else "p.sold_to_party_name"
                group_by = [select_key, select_name]
            else:
                # Join to superstockist master
                dim = "tbl_superstockist_master"
                if _table_exists(engine, dim):
                    keys = _first_working_key(engine, fact, dim) or ("sold_to_party","superstockist_id")
                    fkey, dkey = keys
                    join_sql = f'LEFT JOIN "{dim}" d ON p.{fkey} = d.{dkey}'
                    dim_cols = _existing_cols(engine, dim)
                    name_col = "superstockist_name" if "superstockist_name" in dim_cols else dkey
                    select_key  = f"p.{fkey}"
                    select_name = f"d.{name_col}"
                    group_by = [select_key, select_name]
            # If still nothing, fallback to any superstockist-ish column on fact
            if not group_by:
                for cand in ("super_stockist_name","sold_to_party","sold_to_party_name"):
                    if cand in fcols:
                        expr = f"p.{cand}"
                        select_key = expr
                        group_by = [expr]
                        break

        # ---------- DISTRIBUTOR (Primary) ----------
        elif entity["kind"] == "distributor" and rp == "primary":
            fact_name = next((c for c in entity["fact_cols"] if c in fcols and "name" in c.lower()), None)
            fact_key  = next((c for c in entity["fact_cols"] if c in fcols), None)
            if fact_name:
                select_name = f"p.{fact_name}"
                select_key  = f"p.{fact_key}" if fact_key else select_name
                group_by = [select_key, select_name]
            else:
                dim = "tbl_distributor_master"
                if _table_exists(engine, dim):
                    keys = _first_working_key(engine, fact, dim) or ("distributor_id","distributor_erp_id")
                    fkey, dkey = keys
                    join_sql = f'LEFT JOIN "{dim}" d ON p.{fkey} = d.{dkey}'
                    dim_cols = _existing_cols(engine, dim)
                    name_col = "distributor_name" if "distributor_name" in dim_cols else dkey
                    select_key  = f"p.{fkey}"
                    select_name = f"d.{name_col}"
                    group_by = [select_key, select_name]

        # ---------- PRODUCT (both domains) ----------
        elif entity["kind"] == "product":
            fact_name = next((c for c in entity["fact_cols"] if c in fcols and any(k in c.lower() for k in ("name","description"))), None)
            fact_key  = next((c for c in entity["fact_cols"] if c in fcols and any(k in c.lower() for k in ("product","material","sku","base_pack_design"))), None)
            if fact_name:
                select_name = f"p.{fact_name}"
                select_key  = f"p.{fact_key}" if fact_key else select_name
                group_by = [select_key, select_name]
            else:
                dim = "tbl_product_master"
                if _table_exists(engine, dim):
                    keys = _first_working_key(engine, fact, dim)
                    if keys:
                        fkey, dkey = keys
                        join_sql = f'LEFT JOIN "{dim}" d ON p.{fkey} = d.{dkey}'
                        dim_cols = _existing_cols(engine, dim)
                        name_col = ("product_name" if "product_name" in dim_cols
                                    else "base_pack_design_name" if "base_pack_design_name" in dim_cols
                                    else dkey)
                        select_key  = f"p.{fkey}"
                        select_name = f"d.{name_col}"
                        group_by = [select_key, select_name]

        # ---------- Other/Generic entity kinds ----------
        if not group_by:
            # generic: use any available fact column from this entity kind
            fact_any = next((c for c in entity["fact_cols"] if c in fcols), None)
            if fact_any:
                select_key = f"p.{fact_any}"
                group_by = [select_key]

    # Build SQL
    if group_by:
        # Top-N decision already sanitized to ignore time numbers
        if topn is None and re.search(r"\b(highest|most|top|best)\b", qtext.lower()):
            topn = 10 if "top" in qtext.lower() else 1
        limit_sql = f"\nLIMIT {int(topn)}" if topn else ""
        name_sql = f", {select_name} AS display_name" if select_name else ""
        gb = ", ".join(group_by)
        sql = f"""
SELECT
  {select_key} AS entity_key{name_sql},
  SUM(p.{measure}) AS total_sales
FROM "{fact}" p
{join_sql}
{where_sql}
GROUP BY {gb}
ORDER BY SUM(p.{measure}) DESC{limit_sql}
""".strip()
        state["sql_query"] = sql
        state["final_answer"] = False
        state["route"] = rp
        return state

    # No entity → simple total
    sql = f"""
SELECT
  SUM(p.{measure}) AS total_value
FROM "{fact}" p
{where_sql}
""".strip()
    state["sql_query"] = sql
    state["final_answer"] = False
    state["route"] = rp
    return state
