# agents/create_sql_query.py
from __future__ import annotations
import re, json
from typing import Dict, List, Optional, Tuple
from difflib import SequenceMatcher
from sqlalchemy import text
from sqlalchemy.engine import Engine

# ---- optional relationships file ----
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
        ("product_id","product_id"), ("product_id","product"), ("product_id","product_erp_id"),
        ("material","product_id"), ("material","product"), ("material","material"), ("material","material_code"),
        ("sku_id","sku_id"), ("base_pack_design_id","base_pack_design_id"),
        ("distributor_id","distributor_erp_id"), ("distributor_code","distributor_code"),
        ("sold_to_party","superstockist_id"), ("sold_to_party","sold_to_party"),
    ]
    for fkey, dkey in pairs:
        if fkey in fcols and dkey in dcols:
            return (fkey, dkey)
    for c in fcols:
        if c in dcols:
            return (c, c)
    return None

# ---------------- Parse the question ----------------
_USER_Q_RE = re.compile(r"USER QUESTION:\s*(.*)", re.IGNORECASE | re.DOTALL)
def _question(state: dict) -> str:
    raw = (state.get("user_query") or "").strip()
    m = _USER_Q_RE.search(raw)
    return (m.group(1).strip() if m else raw)

_NUM_WORDS = {"one":1,"two":2,"three":3,"four":4,"five":5,"six":6,"seven":7,"eight":8,"nine":9,"ten":10}

def _parse_window(q: str) -> Optional[str]:
    t = (q or "").lower()
    m = re.search(r"last\s+(\d+)\s*(month|months|week|weeks|day|days|year|years)\b", t)
    if m:
        n, unit = int(m.group(1)), m.group(2)
        if not unit.endswith("s"): unit += "s"
        return f"{n} {unit}"
    m = re.search(r"last\s+(one|two|three|four|five|six|seven|eight|nine|ten)\s*(month|months|week|weeks|day|days|year|years)\b", t)
    if m:
        n = _NUM_WORDS[m.group(1)]
        unit = m.group(2)
        if not unit.endswith("s"): unit += "s"
        return f"{n} {unit}"
    if re.search(r"\blast\s+month\b", t): return "1 months"
    if re.search(r"\blast\s+week\b",  t): return "1 weeks"
    if re.search(r"\blast\s+day\b",   t): return "1 days"
    return None

def _parse_topn(q: str) -> Optional[int]:
    t = (q or "").lower()
    m = re.search(r"\btop\s+(\d{1,3})\b", t)
    if m: return int(m.group(1))
    m = re.search(r"\btop\s+(one|two|three|four|five|six|seven|eight|nine|ten)\b", t)
    if m: return _NUM_WORDS[m.group(1)]
    return None

def _parse_threshold(q: str) -> Optional[Tuple[str,int]]:
    t = (q or "").lower()
    m = re.search(r"\b(less\s+than|greater\s+than|over|under)\s+(\d{1,9})\s*(qty|quantity|units|pieces|pcs)?\b", t)
    if not m: return None
    op = m.group(1)
    n = int(m.group(2))
    if "less" in op or "under" in op: return ("<", n)
    return (">", n)

_MONTHS = {
    "jan":1,"january":1,"feb":2,"february":2,"mar":3,"march":3,"apr":4,"april":4,"may":5,
    "jun":6,"june":6,"jul":7,"july":7,"aug":8,"august":8,"sep":9,"sept":9,"september":9,
    "oct":10,"october":10,"nov":11,"november":11,"dec":12,"december":12
}
def _parse_absolute_date(q: str) -> Optional[str]:
    t = (q or "").lower()
    m = re.search(r"\b(\d{1,2})(st|nd|rd|th)?\s+([a-z]+)\s+(\d{4})\b", t)
    if m:
        d = int(m.group(1)); mon = _MONTHS.get(m.group(3)[:3], None); y = int(m.group(4))
        if mon: return f"{y:04d}-{mon:02d}-{d:02d}"
    m = re.search(r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{4})\b", t)
    if m:
        d = int(m.group(1)); mon = int(m.group(2)); y = int(m.group(3))
        return f"{y:04d}-{mon:02d}-{d:02d}"
    return None

def _parse_month_year(q: str) -> Optional[Tuple[int, Optional[int]]]:
    t = (q or "").lower()
    m = re.search(r"\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t|tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\b(?:\s+(\d{4}))?", t)
    if not m: return None
    mon = _MONTHS[m.group(1)]
    year = int(m.group(2)) if m.group(2) else None
    return mon, year

STOP = {
    "total","overall","sales","sale","sold","amount","value","qty","quantity","quantities","units","volume","pieces","pcs",
    "for","of","in","by","last","this","that","these","those","month","months","week","weeks","day","days","year","years",
    "the","a","an","and","or","to","from","with","has","have","is","are","was","were","most","top","best","highest",
    "product","products","distributor","distributors","super","stockist","stockists","area","state","region","city","each",
    "who","bought","less","more","greater","than","mom","m-o-m","growth","growing","across","per","each","plus","'s"
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

def _approx_has(text: str, term: str, thr: float = 0.88) -> bool:
    tl = text.lower()
    term = term.lower()
    if term in tl: return True
    for w in re.findall(r"[a-z0-9]+", tl):
        if SequenceMatcher(None, term, w).ratio() >= thr:
            return True
    return False

def _any_terms(text: str, terms: List[str]) -> bool:
    return any(_approx_has(text, t) for t in terms)

# ---------------- Measure & date pickers ----------------
def _pick_measure(engine: Engine, fact: str, hint: str) -> Optional[str]:
    cols = _columns(engine, fact)
    names = list(cols.keys())
    value_pref = [c for c in names if any(k in c.lower() for k in ("invoice_value","sales_value","amount","value"))]
    qty_pref   = [c for c in names if any(k in c.lower() for k in ("invoiced_total_quantity","actual_billed_quantity","ordered_quantity","qty","quantity"))]
    if hint == "value" and value_pref: return value_pref[0]
    if hint == "qty"   and qty_pref:   return qty_pref[0]
    for c in qty_pref:   return c
    for c in value_pref: return c
    for c, t in cols.items():
        if any(k in (t or "").lower() for k in ("int","numeric","decimal","double","real","bigint","smallint")):
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

def _find_product_join_keys(engine: Engine, fact: str) -> Optional[Tuple[str, str]]:
    if not _table_exists(engine, "tbl_product_master"):
        return None
    fcols = set(_columns(engine, fact).keys())
    dcols = set(_columns(engine, "tbl_product_master").keys())
    candidates = [
        ("product_id", "product_id"), ("product_id","product"), ("product_id","product_erp_id"),
        ("material", "product_id"), ("material","product"), ("material","material"), ("material","material_code"),
        ("sku_id","sku_id"), ("base_pack_design_id","base_pack_design_id"),
    ]
    for fk, dk in candidates:
        if fk in fcols and dk in dcols:
            return fk, dk
    for c in fcols:
        if c in dcols:
            return c, c
    return None

def _product_display_expr(engine: Engine, fact_cols: List[str], have_prod: bool) -> str:
    if have_prod:
        dcols = _columns(engine, "tbl_product_master")
        for cand in ("base_pack_design_name","product_name","material_description"):
            if cand in dcols:
                return f"d_prod.{cand}"
    for cand in ("product_name","material_description"):
        if cand in fact_cols:
            return f"p.{cand}"
    if "product_id" in fact_cols:
        return "p.product_id::text"
    if "material" in fact_cols:
        return "p.material::text"
    return "'(unknown SKU)'"

def _resolve_distributor_key_label(engine: Engine, fact: str, allowed: set, current_join_sql: str, fact_cols: List[str]) -> Tuple[str, str, str]:
    join_sql = current_join_sql
    if "distributor_id" in fact_cols and "distributor_name" in fact_cols:
        return "p.distributor_id", "p.distributor_name", join_sql
    if "distributor_id" in fact_cols:
        return "p.distributor_id", "p.distributor_id::text", join_sql
    if "distributor_code" in fact_cols and "distributor_name" in fact_cols:
        return "p.distributor_code", "p.distributor_name", join_sql
    if "distributor_code" in fact_cols:
        return "p.distributor_code", "p.distributor_code::text", join_sql
    if "distributor_name" in fact_cols:
        return "p.distributor_name", "p.distributor_name", join_sql
    if _table_exists(engine, "tbl_distributor_master") and (not allowed or "tbl_distributor_master" in allowed):
        jk = _first_working_key(engine, fact, "tbl_distributor_master")
        if jk:
            fkey, dkey = jk
            join_sql += f'\nLEFT JOIN "tbl_distributor_master" d_dist ON p.{fkey} = d_dist.{dkey}'
            cols = _columns(engine, "tbl_distributor_master")
            label = None
            for c in ("distributor_name","name","distributor_code","distributor_erp_id"):
                if c in cols:
                    label = f"d_dist.{c}"; break
            if not label: label = f"d_dist.{dkey}"
            return f"d_dist.{dkey}", label, join_sql
    if "sold_to_party" in fact_cols and "sold_to_party_name" in fact_cols:
        return "p.sold_to_party", "p.sold_to_party_name", join_sql
    if "sold_to_party_name" in fact_cols:
        return "p.sold_to_party_name", "p.sold_to_party_name", join_sql
    any_col = fact_cols[0] if fact_cols else "1"
    return any_col, any_col, join_sql

def _resolve_geo_label(engine: Engine, fact: str, allowed: set, current_join_sql: str, fact_cols: List[str]) -> Tuple[str, str, str, List[str]]:
    join_sql = current_join_sql
    geo_cols_present = [c for c in ("state","region","sales_district","city","area","district") if c in fact_cols]
    if geo_cols_present:
        col = geo_cols_present[0]
        return f"p.{col}", f"p.{col}", join_sql, geo_cols_present
    if _table_exists(engine, "tbl_superstockist_master") and (not allowed or "tbl_superstockist_master" in allowed):
        jk = _first_working_key(engine, fact, "tbl_superstockist_master")
        if jk:
            fkey, dkey = jk
            join_sql += f'\nLEFT JOIN "tbl_superstockist_master" d_ss ON p.{fkey} = d_ss.{dkey}'
            dcols = _columns(engine, "tbl_superstockist_master")
            geo_cols = [c for c in ("state","region","sales_district","city","area","district") if c in dcols]
            if geo_cols:
                col = geo_cols[0]
                return f"d_ss.{col}", f"d_ss.{col}", join_sql, geo_cols
    return "NULL", "NULL", join_sql, []

def _build_entity_filters(state: dict, engine: Engine, fact: str, fact_cols: List[str]) -> Tuple[str, List[str]]:
    allowed = set(state.get("allowed_tables", []) or state.get("tables", []))
    confirmed_entities: List[str] = state.get("confirmed_entities", []) or []
    toks: List[str] = state.get("tokens", []) or _tokens(_question(state))

    join_sql = ""
    filters: List[str] = []

    # Distributor
    if "distributor" in confirmed_entities:
        if _table_exists(engine,"tbl_distributor_master") and (not allowed or "tbl_distributor_master" in allowed):
            keys = _first_working_key(engine, fact, "tbl_distributor_master") or ("distributor_id","distributor_erp_id")
            fkey, dkey = keys
            join_sql += f'\nLEFT JOIN "tbl_distributor_master" d_dist ON p.{fkey} = d_dist.{dkey}'
            name_cols = [c for c in ("distributor_name","name","distributor_code","distributor_erp_id") if c in _columns(engine,"tbl_distributor_master")]
            like_dim = _or_like("d_dist", name_cols or [dkey], toks)
            if like_dim: filters.append(like_dim)
        if "distributor_name" in fact_cols:
            like_fact = _or_like("p", ["distributor_name"], toks)
            if like_fact: filters.append(like_fact)

    # Superstockist
    if "superstockist" in confirmed_entities:
        if _table_exists(engine,"tbl_superstockist_master") and (not allowed or "tbl_superstockist_master" in allowed):
            keys = _first_working_key(engine, fact, "tbl_superstockist_master") or ("sold_to_party","superstockist_id")
            fkey, dkey = keys
            join_sql += f'\nLEFT JOIN "tbl_superstockist_master" d_ss ON p.{fkey} = d_ss.{dkey}'
            name_cols = [c for c in ("superstockist_name","super_stockist_name","sold_to_party_name") if c in _columns(engine,"tbl_superstockist_master")]
            like_dim = _or_like("d_ss", name_cols or [dkey], toks)
            if like_dim: filters.append(like_dim)
        if "sold_to_party_name" in fact_cols:
            like_fact = _or_like("p", ["sold_to_party_name"], toks)
            if like_fact: filters.append(like_fact)

    # Product
    if "product" in confirmed_entities:
        if _table_exists(engine,"tbl_product_master") and (not allowed or "tbl_product_master" in allowed):
            keys = _first_working_key(engine, fact, "tbl_product_master") or ("material","product_id")
            fkey, dkey = keys
            join_sql += f'\nLEFT JOIN "tbl_product_master" d_prod ON p.{fkey} = d_prod.{dkey}'
            prod_cols = [c for c in ("base_pack_design_name","product_name","material_description","product","material") if c in _columns(engine,"tbl_product_master")]
            like_dim = _or_like("d_prod", prod_cols or [dkey], toks)
            if like_dim: filters.append(like_dim)
        prod_fact_cols = [c for c in ("product_name","material_description","base_pack_design_name","material") if c in fact_cols]
        if prod_fact_cols:
            like_fact = _or_like("p", prod_fact_cols, toks)
            if like_fact: filters.append(like_fact)

    return join_sql, filters

# -------------------------------------------------------

def create_sql_query(state: dict) -> dict:
    qtext = _question(state)
    engine: Engine = state.get("engine")
    if engine is None:
        state.update(final_answer=True, query_result="-- Error: No DB engine.")
        return state

    rp = (state.get("route_preference") or "").lower().strip()
    if rp not in ("primary","shipment"):
        rp = "shipment" if any(w in qtext.lower() for w in ("shipment","dispatch","secondary","delivery","invoice")) else "primary"

    allowed = set(state.get("allowed_tables", []) or state.get("tables", []))
    shipment_pref = ["tbl_shipment","tbl_dispatch","tbl_secondary","tbl_shipments"]
    primary_pref  = ["tbl_primary"]
    pref_list = primary_pref if rp == "primary" else shipment_pref
    fact = None
    for cand in pref_list:
        if allowed and cand not in allowed: continue
        if _table_exists(engine, cand): fact = cand; break
    if not fact:
        state.update(final_answer=True, query_result=f"-- Error: {rp} table not available in DB.")
        return state

    fact_cols = _existing_cols(engine, fact)

    # time & measure
    window = _parse_window(qtext)
    date_col = _pick_date(engine, fact)
    metric_hint = ("value" if any(k in qtext.lower() for k in ("value","amount","rs","₹","inr"))
                   else "qty" if any(k in qtext.lower() for k in ("qty","quantity","units","pieces","pcs","billed","sold"))
                   else "auto")
    measure = _pick_measure(engine, fact, metric_hint)
    if not measure:
        state.update(final_answer=True, query_result=f"-- Error: Could not pick a numeric measure from {fact}.")
        return state

    # entity filters (tokens, not enumerated values)
    entity_join_sql, entity_filters = _build_entity_filters(state, engine, fact, fact_cols)

    # time filters
    time_filters: List[str] = []
    abs_day = _parse_absolute_date(qtext)
    if abs_day and date_col:
        time_filters.append(f"DATE(p.{date_col}) = DATE '{abs_day}'")
    my = _parse_month_year(qtext)
    if my and date_col:
        mon, yr = my
        if yr:
            time_filters.append(f"EXTRACT(MONTH FROM p.{date_col}) = {mon} AND EXTRACT(YEAR  FROM p.{date_col}) = {yr}")
        else:
            time_filters.append(f"EXTRACT(MONTH FROM p.{date_col}) = {mon} AND EXTRACT(YEAR  FROM p.{date_col}) = EXTRACT(YEAR FROM CURRENT_DATE)")
    if window == "1 months" and date_col:
        time_filters.append(
            f"p.{date_col} >= date_trunc('month', CURRENT_DATE - INTERVAL '1 month') AND "
            f"p.{date_col} <  date_trunc('month', CURRENT_DATE)"
        )
    elif window and date_col and not abs_day and not my:
        time_filters.append(f"p.{date_col} >= (CURRENT_DATE - INTERVAL '{window}')")

    where_entity_sql = ("WHERE " + " AND ".join(entity_filters)) if entity_filters else ""
    where_all_sql    = ("WHERE " + " AND ".join([*entity_filters, *time_filters])) if (entity_filters or time_filters) else ""

    t = qtext.lower().strip()
    toks = _tokens(qtext)

    # ----- pattern: highest growing SKU across each distributor (MoM) -----
    if (
        _any_terms(t, ["highest","top","most"]) and
        _any_terms(t, ["growth","growing","mom","m-o-m"]) and
        _any_terms(t, ["sku","product","material"]) and
        _any_terms(t, ["distributor"]) and
        date_col
    ):
        dist_key, dist_label, join_sql = _resolve_distributor_key_label(engine, fact, allowed, entity_join_sql, fact_cols)
        have_prod = _table_exists(engine, "tbl_product_master") and (not allowed or "tbl_product_master" in allowed)
        pj = ""
        if have_prod:
            jk = _find_product_join_keys(engine, fact)
            if jk:
                fkey, dkey = jk
                pj = f'\nLEFT JOIN "tbl_product_master" d_prod ON p.{fkey} = d_prod.{dkey}'
        sku_label = _product_display_expr(engine, fact_cols, have_prod)

        sql = f"""
WITH latest_month_per_dist AS (
  SELECT {dist_key} AS dist_key, date_trunc('month', MAX(p.{date_col})) AS m0_start
  FROM "{fact}" p{join_sql}
  {where_entity_sql}
  GROUP BY dist_key
), base AS (
  SELECT
    {dist_label} AS distributor,
    {sku_label} AS sku,
    SUM(CASE WHEN p.{date_col} >= b.m0_start AND p.{date_col} < b.m0_start + INTERVAL '1 month' THEN p.{measure} END) AS m0,
    SUM(CASE WHEN p.{date_col} >= b.m0_start - INTERVAL '1 month' AND p.{date_col} < b.m0_start THEN p.{measure} END) AS m1
  FROM "{fact}" p
  LEFT JOIN latest_month_per_dist b ON {dist_key} = b.dist_key
  {join_sql}{pj}
  {where_entity_sql}
  GROUP BY {dist_label}, {sku_label}
), ranked AS (
  SELECT *,
         CASE WHEN COALESCE(m1,0)=0 THEN NULL
              ELSE (COALESCE(m0,0)-COALESCE(m1,0))/NULLIF(COALESCE(m1,0),0)
          END AS mom_growth,
         ROW_NUMBER() OVER (PARTITION BY distributor
                            ORDER BY
                              CASE WHEN COALESCE(m1,0)=0 THEN -1
                                   ELSE (COALESCE(m0,0)-COALESCE(m1,0))/NULLIF(COALESCE(m1,0),0) END
                              DESC NULLS LAST) AS rk
  FROM base
)
SELECT distributor, sku, m0, m1, mom_growth
FROM ranked
WHERE rk = 1
ORDER BY distributor
""".strip()
        state.update(sql_query=sql, final_answer=False, route=rp)
        return state

    # ----- pattern: overall top MoM SKU -----
    if _any_terms(t, ["which","what","top","most","highest"]) and _any_terms(t, ["growth","growing","mom","m-o-m"]) and date_col:
        have_prod = _table_exists(engine, "tbl_product_master") and (not allowed or "tbl_product_master" in allowed)
        pj = ""
        if have_prod:
            jk = _find_product_join_keys(engine, fact)
            if jk:
                fkey, dkey = jk
                pj = f'\nLEFT JOIN "tbl_product_master" d_prod ON p.{fkey} = d_prod.{dkey}'
        sku_label = _product_display_expr(engine, fact_cols, have_prod)

        sql = f"""
WITH latest_month AS (
  SELECT date_trunc('month', MAX(p.{date_col})) AS m0_start
  FROM "{fact}" p{entity_join_sql}
  {where_entity_sql}
), base AS (
  SELECT
    {sku_label} AS sku,
    SUM(CASE WHEN p.{date_col} >= b.m0_start AND p.{date_col} < b.m0_start + INTERVAL '1 month' THEN p.{measure} END) AS m0,
    SUM(CASE WHEN p.{date_col} >= b.m0_start - INTERVAL '1 month' AND p.{date_col} < b.m0_start THEN p.{measure} END) AS m1
  FROM "{fact}" p, latest_month b{pj}
  {entity_join_sql}
  {where_entity_sql}
  GROUP BY {sku_label}
)
SELECT sku, m0, m1,
       CASE WHEN COALESCE(m1,0)=0 THEN NULL ELSE (COALESCE(m0,0)-COALESCE(m1,0))/NULLIF(COALESCE(m1,0),0) END AS mom_growth
FROM base
ORDER BY mom_growth DESC NULLS LAST
LIMIT 1
""".strip()
        state.update(sql_query=sql, final_answer=False, route=rp)
        return state

    # ----- pattern: top selling SKU of each distributor (Top N optional) -----
    if _any_terms(t, ["top","best","highest","most"]) and _any_terms(t, ["sku","product","material"]) and _any_terms(t, ["distributor"]):
        topn = _parse_topn(qtext) or 1
        dist_key, dist_label, join_sql = _resolve_distributor_key_label(engine, fact, allowed, entity_join_sql, fact_cols)
        have_prod = _table_exists(engine, "tbl_product_master") and (not allowed or "tbl_product_master" in allowed)
        pj = ""
        if have_prod:
            jk = _find_product_join_keys(engine, fact)
            if jk:
                fkey, dkey = jk
                pj = f'\nLEFT JOIN "tbl_product_master" d_prod ON p.{fkey} = d_prod.{dkey}'
        sku_label = _product_display_expr(engine, fact_cols, have_prod)

        sql = f"""
WITH agg AS (
  SELECT
    {dist_label} AS distributor,
    {sku_label} AS sku,
    SUM(p.{measure}) AS total_qty
  FROM "{fact}" p{join_sql}{pj}
  {where_all_sql}
  GROUP BY {dist_label}, {sku_label}
), ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY distributor ORDER BY total_qty DESC NULLS LAST) AS rk
  FROM agg
)
SELECT distributor, sku, total_qty
FROM ranked
WHERE rk <= {int(topn)}
ORDER BY distributor, total_qty DESC
""".strip()
        state.update(sql_query=sql, final_answer=False, route=rp)
        return state

    # ----- pattern: area/state/region wise sales (last month supported) -----
    if _any_terms(t, ["area wise","area-wise","state wise","region wise","city wise","area","state","region","city"]):
        geo_key, geo_label, join_sql, geo_cols = _resolve_geo_label(engine, fact, allowed, entity_join_sql, fact_cols)
        # If user typed a specific region token, add LIKE across available geo cols
        geo_like = _or_like("p", [c for c in geo_cols if c in fact_cols], toks) if geo_cols else None
        filters = [*entity_filters, *time_filters]
        if geo_like: filters.append(geo_like)
        where_geo_sql = "WHERE " + " AND ".join(filters) if filters else ""
        sql = f"""
SELECT
  {geo_label} AS area,
  SUM(p.{measure}) AS total
FROM "{fact}" p{join_sql}
{where_geo_sql}
GROUP BY {geo_label}
ORDER BY total DESC NULLS LAST
""".strip()
        state.update(sql_query=sql, final_answer=False, route=rp)
        return state

    # ----- pattern: top N SKU across each area -----
    if _any_terms(t, ["top"]) and _any_terms(t, ["sku","product","material"]) and _any_terms(t, ["area","state","region","city"]):
        topn = _parse_topn(qtext) or 5
        geo_key, geo_label, join_sql, _ = _resolve_geo_label(engine, fact, allowed, entity_join_sql, fact_cols)
        have_prod = _table_exists(engine, "tbl_product_master") and (not allowed or "tbl_product_master" in allowed)
        pj = ""
        if have_prod:
            jk = _find_product_join_keys(engine, fact)
            if jk:
                fkey, dkey = jk
                pj = f'\nLEFT JOIN "tbl_product_master" d_prod ON p.{fkey} = d_prod.{dkey}'
        sku_label = _product_display_expr(engine, fact_cols, have_prod)

        sql = f"""
WITH agg AS (
  SELECT
    {geo_label} AS area,
    {sku_label} AS sku,
    SUM(p.{measure}) AS total_qty
  FROM "{fact}" p{join_sql}{pj}
  {where_all_sql}
  GROUP BY {geo_label}, {sku_label}
), ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY area ORDER BY total_qty DESC NULLS LAST) AS rk
  FROM agg
)
SELECT area, sku, total_qty
FROM ranked
WHERE rk <= {int(topn)}
ORDER BY area, total_qty DESC
""".strip()
        state.update(sql_query=sql, final_answer=False, route=rp)
        return state

    # ----- pattern: distinct products threshold (entity == superstockist|distributor) -----
    m = re.search(r"(super\s*stockist|distributor)s?.*?(?:sold|buy|bought).*?more\s+than\s+(\d+)\s+distinct\s+products", t)
    if m and date_col:
        entity_kind = "superstockist" if "super" in m.group(1) else "distributor"
        N = int(m.group(2))
        if entity_kind == "distributor":
            ent_key, ent_label, join_sql = _resolve_distributor_key_label(engine, fact, allowed, entity_join_sql, fact_cols)
        else:
            join_sql = entity_join_sql
            if _table_exists(engine, "tbl_superstockist_master") and (not allowed or "tbl_superstockist_master" in allowed):
                jk = _first_working_key(engine, fact, "tbl_superstockist_master") or ("sold_to_party","superstockist_id")
                fkey, dkey = jk
                join_sql += f'\nLEFT JOIN "tbl_superstockist_master" d_ss ON p.{fkey} = d_ss.{dkey}'
                dcols = _columns(engine,"tbl_superstockist_master")
                ent_key, ent_label = f"d_ss.{dkey}", ("d_ss.superstockist_name" if "superstockist_name" in dcols else f"d_ss.{dkey}")
            elif "sold_to_party_name" in fact_cols:
                ent_key, ent_label = "p.sold_to_party", "p.sold_to_party_name"
            else:
                ent_key, ent_label = "1", "'(unknown)'"
        prod_key = "p.product_id" if "product_id" in fact_cols else ("p.material" if "material" in fact_cols else "p.product_name")
        sql = f"""
SELECT
  {ent_label} AS entity,
  COUNT(DISTINCT {prod_key}) AS distinct_products
FROM "{fact}" p{join_sql}
{where_all_sql}
GROUP BY {ent_label}
HAVING COUNT(DISTINCT {prod_key}) > {int(N)}
ORDER BY distinct_products DESC
""".strip()
        state.update(sql_query=sql, final_answer=False, route=rp)
        return state

    # ----- pattern: quantity threshold for distributors (e.g., "less than 10 qty distributors") -----
    thr = _parse_threshold(qtext)
    if thr and _any_terms(t, ["distributor"]):
        op, n = thr
        dist_key, dist_label, join_sql = _resolve_distributor_key_label(engine, fact, allowed, entity_join_sql, fact_cols)
        sql = f"""
SELECT
  {dist_label} AS distributor,
  SUM(p.{measure}) AS total_qty
FROM "{fact}" p{join_sql}
{where_all_sql}
GROUP BY {dist_label}
HAVING SUM(p.{measure}) {op} {n}
ORDER BY total_qty ASC
""".strip()
        state.update(sql_query=sql, final_answer=False, route=rp)
        return state

    # ----- pattern: last sales (date) possibly with product & region filters -----
    if _any_terms(t, ["last sale","last sales","last sold","last invoice"]) and date_col:
        sql = f"""
SELECT MAX(p.{date_col}) AS last_sales_date
FROM "{fact}" p{entity_join_sql}
{where_all_sql}
""".strip()
        state.update(sql_query=sql, final_answer=False, route=rp)
        return state

    # ----- default: SUM total with whatever filters -----
    sql = f"""
SELECT
  SUM(p.{measure}) AS total_value
FROM "{fact}" p{entity_join_sql}
{where_all_sql}
""".strip()
    state.update(sql_query=sql, final_answer=False, route=rp)
    return state
