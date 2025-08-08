from sqlalchemy import text, create_engine
import pickle

# DB + knowledge loading
engine = create_engine("postgresql+psycopg2://postgres:12345678@localhost:5432/LLM_Haldiram_primary")
dict_knowledge = pickle.load(open("kb_haldiram_primary.pkl", "rb"))

# Entity columns to match against real values
ENTITY_COLUMNS = ["super_stockist_name", "distributor_name", "product_name"]

# Preload known column names from schema (for fallback if needed)
all_schema_columns = set()
for table_desc in dict_knowledge.values():
    if isinstance(table_desc, str):
        for line in table_desc.splitlines():
            if ":" in line:
                col = line.split(":")[0].strip().replace("`", "").replace('"', '')
                all_schema_columns.add(col.lower())

# Optional: fallback intent mapping
fallback_column_groups = {
    "sales": ["invoiced_total_quantity", "invoice_value", "sales_value", "volume"],
    "date": ["sales_order_date", "month", "year"],
    "location": ["region", "state", "zone", "city"],
    "distributor": ["distributor_name", "distributor_id", "distributor_erp_id"],
    "super_stockist": ["super_stockist_name", "super_stockist_id"]
}


def check_entity_node(state):
    user_query = state.get("cleaned_user_query", "").lower()
    identified_entity = None
    matched_value = None
    fallback_intents = []

    # === STEP 1: Try to match actual entity values from DB
    try:
        for column in ENTITY_COLUMNS:
            query = text(f"SELECT DISTINCT {column} FROM tbl_primary WHERE {column} IS NOT NULL LIMIT 500")
            with engine.connect() as conn:
                values = conn.execute(query).fetchall()

            for row in values:
                val = str(row[0]).strip().lower()
                if val and val in user_query:
                    identified_entity = column
                    matched_value = val
                    print(f"✅ Matched entity: '{val}' in column '{column}'")
                    break

            if identified_entity:
                break
    except Exception as e:
        print(f"❌ Error while matching entity: {e}")

    # === STEP 2: Fallback intent inference from keywords
    if not identified_entity:
        print("ℹ️ No direct entity match found. Trying fallback intent detection...")
        for intent, keywords in fallback_column_groups.items():
            for keyword in keywords:
                if keyword.lower() in user_query:
                    fallback_intents.append(intent)
                    print(f"🧠 Fallback intent matched: '{intent}' via keyword '{keyword}'")
                    break  # avoid adding multiple for same intent

        if not fallback_intents:
            print("⚠️ No fallback intents identified either.")

    # === Final summary
    if identified_entity:
        print(f"🔍 Final identified entity: {identified_entity}")
        print(f"🔍 Matched value from user query: {matched_value}")
    elif fallback_intents:
        print(f"🔍 Inferred fallback intents: {fallback_intents}")
    else:
        print("⚠️ No match (entity or intent) found.")

    # === Save to state
    state["identified_entity"] = identified_entity
    state["matched_entity_value"] = matched_value
    state["fallback_intents"] = fallback_intents if not identified_entity else None

    return state
