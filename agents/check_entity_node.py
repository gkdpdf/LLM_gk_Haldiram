from sqlalchemy import text, create_engine
import pickle
import urllib.parse 

# Load engine and knowledge base
password = urllib.parse.quote_plus("Iameighteeni@18")
engine = create_engine(f"postgresql+psycopg2://postgres:{password}@localhost:5432/LLM_Haldiram_primary")
dict_knowledge = pickle.load(open("kb_haldiram_primary.pkl", "rb"))

ENTITY_COLUMNS = ["super_stockist_name", "distributor_name", "product_name"]

# Flatten all schema columns from annotated KB
all_schema_columns = set()
for table_desc in dict_knowledge.values():
    if isinstance(table_desc, str):
        for line in table_desc.splitlines():
            if ":" in line:
                col_name = line.split(":")[0].strip().replace("`", "").replace('"', '')
                all_schema_columns.add(col_name.lower())

def check_entity_node(state):
    user_query = state.get("cleaned_user_query", "").lower()
    identified_entity = None
    fallback_columns = []

    # STEP 1: Try to match known entity values from the database
    try:
        for column in ENTITY_COLUMNS:
            query = text(f"SELECT DISTINCT {column} FROM tbl_primary LIMIT 500")
            with engine.connect() as conn:
                results = conn.execute(query).fetchall()
            
            for row in results:
                value = str(row[0]).strip().lower()
                if value and value in user_query:
                    identified_entity = column
                    print(f"✅ Found exact entity match: {value} in column '{column}'")
                    break
            if identified_entity:
                break
    except Exception as e:
        print(f"❌ Error during entity match: {e}")

    # STEP 2: If no match, fallback to inferring from known schema column names
    if not identified_entity:
        print("ℹ️ No entity found. Checking for fallback schema column matches...")

        for col in all_schema_columns:
            if col in user_query:
                fallback_columns.append(col)
                print(f"🧠 Inferred schema-related keyword: {col}")
        
        # Remove redundancy or prioritize some fallback groups if needed here

    # Logging
    if identified_entity:
        print(f"🔍 Final Identified Entity: {identified_entity}")
    elif fallback_columns:
        print(f"🔍 Inferred Fallback Columns: {fallback_columns}")
    else:
        print("⚠️ Could not infer any relevant column.")

    # Save results to state
    state["identified_entity"] = identified_entity
    state["general_focus_columns"] = fallback_columns if not identified_entity else None
    return state
