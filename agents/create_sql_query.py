from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from dotenv import load_dotenv
import pickle
import json
import re

load_dotenv(override=True)

# Load schema and relationships
with open("kb_haldiram_primary.pkl", "rb") as f:
    total_table_dict = pickle.load(f)

with open("relationship_tables.txt", "r", encoding="utf-8") as f:
    relationship_json_str = f.read()

# ✅ Column type overrides
COLUMN_TYPE_OVERRIDES = {
    "tbl_primary.bill_date": "DATE (use TO_DATE(bill_date, 'DD/MM/YY'))",
    "tbl_primary.sales_order_date": "DATE (use TO_DATE(sales_order_date, 'DD/MM/YY'))",
    "tbl_primary.invoiced_total_quantity": "NUMERIC",
    "tbl_orders.order_amount": "NUMERIC",
}

# ✅ Step 1: Filter only selected tables
def filter_dict_tables(total_table_dict, tables):
    return {k: total_table_dict[k] for k in tables if k in total_table_dict}

# ✅ Step 2: Strip `<sample values: ...>` from schema lines
def strip_sample_values(text: str) -> str:
    return re.sub(r"<sample values:.*?>", "", text)

# ✅ Step 3: Apply column type overrides & clean schema
def apply_type_overrides(table_dict: dict, overrides: dict) -> dict:
    updated_dict = {}
    for table_name, schema_text in table_dict.items():
        updated_lines = []
        for line in schema_text.splitlines():
            line = strip_sample_values(line)
            match = re.match(r'^([a-zA-Z0-9_]+)\s*:\s*(.+?),\s*datatype:\s*(\w+)', line)
            if match:
                col, desc, dtype = match.groups()
                full_col = f"{table_name}.{col}"
                new_type = overrides.get(full_col)
                if new_type:
                    updated_line = re.sub(r'datatype:\s*\w+', f'datatype: {new_type}', line)
                    updated_lines.append(updated_line)
                else:
                    updated_lines.append(line)
            else:
                updated_lines.append(line)
        updated_dict[table_name] = "\n".join(updated_lines)
    return updated_dict

# ✅ Step 4: Convert schema to markdown format
def create_markdown_from_dict(annotated_dict: dict) -> str:
    blocks = []
    for table_name, content in annotated_dict.items():
        blocks.append(f"### **{table_name}**\n```json\n{content}\n```")
    return "\n\n".join(blocks)

# ✅ Step 5: Setup LLM (use gpt-3.5-turbo or gpt-4o)
llm = ChatOpenAI(model="gpt-4o", temperature=0)

# ✅ Step 6: SQL generation function
def create_sql_query(state: dict) -> dict:
    sql_tables = state["tables"]
    user_query = state["cleaned_user_query"]

    # Inject entity info from earlier node
    identified_entity = state.get("identified_entity")
    matched_value = state.get("matched_entity_value")
    fallback_intents = state.get("fallback_intents")

    # ✅ Step 6.1: Improve the query with explicit entity injection
    if identified_entity and matched_value:
        # Replace vague mentions like 'distributor' in the cleaned query
        user_query = re.sub(r"(distributor|retailer|partner|vendor)", identified_entity.replace("_", " "), user_query, flags=re.IGNORECASE)
        # Append forced filter condition
        user_query += f" Filter only for {identified_entity} = '{matched_value}'"
    elif fallback_intents:
        user_query += f" Focus on intent: {', '.join(fallback_intents)}"

    # ✅ Step 6.2: Prepare schema for LLM
    filtered_schema = filter_dict_tables(total_table_dict, sql_tables)
    overridden_schema = apply_type_overrides(filtered_schema, COLUMN_TYPE_OVERRIDES)
    schema_markdown = create_markdown_from_dict(overridden_schema)

    # ✅ Step 6.3: Prompt setup
    prompt = ChatPromptTemplate.from_messages([
        ("system", f"""
You are an expert in PostgreSQL and relational databases.

Your task is to write a **correct PostgreSQL query** based strictly on:
1. The cleaned user query.
2. The provided table schema (columns, datatypes).
3. The relationships between the tables.

⚠️ Return ONLY the valid SQL query — no markdown, no explanation.

📚 Schema:
{schema_markdown}

🔗 Table Relationships:
{relationship_json_str}
"""),
        ("human", "User query: {user_query}\nOnly use these tables: {tables}")
    ])

    # ✅ Step 6.4: LLM invocation
    chain = prompt | llm | StrOutputParser()
    output = chain.invoke({
        "user_query": user_query,
        "tables": ", ".join(sql_tables)
    })

    # ✅ Step 6.5: Clean SQL result
    cleaned_sql = re.sub(r"^```sql\s*|```$", "", output.strip(), flags=re.IGNORECASE).strip()
    state["sql_query"] = cleaned_sql
    return state
