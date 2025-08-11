from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from dotenv import load_dotenv
import pickle
import re

load_dotenv(override=True)

# Load schema and relationships
with open("kb_haldiram_primary.pkl", "rb") as f:
    total_table_dict = pickle.load(f)

with open("relationship_tables.txt", "r", encoding="utf-8") as f:
    relationship_json_str = f.read()

COLUMN_TYPE_OVERRIDES = {
    "tbl_primary.bill_date": "DATE (use TO_DATE(bill_date, 'DD/MM/YY'))",
    "tbl_primary.sales_order_date": "DATE (use TO_DATE(sales_order_date, 'DD/MM/YY'))",
    "tbl_primary.invoiced_total_quantity": "NUMERIC",
    "tbl_orders.order_amount": "NUMERIC",
}

def filter_dict_tables(total_table_dict, tables):
    return {k: total_table_dict[k] for k in tables if k in total_table_dict}

def strip_sample_values(text: str) -> str:
    return re.sub(r"<sample values:.*?>", "", text)

def apply_type_overrides(table_dict: dict, overrides: dict) -> dict:
    updated_dict = {}
    for table_name, schema_text in table_dict.items():
        updated_lines = []
        for line in schema_text.splitlines():
            line = strip_sample_values(line)
            m = re.match(r'^([a-zA-Z0-9_]+)\s*:\s*(.+?),\s*datatype:\s*(\w+)', line)
            if m:
                col, desc, dtype = m.groups()
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

def create_markdown_from_dict(annotated_dict: dict) -> str:
    blocks = []
    for table_name, content in annotated_dict.items():
        blocks.append(f"### **{table_name}**\n```json\n{content}\n```")
    return "\n\n".join(blocks)

llm = ChatOpenAI(model="gpt-4o", temperature=0)

def create_sql_query(state: dict) -> dict:
    sql_tables = state["tables"]
    user_query = state["cleaned_user_query"]

    identified_entity = state.get("identified_entity")
    matched_value = state.get("matched_entity_value")
    fallback_intents = state.get("fallback_intents")

    if identified_entity and matched_value:
        user_query = re.sub(
            r"(distributor|retailer|partner|vendor)",
            identified_entity.replace("_", " "),
            user_query,
            flags=re.IGNORECASE
        )
        user_query += f" Filter only for {identified_entity} = '{matched_value}'"
    elif fallback_intents:
        user_query += f" Focus on intent: {', '.join(fallback_intents)}"

    filtered_schema = filter_dict_tables(total_table_dict, sql_tables)
    overridden_schema = apply_type_overrides(filtered_schema, COLUMN_TYPE_OVERRIDES)
    schema_markdown = create_markdown_from_dict(overridden_schema)

    prompt = ChatPromptTemplate.from_messages([
        ("system", f"""
You are an expert in PostgreSQL and relational databases.

Your task is to write a **correct PostgreSQL query** based strictly on:
1. The cleaned user query.
2. The provided table schema (columns, datatypes).
3. The relationships between the tables.

Return ONLY the valid SQL query — no markdown, no explanation.

Schema:
{schema_markdown}

Table Relationships:
{relationship_json_str}
"""),
        ("human", "User query: {user_query}\nOnly use these tables: {tables}")
    ])

    chain = prompt | llm | StrOutputParser()
    output = chain.invoke({"user_query": user_query, "tables": ", ".join(sql_tables)})

    cleaned_sql = re.sub(r"^```sql\s*|```$", "", output.strip(), flags=re.IGNORECASE).strip()
    state["sql_query"] = cleaned_sql
    return state
