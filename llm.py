from pathlib import Path
import os
import glob
from typing import TypedDict, Any
from dotenv import load_dotenv

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Date, Numeric, String, Integer, TIMESTAMP

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from twilio.rest import Client
from twilio.base.exceptions import TwilioRestException

from langchain_openai import ChatOpenAI
from langgraph.graph import StateGraph, START, END

# === Agents ===
from agents.sql_cleaned_query_agent import clean_query_node
from agents.find_tables import find_tables_node
from agents.create_sql_query import create_sql_query
from agents.execute_sql_query import execute_sql_query
from agents.check_entity_node import check_entity_node
from agents.summarize_results import summarize_results
from agents.rewrite_sql_query import rewrite_sql_query

# -----------------------------------------------------------------------------
# Environment & setup
# -----------------------------------------------------------------------------
load_dotenv()
api_key = os.getenv("GROQ_API_KEY")

TWILIO_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_NUMBER = os.getenv("TWILIO_NUMBER")
client = Client(TWILIO_SID, TWILIO_TOKEN)

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

llm = ChatOpenAI(
    model="gpt-4o",
    temperature=0,
    max_tokens=None,
    timeout=None,
    max_retries=2,
)

# -----------------------------------------------------------------------------
# DB bootstrap (comment out if DB already has the tables)
# -----------------------------------------------------------------------------
TABLE_COLUMN_TYPES = {
    "tbl_primary": {
        "bill_date": Date,
        "sales_order_date": Date,
        "invoiced_total_quantity": Numeric,
        "distributor_id": String,
        "distributor_name": String
    },
    "tbl_orders": {"order_amount": Numeric},
    "tbl_users": {"signup_time": TIMESTAMP},
    # Optional shipment table (only if you have it)
    "tbl_shipment": {
        "invoice_date": Date,
        "actual_billed_quantity": Numeric,
        "sold_to_party": Integer,
        "supplying_plant": String,
        "sales_district": String,
        "sold_to_party_name": String,
        "city": String,
        "material": String,
        "material_description": String,
    },
}

DATE_COLUMNS = {
    "tbl_primary": {
        "bill_date": "%d/%m/%y",
        "sales_order_date": "%d/%m/%y"
    },
    "tbl_shipment": {
        "invoice_date": "%d/%m/%y"
    }
}

def configure_db_postgres():
    pg_engine = create_engine(
        "postgresql+psycopg2://postgres:12345678@localhost:5432/LLM_Haldiram_primary"
    )
    csv_folder = Path.cwd() / "cooked_data_gk"
    if csv_folder.exists():
        for csv_file in glob.glob(str(csv_folder / "*.csv")):
            table_name = Path(csv_file).stem.lower()
            df = pd.read_csv(csv_file)

            if table_name in DATE_COLUMNS:
                for col, fmt in DATE_COLUMNS[table_name].items():
                    if col in df.columns:
                        try:
                            df[col] = pd.to_datetime(df[col], format=fmt, errors="coerce").dt.date
                        except Exception as e:
                            print(f"❌ Date parsing failed for {table_name}.{col}: {e}")

            dtype_mapping = TABLE_COLUMN_TYPES.get(table_name, {})
            try:
                df.to_sql(
                    name=table_name,
                    con=pg_engine,
                    index=False,
                    if_exists="replace",
                    dtype=dtype_mapping
                )
                print(f"✅ Loaded table: {table_name}")
            except Exception as e:
                print(f"❌ Error loading {table_name}: {e}")

    return pg_engine

engine = configure_db_postgres()
print("✅ DB ready.")

# -----------------------------------------------------------------------------
# State & Workflow
# -----------------------------------------------------------------------------
class FinalState(TypedDict, total=False):
    user_query: str
    cleaned_user_query: str
    tables: list[str]
    sql_query: str
    query_result: str
    exec_success: bool
    error_message: str
    rows: list
    columns: list
    identified_entity: str | None
    matched_entity_value: str | None
    confidence: float
    method: str
    fallback_intents: list
    retry_count: int
    final_answer: bool
    awaiting_route_choice: bool
    route_preference: str
    session_id: str
    # plumbing
    measure_override: str
    date_override: str
    months_override: int
    # pass engine
    engine: Any

MAX_RETRIES = 2

def _exec_router(s: dict):
    if s.get("final_answer"):
        return "summarize_results"
    if s.get("exec_success"):
        return "summarize_results"
    if int(s.get("retry_count", 0)) < MAX_RETRIES:
        return "rewrite_sql_query"
    return "summarize_results"

graph = StateGraph(FinalState)
graph.add_node("clean_query_node", clean_query_node)
graph.add_node("check_entity_node", lambda state: check_entity_node(state, engine))
graph.add_node("find_tables_node", find_tables_node)
graph.add_node("create_sql_query", create_sql_query)
graph.add_node("execute_sql_query", execute_sql_query)
graph.add_node("rewrite_sql_query", rewrite_sql_query)
graph.add_node("summarize_results", summarize_results)

graph.add_edge(START, "clean_query_node")
graph.add_conditional_edges(
    "clean_query_node",
    lambda s: "summarize_results" if s.get("final_answer") else "check_entity_node",
    {"summarize_results": "summarize_results", "check_entity_node": "check_entity_node"}
)
graph.add_edge("check_entity_node", "find_tables_node")
graph.add_edge("find_tables_node", "create_sql_query")
graph.add_edge("create_sql_query", "execute_sql_query")
graph.add_conditional_edges("execute_sql_query", _exec_router,
    {"summarize_results": "summarize_results", "rewrite_sql_query": "rewrite_sql_query"}
)
graph.add_edge("rewrite_sql_query", "execute_sql_query")
graph.add_edge("summarize_results", END)

workflow = graph.compile()

def llm_reply(txt: str, *, session_id: str | None = None, route_pref: str | None = None) -> dict:
    initial_state: FinalState = {
        "user_query": txt,
        "engine": engine,
    }
    if session_id:
        initial_state["session_id"] = session_id
    if route_pref:
        initial_state["route_preference"] = route_pref
    return workflow.invoke(initial_state)

# ------------- Twilio webhook (optional) -----------------
def send_message(to_number, body_text):
    try:
        message = client.messages.create(
            from_=f"whatsapp:{TWILIO_NUMBER}",
            body=body_text,
            to=to_number
        )
    except TwilioRestException as e:
        print(f"❌ Error sending message to {to_number}")
        print(f"🔻 Status Code: {e.status}")
        print(f"🔻 Twilio Error Code: {e.code}")
        print(f"🔻 Error Message: {e.msg}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

@app.post("/")
async def receive_whatsapp_message(request: Request):
    form = await request.form()
    sender_id = str(form.get("From"))
    text_msg = (form.get("Body") or "").strip()
    print("📩 Received:", sender_id, text_msg)

    result = llm_reply(text_msg, session_id=sender_id)
    reply = result.get('query_result', 'No response.')
    print(reply)
    send_message(str(sender_id), reply)
    return {"status": "OK", "message": reply}

# ------------- Local terminal runner -----------------
if __name__ == "__main__":
    print("Type a query. If I ask 'primary or shipment?', just reply with one of them.")
    pending = None  

    while True:
        user_input = input("You: ").strip()
        if not user_input:
            continue

        # Follow-up choice flow
        if pending and user_input.lower() in ("primary", "shipment"):
            result = llm_reply(pending, route_pref=user_input.lower())
            #print(workflow.invoke(initial_state))
            print("Bot:", result.get("query_result") or "No response.")
            pending = None
            continue

        # Normal flow
        result = llm_reply(user_input)
        print("Bot:", result.get("query_result") or "No response.")
        if result.get("awaiting_route_choice"):
            pending = user_input
