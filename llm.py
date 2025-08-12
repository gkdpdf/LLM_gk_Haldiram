from pathlib import Path
import os
import glob
import json
from typing import TypedDict
from dotenv import load_dotenv

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import Date, Numeric, String, Float, Integer, TIMESTAMP

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
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
# DB bootstrap: load CSVs into Postgres (optional if DB already has tables)
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
    "tbl_users": {"signup_time": TIMESTAMP}
}

DATE_COLUMNS = {
    "tbl_primary": {
        "bill_date": "%d/%m/%y",
        "sales_order_date": "%d/%m/%y"
    }
}

def configure_db_postgres():
    pg_engine = create_engine(
        "postgresql+psycopg2://postgres:12345678@localhost:5432/LLM_Haldiram_primary"
    )
    csv_folder = Path.cwd() / "cooked_data_gk"
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

pg_engine = configure_db_postgres()
print("✅ DB ready.")

# -----------------------------------------------------------------------------
# State & Workflow
# -----------------------------------------------------------------------------
class FinalState(TypedDict):
    user_query: str
    cleaned_user_query: str
    tables: list[str]
    sql_query: str
    query_result: str
    exec_success: bool
    error_message: str
    rows: list
    columns: list
    identified_entity: str
    matched_entity_value: str
    confidence: float
    method: str
    fallback_intents: list
    retry_count: int
    final_answer: bool

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
graph.add_node("check_entity_node", check_entity_node)
graph.add_node("find_tables_node", find_tables_node)          # << added
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

graph.add_edge("check_entity_node", "find_tables_node")       # << changed
graph.add_edge("find_tables_node", "create_sql_query")        # << added
graph.add_edge("create_sql_query", "execute_sql_query")
graph.add_conditional_edges("execute_sql_query", _exec_router,
    {"summarize_results": "summarize_results", "rewrite_sql_query": "rewrite_sql_query"}
)
graph.add_edge("rewrite_sql_query", "execute_sql_query")
graph.add_edge("summarize_results", END)

workflow = graph.compile()

def llm_reply(txt: str) -> dict:
    initial_state: FinalState = {
        "user_query": txt,
        "cleaned_user_query": "",
        "tables": [],  # let find_tables_node supply this
        "sql_query": "",
        "query_result": "",
        "exec_success": False,
        "error_message": "",
        "rows": [],
        "columns": [],
        "identified_entity": "",
        "matched_entity_value": "",
        "confidence": 0.0,
        "method": "",
        "fallback_intents": [],
        "retry_count": 0,
        "final_answer": False,
    }
    result = workflow.invoke(initial_state)
    return result

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
    text_msg = form.get("Body")
    print("📩 Received:", sender_id, text_msg)

    reply = llm_reply(text_msg).get('query_result', 'No response.')
    print(reply)
    send_message(str(sender_id), reply)
    return {"status": "OK", "message": reply}

if __name__ == "__main__":
    demo = {"user_query": "Bhujia 100 gm sales "}
    print(workflow.invoke(demo))
