from pathlib import Path
from langchain.agents import initialize_agent, Tool
from langchain.sql_database import SQLDatabase
from langchain.agents.agent_types import AgentType
from langchain.agents.agent_toolkits import SQLDatabaseToolkit
from sqlalchemy import create_engine
import sqlite3
from langchain_groq import ChatGroq
from dotenv import load_dotenv
# from table_relationships import describe_table_relationships
# from tbl_col_info import table_info_and_examples
import os
import pandas as pd
import re
import requests
import glob
import pandas as pd
from sqlalchemy import create_engine
from langchain_community.utilities.sql_database import SQLDatabase
from pathlib import Path
import glob
from langgraph.graph import StateGraph, START, END
from typing import Dict, Any, TypedDict, Annotated
from operator import add
import pickle
from IPython.display import Image
from thefuzz import process
from datetime import datetime
import json
import tqdm
import pandas as pd
from sqlalchemy import create_engine,  text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.types import Integer, Float, String
from sqlalchemy.types import String, Date, Numeric, Float, Integer, TIMESTAMP


from agents.sql_cleaned_query_agent import clean_query_node
from agents.find_tables import find_tables_node
from agents.create_sql_query import create_sql_query
from agents.execute_sql_query import execute_sql_query
from agents.check_entity_node import check_entity_node
from agents.get_current_date import get_current_date_node
from twilio.rest import Client
from twilio.base.exceptions import TwilioRestException

# Load .env file and get GROQ API key
load_dotenv()
api_key = os.getenv("GROQ_API_KEY")


TWILIO_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
# auth_token = f'[{TWILIO_TOKEN}]'
TWILIO_NUMBER = os.getenv("TWILIO_NUMBER")
client = Client(TWILIO_SID, TWILIO_TOKEN)


from langchain_openai import ChatOpenAI
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.middleware.cors import CORSMiddleware
import httpx

app = FastAPI()

# Middleware to handle CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

llm = ChatOpenAI(
    model="gpt-4o",
    temperature=0,
    max_tokens=None,
    timeout=None,
    max_retries=2,
    # api_key="...",  # if you prefer to pass api key in directly instaed of using env vars
    # base_url="...",
    # organization="...",
    # other params...
)



# Define type overrides per table
TABLE_COLUMN_TYPES = {
    "tbl_primary": {
        "bill_date": Date,
        "sales_order_date": Date,
        "invoiced_total_quantity": Numeric,
        "distributor_id": String,
        "distributor_name": String
    },
    "tbl_orders": {
        "order_amount": Numeric
    },
    "tbl_users": {
        "signup_time": TIMESTAMP
    }
}

# Define columns that need date parsing with their expected format
DATE_COLUMNS = {
    "tbl_primary": {
        "bill_date": "%d/%m/%y",
        "sales_order_date": "%d/%m/%y"
    }
}

def configure_db_postgres():
    # ✅ Create PostgreSQL engine using psycopg2
    pg_engine = create_engine(
        "postgresql+psycopg2://postgres:12345678@localhost:5432/LLM_Haldiram_primary"
    )

    csv_folder = Path.cwd() / "cooked_data_gk"
    for csv_file in glob.glob(str(csv_folder / "*.csv")):
        table_name = Path(csv_file).stem.lower()
        df = pd.read_csv(csv_file)

        # ✅ Parse date columns if defined
        if table_name in DATE_COLUMNS:
            for col, fmt in DATE_COLUMNS[table_name].items():
                if col in df.columns:
                    try:
                        df[col] = pd.to_datetime(df[col], format=fmt, errors="coerce").dt.date
                    except Exception as e:
                        print(f"❌ Date parsing failed for {table_name}.{col}: {e}")

        # ✅ Apply dtype mapping if available
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

    # ✅ Return LangChain-compatible PostgreSQL connection
    return pg_engine, SQLDatabase.from_uri(
        "postgresql+psycopg2://postgres:12345678@localhost:5432/LLM_Haldiram_primary"
    )
# 🔌 Connect to DB and print tables
pg_engine, db = configure_db_postgres()
print("📄 Tables Loaded:", db.get_table_names())




print(db.dialect)
print(db.get_usable_table_names())

class finalstate(TypedDict):
    user_query: str
    cleaned_user_query: str
    tables: list[str]
    # order_out: str
    # product_out: str
    # filtered_col : str
    # filter_extractor: list[str]
    # fuzz_match: list[str]
    sql_query: str
    query_result: str


## defining nodes
graph = StateGraph(finalstate)

graph.add_node("clean_query_node", clean_query_node)
graph.add_node("check_entity_node", check_entity_node)  ## new 
graph.add_node("find_tables_node", find_tables_node)
graph.add_node("get_current_date",get_current_date_node)
graph.add_node("create_sql_query", create_sql_query)
graph.add_node("execute_sql_query", execute_sql_query)

# edges
graph.add_edge(START, 'clean_query_node')

graph.add_conditional_edges(
    "clean_query_node",
    lambda s: END if s.get("final_answer") else "check_entity_node",
    {END: END, "check_entity_node": "check_entity_node"}
)

# graph.add_edge('clean_query_node', 'check_entity_node')  
graph.add_edge('check_entity_node', 'find_tables_node')  
graph.add_edge('find_tables_node','get_current_date')
graph.add_edge('get_current_date', 'create_sql_query')
graph.add_edge('create_sql_query', 'execute_sql_query')
graph.add_edge('execute_sql_query', END) 


workflow = graph.compile()

initial_state = {"user_query" : "CNC biscutis ki sales"}

# print(workflow.invoke(initial_state)['query_result'])
print(workflow.invoke(initial_state))

def llm_reply(txt):
    initial_state = {"user_query" : txt}
    result = workflow.invoke(initial_state)
    return result


def send_message(to_number, body_text):
    try:
        message = client.messages.create(
            from_=f"whatsapp:{TWILIO_NUMBER}",
            body=body_text,
            to=to_number
            )
        # print(f"Message sent to {to_number}: {message.body}")
    except TwilioRestException as e:
        print(f"❌ Error sending message to {to_number}")
        print(f"🔻 Status Code: {e.status}")
        print(f"🔻 Twilio Error Code: {e.code}")
        print(f"🔻 Error Message: {e.msg}")
        # print(f"🔻 More Info: {e.more_info}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

@app.post("/")
async def receive_whatsapp_message(request: Request):
    # text = parse_qs(await request.body())[b'Body'][0].decode()
    form = await request.form()
    sender_id = str(form.get("From"))   
    print(sender_id)# e.g., "whatsapp:+9198XXXX"
    text = form.get("Body") 
    # print(text)# Message body
    print("📩 Received:", sender_id, text)
    
    reply = llm_reply(text)['query_result']
    print(reply)
    send_message(str(sender_id), reply)
    return {"status": "OK","message":reply} 


