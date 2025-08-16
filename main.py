from twilio.rest import Client
from twilio.base.exceptions import TwilioRestException

from pathlib import Path
from langchain.agents import initialize_agent, Tool
from langchain.sql_database import SQLDatabase
from langchain.agents.agent_types import AgentType
from langchain.agents.agent_toolkits import SQLDatabaseToolkit
import os
import pandas as pd
import re
import requests
import glob
from sqlalchemy.types import Date

from langgraph.graph import StateGraph, START, END
from typing import Dict, Any, TypedDict, Annotated
import operator
import pickle
from IPython.display import Image

from thefuzz import process
from datetime import datetime
import json
import tqdm


from sqlalchemy import create_engine,  text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.types import Integer, Float, String

from agents.sql_cleaned_query_agent import clean_query_node
from agents.find_tables import find_tables_node
from agents.create_sql_query import create_sql_query
from agents.execute_sql_query import execute_sql_query
from agents.rewrite_sql_query import rewrite_sql_query
from agents.summarise_query_results import summarise_results

from dotenv import load_dotenv
load_dotenv(override=True)

import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

import urllib.parse 

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


TWILIO_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
# auth_token = f'[{TWILIO_TOKEN}]'
TWILIO_NUMBER = os.getenv("TWILIO_NUMBER")
client = Client(TWILIO_SID, TWILIO_TOKEN)

VERIFY_TOKEN = "abcdef"
ACCESS_TOKEN = "EAALEcUlgHZAsBPFGHFEfdWsbMHNCZA9XdZCJZCGoAmtPmRA5Q79SU4RsNSXePS8ZCDkhSLdaU8wZBziYyUM8W6I9qKxS7Wvow2mJNNBVwv6dTEavXZCWX9MFJn8rOkOfCA1reOGIedqQ7BChWnJYZBkYU8BJZBrIXsDuAYOZAgCycFIJrqvIyV02KqX9lLeKPUdKeT0ZCib0rZBRcYrk2Yq5Pnh8tabPruFof2TPzXtVNtIWuF186wZDZDEAALEcUlgHZAsBPBr95jnfVEgIgEB0W1rs6bnbIhRiQRotIgnw0Otx9lYnwCXCplbheVmDrnu4LZB6BZCOG4U0OKclZCnqPoZAMt8Utxo0A2kE3fr4OSzzReUmzaXrtMuvtMndAcJfZBIhx4NrPhis73eAYwPPfVRyfCb5E3w3PmJpdSnKX4GXzCTcrS6BqZCKw6mTufYZC0v5ZC8urUCARxCa4K8EX2UZArlhKrXW2r8uUZCtoIeYkZD"
PHONE_NUMBER_ID = "+9189547783378"

@app.get("/webhook")
async def verify(request: Request):
    params = dict(request.query_params)
    if params.get("hub.mode") == "subscribe" and params.get("hub.verify_token") == VERIFY_TOKEN:
        return int(params["hub.challenge"])
    return {"status" : "Verification failed"}


# Load .env file and get GROQ API key
load_dotenv()
api_key = os.getenv("GROQ_API_KEY")

if not api_key:
    print("GROQ_API_KEY not found in .env file.")
    
# Setup the LLM
# llm = ChatGroq(groq_api_key=api_key, model_name="Llama3-8b-8192", streaming=True)

# Load engine and knowledge base
password = urllib.parse.quote_plus("Iameighteeni@18")
def configure_db():
    # ✅ Create MySQL engine using pymysql
    mysql_engine = create_engine(
        f"postgresql+psycopg2://postgres:{password}@localhost:5432/LLM_Haldiram_primary"
    )

    csv_folder = Path.cwd() / "cooked_data_gk"
    for csv_file in glob.glob(str(csv_folder / "*.csv")):
        table_name = Path(csv_file).stem.lower()
        df = pd.read_csv(csv_file)

        # ✅ Save each CSV as table in MySQL
        df.to_sql(name=table_name, con=mysql_engine, index=False, if_exists="replace")
        print(f"✅ Loaded table: {table_name}")
        
                # ✅ Convert bill_date column from text to date
        if table_name == "tbl_primary":
            with mysql_engine.connect() as conn:
                conn.execute(
                    text("""
                        ALTER TABLE public.tbl_primary
                        ALTER COLUMN bill_date TYPE date
                        USING TO_DATE(bill_date, 'DD/MM/YY')
                    """)
                )
                conn.commit()
                print("✅ Converted bill_date to DATE type")

    # ✅ Return LangChain-compatible MySQL connection using pymysql
    return mysql_engine,SQLDatabase.from_uri(
        f"postgresql+psycopg2://postgres:{password}@localhost:5432/LLM_Haldiram_primary"
    )

# 🔌 Connect to DB and print tables
mysql_engine, db = configure_db()
print("📄 Tables Loaded:", db.get_table_names())

class finalstate(TypedDict):
    user_query: str
    cleaned_user_query: str
    tables: list[str]
    dataframe : pd.DataFrame
    failed_query: Annotated[list[str], operator.add] 
    query_error_message : Annotated[list[str], operator.add]
    retry_count : int
    is_empty_result : bool
    sql_query: str
    query_results: str
    summary_results: str
    
### Create a node to check wether the query was failed or not
def check_query_failed(state: finalstate) -> finalstate:
    # if state["is_empty_result"]:
    #     return "empty_result" 
    if state["query_results"] == "Query executed successfully":
        return "success"
    else:
        return "failed"
    
# Defining nodes
graph = StateGraph(finalstate)

graph.add_node("clean_query_node", clean_query_node)
graph.add_node("find_tables_node", find_tables_node)
graph.add_node("create_sql_query", create_sql_query)
graph.add_node("execute_sql_query", execute_sql_query)
graph.add_node("rewrite_sql_query", rewrite_sql_query)
graph.add_node("summarise_results", summarise_results)

# edges
graph.add_edge(START, 'clean_query_node')
graph.add_edge('clean_query_node', 'find_tables_node')
graph.add_edge('find_tables_node', 'create_sql_query')
graph.add_edge('create_sql_query', 'execute_sql_query')
graph.add_conditional_edges('execute_sql_query', check_query_failed, {"success" : "summarise_results", "failed" : "rewrite_sql_query"})
graph.add_edge('rewrite_sql_query', 'execute_sql_query')
graph.add_edge('summarise_results', END)

workflow = graph.compile()




def llm_reply(text):
    # try:
    response = workflow.invoke({"user_query" : text, "is_empty_result" : False})
        # sql_match = re.search(r"(SELECT\s.+?;)", response, re.IGNORECASE | re.DOTALL)

        # if not sql_match:
        #     print("No SQL query found in response.")
        #     print(response)
        #     return response

        # query = sql_match.group(1).strip()
        # result = db.run(query)

        # print("Executed SQL Query:")
        # print(query)

        # if isinstance(result, list):
        #     df = pd.DataFrame(result)

        #     # Format columns
        #     df.columns = [col.replace("_", " ").title() for col in df.columns]

        #     # Round numeric values
        #     for col in df.select_dtypes(include='float'):
        #         df[col] = df[col].round(2)

        #     # Format MRP column if it exists
        #     if "Mrp" in df.columns:
        #         df["Mrp"] = df["Mrp"].apply(lambda x: f"₹{x:.2f}")

        #     display(df)
        # else:
        #     print("Query result:")
        #     print(result)

    return response

    # except Exception as e:
    #     print("Error:", e)
    #     return f"Error occurred: {e}"


# ✅ Send response to WhatsApp
def send_whatsapp_message(to_number: str, reply_text: str):
    try:
        print('to number', to_number)
        print('reply text', reply_text)
        message = client.messages.create(
            from_=f"whatsapp:{TWILIO_NUMBER}",
            to=f"whatsapp:{to_number}",
            body=reply_text
        )
        print(f"✅ Message sent to {to_number}. SID: {message.sid}")
    except Exception as e:
        print("❌ Twilio send error:", e)

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
    
    reply = llm_reply(text)
    send_message(str(sender_id), reply)
    return {"status": "OK", "reply":reply} 

    