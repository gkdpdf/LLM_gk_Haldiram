from pathlib import Path
from langchain.agents import initialize_agent, Tool
from langchain.sql_database import SQLDatabase
from langchain.agents.agent_types import AgentType
from langchain.agents.agent_toolkits import SQLDatabaseToolkit
from sqlalchemy import create_engine
import sqlite3
# from langchain_groq import ChatGroq
from langchain_openai import ChatOpenAI
from dotenv import load_dotenv
from table_relationships import describe_table_relationships
from tbl_col_info import table_info_and_examples
from calculator import calculator_tool
from sql_tool import clean_sql_query
from today import get_today_date
import os
import pandas as pd
import re
import requests
from user_query import rewrite_user_query
from dotenv import load_dotenv
load_dotenv(override=True)
from urllib.parse import parse_qs
from twilio.rest import Client
from twilio.base.exceptions import TwilioRestException

import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)


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

# Configure and return SQLite database connection
def configure_db():
    import glob
    conn = sqlite3.connect(":memory:")
    csv_folder = Path.cwd() / "cooked_data_gk"
    for csv_file in glob.glob(str(csv_folder / "*.csv")):
        table_name = Path(csv_file).stem.lower()
        df = pd.read_csv(csv_file)
        df.to_sql(table_name, conn, index=False, if_exists="replace")
    return SQLDatabase.from_uri("sqlite://", engine_args={"creator": lambda: conn})

# Connect to DB
db = configure_db()
# print("📄 Tables Loaded:", db.get_table_names())

# Tools
relationship_tool = Tool(
    name="TableRelationships",
    func=describe_table_relationships,
    description="Use this tool to understand how tables are related before writing SQL queries."
)
info_example_tool = Tool(
    name="TableInfoAndExamples",
    func=table_info_and_examples,
    description="Use this tool to understand available tables and columns see example queries.",
)

# Register the tool
query_rewriter_tool = Tool(
    name="UserQueryRewriter",
    func=rewrite_user_query,
    description="""
Rewrites unclear, vague, or casual user queries into specific retail-related data questions 
that fit the SQLite retail database schema (products, MRP, orders, discounts, schemes, sales, retailers, etc.).

Use this tool first when:
- The user query is ambiguous or casual (e.g. 'how’s the business?', 'top products?', 'anything on crocin?')
- The user hasn’t mentioned specific columns or table names, but you still need to create a query.

The output should be a **clear rewritten query** suitable for generating SQL.
Do NOT return SQL from this tool — only return the rewritten data question.
"""
)

current_date_tool = Tool(
    name="CurrentDateProvider",
    func=get_today_date,
    description="""
Returns today's date in YYYY-MM-DD format.

Use this when the user asks about:
- Sales/orders "today"
- Discounts/schemes active "today"
- Anything filtered to the current date

Always call this before generating SQL if the query depends on "today".
"""
)


calculator = Tool(
    name="calculator",
    func=calculator_tool,
    description="Use this tool to perform basic math like 5+3, 10/2, 4*6, 12-4,percentage. Input should be a valid arithmetic expression."
)

sql_cleanup_tool = Tool(
    name="clean_sql_query",
    func=clean_sql_query,
    description="Cleans SQL input by removing markdown formatting like ```sql ... ```. Use before passing to the SQL executor if needed."
)

toolkit = SQLDatabaseToolkit(db=db, llm=llm)
tools = toolkit.get_tools() + [relationship_tool, info_example_tool,query_rewriter_tool,current_date_tool,calculator,sql_cleanup_tool]
agent = initialize_agent(
    tools=tools,
    llm=llm,
    agent_type=AgentType.OPENAI_FUNCTIONS,
    verbose=True,
    handle_parsing_errors=True,
    agent_kwargs=
 {
  "system_message": """
  
  "You are an expert SQL assistant helping query a SQLite-based retail database. Your users are non-technical (e.g., retailers, sales reps).

🧠 Behavior Instructions:
1. If the user question is vague, casual, or not specific enough, first use the tool `UserQueryRewriter` to rewrite it.
2. Then follow this strict response format:
   - Action: <Tool name or Final Answer>
   - Action Input: <tool input or SQL query>

🧠 Behavior Guidelines:
1. Always make sure the SQL is syntactically correct for SQLite.
2. Use table and column names exactly as they exist in the database.
3. Show only SELECT queries unless asked otherwise.
4. Use LIMIT 5 by default unless the user asks for more.
5. When answering questions like \"what's the margin of X\", return SKU_Name and computed margin (MRP - Price).
6. Keep answers short, retailer-friendly, and non-technical.

⚠️ SQL Output Rules:
- Do NOT use triple backticks or markdown formatting.
- SQL must be plain text only — safe to execute without preprocessing.
- Do not include 'sql' or any explanation before or after the query.
- Use 'like' or 'lower(column) like lower(?)' for case-insensitive filters.
- If you got blank result via SQL queries then report null with correct context.

📦 Examples:

✅ Example 1 — User: Which product sold the most?
Action: Final Answer  
Action Input:  
SELECT sku_name, SUM(order_quantity) AS total_quantity_sold  
FROM retailer_order_product_details  
GROUP BY sku_name  
ORDER BY total_quantity_sold DESC  
LIMIT 1;

✅ Example 2 — User: Show me the top 5 SKUs by margin  
Action: Final Answer  
Action Input:  
SELECT sku_name, (mrp - price) AS margin  
FROM products  
ORDER BY margin DESC  
LIMIT 5;

✅ Example 3 — User: What's the total sales for Parle G?  
Action: Final Answer  
Action Input:  
SELECT sku_name, SUM(order_amount) AS total_sales  
FROM retailer_order_product_details  
WHERE lower(sku_name) LIKE lower('%parle g%')  
GROUP BY sku_name;

✅ Example 4 — User: List all retailers in Delhi  
Action: Final Answer  
Action Input:  
SELECT retailer_name, city  
FROM retailers  
WHERE lower(city) LIKE lower('%delhi%')  
LIMIT 5;

✅ Example 5 — User: Get last 5 orders  
Action: Final Answer  
Action Input:  
SELECT order_id, retailer_id, order_date, order_amount  
FROM retailer_orders  
ORDER BY order_date DESC  
LIMIT 5;
"""
}
)


def llm_reply(text):
    # try:
    response = agent.run(text)
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
    return {"status": "OK"}

    