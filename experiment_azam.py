#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pathlib import Path
from langchain.agents import initialize_agent, Tool
from langchain.sql_database import SQLDatabase
from langchain.agents.agent_types import AgentType
from langchain.agents.agent_toolkits import SQLDatabaseToolkit
from sqlalchemy import create_engine
import sqlite3
from langchain_groq import ChatGroq
from dotenv import load_dotenv
from table_relationships import describe_table_relationships
from tbl_col_info import table_info_and_examples
import os
import pandas as pd
import re
import requests
import glob


# In[2]:


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


# In[3]:


from agents.sql_cleaned_query_agent import clean_query_node
from agents.find_tables import find_tables_node
from agents.create_sql_query import create_sql_query
from agents.execute_sql_query import execute_sql_query


# In[4]:


# Load .env file and get GROQ API key
load_dotenv()
api_key = os.getenv("GROQ_API_KEY")


# In[5]:


from langchain_openai import ChatOpenAI


# In[6]:


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


# In[7]:


# Configure and return SQLite database connectiondef configure_db():

def configure_db():
    # ✅ Create MySQL engine using pymysql
    mysql_engine = create_engine(
        "mysql+pymysql://root:Iameighteeni%4018@127.0.0.1:3306/txt2sql"
    )

    csv_folder = Path.cwd() / "cooked_data_gk"
    for csv_file in glob.glob(str(csv_folder / "*.csv")):
        table_name = Path(csv_file).stem.lower()
        df = pd.read_csv(csv_file)

        # ✅ Save each CSV as table in MySQL
        df.to_sql(name=table_name, con=mysql_engine, index=False, if_exists="replace")
        print(f"✅ Loaded table: {table_name}")

    # ✅ Return LangChain-compatible MySQL connection using pymysql
    return mysql_engine,SQLDatabase.from_uri(
        "mysql+pymysql://root:Iameighteeni%4018@127.0.0.1:3306/txt2sql"
    )

# 🔌 Connect to DB and print tables
mysql_engine, db = configure_db()
print("📄 Tables Loaded:", db.get_table_names())


# In[8]:


print(db.dialect)
print(db.get_usable_table_names())


# In[9]:


class finalstate(TypedDict):
    user_query: str
    cleaned_user_query: str
    tables: list[str]
    failed_query : str
    # order_out: str
    # product_out: str
    # filtered_col : str
    # filter_extractor: list[str]
    # fuzz_match: list[str]
    sql_query: str
    query_results: str


# ## Defining Nodes

# In[10]:


graph = StateGraph(finalstate)

graph.add_node("clean_query_node", clean_query_node)
graph.add_node("find_tables_node", find_tables_node)
graph.add_node("create_sql_query", create_sql_query)
graph.add_node("execute_sql_query", execute_sql_query)

# edges
graph.add_edge(START, 'clean_query_node')
graph.add_edge('clean_query_node', 'find_tables_node')
graph.add_edge('find_tables_node', 'create_sql_query')
graph.add_edge('create_sql_query', 'execute_sql_query')

graph.add_edge('execute_sql_query', END)

workflow = graph.compile()


# In[11]:


# import nest_asyncio
# nest_asyncio.apply()


# In[12]:


# from langchain_core.runnables.graph_mermaid import MermaidDrawMethod
# from IPython.display import Image

# # This will now work in Jupyter
# Image(workflow.get_graph().draw_mermaid_png(draw_method=MermaidDrawMethod.PYPPETEER))



# In[13]:


initial_state = {"user_query" : "what is the discount or scheme I willl get 10 qts of Eno"}


# In[14]:


workflow.invoke(initial_state)


# # Old code

# In[15]:


# # Tools
# relationship_tool = Tool(
#     name="TableRelationships",
#     func=describe_table_relationships,
#     description="Use this tool to understand how tables are related before writing SQL queries."
# )
# info_example_tool = Tool(
#     name="TableInfoAndExamples",
#     func=table_info_and_examples,
#     description="Use this tool to understand available tables and columns see example queries.",
# )


# In[16]:


# toolkit = SQLDatabaseToolkit(db=db, llm=llm)
# tools = toolkit.get_tools() + [relationship_tool, info_example_tool]


# In[17]:


# tools


# In[ ]:





# In[18]:


# agent = initialize_agent(
#     tools=tools,
#     llm=llm,
#     agent_type=AgentType.OPENAI_FUNCTIONS,
#     verbose=True,
#     handle_parsing_errors=True,
#     agent_kwargs={
#         "system_message": """
# You are an expert SQL assistant helping query a SQLite-based retail database which gives one line answer.

# ✅ Instructions:
# 1. If table info is missing, always use `TableInfoAndExamples` first and then check for 
# 'TableRelationships' for the relationships between tables.

# 2. Use this response format:
#    - Action: <tool-name or Final Answer>
#    - Action Input: <input>
# 3. Execute real SQL queries after table identification.
# 4. Only show SELECT query results, not intermediate thoughts or tools.
# 5. When asked about "top discount schemes", use:
# SELECT name, discount_percent FROM tbl_scheme WHERE is_active = 1 ORDER BY discount_percent DESC;

# 6. When asked about MRP of the brands, check in the tbl_product_master
# 7. When asked about products with have top discounts then use 'TableRelationships' for joining and fetch products.
# """
#     }
# )


# In[19]:


# # Chat input
# user_query = input()


# In[20]:


# response = agent.run(user_query)


# In[21]:


# response


# In[22]:


# sql_match = re.search(r"(SELECT\s.+?;)", response, re.IGNORECASE | re.DOTALL)
# print(sql_match)


# In[23]:


# try:
#     sql_match = re.search(r"(SELECT\s.+?;)", response, re.IGNORECASE | re.DOTALL)
#     if sql_match:
#         query = sql_match.group(1).strip()
#         result = db.run(query)

#         if isinstance(result, list):
#             df = pd.DataFrame(result)
#             print("Executed SQL Query:")
#             print(query)

#             # Clean column names for display
#             df.columns = [col.replace("_", " ").title() for col in df.columns]

#             # Round numeric columns (like discount)
#             for col in df.select_dtypes(include=['float']):
#                 df[col] = df[col].round(2)

#             # Format currency columns if MRP exists
#             if "Mrp" in df.columns:
#                 df["Mrp"] = df["Mrp"].apply(lambda x: f"₹{x:.2f}")

#             # Display DataFrame
#             display(df)

#         else:
#             print("Query result:")
#             print(result)
#     else:
#         print("No SQL query found in response.")
#         print(response)
# except Exception as e:
#     print("Error:", e)


# In[ ]:




