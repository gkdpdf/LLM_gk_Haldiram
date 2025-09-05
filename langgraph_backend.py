#!/usr/bin/env python
# coding: utf-8

# In[22]:


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
from sqlalchemy.types import Date
from langgraph.types import interrupt, Command
from langgraph.checkpoint.memory import InMemorySaver
import sqlparse
import uuid
import textwrap


# In[23]:


from langgraph.graph import StateGraph, START, END
from typing import Annotated, List, Literal, NotRequired, TypedDict, Union, Any, Dict
import operator
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


# In[24]:


from agents.sql_cleaned_query_agent import clean_query_node
from agents.review_clean_query import review_cleaned_query_node
from agents.find_tables import find_tables_node
from agents.create_sql_query_filtered_columns import create_sql_query
from agents.fuzzy_wuzzy import call_match
from agents.execute_sql_query import execute_sql_query
from agents.add_filter_sql_query import add_filter_sql_query
from agents.rewrite_sql_query import rewrite_sql_query
from agents.summarise_query_results import summarise_results


# In[25]:


# Load .env file and get GROQ API key
load_dotenv()
api_key = os.getenv("GROQ_API_KEY")


# In[26]:


import urllib.parse 

# Load engine and knowledge base
password = urllib.parse.quote_plus("Iameighteeni@18")


# In[27]:


from langchain_openai import ChatOpenAI


# In[28]:


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


# In[29]:


# Configure and return SQLite database connectiondef configure_db():

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


# In[30]:


print(db.dialect)
print(db.get_usable_table_names())


# In[31]:


# Filter list formats the LLM may return
FilterNo = List[Literal["no"]]
FilterTriplet = List[str]                     # ["table", "column", "value(s)"]
FilterYes = List[Union[Literal["yes"], FilterTriplet]]  # ["yes", [...], [...]]


# In[32]:


class finalstate(TypedDict):
    # Existing
    user_query: str
    cleaned_user_query: str
    tables: List[str]
    dataframe: pd.DataFrame
    failed_query: Annotated[List[str], operator.add]
    query_error_message: Annotated[List[str], operator.add]
    retry_count: int
    is_empty_result: bool
    sql_before_filter_query : str
    sql_query: str
    query_results: str
    summary_results: str
    filter_extractor: NotRequired[Union[FilterNo, FilterYes]]  
    fuzz_match : List[str] 
    


# In[33]:


### Create a node to check wether the query was failed or not
def check_query_failed(state: finalstate) -> finalstate:
    # if state["is_empty_result"]:
    #     return "empty_result" 
    if state["query_results"] == "Query executed successfully":
        return "success"
    else:
        return "failed"


# In[34]:


def fuzz_match(state: finalstate):
    val = state['filter_extractor']
    print("Solving for getting right filter vaues.........")
    lst = call_match(val)
    print("done filtering...........................")
    return {"fuzz_match" : lst}


# ## Defining Nodes

# In[35]:


graph = StateGraph(finalstate)

graph.add_node("clean_query_node", clean_query_node)
graph.add_node("review_cleaned_query_node", review_cleaned_query_node)
graph.add_node("find_tables_node", find_tables_node)
graph.add_node("create_sql_query", create_sql_query)
graph.add_node("fuzz_match", fuzz_match)
graph.add_node("execute_sql_query", execute_sql_query)
graph.add_node('add_filter_sql_query', add_filter_sql_query)
graph.add_node("rewrite_sql_query", rewrite_sql_query)
graph.add_node("summarise_results", summarise_results)

# edges
graph.add_edge(START, 'clean_query_node')
# Human in the loop
graph.add_edge('clean_query_node', 'review_cleaned_query_node')
graph.add_edge('review_cleaned_query_node', 'find_tables_node')
graph.add_edge('find_tables_node', 'create_sql_query')
# Human in the loop
graph.add_edge('create_sql_query', 'fuzz_match')
graph.add_edge('fuzz_match', 'add_filter_sql_query')
graph.add_edge('add_filter_sql_query', 'execute_sql_query')
graph.add_conditional_edges('execute_sql_query', check_query_failed, {"success" : "summarise_results", "failed" : "rewrite_sql_query"})
graph.add_edge('rewrite_sql_query', 'execute_sql_query')
graph.add_edge('summarise_results', END)


# Create a config 
config = {"configurable" : {"thread_id" : str(uuid.uuid4())}}
workflow = graph.compile(checkpointer=InMemorySaver())


# initial_state = {"user_query" : "sales of plak sev from manufacturing plants", "is_empty_result" : False}

# result = workflow.invoke(initial_state, config=config)


# If result is the dict you showed earlier
# interrupt_obj = result["__interrupt__"][0]   # this is an Interrupt object

# # Access the value attribute (a dict), then get 'cleaned_query'
# cleaned_query = interrupt_obj.value["cleaned_query"]

# print("Cleaned query:", textwrap.fill(cleaned_query, width=80))



# user_input = input("Enter the feedback")


# resume_result = workflow.invoke(Command(resume=user_input), config=config)
# print(resume_result)


def pretty_print_result(result: dict):
    print("\n=== Query Details ===")
    print(f"User Query: {result.get('user_query')}")
    print(f"Cleaned Query: {result.get('cleaned_user_query')}")
    print(f"Tables Used: {', '.join(result.get('tables', []))}")
    
    print("\n=== Execution Results ===")
    print(f"Query Results: {result.get('query_results')}")

    print(f"Dataframe: {result.get('dataframe')}")
    print(f"Empty Result?: {result.get('is_empty_result')}")
    
    print("\n=== Error Info ===")
    print(f"Failed Queries: {result.get('failed_query')}")
    print(f"Error Messages: {result.get('query_error_message')}")
    
    print("\n=== Fuzzy Matches ===")
    for match in result.get("fuzz_match", []):
        print(" - " + ", ".join(match))
    
    print("\n=== Filter Extractor ===")
    print(result.get("filter_extractor"))
    
    aql_query = result.get('sql_query')
    print("\nSQL Query:")
    print(sqlparse.format(aql_query, reindent=True, keyword_case="upper"))
    print(f"Summary Results: {result.get('summary_results')}")



# pretty_print_result(resume_result)






