import streamlit as st
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

# Load .env file and get GROQ API key
load_dotenv()
api_key = os.getenv("GROQ_API_KEY")

# Streamlit page config
st.set_page_config(page_title="IQ Bot- Sales Assistance", page_icon="🧠")
st.title("🧠 IQ Bot- Sales Assistance")

if not api_key:
    st.error("GROQ_API_KEY not found in .env file.")
    st.stop()

# Setup the LLM
llm = ChatGroq(groq_api_key=api_key, model_name="Llama3-8b-8192", streaming=True)

# Configure and return SQLite database connection
def configure_db():
    import glob
    conn = sqlite3.connect(":memory:")
    csv_folder = Path(__file__).parent / "cooked_data_gk"
    for csv_file in glob.glob(str(csv_folder / "*.csv")):
        table_name = Path(csv_file).stem.lower()
        df = pd.read_csv(csv_file)
        df.to_sql(table_name, conn, index=False, if_exists="replace")
    return SQLDatabase.from_uri("sqlite://", engine_args={"creator": lambda: conn})

# Connect to DB
db = configure_db()
st.write("📄 Tables Loaded:", db.get_table_names())

# Tools
relationship_tool = Tool(
    name="TableRelationships",
    func=describe_table_relationships,
    description="Use this tool to understand how tables are related before writing SQL queries."
)
info_example_tool = Tool(
    name="TableInfoAndExamples",
    func=table_info_and_examples,
    description="Use this tool to understand available tables and see example queries.",
)

toolkit = SQLDatabaseToolkit(db=db, llm=llm)
tools = toolkit.get_tools() + [relationship_tool, info_example_tool]

agent = initialize_agent(
    tools=tools,
    llm=llm,
    agent_type=AgentType.OPENAI_FUNCTIONS,
    verbose=False,
    handle_parsing_errors=True,
    agent_kwargs={
        "system_message": """
You are an expert SQL assistant helping query a SQLite-based retail database which gives one liner answer.

✅ Instructions:
1. If table info is missing, always use `TableInfoAndExamples` first.

2. Use this response format:
   - Action: <tool-name or Final Answer>
   - Action Input: <input>
3. Execute real SQL queries after table identification.
4. Only show SELECT query results, not intermediate thoughts or tools.
5. When asked about "scheme", use:
SELECT name, discount_percent FROM tbl_scheme WHERE is_active = 1 ORDER BY discount_percent DESC;
"""
    }
)

# Session state for message history
if "messages" not in st.session_state or st.sidebar.button("Clear message history"):
    st.session_state["messages"] = [{"role": "assistant", "content": "How can I help you?"}]

# Show previous chat
for msg in st.session_state.messages:
    st.chat_message(msg["role"]).write(msg["content"])

# Chat input
user_query = st.chat_input(placeholder="Ask anything from the database")

if user_query:
    st.session_state.messages.append({"role": "user", "content": user_query})
    st.chat_message("user").write(user_query)

    with st.chat_message("assistant"):
        response = agent.run(user_query)

        try:
            sql_match = re.search(r"(SELECT\s.+?;)", response, re.IGNORECASE | re.DOTALL)
            if sql_match:
                query = sql_match.group(1).strip()
                result = db.run(query)

                if isinstance(result, list):
                    df = pd.DataFrame(result)
                    st.session_state.messages.append({"role": "assistant", "content": query})
                    st.code(query)
                    st.dataframe(df)
                else:
                    st.write("Query result:")
                    st.write(result)
            else:
                st.session_state.messages.append({"role": "assistant", "content": response})
                st.write(response)
        except Exception as e:
            st.warning(f"Error executing SQL: {e}")
            st.session_state.messages.append({"role": "assistant", "content": response})
            st.write(response)
