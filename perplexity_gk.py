import streamlit as st
from pathlib import Path
from langchain.agents import initialize_agent, Tool
from langchain.sql_database import SQLDatabase
from langchain.agents.agent_types import AgentType
from langchain.agents.agent_toolkits import SQLDatabaseToolkit
import sqlite3
import pandas as pd
import requests
import re
import glob
from langchain.llms.base import LLM
from langchain.agents.agent import AgentExecutor

from table_relationships import describe_table_relationships
from tbl_col_info import table_info_and_examples

# ----- Streamlit Setup -----
st.set_page_config(page_title="IQ Bot - Sales Assistant", page_icon="🧠")
st.title("🧠 IQ Bot - Sales Assistant")

# ----- Perplexity API -----
PPLX_API_KEY = "--"
PPLX_URL = "https://api.perplexity.ai/chat/completions"
HEADERS = {
    "Authorization": f"Bearer {PPLX_API_KEY}",
    "Content-Type": "application/json"
}

def call_perplexity(prompt):
    payload = {
        "model": "sonar-pro",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.3,
        "max_tokens": 500
    }
    try:
        resp = requests.post(PPLX_URL, headers=HEADERS, json=payload)
        return resp.json()['choices'][0]['message']['content'].strip()
    except Exception as e:
        return f"Error: {e}"

class PerplexityLLM(LLM):
    def _call(self, prompt, stop=None):
        return call_perplexity(prompt)
    @property
    def _llm_type(self) -> str:
        return "perplexity-sonar"

llm = PerplexityLLM()

# ----- Load SQLite DB from CSVs -----
def configure_db():
    conn = sqlite3.connect(":memory:")
    csv_folder = Path(__file__).parent / "cooked_data_gk"
    for csv_file in glob.glob(str(csv_folder / "*.csv")):
        table_name = Path(csv_file).stem.lower()
        df = pd.read_csv(csv_file)
        df.to_sql(table_name, conn, index=False, if_exists="replace")
    return SQLDatabase.from_uri("sqlite://", engine_args={"creator": lambda: conn})

db = configure_db()
st.write("📄 Tables Loaded:", db.get_table_names())

# ----- Tools -----
relationship_tool = Tool(
    name="TableRelationships",
    func=describe_table_relationships,
    description="Use this tool to understand how tables are related before writing SQL queries."
)

info_example_tool = Tool(
    name="TableInfoAndExamples",
    func=table_info_and_examples,
    description="Use this tool to understand available tables and columns and see example queries."
)

toolkit = SQLDatabaseToolkit(db=db, llm=llm)
tools = toolkit.get_tools() + [relationship_tool, info_example_tool]

# ----- Agent -----
agent = initialize_agent(
    tools=tools,
    llm=llm,
    agent_type=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
    verbose=True,
    handle_parsing_errors=True,
    agent_kwargs={
        "system_message": """
You are a data analyst assistant. Only use the SQLite database to answer and use "db" only.
Don't make your own sample data.
NEVER make up answers. If table info is missing, use the tools provided.

✅ Format:
- Action: <tool name or Final Answer>
- Action Input: <SQL query or tool input>

⚠️ Rules:
- Do not hallucinate answers or invent any content.
- Must run SELECT queries and show actual results.
- Never return responses without querying the database.

Example: 
User: Which product has highest MRP?
Answer:
Action: Final Answer
Action Input: SELECT product_name, mrp FROM tbl_product_master ORDER BY mrp DESC LIMIT 1;
"""
    }
)

# ----- Session State -----
if "messages" not in st.session_state or st.sidebar.button("Clear message history"):
    st.session_state["messages"] = [{"role": "assistant", "content": "How can I help you today?"}]

for msg in st.session_state.messages:
    st.chat_message(msg["role"]).write(msg["content"])

# ----- Chat Interface -----
user_query = st.chat_input(placeholder="Ask anything from your database")

if user_query:
    st.session_state.messages.append({"role": "user", "content": user_query})
    st.chat_message("user").write(user_query)

    with st.chat_message("assistant"):
        try:
            if isinstance(agent, AgentExecutor):
                result = agent.invoke({"input": user_query})
                response = result.get("output", "")
                intermediate_steps = result.get("intermediate_steps", [])
            else:
                response = agent.run(user_query)
                intermediate_steps = []

            # Thought process
            if intermediate_steps:
                with st.expander("🧠 Agent Thought Process", expanded=False):
                    for i, step in enumerate(intermediate_steps):
                        action, observation = step
                        st.markdown(f"**Step {i+1}**")
                        st.markdown(f"- 🛠 **Tool:** `{action.tool}`")
                        st.markdown(f"- 🔢 **Input:** `{action.tool_input}`")
                        st.markdown(f"- 📋 **Observation:** {observation}")

            # SQL Execution
            sql_match = re.search(r"(SELECT\s.+?;)", response, re.IGNORECASE | re.DOTALL)
            if sql_match:
                query = sql_match.group(1).strip()
                result = db.run(query)

                if isinstance(result, list):
                    df = pd.DataFrame(result)
                    st.session_state.messages.append({"role": "assistant", "content": query})

                    # Clean DataFrame
                    df.columns = [col.replace("_", " ").title() for col in df.columns]
                    for col in df.select_dtypes(include=['float']):
                        df[col] = df[col].round(2)
                    if "Mrp" in df.columns:
                        df["Mrp"] = df["Mrp"].apply(lambda x: f"₹{x:.2f}")

                    st.code(query, language="sql")
                    st.dataframe(df, use_container_width=True)
                else:
                    st.write("Query result:")
                    st.write(result)
            else:
                # No query found in response
                st.session_state.messages.append({"role": "assistant", "content": response})
                st.write(response)

        except Exception as e:
            st.error(f"❌ Error: {str(e)}")
