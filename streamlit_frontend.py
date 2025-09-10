import streamlit as st
from langgraph.types import Command
from langchain_core.messages import HumanMessage
from langgraph.types import Command
from langgraph.checkpoint.memory import InMemorySaver
from langgraph_backend import workflow
import operator
import textwrap
import uuid
from typing import Annotated, List, Literal, NotRequired, TypedDict, Union, Any, Dict
import pandas as pd

from astream_events_handler import invoke_our_graph

import asyncio


# Create a thread id
THREAD_ID = str(uuid.uuid4())
# Create the configuration
CONFIG = {"configurable":{"thread_id":THREAD_ID}}

# Create a streamlit app
st.set_page_config(page_title="Haldiram Chatbot", layout="wide")

st.title("💬 Sales Assistant")

st.markdown("Chat with your database using natural language")

# Initialize message history in session state
if "message_history" not in st.session_state:
    st.session_state["message_history"] = []
    
if "graph_resume" not in st.session_state:
    st.session_state.graph_resume = False
    
if "state" not in st.session_state:
    st.session_state.state = {}
    
# Load the conversation history
for message in st.session_state["message_history"]:
    with st.chat_message(message["role"]):
        st.text(message["content"])
        
# User input via chat box
user_input = st.chat_input()

if user_input:
    # Add user message to history
    st.session_state["message_history"].append({"role":"user", "content":user_input})
    st.chat_message("user").write(user_input)
    
    with st.chat_message("assistant"):
        placeholder = st.container()
        
        resume_graph_state = {
            "graph_resume":st.session_state.graph_resume
        }
        
        langgraph_state = st.session_state.state
        
        chat_input = {"user_query": user_input}
        if "user_query" not in langgraph_state:
            langgraph_state["user_query"] = user_input
        
        response = asyncio.run(invoke_our_graph(chat_input, placeholder, langgraph_state, resume_graph_state))
        
        if type(response) is dict:
            operation = response["op"]
            if operation == "on_waiting_feedback":
                issue = response["msg"]
                st.session_state.graph_resume = True
                
            elif operation == "query_successful":
                summary_results = response["msg"]
                st.session_state["message_history"].append({"role":"assistant", "content":summary_results})
                st.chat_message("assistant").write(summary_results)
                st.session_state.graph_resume = False
                st.session_state.state = {}
            else:
                placeholder.error("An unexpected error occurred.")
        else:
            placeholder.error("An unexpected error occurred.")
        

            

        

