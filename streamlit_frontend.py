import streamlit as st
from langgraph.types import Command
from langchain_core.messages import HumanMessage
from langgraph.types import Command
from langgraph.checkpoint.memory import InMemorySaver
from langgraph_backend import workflow
import textwrap
import uuid

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
    
# Load the conversation history
for message in st.session_state["message_history"]:
    with st.chat_message(message["role"]):
        st.text(message["content"])
        
# User input via chat box
user_input = st.chat_input("Type your query here .....")

if user_input:
    # Add user message to history
    st.session_state["message_history"].append({"role":"user", "content":user_input})
    with st.chat_message("user"):
        st.text(user_input)
        

    # Run the workflow
    initial_result = {"user_query" : user_input, "is_empty_result":False}
    result = workflow.invoke(initial_result, config=CONFIG)
    
    if "__interrupt__" in result:
        interrupt_object = result["__interrupt__"][0]
        cleaned_query = interrupt_object.value["cleaned_query"]
        
        
        with st.chat_message("assistant"):
            st.markdown("Do you want to modify it? Reply with new text or type **no**. \n\n_(Waiting for your feedback...)_")
        
        feedback_input = st.chat_input("Type your modification or 'no' to accept")

        if feedback_input:
            # Add user feedback to history
            st.session_state["message_history"].append({"role": "user", "content": feedback_input})
            with st.chat_message("user"):
                st.text(feedback_input)

            # Decide what to resume with
            if feedback_input.lower().strip() == "no":
                resume_result = workflow.invoke(Command(resume="Keep the query as it is."), config=CONFIG)
            else:
                resume_result = workflow.invoke(Command(resume=feedback_input), config=CONFIG)
    with st.chat_message("assistant"):
        if result.get("summary_results"):
            st.markdown(result.get("summary_results"))
    st.session_state["message_history"].append({"role" : "assistant", "content":result.get("summary_results")})
            

        

