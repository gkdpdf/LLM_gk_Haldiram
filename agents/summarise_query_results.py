from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_openai import ChatOpenAI
from langchain_community.utilities import SQLDatabase
from dotenv import load_dotenv

llm = ChatOpenAI(model="gpt-4o", temperature=0)

result_summary_prompt = ChatPromptTemplate.from_messages([
    ("system", 
     "You are a helpful assistant that summarizes SQL query results for a WhatsApp chatbot. "
     "Your summaries must be very concise, use plain language, and fit in as little short sentences as possible. "
     "Avoid technical details or SQL jargon or user query. Focus only on the key insight."),
    ("human", 
     "SQL query:\n{sql_query}\n\nResults:\n{query_result}\n\n"
     "Reply with a short WhatsApp-friendly summary of the results.")
])

result_summary_chain = result_summary_prompt | llm | StrOutputParser()

def summarise_results(state:dict) -> dict:
    sql_query = state["sql_query"]
    raw_result = state["dataframe"]
    if len(raw_result) > 0:
        summary = result_summary_chain.invoke({"sql_query" :sql_query, "query_result" : raw_result})
        state["summary_results"] = summary
    else:
        state["summary_results"] = "There was no relevant data found for the query try and different query"
    return state
    
