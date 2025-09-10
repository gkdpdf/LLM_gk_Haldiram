from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.callbacks import adispatch_custom_event
from langchain_core.runnables import RunnableConfig
from dotenv import load_dotenv

load_dotenv(override=True)

with open("annotated_schema_haldiram_primary_azam.md", "r") as f:
    schema_markdown = f.read()
    
with open("vectordb_haldiram.md", "r") as f:
    vector_db_markdown = f.read()
    
#llm

llm = ChatOpenAI(model="gpt-4.1-nano", temperature=0)

query_clean_prompt = ChatPromptTemplate.from_messages([
    ("system", f"""
     You are a product manager for a Text-to-SQL system. 
     
     You will be given:
     - User's natural language query
     - Database schema in markdown format

     Your job is to rewrite the query clearly and concisely.
     
     Important instructions:
        - The manufacturing plant sell items to super stockists whose data is present in the tbl_shipment
        - the super stockists sell items to distributors whose data is present in tbl_primary
        - the items sold to distributors signifies the demand in market
     
     Rules:
     - Always keep the output in **bullet point format**.
     - Summarize intent, entities, and metrics separately.
     - Use clear terms from the schema (resolve distributor vs. super stockist carefully).
     - Do NOT output SQL.
     - Keep it short and precise, avoid extra commentary.
     
     --- SCHEMA (markdown) ---
     {schema_markdown}
     """),
    ("human", "User query: {user_query}")
])

chain = query_clean_prompt | llm | StrOutputParser()

async def clean_query_node(state:dict, config:RunnableConfig) -> dict:
    user_query = state["user_query"]
    cleaned_query = chain.invoke({"user_query" : user_query})
    state["cleaned_user_query"] = cleaned_query
    await adispatch_custom_event(
        "cleaned_query",
        {"cleaned_user_query": cleaned_query},
        config=config
    )
    return state
 
