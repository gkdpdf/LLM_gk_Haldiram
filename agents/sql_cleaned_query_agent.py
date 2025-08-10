from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from dotenv import load_dotenv

load_dotenv(override=True)

with open("annotated_schema_haldiram_primary_azam.md", "r") as f:
    schema_markdown = f.read()
    
#llm

llm = ChatOpenAI(model="gpt-4.1-nano", temperature=0)

query_clean_prompt = ChatPromptTemplate.from_messages([
    ("system", f"""
     You are a product manager for a Text to SQL system who takes query from the team and converts the query to be more precise as per the database schema.
     
     You will be given a database schema in markdown format and user's natural language query.
     
     Your task is to:
     - Improve the query so that it clearly refrences valid table/column name as per the schema.
     
     Make sure to preserve the user's intent while correcting phrasing or structure. The output query should not be an sql query but written in natural language only but a l
     Schema:
     {schema_markdown}
     """),
    ("human", "User query: {user_query}")
])

chain = query_clean_prompt | llm | StrOutputParser()

def clean_query_node(state:dict) -> dict:
    user_query = state["user_query"]
    cleaned_query = chain.invoke({"user_query" : user_query})
    state["cleaned_user_query"] = cleaned_query
    return state
 
