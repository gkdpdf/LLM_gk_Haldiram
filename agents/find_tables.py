from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from dotenv import load_dotenv
from langchain_core.output_parsers import PydanticOutputParser, StrOutputParser
from pydantic import BaseModel, Field, RootModel
from typing import List, Literal

load_dotenv(override=True)

with open("annotated_schema.md", "r") as f:
    schema_markdown = f.read()

llm = ChatOpenAI(model="gpt-4o", temperature=0)

query_clean_prompt = ChatPromptTemplate.from_messages([
    ("system", f"""
    You are an expert in SQL and relational databases.

    Given a user's  query and a markdown-formatted database schema, your task is to return a **valid JSON list** of only the table names required to answer the query.

    Instructions:
    - Carefully read the user query and determine which tables contain the necessary data.
    - Do **not** explain or generate SQL queries.
    
    Return ONLY a list like this: ["table_name_1", "table_name_2"]

    Do not include explanations, markdown, or bullet points.

    Valid table names:
    ["product_master", "retailer_master", "retailer_order_summary", "retailer_order_product_details", "distributor_closing_stock", "scheme_details"]
    

    Schema:
    {schema_markdown}
    """),
    ("human", "User query: {user_query}")
])

AllowedTables = Literal[
    "retailer_order_summary",
    "retailer_order_product_details",
    "distributor_closing_stock",
    "product_master",
    "retailer_master",
    "scheme_details"
]

# class TableList(BaseModel):
#     __root__:List[AllowedTables]

class TableList(RootModel[List[AllowedTables]]):
    pass
    
table_list_parser = PydanticOutputParser(pydantic_object=TableList)

chain = query_clean_prompt | llm | table_list_parser
# chain = query_clean_prompt | llm | StrOutputParser()

def find_tables_node(state:dict) -> dict:
    user_query = state["cleaned_user_query"]
    output = chain.invoke({"user_query" : user_query})
    state["tables"] = output.root
    return state