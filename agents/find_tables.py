from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import PydanticOutputParser, StrOutputParser
from pydantic import BaseModel, Field, RootModel
from typing import List, Literal
from dotenv import load_dotenv
import json

load_dotenv(override=True)

# 🔹 Load schema markdown
with open("annotated_schema_haldiram_primary_azam.md", "r") as f: 
    schema_markdown = f.read()

# 🔹 Allowed table names
AllowedTables = Literal[
    "tbl_distributor_master",
    "tbl_Primary",
    "tbl_Product_Master",
    "tbl_superstockist_master",
]


class TableList(RootModel[List[AllowedTables]]):
    pass

table_list_parser = PydanticOutputParser(pydantic_object=TableList)

# # 🔹 Common alias corrections (add more if needed)
# alias_map = {
#     "product_master": "tbl_product_master",
#     "primary": "tbl_primary",
#     "distributor_master": "tbl_distributor_master",
#     "superstockist_master": "tbl_superstockist_master"
# }

# def fix_aliases(output: list[str]) -> list[str]:
#     return [alias_map.get(t, t) for t in output]

# 🔹 LLM setup
llm = ChatOpenAI(model="gpt-4o", temperature=0)

# 🔹 Prompt setup
query_clean_prompt = ChatPromptTemplate.from_messages([
    ("system", f"""
You are an expert in SQL and relational databases.

Given a user query and a markdown-formatted database schema, return a valid **JSON list** of only the table names required to answer the query.

⚠️ ONLY return a list like:
["tbl_product_master", "tbl_secondary"]

⚠️ Do not explain anything or return aliases.
⚠️ Do not include markdown or bullet points.
⚠️ Use only exact matches from this list:

{AllowedTables}

Schema:
{schema_markdown}
    """),
    ("human", "User query: {user_query}")
])

# 🔹 Chain with StrOutputParser
chain = query_clean_prompt | llm | table_list_parser

def find_tables_node(state:dict) -> dict:
    user_query = state["cleaned_user_query"]
    output = chain.invoke({"user_query" : user_query})
    state["tables"] = output.root
    return state