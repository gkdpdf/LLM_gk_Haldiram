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
template_filter_check = ChatPromptTemplate.from_messages([
    ("system", """
You are an expert assistant designed to help a text-to-SQL agent determine whether filters (i.e., WHERE clauses) are required for answering a user's natural language question using a SQL query on a database.

Your job is to:
1. Carefully analyze the user question and identify if any filtering condition is implied (e.g., city = 'Campinas', date range, payment type = 'credit_card', etc.).
2. Use the provided list of tables and columns (with sample values) to identify which specific string datatype **columns** would be involved in such filtering.
     
3. Determine whether a **filter is needed** ONLY for string datatype columns:
    - If **yes**, return a list in the format:
      ["yes", ["<table>", "<column>", "<filter values exactly as stated in the user question>"], ["<table2>", "<column2>", "<filter values exactly as stated in the user question>"], ...]
    - If **no filter is needed**, return: ["no"]
4. For the third item in each filter entry, suggest value(s) **exactly as stated in the user query**, even if they are different from the sample values.
   - If user says "New York" and the column has "SF" it means in actual columns values are in abbrevation, so output "NY". Suggest based on user question and sample values.
   - If user says "credit card or boleto", output "credit card, boleto"
5. Only include columns in the output that help **narrow down** the dataset, such as city, state, payment method etc.
6. For float or integer or DATE datatype columns just give ["no"] as output.  For date kind of columns give output as ["no"]
7. Output should be STRICTLY in form of list.

⚠️ Be careful not to include aggregation or grouping columns like `customer_id`, `order_id`, or `product_id` unless they are being **explicitly** filtered in the question.

Example outputs:
["yes", ["customers", "customer_city", "Campinas"], ["orders", "order_status", "delivered, shipped"]]
["yes", ["order_payments", "payment_type", "credit card, boleto"]]
["no"]
    """),

    ("human", '''
Given a user query , decide if the SQL query to answer this question requires filters.

Only return a list in the exact format described:
- "yes" if filtering is needed, followed by the relevant table-column-filter entries.
- "no" if the question can be answered using full-table aggregates or joins without conditions. For float ot integer or date datatype columns also just give ["no"] as output.
- Make sure that output should be strictly in terms of list or list of lists. Make sure strings within these lists are properly closed by ".

Here is the user question:
{query}

''')
])

# 🔹 Chain with StrOutputParser
chain = query_clean_prompt | llm | table_list_parser

def find_tables_node(state:dict) -> dict:
    user_query = state["cleaned_user_query"]
    output = chain.invoke({"user_query" : user_query})
    state["tables"] = output.root
    return state