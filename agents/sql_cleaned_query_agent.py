from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from dotenv import load_dotenv

load_dotenv(override=True)

# Load schema markdown
with open("annotated_schema_haldiram_primary.md", "r") as f:
    schema_markdown = f.read()

# Setup LLM (you can use gpt-3.5-turbo or gpt-4o if needed)
llm = ChatOpenAI(model="gpt-4o", temperature=0)

# Create the prompt for query cleaning
query_clean_prompt = ChatPromptTemplate.from_messages([
    ("system", f"""
You are a product manager for a Text-to-SQL system.

You will be given:
- A database schema (markdown format)
- A user's natural language query

Your job is to:
1. Rewrite the user's query more precisely using correct table/column names from the schema
2. Preserve the **original intent** of the user
3. Ensure that the query uses exact column or table names found in the schema when possible

⚠️ Do NOT generate SQL. The output must remain in natural language but schema-aligned.

📚 Schema:
{schema_markdown}
"""),
    ("human", "User query: {user_query}")
])

# Create the LangChain chain
chain = query_clean_prompt | llm | StrOutputParser()

# Final node
def clean_query_node(state: dict) -> dict:
    user_query = state["user_query"]
    cleaned_query = chain.invoke({"user_query": user_query})
    state["cleaned_user_query"] = cleaned_query
    return state
